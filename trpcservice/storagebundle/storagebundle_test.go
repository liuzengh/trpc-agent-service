package storagebundle

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// countingSessionService is a session.Service that only counts closes.
//
// The interface is embedded rather than implemented: nothing in this package
// calls a session method, and a nil embedded interface panics loudly if that
// ever stops being true.
type countingSessionService struct {
	session.Service
	closeErr error

	mu     sync.Mutex
	closes int
}

func (s *countingSessionService) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	return s.closeErr
}

func (s *countingSessionService) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

func testScope(tenantID string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: tenantID}
}

func TestBundleValidateRequiresASessionService(t *testing.T) {
	require.ErrorIs(t, Bundle{}.Validate(), ErrIncompleteBundle)
	require.NoError(t, Bundle{Session: &countingSessionService{}}.Validate())
}

// A borrowed lease releases nothing, however often it is released. It is what a
// process default is handed out as, and closing that store would take it away
// from every other holder.
func TestBorrowNeverClosesTheBundle(t *testing.T) {
	sessions := &countingSessionService{}
	lease := Borrow(Bundle{Session: sessions})

	require.Same(t, sessions, lease.Bundle().Session)
	for range 3 {
		require.NoError(t, lease.Release())
	}
	require.Zero(t, sessions.closeCount())
}

// An owned lease closes exactly once and reports the same result to every
// caller, so a Runtime closed twice cannot double-close its store and cannot
// lose the failure of the first close either.
func TestOwnClosesOnceAndKeepsTheError(t *testing.T) {
	sessions := &countingSessionService{}
	lease := Own(Bundle{Session: sessions})

	require.Same(t, sessions, lease.Bundle().Session)
	require.NoError(t, lease.Release())
	require.NoError(t, lease.Release())
	require.Equal(t, 1, sessions.closeCount())

	closeErr := errors.New("flush failed")
	failing := &countingSessionService{closeErr: closeErr}
	failingLease := Own(Bundle{Session: failing})
	require.ErrorIs(t, failingLease.Release(), closeErr)
	require.ErrorIs(t, failingLease.Release(), closeErr)
	require.Equal(t, 1, failing.closeCount())
}

func TestOwnReleasesConcurrentlyExactlyOnce(t *testing.T) {
	sessions := &countingSessionService{}
	lease := Own(Bundle{Session: sessions})

	const releasers = 16
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	// The result of each Release comes back over a channel instead of being
	// asserted where it happens: require calls t.FailNow, which only stops the
	// test when it runs on the test's own goroutine. Anywhere else it stops that
	// goroutine, and this one would then never reach done.Done.
	releaseErrs := make(chan error, releasers)
	for range releasers {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			releaseErrs <- lease.Release()
		}()
	}
	start.Done()
	done.Wait()
	close(releaseErrs)

	for err := range releaseErrs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, sessions.closeCount())
}

// An empty owned Bundle has nothing to close, and releasing it must be a no-op
// rather than a nil dereference: it is the value an owning constructor holds
// while it is still deciding whether it can build anything at all.
func TestOwnReleasesAnEmptyBundleWithoutPanicking(t *testing.T) {
	require.NoError(t, Own(Bundle{}).Release())
}

// Fixed hands out a fresh borrowed lease per call. The compatibility entry
// point resolves once per Runtime and there may be many Runtimes on one process
// store, so one holder releasing must not affect another — and none of them may
// close a store the caller still owns.
func TestFixedServesBorrowedLeasesPerResolve(t *testing.T) {
	sessions := &countingSessionService{}
	resolver := Fixed(Bundle{Session: sessions})

	first, err := resolver.Resolve(context.Background(), testScope("tenant-a"), "")
	require.NoError(t, err)
	second, err := resolver.Resolve(context.Background(), testScope("tenant-a"), "")
	require.NoError(t, err)
	require.NotSame(t, first, second, "a shared lease would let one holder release another's")
	require.Same(t, sessions, first.Bundle().Session)
	require.Same(t, sessions, second.Bundle().Session)

	require.NoError(t, first.Release())
	require.Same(t, sessions, second.Bundle().Session)
	require.NoError(t, second.Release())
	require.NoError(t, second.Release())
	require.Zero(t, sessions.closeCount(), "Fixed borrows a store its caller owns")
}

// The whole point of the compatibility entry point: it cannot honour a profile
// reference, so it refuses one instead of serving the store the revision did
// not name.
func TestFixedRefusesANamedProfile(t *testing.T) {
	resolver := Fixed(Bundle{Session: &countingSessionService{}})

	lease, err := resolver.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, ErrProfileNotFound)
	require.Nil(t, lease)
	require.ErrorContains(t, err, "p1")
}

// Resolver is a tenant-scoped interface, so an unusable scope is a bad request
// at every implementation of it.
//
// Fixed has nothing to look a tenant up in, which is not a reason to answer
// where a Router would refuse: the same call would then be served or refused
// depending on which Resolver the process happened to be wired with, and the
// compatibility path is exactly the one nobody looks at twice. Every caller
// today validates the scope several steps earlier, which is why this is checked
// rather than assumed.
func TestFixedRefusesAnUnusableScope(t *testing.T) {
	sessions := &countingSessionService{}
	fixed := Fixed(Bundle{Session: sessions})
	router, err := NewRouter(Options{
		Default: Bundle{Session: sessions},
		Source:  NoProfiles(),
		Factory: testFactory(t, ProcessConstraints{}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, router.Close()) })

	for _, tc := range []struct {
		name  string
		scope tenant.TenantContext
	}{
		{"no tenant at all", tenant.TenantContext{}},
		{"blank tenant", tenant.TenantContext{TenantID: "   "}},
		{"tenant with a separator in it", tenant.TenantContext{TenantID: "tenant-a\x00b"}},
		{"tenant that is a traversal", tenant.TenantContext{TenantID: "../tenant-b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The default id, which is the one that would otherwise be served: a
			// scope check that only guarded the named branch would guard nothing.
			lease, err := fixed.Resolve(context.Background(), tc.scope, "")
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			require.Nil(t, lease)

			// The same call, the same refusal, from the other implementation.
			lease, err = router.Resolve(context.Background(), tc.scope, "")
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			require.Nil(t, lease)
		})
	}

	require.Zero(t, sessions.closeCount())
}

// An id that could never name a profile is a bad request, not a miss.
//
// Reporting it as ErrProfileNotFound would claim a lookup happened, and would
// put a string this resolver never examined into an error message —
// tenant.ValidateResourceID deliberately keeps the rejected value out of its
// own. Router refuses these before the id can become a lookup, a cache key or a
// singleflight key; Fixed has no such keys, and refuses them anyway so the two
// answer alike.
func TestFixedRefusesAMalformedProfileID(t *testing.T) {
	fixed := Fixed(Bundle{Session: &countingSessionService{}})

	for _, profileID := range []string{
		"   ",
		"../../etc/passwd",
		"tenant-a\x00p1",
		"p1\n",
		"-leading-dash",
		strings.Repeat("p", 129),
	} {
		t.Run(strconv.Quote(profileID), func(t *testing.T) {
			lease, err := fixed.Resolve(context.Background(), testScope("tenant-a"), profileID)
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
			require.NotErrorIs(t, err, ErrProfileNotFound)
			require.Nil(t, lease)
			require.NotContains(t, err.Error(), profileID,
				"the rejected id must not be echoed back")
		})
	}

	// A well-formed id this resolver simply cannot honour is still the refusal
	// it always was: the check above narrows what "not found" means, it does not
	// replace it.
	lease, err := fixed.Resolve(context.Background(), testScope("tenant-a"), "tenant-postgres")
	require.ErrorIs(t, err, ErrProfileNotFound)
	require.Nil(t, lease)
}

func TestFixedRefusesAnEmptyBundleAndABadContext(t *testing.T) {
	lease, err := Fixed(Bundle{}).Resolve(context.Background(), testScope("tenant-a"), "")
	require.ErrorIs(t, err, ErrIncompleteBundle)
	require.Nil(t, lease)

	//nolint:staticcheck // a nil context is exactly what is under test here.
	lease, err = Fixed(Bundle{Session: &countingSessionService{}}).
		Resolve(nil, testScope("tenant-a"), "")
	require.Error(t, err)
	require.Nil(t, lease)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	lease, err = Fixed(Bundle{Session: &countingSessionService{}}).
		Resolve(cancelled, testScope("tenant-a"), "")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, lease)
}

// Fixed, Router and every future Resolver are one interface, so a caller that
// holds a Resolver cannot tell which it has — which is what lets the Runtime
// have exactly one storage path.
func TestResolverInterfaceIsSatisfiedByBothImplementations(t *testing.T) {
	var resolvers []Resolver

	resolvers = append(resolvers, Fixed(Bundle{Session: &countingSessionService{}}))

	router, err := NewRouter(Options{
		Default: Bundle{Session: &countingSessionService{}},
		Source:  NoProfiles(),
		Factory: testFactory(t, ProcessConstraints{}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, router.Close()) })
	resolvers = append(resolvers, router)

	for _, resolver := range resolvers {
		lease, err := resolver.Resolve(context.Background(), testScope("tenant-a"), "")
		require.NoError(t, err)
		require.NoError(t, lease.Bundle().Validate())
		require.NoError(t, lease.Release())
	}
}
