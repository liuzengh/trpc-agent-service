package storagebundle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// resolution is what a goroutine hands back to the test goroutine: the pair
// Resolve returned, and no judgement about it.
//
// require calls t.FailNow, and t.FailNow is only defined on the goroutine
// running the test. Called anywhere else it runs runtime.Goexit, which stops
// that goroutine — so the failure is reported late or not at all, and whatever
// the test was about to receive from it is never sent. A concurrency test that
// fails would then hang instead, either on the receive or on a Cleanup that
// closes a Router still waiting for a lease nobody will release.
type resolution struct {
	lease Lease
	err   error
}

// builtStore is one Bundle a controlledFactory produced, kept so a test can ask
// whether it was closed and how often.
type builtStore struct {
	profileID string
	sessions  *countingSessionService
}

// controlledFactory is a Factory a test can stall, fail and observe.
//
// It replaces sessionFactory in the Router tests on purpose: what is under test
// here is the Router's lifecycle, and that has to be observable at the moments
// a real Factory gives no handle on — halfway through a build, and after the
// Router it is building for has closed.
type controlledFactory struct {
	// gate blocks every Build until it is closed. A nil gate never blocks.
	gate chan struct{}
	// entered receives one Profile per Build that has reached the gate, so a
	// test can wait for a build to be genuinely in flight instead of sleeping.
	entered chan Profile

	mu         sync.Mutex
	buildErr   error
	closeErrs  map[string]error
	built      []*builtStore
	closeOrder []string
	ctxErrs    []error
}

func newControlledFactory() *controlledFactory {
	return &controlledFactory{closeErrs: make(map[string]error)}
}

// blocking returns a factory whose builds stall until releaseBuilds is called.
func blockingFactory() *controlledFactory {
	factory := newControlledFactory()
	factory.gate = make(chan struct{})
	factory.entered = make(chan Profile, 8)
	return factory
}

func (f *controlledFactory) releaseBuilds() {
	close(f.gate)
}

func (f *controlledFactory) Build(
	ctx context.Context,
	profile Profile,
) (Bundle, func() error, error) {
	if f.entered != nil {
		f.entered <- profile
	}
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	buildErr := f.buildErr
	f.mu.Unlock()
	if buildErr != nil {
		return Bundle{}, nil, buildErr
	}
	store := &builtStore{profileID: profile.ID, sessions: &countingSessionService{}}
	f.mu.Lock()
	f.built = append(f.built, store)
	f.mu.Unlock()
	return Bundle{Session: store.sessions}, func() error {
		f.mu.Lock()
		f.closeOrder = append(f.closeOrder, profile.ID)
		closeErr := f.closeErrs[profile.ID]
		f.mu.Unlock()
		_ = store.sessions.Close()
		return closeErr
	}, nil
}

func (f *controlledFactory) buildCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.built)
}

func (f *controlledFactory) stores() []*builtStore {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*builtStore(nil), f.built...)
}

func (f *controlledFactory) closes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.closeOrder...)
}

func (f *controlledFactory) buildContextErrors() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]error(nil), f.ctxErrs...)
}

// mutableSource is a ProfileSource that breaks the immutability contract on
// demand. No legitimate source may do this, which is exactly why the Router has
// to be tested against one that does.
type mutableSource struct {
	mu       sync.Mutex
	profiles map[profileKey]Profile
	err      error
	lookups  int
}

func newMutableSource(profiles ...Profile) *mutableSource {
	source := &mutableSource{profiles: make(map[profileKey]Profile)}
	for _, profile := range profiles {
		source.set(profile)
	}
	return source
}

func (s *mutableSource) set(profile Profile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.profiles[profileKey{tenantID: profile.TenantID, profileID: profile.ID}] = profile
}

// fail makes every later lookup return err, and nil restores the source. It is
// how a profile store that is momentarily unreachable is expressed: the profile
// still exists, this process just cannot read it right now.
func (s *mutableSource) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *mutableSource) ResolveProfile(
	_ context.Context,
	scope tenant.TenantContext,
	profileID string,
) (Profile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups++
	if s.err != nil {
		return Profile{}, s.err
	}
	profile, found := s.profiles[profileKey{tenantID: scope.TenantID, profileID: profileID}]
	if !found {
		return Profile{}, ErrProfileNotFound
	}
	return profile, nil
}

func (s *mutableSource) lookupCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookups
}

// fixedAnswerSource answers every lookup with one profile, whatever was asked
// for. It stands in for a source that is broken or hostile.
type fixedAnswerSource struct {
	profile Profile
}

func (s fixedAnswerSource) ResolveProfile(
	context.Context, tenant.TenantContext, string,
) (Profile, error) {
	return s.profile, nil
}

type routerFixture struct {
	router   *Router
	factory  *controlledFactory
	source   *mutableSource
	defaults *countingSessionService
}

// newRouterFixture builds a Router over a controlled Factory and a mutable
// source holding one in-memory profile per tenant listed.
func newRouterFixture(t *testing.T, factory *controlledFactory, profiles ...Profile) routerFixture {
	t.Helper()
	source := newMutableSource(profiles...)
	defaults := &countingSessionService{}
	router, err := NewRouter(Options{
		Default: Bundle{Session: defaults},
		Source:  source,
		Factory: factory,
	})
	require.NoError(t, err)
	return routerFixture{router: router, factory: factory, source: source, defaults: defaults}
}

// completesQuickly reports whether fn returns, rather than blocking on a lock
// the Router should not be holding. It never hangs the test: a call that does
// deadlock is reported as a failure and left to unblock on its own.
func completesQuickly(fn func()) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

// blocksFor reports whether fn is still running after d. It is how "Close waits"
// is asserted: the only evidence of waiting is that it has not returned.
func blocksFor(fn func(), d time.Duration) (blocked bool, done <-chan struct{}) {
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		fn()
	}()
	select {
	case <-finished:
		return false, finished
	case <-time.After(d):
		return true, finished
	}
}

func TestNewRouterRequiresItsCollaborators(t *testing.T) {
	sessions := &countingSessionService{}
	factory := testFactory(t, ProcessConstraints{})

	router, err := NewRouter(Options{Default: Bundle{Session: sessions}, Factory: factory})
	require.ErrorContains(t, err, "profile source is required")
	require.Nil(t, router)

	router, err = NewRouter(Options{Default: Bundle{Session: sessions}, Source: NoProfiles()})
	require.ErrorContains(t, err, "bundle factory is required")
	require.Nil(t, router)

	// A Router with no default cannot serve the revisions that name no profile,
	// which is nearly all of them. That has to be a startup failure and not a
	// per-request one.
	router, err = NewRouter(Options{Source: NoProfiles(), Factory: factory})
	require.ErrorIs(t, err, ErrIncompleteBundle)
	require.Nil(t, router)
}

// The empty profile id is this process's default store, borrowed. It is mapped
// here and nowhere else, so no caller has to know whether a default exists.
func TestRouterResolvesTheEmptyProfileToTheProcessDefault(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory())
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	first, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "")
	require.NoError(t, err)
	second, err := fixture.router.Resolve(context.Background(), testScope("tenant-b"), "")
	require.NoError(t, err)

	require.Same(t, fixture.defaults, first.Bundle().Session)
	require.Same(t, fixture.defaults, second.Bundle().Session, "one process, one default store")
	require.Zero(t, fixture.factory.buildCount(), "the default is borrowed, never built")
	require.Zero(t, fixture.router.CacheSize())

	require.NoError(t, first.Release())
	require.NoError(t, second.Release())
	require.Zero(t, fixture.defaults.closeCount())
}

func TestRouterBuildsAndCachesADynamicProfile(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory(), inMemoryProfile("tenant-a", "p1"))
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	first, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.NotSame(t, fixture.defaults, first.Bundle().Session)

	second, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.Same(
		t, first.Bundle().Session, second.Bundle().Session,
		"one (tenant, profile id) is one Bundle",
	)
	require.Equal(t, 1, fixture.factory.buildCount())
	require.Equal(t, 1, fixture.router.CacheSize())

	// The source is consulted on every call, not once per cache entry. That is
	// what makes the immutability check live rather than a build-time snapshot.
	require.Equal(t, 2, fixture.source.lookupCount())

	require.NoError(t, first.Release())
	require.NoError(t, second.Release())
}

// The same profile id under two tenants is two profiles and two Bundles. A
// cache keyed by id alone would serve one tenant's conversations from the
// other's store.
func TestRouterKeysTheCacheByTenantAndProfile(t *testing.T) {
	fixture := newRouterFixture(
		t,
		newControlledFactory(),
		inMemoryProfile("tenant-a", "p1"),
		inMemoryProfile("tenant-b", "p1"),
	)
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	tenantA, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	tenantB, err := fixture.router.Resolve(context.Background(), testScope("tenant-b"), "p1")
	require.NoError(t, err)

	require.NotSame(t, tenantA.Bundle().Session, tenantB.Bundle().Session)
	require.Equal(t, 2, fixture.factory.buildCount())
	require.Equal(t, 2, fixture.router.CacheSize())

	require.NoError(t, tenantA.Release())
	require.NoError(t, tenantB.Release())
}

// A tenant that has no such profile gets ErrProfileNotFound, and a tenant that
// asks for another tenant's profile gets exactly the same answer — the source
// is keyed by the caller's tenant, so the other one is not reachable rather
// than merely filtered.
func TestRouterRefusesAProfileTheTenantDoesNotHave(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory(), inMemoryProfile("tenant-a", "p1"))
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-b"), "p1")
	require.ErrorIs(t, err, ErrProfileNotFound)
	require.Nil(t, lease)

	lease, err = fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p2")
	require.ErrorIs(t, err, ErrProfileNotFound)
	require.Nil(t, lease)

	require.Zero(t, fixture.factory.buildCount())
	require.Zero(t, fixture.router.CacheSize(), "a refusal is not a cache entry")
}

// A source that answers with a profile belonging to another tenant is not
// followed. The Bundle it describes would be the wrong tenant's storage, and
// the Router is the last place that can still tell.
func TestRouterRefusesASourceThatCrossesTenants(t *testing.T) {
	defaults := &countingSessionService{}
	factory := newControlledFactory()
	router, err := NewRouter(Options{
		Default: Bundle{Session: defaults},
		Source:  fixedAnswerSource{profile: inMemoryProfile("tenant-b", "p1")},
		Factory: factory,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, router.Close()) })

	lease, err := router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, tenant.ErrTenantScope)
	require.Nil(t, lease)
	require.Zero(t, factory.buildCount())
}

// A source that answers with a different id than the one asked for is broken,
// and the safe reading of a broken answer is that the requested profile was not
// found — never that this other profile will do.
func TestRouterRefusesASourceThatAnswersWithAnotherProfile(t *testing.T) {
	defaults := &countingSessionService{}
	factory := newControlledFactory()
	router, err := NewRouter(Options{
		Default: Bundle{Session: defaults},
		Source:  fixedAnswerSource{profile: inMemoryProfile("tenant-a", "p2")},
		Factory: factory,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, router.Close()) })

	lease, err := router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, ErrProfileNotFound)
	require.Nil(t, lease)
	require.Zero(t, factory.buildCount())
}

// An id that could never name a profile is a bad request, and it is refused
// before it becomes a lookup, a cache key or a singleflight key.
func TestRouterRefusesAMalformedProfileIDBeforeConsultingTheSource(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory())
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	for _, profileID := range []string{
		"../../etc/passwd",
		"p 1",
		"p\x001",
		"-leading-dash",
	} {
		lease, err := fixture.router.Resolve(
			context.Background(), testScope("tenant-a"), profileID)
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		require.Nil(t, lease)
	}
	require.Zero(t, fixture.source.lookupCount())
	require.Zero(t, fixture.factory.buildCount())
}

func TestRouterValidatesTheScopeAndTheContext(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory(), inMemoryProfile("tenant-a", "p1"))
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	lease, err := fixture.router.Resolve(context.Background(), tenant.TenantContext{}, "")
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	require.Nil(t, lease)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	lease, err = fixture.router.Resolve(cancelled, testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, lease)

	//nolint:staticcheck // a nil context is exactly what is under test here.
	lease, err = fixture.router.Resolve(nil, testScope("tenant-a"), "p1")
	require.Error(t, err)
	require.Nil(t, lease)

	require.Zero(t, fixture.factory.buildCount())
}

// Concurrent resolutions of one profile build it once. Without this each
// arriving request would open its own store, and the last one to finish would
// be the one everybody else silently stopped using.
func TestRouterBuildsEachProfileOnceUnderConcurrency(t *testing.T) {
	factory := blockingFactory()
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	const callers = 24
	results := make(chan resolution, callers)
	var started sync.WaitGroup
	started.Add(callers)
	for range callers {
		go func() {
			started.Done()
			lease, err := fixture.router.Resolve(
				context.Background(), testScope("tenant-a"), "p1")
			results <- resolution{lease: lease, err: err}
		}()
	}
	started.Wait()
	// At least one Build has to be in flight before the gate opens, otherwise
	// this test would pass on a Router that built nothing at all.
	require.Equal(t, "p1", (<-factory.entered).ID)
	factory.releaseBuilds()

	var acquired []Lease
	var failures []error
	for range callers {
		result := <-results
		if result.err != nil {
			failures = append(failures, result.err)
			continue
		}
		acquired = append(acquired, result.lease)
	}
	// Registered after the Cleanup that closes the Router, so it runs before it.
	// Every assertion below is then free to stop the test without leaving a live
	// lease for Close to wait on.
	t.Cleanup(func() {
		for _, lease := range acquired {
			_ = lease.Release()
		}
	})

	require.Empty(t, failures)
	require.Len(t, acquired, callers)
	require.Equal(t, 1, factory.buildCount(), "one profile, one build")
	for _, lease := range acquired {
		require.Same(t, acquired[0].Bundle().Session, lease.Bundle().Session)
	}
	for _, lease := range acquired {
		require.NoError(t, lease.Release())
	}
}

// The default path is a reference count too, and it has to survive the same
// concurrency: every caller gets the one process store and every lease is
// counted, so Close cannot walk past a live holder.
func TestRouterHandsOutTheDefaultConcurrently(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory())

	const callers = 24
	results := make(chan resolution, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := fixture.router.Resolve(
				context.Background(), testScope("tenant-a"), "")
			results <- resolution{lease: lease, err: err}
		}()
	}
	wg.Wait()
	close(results)

	// Drained and released in full before anything is asserted. This test closes
	// the Router itself at the end, and Close waits for every lease: an assertion
	// that stopped the test with the other twenty-three still held would leave
	// nobody to release them. Each Bundle is read on the way past, while its
	// lease is still live, rather than out of a lease that has been released.
	type observed struct {
		bundle     Bundle
		err        error
		releaseErr error
	}
	var seen []observed
	for result := range results {
		record := observed{err: result.err}
		if result.lease != nil {
			record.bundle = result.lease.Bundle()
			record.releaseErr = result.lease.Release()
		}
		seen = append(seen, record)
	}

	require.Len(t, seen, callers)
	for _, record := range seen {
		require.NoError(t, record.err)
		require.Same(t, fixture.defaults, record.bundle.Session)
		require.NoError(t, record.releaseErr)
	}
	require.NoError(t, fixture.router.Close())
	require.Zero(t, fixture.defaults.closeCount())
}

// One caller going away must not cancel a build every other caller is waiting
// on. The build runs under the Router's lifecycle context precisely so a
// cancelled HTTP request cannot take the store down with it.
func TestRouterCancellingOneCallerLeavesTheSharedBuildRunning(t *testing.T) {
	factory := blockingFactory()
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	leavingCtx, leave := context.WithCancel(context.Background())
	leavingErr := make(chan error, 1)
	go func() {
		lease, err := fixture.router.Resolve(leavingCtx, testScope("tenant-a"), "p1")
		if lease != nil {
			_ = lease.Release()
		}
		leavingErr <- err
	}()
	require.Equal(t, "p1", (<-factory.entered).ID)

	// A second caller joins the build already in flight, then the first leaves.
	staying := make(chan resolution, 1)
	go func() {
		lease, err := fixture.router.Resolve(
			context.Background(), testScope("tenant-a"), "p1")
		staying <- resolution{lease: lease, err: err}
	}()
	leave()
	require.ErrorIs(t, <-leavingErr, context.Canceled)

	factory.releaseBuilds()
	joined := <-staying
	require.NoError(t, joined.err)
	lease := joined.lease
	// Registered after the Cleanup that closes the Router, so it runs first. The
	// assertions below hold a live lease, and Close waits for it.
	t.Cleanup(func() { _ = lease.Release() })
	require.NoError(t, lease.Bundle().Validate())
	require.Equal(t, 1, factory.buildCount())
	require.Empty(t, factory.closes(), "the abandoned caller must not close a live store")

	// And the store is genuinely usable: nothing released it on the way past.
	require.Zero(t, lease.Bundle().Session.(*countingSessionService).closeCount())
	require.NoError(t, lease.Release())
}

// The id is the version. Content that moved under a stable id is reported, not
// followed: the Bundle in hand may already be the wrong storage, and building a
// second one under the same key would make two answers to one question.
func TestRouterReportsProfileContentThatChangedUnderAStableID(t *testing.T) {
	factory := newControlledFactory()
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	original, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	originalSessions := original.Bundle().Session

	// The source now answers the same id with different content, which no real
	// source may do.
	fixture.source.set(Profile{
		TenantID: "tenant-a",
		ID:       "p1",
		Session: SessionSpec{
			Backend:  postgresProfile("tenant-a", "p1").Session.Backend,
			Postgres: &PostgresSpec{DSNRef: "env:TENANT_A_SESSION_DSN"},
		},
	})

	lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, ErrProfileChanged)
	require.Nil(t, lease)
	require.ErrorContains(t, err, "p1")
	require.ErrorContains(t, err, "tenant-a")

	// Nothing was rebuilt and nothing was evicted: the cache is exactly as it
	// was, and the holder that already has a lease still has a live store.
	require.Equal(t, 1, factory.buildCount())
	require.Equal(t, 1, fixture.router.CacheSize())
	require.Empty(t, factory.closes())
	require.Same(t, originalSessions, original.Bundle().Session)

	// Repeating the resolution repeats the refusal rather than eventually
	// accepting the new content.
	_, err = fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, ErrProfileChanged)
	require.Equal(t, 1, factory.buildCount())

	require.NoError(t, original.Release())
}

// ErrProfileChanged is the report of a broken source, so a source that is not
// broken must never provoke it — and "not broken" has to include a caller that
// still holds the Profile it published.
//
// This is the failure the copies in MemoryProfileSource prevent, seen where it
// would be felt. Router fingerprints what the source hands back on every single
// Resolve, so one aliased pointer would take a tenant's storage permanently out
// of service: nothing is rebuilt and nothing is evicted, so every later request
// for that id fails the same way until the process restarts.
func TestRouterKeepsServingWhenACallerEditsThePointersItPublished(t *testing.T) {
	published := postgresProfile("tenant-a", "p1")
	source := NewMemoryProfileSource()
	require.NoError(t, source.Put(published))

	factory := newControlledFactory()
	router, err := NewRouter(Options{
		Default: Bundle{Session: &countingSessionService{}},
		Source:  source,
		Factory: factory,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, router.Close()) })

	first, err := router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.NoError(t, first.Release())
	require.Equal(t, 1, factory.buildCount())

	// Both routes at once: the value Put was handed, and the value a resolution
	// handed back. Neither may reach the content the Bundle was built from.
	published.Session.Postgres.Schema = "someone_elses_schema"
	handedBack, err := source.ResolveProfile(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	handedBack.Session.Postgres.DSNRef = "env:SOMEWHERE_ELSE"

	second, err := router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err, "an edit through a published pointer took the profile out of service")
	require.NotErrorIs(t, err, ErrProfileChanged)
	require.Same(t, first.Bundle().Session, second.Bundle().Session)
	require.Equal(t, 1, factory.buildCount(), "the cached Bundle still answers")
	require.NoError(t, second.Release())
}

// A source that cannot answer right now is a failure of this request, not of
// the profile. Nothing is built, nothing is cached, and the failure does not
// outlive it: the profile store being briefly unreachable must not take a
// tenant's storage out of service until the process restarts.
func TestRouterSurfacesATransientSourceFailureAndRecoversFromIt(t *testing.T) {
	factory := newControlledFactory()
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	unreachable := errors.New("profile store is unreachable")
	fixture.source.fail(unreachable)

	lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, unreachable)
	require.Nil(t, lease)
	// Not reported as any of the sentinels that mean "this profile is wrong".
	// The layer above turns those into a refusal the caller cannot retry out of,
	// and this one is precisely the kind it can.
	require.NotErrorIs(t, err, ErrProfileNotFound)
	require.NotErrorIs(t, err, ErrInvalidProfile)
	require.NotErrorIs(t, err, ErrProfileChanged)
	require.Zero(t, factory.buildCount(), "a source that did not answer caused a build")
	require.Zero(t, fixture.router.CacheSize())

	// The same id, once the source can answer again. Nothing about the failure
	// was remembered.
	fixture.source.fail(nil)
	lease, err = fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.Equal(t, 1, factory.buildCount())
	require.Equal(t, 1, fixture.router.CacheSize())
	require.NoError(t, lease.Release())
}

// A profile that could never be built is refused before anything is built from
// it, and it is refused on the way in rather than by the Factory.
//
// Fingerprint validates, and it has to: the fingerprint is what every later
// resolution of this id is compared against, so content that cannot be built
// must not be recorded as though it had been. A source can also be wrong in a
// way no Factory would catch — the id and the tenant are checked before this,
// but the spec itself is only checked here.
func TestRouterRefusesAProfileTheSourceCannotDescribe(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile Profile
	}{
		{
			name: "a backend nothing knows",
			profile: Profile{
				TenantID: "tenant-a", ID: "p1",
				Session: SessionSpec{Backend: "granite"},
			},
		},
		{
			name: "a backend with its settings missing",
			profile: Profile{
				TenantID: "tenant-a", ID: "p1",
				Session: SessionSpec{
					Backend: postgresProfile("tenant-a", "p1").Session.Backend,
				},
			},
		},
		{
			name: "a backend carrying another backend's settings",
			profile: Profile{
				TenantID: "tenant-a", ID: "p1",
				Session: SessionSpec{
					Backend:  inMemoryProfile("tenant-a", "p1").Session.Backend,
					Postgres: &PostgresSpec{DSNRef: "env:TENANT_A_SESSION_DSN"},
				},
			},
		},
		{
			name: "a reference that is not a reference",
			profile: Profile{
				TenantID: "tenant-a", ID: "p1",
				Session: SessionSpec{
					Backend: postgresProfile("tenant-a", "p1").Session.Backend,
					// A DSN pasted in where its name belonged. This is the case
					// the sentinel exists for, and the reason the message must
					// never carry the value back.
					Postgres: &PostgresSpec{
						DSNRef: "postgres://user:hunter2@db.internal:5432/sessions",
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory := newControlledFactory()
			fixture := newRouterFixture(t, factory, tc.profile)
			t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

			lease, err := fixture.router.Resolve(
				context.Background(), testScope("tenant-a"), "p1")
			require.ErrorIs(t, err, ErrInvalidProfile)
			require.Nil(t, lease)
			require.Zero(t, factory.buildCount(), "an unbuildable profile was built")
			require.Zero(t, fixture.router.CacheSize())
			require.NotContains(t, err.Error(), "hunter2",
				"a credential in a profile reached an error message")
		})
	}
}

// A build that fails is not cached, so the next request tries again rather than
// inheriting a failure it had nothing to do with.
func TestRouterDoesNotCacheAFailedBuild(t *testing.T) {
	factory := newControlledFactory()
	buildErr := errors.New("upstream refused the connection")
	factory.buildErr = buildErr
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))
	t.Cleanup(func() { require.NoError(t, fixture.router.Close()) })

	lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, buildErr)
	require.Nil(t, lease)
	require.Zero(t, fixture.router.CacheSize())

	factory.mu.Lock()
	factory.buildErr = nil
	factory.mu.Unlock()

	lease, err = fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.Equal(t, 1, fixture.router.CacheSize())
	require.NoError(t, lease.Release())
}

// The Factory's refusals reach the caller intact. A revision whose profile this
// process may not serve has to say so with the sentinel, so the layer above can
// tell "not allowed here" from "temporarily unavailable".
func TestRouterSurfacesFactoryConstraintRefusals(t *testing.T) {
	defaults := &countingSessionService{}
	source := newMutableSource(
		inMemoryProfile("tenant-a", "shared"),
		postgresProfile("tenant-a", "durable"),
	)
	router, err := NewRouter(Options{
		Default: Bundle{Session: defaults},
		Source:  source,
		// A process that coordinates with other Workers and has no durable
		// pins: both a shared in-memory store and a durable one are refused,
		// for opposite reasons.
		Factory: testFactory(t, ProcessConstraints{MultiWorker: true}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, router.Close()) })

	lease, err := router.Resolve(context.Background(), testScope("tenant-a"), "shared")
	require.ErrorIs(t, err, ErrNotSharedAcrossWorkers)
	require.Nil(t, lease)

	lease, err = router.Resolve(context.Background(), testScope("tenant-a"), "durable")
	require.ErrorIs(t, err, ErrPinsNotDurable)
	require.Nil(t, lease)

	require.Zero(t, router.CacheSize())
}

// A Factory that returns a Bundle without its close has handed over something
// nobody can release on schedule. It is not cached, and the only release left —
// the one Own performs — happens immediately rather than never.
func TestRouterRefusesAndReleasesABundleWithNoClose(t *testing.T) {
	sessions := &countingSessionService{}
	defaults := &countingSessionService{}
	router, err := NewRouter(Options{
		Default: Bundle{Session: defaults},
		Source:  newMutableSource(inMemoryProfile("tenant-a", "p1")),
		Factory: factoryFunc(func(context.Context, Profile) (Bundle, func() error, error) {
			return Bundle{Session: sessions}, nil, nil
		}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, router.Close()) })

	lease, err := router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.Error(t, err)
	require.ErrorContains(t, err, "no close")
	require.Nil(t, lease)
	require.Equal(t, 1, sessions.closeCount(), "an unreleasable store is released at once")
	require.Zero(t, router.CacheSize())
}

// A Factory that returns an empty Bundle has built nothing usable. It is
// refused here rather than becoming a nil dereference inside a Runner, and what
// it did build is handed back through the close it supplied.
func TestRouterRefusesAndClosesAnIncompleteBundle(t *testing.T) {
	var closes int
	defaults := &countingSessionService{}
	router, err := NewRouter(Options{
		Default: Bundle{Session: defaults},
		Source:  newMutableSource(inMemoryProfile("tenant-a", "p1")),
		Factory: factoryFunc(func(context.Context, Profile) (Bundle, func() error, error) {
			return Bundle{}, func() error { closes++; return nil }, nil
		}),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, router.Close()) })

	lease, err := router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, ErrIncompleteBundle)
	require.Nil(t, lease)
	require.Equal(t, 1, closes)
	require.Zero(t, router.CacheSize())
}

// factoryFunc adapts a function to the Factory interface.
type factoryFunc func(context.Context, Profile) (Bundle, func() error, error)

func (f factoryFunc) Build(
	ctx context.Context,
	profile Profile,
) (Bundle, func() error, error) {
	return f(ctx, profile)
}

// A closed Router serves nobody, including the callers that only want the
// default store. Refusing is the first thing Close does, before it waits for
// anything, so no new holder can appear behind a wait that has already started.
func TestRouterRefusesEveryResolveAfterClose(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory(), inMemoryProfile("tenant-a", "p1"))
	require.NoError(t, fixture.router.Close())

	lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "")
	require.ErrorIs(t, err, ErrRouterClosed)
	require.Nil(t, lease)

	lease, err = fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.ErrorIs(t, err, ErrRouterClosed)
	require.Nil(t, lease)

	require.Zero(t, fixture.factory.buildCount())
}

// Close waits for the holders of dynamic Bundles. Returning while a Runtime is
// still running would close a store out from under a live conversation.
func TestRouterCloseWaitsForDynamicLeases(t *testing.T) {
	factory := newControlledFactory()
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))

	lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)

	var closeErr error
	blocked, done := blocksFor(func() { closeErr = fixture.router.Close() }, 150*time.Millisecond)
	require.True(t, blocked, "Close returned while a Bundle was still leased")
	require.Empty(t, factory.closes(), "the store was closed under its holder")

	require.NoError(t, lease.Release())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the last lease was released")
	}
	require.NoError(t, closeErr)
	require.Equal(t, []string{"p1"}, factory.closes())
}

// And Close waits for the holders of the *default* Bundle too, even though it
// will never close that one.
//
// The reason is one step further down the shutdown path: the process closes its
// session store immediately after this Router returns. A default lease that did
// not count would let shutdown walk straight past a live Runtime and close the
// session service underneath it.
func TestRouterCloseWaitsForDefaultLeases(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory())

	lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "")
	require.NoError(t, err)

	blocked, done := blocksFor(func() { _ = fixture.router.Close() }, 150*time.Millisecond)
	require.True(t, blocked, "Close returned while the process default was still leased")

	require.NoError(t, lease.Release())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the default lease was released")
	}
	// Waiting for it is not the same as owning it: the default store belongs to
	// whoever built the process's storage and outlives this Router.
	require.Zero(t, fixture.defaults.closeCount())
}

// Close waits for builds that were already in flight when it started. A build
// that finished into a Router that had stopped waiting would be a store with no
// owner and no closer.
func TestRouterCloseWaitsForInFlightBuildsAndReleasesWhatTheyProduce(t *testing.T) {
	factory := blockingFactory()
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))

	resolveErr := make(chan error, 1)
	go func() {
		lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
		if lease != nil {
			_ = lease.Release()
		}
		resolveErr <- err
	}()
	require.Equal(t, "p1", (<-factory.entered).ID)

	blocked, done := blocksFor(func() { _ = fixture.router.Close() }, 150*time.Millisecond)
	require.True(t, blocked, "Close returned while a build was still running")

	// The build only now finishes, into a Router that is already closing. What
	// it produced is this goroutine's to release, and it must actually release
	// it rather than cache it or drop it.
	factory.releaseBuilds()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the in-flight build finished")
	}

	require.ErrorIs(t, <-resolveErr, ErrRouterClosed)
	require.Equal(t, 1, factory.buildCount())
	require.Equal(t, []string{"p1"}, factory.closes(), "a store built after Close was leaked")
	require.Equal(t, 1, factory.stores()[0].sessions.closeCount())
	require.Zero(t, fixture.router.CacheSize())
}

// Close cancels the build context, so a Factory that is waiting on an
// unreachable database stops when the process stops instead of after it.
func TestRouterCloseCancelsTheBuildContext(t *testing.T) {
	factory := blockingFactory()
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))

	go func() {
		lease, _ := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
		if lease != nil {
			_ = lease.Release()
		}
	}()
	require.Equal(t, "p1", (<-factory.entered).ID)

	blocked, done := blocksFor(func() { _ = fixture.router.Close() }, 150*time.Millisecond)
	require.True(t, blocked)
	factory.releaseBuilds()
	<-done

	contextErrors := factory.buildContextErrors()
	require.Len(t, contextErrors, 1)
	require.ErrorIs(
		t, contextErrors[0], context.Canceled,
		"the build context outlived the Router that owns it",
	)
}

// Bundles are closed in reverse order of construction, which is the order that
// undoes them, and only the ones this Router built.
func TestRouterCloseReleasesOwnedBundlesInReverseOrder(t *testing.T) {
	factory := newControlledFactory()
	fixture := newRouterFixture(
		t,
		factory,
		inMemoryProfile("tenant-a", "p1"),
		inMemoryProfile("tenant-a", "p2"),
		inMemoryProfile("tenant-b", "p3"),
	)

	for _, resolution := range []struct{ tenantID, profileID string }{
		{"tenant-a", "p1"},
		{"tenant-a", "p2"},
		{"tenant-b", "p3"},
	} {
		lease, err := fixture.router.Resolve(
			context.Background(), testScope(resolution.tenantID), resolution.profileID)
		require.NoError(t, err)
		require.NoError(t, lease.Release())
	}
	// A default lease taken and released along the way must not appear in the
	// close order at all.
	defaultLease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "")
	require.NoError(t, err)
	require.NoError(t, defaultLease.Release())

	require.NoError(t, fixture.router.Close())
	require.Equal(t, []string{"p3", "p2", "p1"}, factory.closes())
	require.Zero(t, fixture.defaults.closeCount())
	require.Zero(t, fixture.router.CacheSize())
}

// Every close failure is reported, not just the first, and every caller of
// Close gets the same answer. A store that failed to flush on shutdown is
// exactly the kind of thing that turns into "the last turn of every
// conversation is missing" a week later.
func TestRouterCloseIsIdempotentAndReportsEveryFailure(t *testing.T) {
	factory := newControlledFactory()
	firstErr := errors.New("p1 refused to flush")
	secondErr := errors.New("p2 refused to flush")
	factory.closeErrs["p1"] = firstErr
	factory.closeErrs["p2"] = secondErr
	fixture := newRouterFixture(
		t, factory, inMemoryProfile("tenant-a", "p1"), inMemoryProfile("tenant-a", "p2"))

	for _, profileID := range []string{"p1", "p2"} {
		lease, err := fixture.router.Resolve(
			context.Background(), testScope("tenant-a"), profileID)
		require.NoError(t, err)
		require.NoError(t, lease.Release())
	}

	closeErr := fixture.router.Close()
	require.ErrorIs(t, closeErr, firstErr)
	require.ErrorIs(t, closeErr, secondErr)

	require.Equal(t, closeErr, fixture.router.Close())
	require.Equal(t, closeErr, fixture.router.Close())
	require.Len(t, factory.closes(), 2, "a second Close closed the same stores again")
}

func TestRouterCloseIsConcurrentSafe(t *testing.T) {
	factory := newControlledFactory()
	closeErr := errors.New("p1 refused to flush")
	factory.closeErrs["p1"] = closeErr
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))

	lease, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.NoError(t, lease.Release())

	results := make(chan error, 16)
	var start sync.WaitGroup
	start.Add(1)
	for range 16 {
		go func() {
			start.Wait()
			results <- fixture.router.Close()
		}()
	}
	start.Done()
	for range 16 {
		require.ErrorIs(t, <-results, closeErr)
	}
	require.Len(t, factory.closes(), 1)
}

// A lease released twice must count once. A second decrement would let Close
// walk past a holder that is still there — the count would reach zero while a
// Runtime was still using the store.
func TestRouterLeaseReleaseIsIdempotent(t *testing.T) {
	factory := newControlledFactory()
	fixture := newRouterFixture(t, factory, inMemoryProfile("tenant-a", "p1"))

	first, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	second, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, first.Release())
	}
	require.Empty(t, factory.closes(), "an over-released lease closed a store still in use")

	// The second holder is still counted, so Close still waits for it.
	blocked, done := blocksFor(func() { _ = fixture.router.Close() }, 150*time.Millisecond)
	require.True(t, blocked)
	require.NoError(t, second.Release())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the surviving lease was released")
	}
	require.Equal(t, []string{"p1"}, factory.closes())
}

// The same rule for the default: releasing one holder's borrowed lease more
// than once must not cancel out another's.
func TestRouterDefaultLeaseReleaseIsIdempotent(t *testing.T) {
	fixture := newRouterFixture(t, newControlledFactory())

	first, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "")
	require.NoError(t, err)
	second, err := fixture.router.Resolve(context.Background(), testScope("tenant-a"), "")
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, first.Release())
	}
	blocked, done := blocksFor(func() { _ = fixture.router.Close() }, 150*time.Millisecond)
	require.True(t, blocked, "Close stopped waiting for a default lease still held")

	require.NoError(t, second.Release())
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the surviving default lease was released")
	}
	require.Zero(t, fixture.defaults.closeCount())
}

// The Factory and the close it supplies are somebody else's code. Running
// either under the Router's lock would put an unknown call inside this
// package's critical section, and a Release arriving from another goroutine
// would deadlock against it.
func TestRouterNeverRunsFactoryOrCloseUnderItsLock(t *testing.T) {
	defaults := &countingSessionService{}
	var router *Router
	var buildReentered, closeReentered bool

	factory := factoryFunc(func(context.Context, Profile) (Bundle, func() error, error) {
		buildReentered = completesQuickly(func() { _ = router.CacheSize() })
		sessions := &countingSessionService{}
		return Bundle{Session: sessions}, func() error {
			closeReentered = completesQuickly(func() { _ = router.CacheSize() })
			return sessions.Close()
		}, nil
	})

	var err error
	router, err = NewRouter(Options{
		Default: Bundle{Session: defaults},
		Source:  newMutableSource(inMemoryProfile("tenant-a", "p1")),
		Factory: factory,
	})
	require.NoError(t, err)

	lease, err := router.Resolve(context.Background(), testScope("tenant-a"), "p1")
	require.NoError(t, err)
	require.True(t, buildReentered, "Factory.Build ran while the Router held its lock")

	require.NoError(t, lease.Release())
	require.NoError(t, router.Close())
	require.True(t, closeReentered, "the Bundle's close ran while the Router held its lock")
}

// A Router that a caller never had is not a reason to panic on the shutdown
// path: a partial startup failure hands back exactly that.
func TestNilRouterIsSafeToCloseAndRefusesToResolve(t *testing.T) {
	var router *Router

	require.NoError(t, router.Close())
	require.Zero(t, router.CacheSize())

	lease, err := router.Resolve(context.Background(), testScope("tenant-a"), "")
	require.Error(t, err)
	require.Nil(t, lease)
}

// Resolving through a Router is a mixed workload in production — defaults,
// dynamic profiles, refusals and releases at once — and the race detector has
// to see all of it against one Router.
func TestRouterUnderMixedConcurrentLoad(t *testing.T) {
	factory := newControlledFactory()
	fixture := newRouterFixture(
		t, factory, inMemoryProfile("tenant-a", "p1"), inMemoryProfile("tenant-b", "p1"))

	const workers = 32
	results := make(chan resolution, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var scope tenant.TenantContext
			var profileID string
			switch worker % 4 {
			case 0:
				scope, profileID = testScope("tenant-a"), ""
			case 1:
				scope, profileID = testScope("tenant-a"), "p1"
			case 2:
				scope, profileID = testScope("tenant-b"), "p1"
			case 3:
				// Nobody's profile: a refusal running alongside everything else.
				scope, profileID = testScope("tenant-a"), "absent"
			}
			lease, err := fixture.router.Resolve(context.Background(), scope, profileID)
			results <- resolution{lease: lease, err: err}
		}()
	}
	wg.Wait()
	close(results)

	// Every outcome is recorded and every lease released before a single
	// assertion runs. This test closes the Router at the end and Close waits for
	// leases, so a require that fired mid-drain would hang rather than fail.
	type outcome struct {
		err         error
		hadLease    bool
		validateErr error
		releaseErr  error
	}
	var outcomes []outcome
	for result := range results {
		record := outcome{err: result.err}
		if result.lease != nil {
			record.hadLease = true
			record.validateErr = result.lease.Bundle().Validate()
			record.releaseErr = result.lease.Release()
		}
		outcomes = append(outcomes, record)
	}

	require.Len(t, outcomes, workers)
	for _, record := range outcomes {
		if record.err != nil {
			require.ErrorIs(t, record.err, ErrProfileNotFound)
			require.False(t, record.hadLease, "a refusal handed back a lease")
			continue
		}
		require.True(t, record.hadLease)
		require.NoError(t, record.validateErr)
		require.NoError(t, record.releaseErr)
	}

	require.Equal(t, 2, factory.buildCount())
	require.NoError(t, fixture.router.Close())
	require.Len(t, factory.closes(), 2)
	require.Zero(t, fixture.defaults.closeCount())
}
