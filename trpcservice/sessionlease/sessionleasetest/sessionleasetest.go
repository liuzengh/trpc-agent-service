// Package sessionleasetest holds the behaviour contract every
// sessionlease.Coordinator implementation has to satisfy.
//
// The suite lives in its own package rather than in a _test.go file beside the
// in-memory reference because the Redis backend, in a second package, has to
// run exactly the same assertions. A conformance suite that is copied instead
// of shared stops being a contract the moment one copy is fixed.
//
// RunCoordinatorSuite takes a factory of factories: each subtest is handed a
// fresh, isolated [Backend], and can build several coordinators over it. Two
// coordinators over one backend is how the suite expresses the situation this
// package exists for — two Workers, one Session — without needing two
// processes.
//
// The suite asserts only what the Coordinator and Lease interfaces promise. It
// never reaches behind them, so it says nothing about key naming, Lua scripts
// or connection handling; those belong to each implementation's own tests. In
// particular it does not assert that a lost holder has stopped writing to the
// Session, because no implementation can promise that.
package sessionleasetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// suiteTimeout bounds every subtest. The suite's own waits are far shorter, so
// this only stops an unreachable backend from hanging until the package
// timeout.
const suiteTimeout = 30 * time.Second

// Callers is how many concurrent acquisitions the single-winner property is
// proven against. Exported so an implementation's own tests contend the same
// way and the two numbers do not drift.
const Callers = 32

// Timings are the lease timings the suite runs with. They are short enough that
// a TTL takeover fits in a test and long enough that an ordinary localhost
// round trip, or a race-instrumented build, does not lose a lease by accident.
//
// Exported so an implementation's own tests can use the same numbers.
func Timings() sessionlease.Config {
	return sessionlease.Config{
		TTL:           600 * time.Millisecond,
		RenewInterval: 100 * time.Millisecond,
		SafetyMargin:  150 * time.Millisecond,
	}
}

// settle is the slack added to a TTL before the suite expects a takeover to be
// possible, and the window a Done channel is expected to close within.
const settle = 400 * time.Millisecond

// Backend is one isolated coordination backend a subtest runs against.
type Backend struct {
	// New builds a coordinator over this backend. Every coordinator it returns
	// coordinates with every other one from the same Backend, and with none
	// from another. The factory owns its own cleanup.
	New func(t *testing.T, cfg sessionlease.Config) sessionlease.Coordinator

	// Break makes this backend unreachable for every coordinator over it, so
	// the suite can prove that Acquire fails closed and that a renewal which
	// cannot be confirmed eventually loses its lease.
	//
	// Leave it nil when the implementation has no such failure mode; the
	// in-memory reference is a map and cannot become unreachable. The subtests
	// that need it are skipped, not silently passed.
	Break func(t *testing.T)
}

// NewBackend builds the isolated backend a single subtest runs against. It must
// return state that is empty and isolated from every other subtest: the suite
// reuses fixed ids such as "tenant-a" across subtests, so a shared backend
// would collide.
type NewBackend func(t *testing.T) Backend

// RunCoordinatorSuite runs the whole contract against newBackend.
func RunCoordinatorSuite(t *testing.T, newBackend NewBackend) {
	t.Helper()

	t.Run("ExcludesTheSameKey", func(t *testing.T) {
		assertExcludesTheSameKey(t, newBackend(t))
	})
	t.Run("IsolatesKeyFields", func(t *testing.T) {
		assertIsolatesKeyFields(t, newBackend(t))
	})
	t.Run("ReleaseAllowsReacquireAndAdvancesTheFence", func(t *testing.T) {
		assertReleaseAllowsReacquire(t, newBackend(t))
	})
	t.Run("ConcurrentAcquireHasExactlyOneWinner", func(t *testing.T) {
		assertConcurrentAcquireHasOneWinner(t, newBackend(t))
	})
	t.Run("AbandonedHolderLosesTheLeaseAfterTheTTL", func(t *testing.T) {
		assertAbandonedHolderLosesTheLease(t, newBackend(t))
	})
	t.Run("StaleReleaseDoesNotDisturbTheNewOwner", func(t *testing.T) {
		assertStaleReleaseDoesNotDisturbTheNewOwner(t, newBackend(t))
	})
	t.Run("CloseStopsRenewalWithoutReleasing", func(t *testing.T) {
		assertCloseStopsRenewalWithoutReleasing(t, newBackend(t))
	})
	t.Run("CloseIsIdempotentAndRefusesAcquire", func(t *testing.T) {
		assertCloseIsIdempotent(t, newBackend(t))
	})
	t.Run("CloseRacingAcquireLeavesNoLockNobodyWasGiven", func(t *testing.T) {
		assertCloseRacingAcquireLeavesNoLockBehind(t, newBackend(t))
	})
	t.Run("ReleaseIsIdempotent", func(t *testing.T) {
		assertReleaseIsIdempotent(t, newBackend(t))
	})
	t.Run("RejectsInvalidInput", func(t *testing.T) {
		assertRejectsInvalidInput(t, newBackend(t))
	})
	t.Run("RejectsUnusableContext", func(t *testing.T) {
		assertRejectsUnusableContext(t, newBackend(t))
	})
	t.Run("UnavailableBackendFailsClosed", func(t *testing.T) {
		assertUnavailableBackendFailsClosed(t, newBackend(t))
	})
	t.Run("UnconfirmedRenewalLosesTheLease", func(t *testing.T) {
		assertUnconfirmedRenewalLosesTheLease(t, newBackend(t))
	})
}

// Context returns the context a subtest and its fixtures should use. It is
// derived from t.Context, so it is cancelled when the subtest ends.
//
// A cleanup that has to undo a write must not use this context: t.Context is
// cancelled before cleanups run, so an implementation's teardown needs a fresh
// context of its own.
func Context(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), suiteTimeout)
	t.Cleanup(cancel)
	return ctx
}

// Key returns the lease scope the suite works on.
func Key() sessiondir.Key {
	return sessiondir.Key{
		TenantID:    "tenant-a",
		AppID:       "assistant",
		PrincipalID: "principal-a",
		SessionID:   "session-a",
		Epoch:       1,
	}
}

// MutateKey returns one variant of [Key] per field, each differing in exactly
// that field. Leases are scoped to the whole key, so every one of these has to
// be independent of the others.
func MutateKey() map[string]sessiondir.Key {
	return map[string]sessiondir.Key{
		"tenant": func() sessiondir.Key {
			key := Key()
			key.TenantID = "tenant-b"
			return key
		}(),
		"app": func() sessiondir.Key {
			key := Key()
			key.AppID = "reporter"
			return key
		}(),
		"principal": func() sessiondir.Key {
			key := Key()
			key.PrincipalID = "principal-b"
			return key
		}(),
		"session": func() sessiondir.Key {
			key := Key()
			key.SessionID = "session-b"
			return key
		}(),
		"epoch": func() sessiondir.Key {
			key := Key()
			key.Epoch = 2
			return key
		}(),
	}
}

func assertExcludesTheSameKey(t *testing.T, backend Backend) {
	t.Helper()
	ctx := Context(t)
	first := backend.New(t, Timings())
	second := backend.New(t, Timings())

	lease, err := first.Acquire(ctx, Key())
	require.NoError(t, err)
	require.NotNil(t, lease)

	_, err = second.Acquire(ctx, Key())
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy,
		"a second Worker must be refused while the first holds the lease")

	requireHeld(t, lease, "a refused acquisition must not disturb the holder")
}

func assertIsolatesKeyFields(t *testing.T, backend Backend) {
	t.Helper()
	ctx := Context(t)
	coordinator := backend.New(t, Timings())

	held, err := coordinator.Acquire(ctx, Key())
	require.NoError(t, err)

	for field, key := range MutateKey() {
		t.Run(field, func(t *testing.T) {
			lease, err := coordinator.Acquire(ctx, key)
			require.NoError(t, err, "a different %s is a different lease scope", field)
			require.NoError(t, lease.Release(Context(t)))
		})
	}

	requireHeld(t, held, "leases on neighbouring keys must not disturb this one")
}

func assertReleaseAllowsReacquire(t *testing.T, backend Backend) {
	t.Helper()
	ctx := Context(t)
	first := backend.New(t, Timings())
	second := backend.New(t, Timings())

	before, err := first.Acquire(ctx, Key())
	require.NoError(t, err)
	require.NoError(t, before.Release(ctx))
	requireDone(t, before, "release ends the lease")

	after, err := second.Acquire(ctx, Key())
	require.NoError(t, err, "the lease is free once its holder released it")
	require.Greater(t, after.Fence(), before.Fence(),
		"every acquisition of a key advances that key's fence")
	require.NoError(t, after.Release(ctx))
}

func assertConcurrentAcquireHasOneWinner(t *testing.T, backend Backend) {
	t.Helper()
	ctx := Context(t)
	coordinator := backend.New(t, Timings())

	leases, errs := acquireConcurrently(t, ctx, coordinator, Key(), Callers)

	require.Len(t, leases, 1, "exactly one of %d concurrent acquisitions may win", Callers)
	require.Len(t, errs, Callers-1)
	for _, err := range errs {
		require.ErrorIs(t, err, sessionlease.ErrSessionBusy,
			"a loser must be told the session is busy, not that coordination failed")
	}
	require.NoError(t, leases[0].Release(ctx))
}

func assertAbandonedHolderLosesTheLease(t *testing.T, backend Backend) {
	t.Helper()
	cfg := Timings()
	first := backend.New(t, cfg)
	second := backend.New(t, cfg)

	// Ending the acquiring context is how a Worker says "my run is over but I
	// am not giving the lock back": renewal stops, the lock is left to expire.
	// It is also the closest the interface gets to simulating a Worker that
	// stopped answering.
	runCtx, abandon := context.WithCancel(Context(t))
	lease, err := first.Acquire(runCtx, Key())
	require.NoError(t, err)
	abandon()

	requireDone(t, lease, "an abandoned holder must stop claiming the lease")

	_, err = second.Acquire(Context(t), Key())
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy,
		"an abandoned lock must not be released early: the TTL is what covers a "+
			"cancelled run's tail writes")

	time.Sleep(cfg.TTL + settle)

	taken, err := second.Acquire(Context(t), Key())
	require.NoError(t, err, "another Worker takes over once the TTL has passed")
	require.Greater(t, taken.Fence(), lease.Fence(), "a takeover advances the fence")
	require.NoError(t, taken.Release(Context(t)))
}

func assertStaleReleaseDoesNotDisturbTheNewOwner(t *testing.T, backend Backend) {
	t.Helper()
	cfg := Timings()
	first := backend.New(t, cfg)
	second := backend.New(t, cfg)

	runCtx, abandon := context.WithCancel(Context(t))
	stale, err := first.Acquire(runCtx, Key())
	require.NoError(t, err)
	abandon()
	requireDone(t, stale, "an abandoned holder must stop claiming the lease")
	time.Sleep(cfg.TTL + settle)

	owner, err := second.Acquire(Context(t), Key())
	require.NoError(t, err)

	require.ErrorIs(t, stale.Release(Context(t)), sessionlease.ErrLeaseLost,
		"releasing a lease that was already lost reports the loss")
	requireHeld(t, owner, "a stale release must not take the lock from its new owner")

	third := backend.New(t, cfg)
	_, err = third.Acquire(Context(t), Key())
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy,
		"the new owner still excludes everybody else after the stale release")

	require.NoError(t, owner.Release(Context(t)))
}

func assertCloseStopsRenewalWithoutReleasing(t *testing.T, backend Backend) {
	t.Helper()
	cfg := Timings()
	first := backend.New(t, cfg)
	second := backend.New(t, cfg)

	lease, err := first.Acquire(Context(t), Key())
	require.NoError(t, err)

	require.NoError(t, first.Close())
	requireDone(t, lease, "closing a coordinator ends the leases it handed out")

	_, err = second.Acquire(Context(t), Key())
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy,
		"closing must not delete the lock: shutdown is exactly when the TTL has "+
			"to cover a cancelled run's tail writes")

	time.Sleep(cfg.TTL + settle)

	taken, err := second.Acquire(Context(t), Key())
	require.NoError(t, err, "the abandoned lock expires by TTL")
	require.NoError(t, taken.Release(Context(t)))
}

func assertCloseIsIdempotent(t *testing.T, backend Backend) {
	t.Helper()
	coordinator := backend.New(t, Timings())

	require.NoError(t, coordinator.Close())
	require.NoError(t, coordinator.Close(), "Close is idempotent")

	_, err := coordinator.Acquire(Context(t), Key())
	require.ErrorIs(t, err, sessionlease.ErrClosed)
}

// assertCloseRacingAcquireLeavesNoLockBehind covers the window between an
// acquisition being admitted and its lock coming back: a Close that lands in
// there must not leave a lock behind that no caller was ever given.
//
// This samples the race rather than forcing it. Nothing in the Coordinator
// interface can stop an acquisition mid-flight, so what is asserted is the
// invariant, which has to hold whichever way the race fell; the interleaving
// itself is pinned deterministically in the sessionlease package's own tests,
// where the backend can be held open on purpose.
func assertCloseRacingAcquireLeavesNoLockBehind(t *testing.T, backend Backend) {
	t.Helper()
	ctx := Context(t)
	cfg := Timings()
	closing := backend.New(t, cfg)
	next := backend.New(t, cfg)

	const acquirers = 4
	var ready, done sync.WaitGroup
	ready.Add(acquirers + 1)
	done.Add(acquirers + 1)
	start := make(chan struct{})

	var mu sync.Mutex
	var held []sessionlease.Lease
	var refusals []error

	for range acquirers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			lease, err := closing.Acquire(ctx, Key())
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				refusals = append(refusals, err)
				return
			}
			held = append(held, lease)
		}()
	}
	var closeErr error
	go func() {
		defer done.Done()
		ready.Done()
		<-start
		closeErr = closing.Close()
	}()

	ready.Wait()
	close(start)
	done.Wait()
	require.NoError(t, closeErr)

	require.LessOrEqual(t, len(held), 1,
		"the lease is exclusive however a Close falls across it")
	for _, err := range refusals {
		require.True(t,
			errors.Is(err, sessionlease.ErrClosed) || errors.Is(err, sessionlease.ErrSessionBusy),
			"an acquisition racing a Close is refused because the coordinator "+
				"went away or because somebody else won; it is never told that "+
				"coordination failed (got %v)", err)
	}

	if len(held) == 1 {
		// A caller was given this lease before the Close, so its lock is a run
		// winding down and belongs to the TTL, exactly as in
		// CloseStopsRenewalWithoutReleasing.
		requireDone(t, held[0], "closing a coordinator ends the leases it handed out")
		return
	}

	lease, err := next.Acquire(ctx, Key())
	require.NoError(t, err,
		"no caller was given this lease, so no lock may be left holding the key: "+
			"an acquisition that was refused never became a run, and there are no "+
			"tail writes for its TTL to cover")
	require.NoError(t, lease.Release(ctx))
}

func assertReleaseIsIdempotent(t *testing.T, backend Backend) {
	t.Helper()
	ctx := Context(t)
	coordinator := backend.New(t, Timings())

	lease, err := coordinator.Acquire(ctx, Key())
	require.NoError(t, err)
	require.NoError(t, lease.Release(ctx))
	require.NoError(t, lease.Release(ctx), "Release is idempotent")
}

func assertRejectsInvalidInput(t *testing.T, backend Backend) {
	t.Helper()
	ctx := Context(t)
	coordinator := backend.New(t, Timings())

	for field, key := range invalidKeys() {
		t.Run(field, func(t *testing.T) {
			_, err := coordinator.Acquire(ctx, key)
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		})
	}

	t.Run("context", func(t *testing.T) {
		//nolint:staticcheck // Passing a nil context is exactly what is asserted.
		_, err := coordinator.Acquire(nil, Key())
		require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	})
}

func assertRejectsUnusableContext(t *testing.T, backend Backend) {
	t.Helper()
	coordinator := backend.New(t, Timings())

	cancelled, cancel := context.WithCancel(Context(t))
	cancel()

	_, err := coordinator.Acquire(cancelled, Key())
	require.ErrorIs(t, err, context.Canceled,
		"a cancelled caller keeps its own error: 'the client went away' is not "+
			"'coordination is down'")
	require.NotErrorIs(t, err, sessionlease.ErrSessionBusy)
}

func assertUnavailableBackendFailsClosed(t *testing.T, backend Backend) {
	t.Helper()
	if backend.Break == nil {
		t.Skip("implementation has no unreachable-backend failure mode")
	}
	coordinator := backend.New(t, Timings())
	backend.Break(t)

	_, err := coordinator.Acquire(Context(t), Key())
	require.ErrorIs(t, err, sessionlease.ErrUnavailable,
		"an unreachable backend fails closed: the caller must not run")
	require.NotErrorIs(t, err, sessionlease.ErrSessionBusy,
		"an unreachable backend is not a busy session; the two get different "+
			"HTTP statuses")
}

func assertUnconfirmedRenewalLosesTheLease(t *testing.T, backend Backend) {
	t.Helper()
	if backend.Break == nil {
		t.Skip("implementation has no unreachable-backend failure mode")
	}
	cfg := Timings()
	coordinator := backend.New(t, cfg)

	lease, err := coordinator.Acquire(Context(t), Key())
	require.NoError(t, err)

	backend.Break(t)

	// A renewal that cannot be confirmed is retried, never counted as success,
	// and gives up before the lock could be taken by somebody else.
	requireDoneWithin(t, lease, cfg.TTL+settle,
		"a holder that cannot confirm its renewal must stop claiming the lease")
}

func invalidKeys() map[string]sessiondir.Key {
	empty := map[string]sessiondir.Key{}
	for field, mutate := range map[string]func(*sessiondir.Key){
		"tenant":    func(k *sessiondir.Key) { k.TenantID = "" },
		"app":       func(k *sessiondir.Key) { k.AppID = "" },
		"principal": func(k *sessiondir.Key) { k.PrincipalID = "" },
		"session":   func(k *sessiondir.Key) { k.SessionID = "bad session id" },
	} {
		key := Key()
		mutate(&key)
		empty[field] = key
	}
	return empty
}

// acquireConcurrently releases Callers goroutines at once and reports which
// acquisitions won. The barrier matters: without it the goroutines would start
// far enough apart that the first one could finish before the last one begins.
func acquireConcurrently(
	t *testing.T,
	ctx context.Context,
	coordinator sessionlease.Coordinator,
	key sessiondir.Key,
	callers int,
) ([]sessionlease.Lease, []error) {
	t.Helper()

	var ready, done sync.WaitGroup
	ready.Add(callers)
	done.Add(callers)
	start := make(chan struct{})

	var mu sync.Mutex
	leases := make([]sessionlease.Lease, 0, callers)
	errs := make([]error, 0, callers)

	for range callers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			lease, err := coordinator.Acquire(ctx, key)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			leases = append(leases, lease)
		}()
	}

	ready.Wait()
	close(start)
	done.Wait()
	return leases, errs
}

func requireHeld(t *testing.T, lease sessionlease.Lease, reason string) {
	t.Helper()
	select {
	case <-lease.Done():
		require.Fail(t, "lease ended unexpectedly", reason)
	default:
	}
}

func requireDone(t *testing.T, lease sessionlease.Lease, reason string) {
	t.Helper()
	requireDoneWithin(t, lease, settle, reason)
}

func requireDoneWithin(t *testing.T, lease sessionlease.Lease, within time.Duration, reason string) {
	t.Helper()
	select {
	case <-lease.Done():
	case <-time.After(within):
		require.Fail(t, "lease did not end", "%s (waited %s)", reason, within)
	}
}
