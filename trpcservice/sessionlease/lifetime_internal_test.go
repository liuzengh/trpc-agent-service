package sessionlease

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
)

// Close's promise is about what is *not* happening by the time it returns, and
// the only honest way to test that is to hold something open across it and
// check that it stayed held. These tests drive Lifetime through the exact
// interleavings a coordinator's Acquire performs, in the same order, so the
// window between "the closed check passed" and "the backend answered" is a step
// the test controls rather than one it has to race for.

func lifetimeTimings() Config {
	return Config{
		TTL:           time.Second,
		RenewInterval: 100 * time.Millisecond,
		SafetyMargin:  200 * time.Millisecond,
	}
}

// requireStillRunning fails if a Close that must still be waiting has returned.
// It cannot fail spuriously: only a Close that stopped waiting can trip it.
func requireStillRunning(t *testing.T, closed <-chan error, reason string) {
	t.Helper()
	select {
	case err := <-closed:
		require.Fail(t, "Close returned too early", "%s (returned %v)", reason, err)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCloseWaitsForAnAcquireItAdmitted(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	life := NewLifetime(lifetimeTimings())
	key := storeKey()

	// The steps MemoryCoordinator.Acquire takes, stopped between the two that
	// matter: the coordinator was open when this caller was let in.
	require.NoError(t, life.Begin())

	closed := make(chan error, 1)
	go func() { closed <- life.Close() }()
	requireStillRunning(t, closed,
		"an Acquire this coordinator admitted was still running")

	// ... and it finishes after Close has already done everything it does. The
	// lock is real by now: this is the caller that won the race.
	acquiredAt := time.Now()
	fence, taken := store.acquire(key, "owner-a", life.Config().TTL)
	require.True(t, taken)
	held := &memoryHolder{store: store, key: key, owner: "owner-a", ttl: life.Config().TTL}

	lease, err := life.HandOut(context.Background(), fence, acquiredAt, held)
	require.ErrorIs(t, err, ErrClosed,
		"an acquisition that finishes after Close must be refused, not handed "+
			"back as a lease whose renewal is already dead")
	require.Nil(t, lease)

	life.End()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned")
	}

	require.Empty(t, store.locks,
		"the lock of a refused acquisition must be given back: that acquisition "+
			"never became a run, so there are no tail writes for a TTL to cover "+
			"and leaving it only keeps the next Worker out")
}

func TestCloseWaitsForTheRenewalLoopsItHandedOut(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	coordinator, err := NewMemoryCoordinator(store, lifetimeTimings())
	require.NoError(t, err)

	lease, err := coordinator.Acquire(t.Context(), storeKey())
	require.NoError(t, err)

	require.NoError(t, coordinator.Close())

	// Checked without blocking, on purpose: if the loop were still running this
	// would have to wait for it, and waiting is what Close was supposed to have
	// already done.
	select {
	case <-lease.(*managedLease).stopped:
	default:
		require.Fail(t, "Close returned while a renewal loop it started was still running",
			"the process closes the connection the coordinator borrowed as soon "+
				"as Close returns")
	}
}

func TestEveryCloseWaits(t *testing.T) {
	t.Parallel()

	life := NewLifetime(lifetimeTimings())
	require.NoError(t, life.Begin())

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- life.Close() }()
	go func() { second <- life.Close() }()

	requireStillRunning(t, first, "an Acquire was still running")
	requireStillRunning(t, second,
		"a second Close returning early would let its caller close the "+
			"connection while the first one was still draining")

	life.End()
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.NoError(t, life.Close(), "Close is idempotent once the coordinator is quiet")
	require.ErrorIs(t, life.Begin(), ErrClosed)
}

func TestCloseCutsAnAcquireShortAndSaysWhoDidIt(t *testing.T) {
	t.Parallel()

	life := NewLifetime(lifetimeTimings())
	require.NoError(t, life.Begin())

	caller := context.Background()
	callCtx, cancel := life.Call(caller)
	defer cancel()
	require.NoError(t, callCtx.Err(), "an open coordinator does not cut anything short")

	closed := make(chan error, 1)
	go func() { closed <- life.Close() }()

	select {
	case <-callCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Close never cut the in-flight acquisition short; it would have " +
			"waited out a command nobody was going to read the reply to")
	}
	require.ErrorIs(t, callCtx.Err(), context.Canceled)
	require.ErrorIs(t, life.Interrupted(caller, callCtx.Err()), ErrClosed,
		"the caller's own context is still fine, so it is owed ErrClosed rather "+
			"than a cancellation it never asked for")

	life.End()
	require.NoError(t, <-closed)

	// A caller that cancelled itself keeps its own error: "the client went away"
	// is not "the coordinator shut down".
	gone, cancelCaller := context.WithCancel(context.Background())
	cancelCaller()
	require.NoError(t, life.Interrupted(gone, context.Canceled))
}

func TestAnAcquireIsBoundedByTheBudgetTheLeaseWouldHaveHad(t *testing.T) {
	t.Parallel()

	cfg := lifetimeTimings()
	life := NewLifetime(cfg)

	// The bound is not a policy number: it is the point past which the loop
	// would give the lease up on its first tick without renewing it once.
	budget := cfg.TTL - cfg.SafetyMargin - cfg.RenewInterval
	require.Positive(t, budget, "Config.Validate is what keeps this positive")

	callCtx, cancel := life.Call(context.Background())
	defer cancel()
	deadline, ok := callCtx.Deadline()
	require.True(t, ok,
		"cancellation alone does not stop a blocking socket read: go-redis takes "+
			"its read deadline from the context and does not watch its Done")
	require.WithinDuration(t, time.Now().Add(budget), deadline, 50*time.Millisecond)

	select {
	case <-callCtx.Done():
	case <-time.After(budget + time.Second):
		t.Fatal("an acquisition ran past the budget its own lease would have had")
	}
	require.ErrorIs(t, callCtx.Err(), context.DeadlineExceeded)
	require.ErrorIs(t, life.Interrupted(context.Background(), callCtx.Err()), ErrUnavailable,
		"a backend that did not answer in time is a 503, not a 409 and not the "+
			"caller's own cancellation")
}

// The tests below are about the check HandOut makes *after* the backend has
// said yes. Lifetime.Call sets a deadline, but a client is free to ignore it —
// go-redis does unless the process built it with ContextTimeoutEnabled — so a
// backend can answer "taken" long after the answer stopped being worth
// anything, and a community backend over such a client is exactly the case this
// package cannot let through.
//
// None of them sleeps, dials, or waits for a timeout. A late confirmation is
// expressed as an acquiredAt stamp read further into the past, which is what a
// late reply is: the lock is dated from before the command went out, so its age
// at hand-out time is the whole of what makes it usable or not.

// handOutFixture is one granted lock, ready to be handed out or refused. The
// holder is the real memoryHolder over a real store, so "the lock was given
// back" is asserted against the store rather than against a spy — and because
// memoryHolder.Release refuses a dead context, an empty store also proves the
// return ran on a context of its own rather than on the caller's.
type handOutFixture struct {
	life  *Lifetime
	store *MemoryStore
	key   sessiondir.Key
	fence uint64
	held  *memoryHolder
}

func newHandOutFixture(t *testing.T, cfg Config) *handOutFixture {
	t.Helper()
	require.NoError(t, cfg.Validate())

	life := NewLifetime(cfg)
	store := NewMemoryStore()
	key := storeKey()
	// The steps a coordinator's Acquire takes before it hands out: admitted,
	// then a lock that the backend really did grant.
	require.NoError(t, life.Begin())
	fence, taken := store.acquire(key, "owner-a", cfg.TTL)
	require.True(t, taken)

	return &handOutFixture{
		life:  life,
		store: store,
		key:   key,
		fence: fence,
		held:  &memoryHolder{store: store, key: key, owner: "owner-a", ttl: cfg.TTL},
	}
}

// budget is the age past which a confirmation is worthless: the renewal loop
// first wakes RenewInterval after the hand-out and gives up at SafetyMargin
// before the lock expires, so a reply that took this long leaves it nothing.
func (f *handOutFixture) budget() time.Duration {
	cfg := f.life.Config()
	return cfg.TTL - cfg.SafetyMargin - cfg.RenewInterval
}

func TestALockConfirmedTooLateIsRefusedAndGivenBack(t *testing.T) {
	t.Parallel()

	fixture := newHandOutFixture(t, lifetimeTimings())
	defer fixture.life.End()

	// Exactly the budget into the past, and time only moves one way, so the
	// check cannot fall on the other side of the boundary on a slow machine.
	acquiredAt := time.Now().Add(-fixture.budget())

	lease, err := fixture.life.HandOut(
		context.Background(), fixture.fence, acquiredAt, fixture.held)
	require.ErrorIs(t, err, ErrUnavailable,
		"a lease with no renewal budget left is not a lease: the loop would give "+
			"it up on its first tick, and until then the caller cannot tell it "+
			"from a live one")
	require.NotErrorIs(t, err, ErrSessionBusy,
		"the session was not busy — this caller won the lock and is being refused "+
			"it because the answer arrived too late; the two get different statuses")
	require.NotErrorIs(t, err, ErrClosed)
	require.Nil(t, lease)
	require.ErrorContains(t, err, fixture.budget().String(),
		"an operator has to be able to tell which budget was missed")

	require.Empty(t, fixture.store.locks,
		"a refused acquisition never became a run, so there are no tail writes "+
			"for a TTL to cover and the lock has to go back rather than keep the "+
			"next Worker out for a full TTL")
}

func TestALateSuccessIsNotDeliveredToACallerThatIsAlreadyGone(t *testing.T) {
	t.Parallel()

	fixture := newHandOutFixture(t, lifetimeTimings())
	defer fixture.life.End()

	gone, cancel := context.WithCancel(context.Background())
	cancel()

	// Fresh: what is wrong here is the caller, not the age of the lock. A client
	// that ignores contexts can finish a command whose caller left long ago.
	lease, err := fixture.life.HandOut(gone, fixture.fence, time.Now(), fixture.held)
	require.ErrorIs(t, err, context.Canceled,
		"a caller that went away keeps its own error, unwrapped: 'the client "+
			"went away' is not 'coordination is down'")
	require.NotErrorIs(t, err, ErrUnavailable)
	require.NotErrorIs(t, err, ErrClosed)
	require.Nil(t, lease)

	require.Empty(t, fixture.store.locks,
		"the return runs on a context of its own; inheriting the caller's dead "+
			"one would fail every release on this path and leave the lock standing")
}

func TestAConfirmationInsideTheBudgetStillBecomesALease(t *testing.T) {
	t.Parallel()

	fixture := newHandOutFixture(t, lifetimeTimings())

	// Comfortably inside the budget but plainly not brand new, so this pins the
	// boundary as the budget rather than as "the reply was instant".
	acquiredAt := time.Now().Add(-fixture.budget() / 2)

	lease, err := fixture.life.HandOut(
		context.Background(), fixture.fence, acquiredAt, fixture.held)
	require.NoError(t, err, "a confirmation with budget left is a lease")
	require.NotNil(t, lease)
	require.Equal(t, fixture.fence, lease.Fence())
	require.NotEmpty(t, fixture.store.locks, "a delivered lease keeps its lock")

	select {
	case <-lease.Done():
		require.Fail(t, "a lease handed out inside its budget must start held")
	default:
	}

	require.NoError(t, lease.Release(context.Background()))
	fixture.life.End()
	require.NoError(t, fixture.life.Close())
}

func TestMemoryAcquireDatesTheLockFromBeforeItTookIt(t *testing.T) {
	t.Parallel()

	const (
		blocked   = 300 * time.Millisecond
		tolerance = 50 * time.Millisecond
	)
	// Renewal is kept far enough away that the loop cannot extend the deadline
	// between Acquire returning and the assertion reading it.
	cfg := Config{TTL: 10 * time.Second, RenewInterval: 2 * time.Second, SafetyMargin: time.Second}
	require.NoError(t, cfg.Validate())

	store := NewMemoryStore()
	coordinator, err := NewMemoryCoordinator(store, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	// Hold the store shut so the acquisition is stuck inside the call that takes
	// the lock. Without that, the two candidate stamps — one read before the
	// backend was asked, one read after it answered — are microseconds apart and
	// nothing could tell them apart.
	store.mu.Lock()

	type acquisition struct {
		startedAt time.Time
		waited    time.Duration
		lease     Lease
		err       error
	}
	entered := make(chan time.Time, 1)
	done := make(chan acquisition, 1)
	go func() {
		startedAt := time.Now()
		entered <- startedAt
		lease, err := coordinator.Acquire(context.Background(), storeKey())
		done <- acquisition{startedAt, time.Since(startedAt), lease, err}
	}()

	startedAt := <-entered
	time.Sleep(blocked)
	store.mu.Unlock()

	got := <-done
	require.NoError(t, got.err)
	require.GreaterOrEqual(t, got.waited, blocked,
		"the acquisition has to have actually been stuck inside the store, or "+
			"this test proves nothing about which side of it the stamp was read")

	require.True(t, got.lease.(*managedLease).deadline().Before(startedAt.Add(cfg.TTL+tolerance)),
		"a lock is dated from before the backend was asked for it, never from "+
			"after it answered: this one answered %s late and the lease still "+
			"has to expire on the backend's schedule", got.waited)
}
