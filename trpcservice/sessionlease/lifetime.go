package sessionlease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// abandonTimeout bounds the release of a lock whose Acquire lost a race with
// Close. It is also the bound on how long that release can hold Close up, which
// is why it is short: nothing is waiting on this lock except the next Worker.
const abandonTimeout = time.Second

// Lifetime is the Acquire/Close bookkeeping every backend shares.
//
// It exists for the same reason [Holder] does. What [Coordinator.Close]
// promises — that it stops every renewal, refuses every later Acquire, and
// returns only once nothing of this coordinator's is still on its way to the
// backend — is one argument, and the last part of it is the one a backend
// re-deriving it would get subtly wrong: cancelling a context tells a renewal
// to stop, it does not wait for the renewal that is already in flight. A caller
// that closes a coordinator and then closes the connection it borrowed would
// pull the connection out from under that renewal.
//
// A Lifetime is safe for concurrent use. Its zero value is not usable; call
// [NewLifetime].
type Lifetime struct {
	cfg Config

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	// active counts everything this coordinator started and has not finished:
	// each Acquire in progress, and each renewal loop it handed out. Close
	// waits for it to reach zero.
	active sync.WaitGroup
}

// NewLifetime returns the bookkeeping for one coordinator. cfg must already
// have been validated.
func NewLifetime(cfg Config) *Lifetime {
	ctx, cancel := context.WithCancel(context.Background())
	return &Lifetime{cfg: cfg.WithDefaults(), ctx: ctx, cancel: cancel}
}

// Config is the lease timing this coordinator hands to every lease.
func (lt *Lifetime) Config() Config { return lt.cfg }

// Context ends when Close is called. It is what stops the renewal loops.
func (lt *Lifetime) Context() context.Context { return lt.ctx }

// Begin registers one Acquire, or reports [ErrClosed] if the coordinator has
// already been closed. Every Begin that returns nil must be paired with an
// [Lifetime.End], and until it is, Close cannot return.
func (lt *Lifetime) Begin() error {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if lt.closed {
		return ErrClosed
	}
	// Registered while closed is still false and under the same lock Close
	// takes to set it, so no registration can appear after Close has started
	// waiting.
	lt.active.Add(1)
	return nil
}

// End finishes the Acquire a [Lifetime.Begin] started.
func (lt *Lifetime) End() { lt.active.Done() }

// acquireBudget is how long an acquisition may wait for its backend.
//
// It is not a policy choice, it is the point past which the answer is worthless.
// A lock is dated from before the command went out, so a reply that took d
// leaves the renewal loop, which first wakes RenewInterval later, a budget of
// TTL - SafetyMargin - RenewInterval - d. Once that is not positive the loop
// gives the lease up on its first tick without ever renewing it, so a reply
// arriving after this budget cannot produce a lease anybody could use.
//
// [Config.Validate] is what makes it positive: RenewInterval + SafetyMargin is
// required to be shorter than TTL.
func (lt *Lifetime) acquireBudget() time.Duration {
	return lt.cfg.TTL - lt.cfg.SafetyMargin - lt.cfg.RenewInterval
}

// Call returns the context an acquisition's backend call should run under: the
// caller's, cancelled by Close, and bounded by [Lifetime.acquireBudget].
//
// How much of that a backend actually honours is worth stating plainly, because
// it is the limit of what Close can promise. Cancellation stops a call that has
// not reached the wire — one queued for a connection, or sleeping between
// retries — and that is where an acquisition against a backend in trouble
// usually is. A command already sent is a blocking socket read, and whether the
// context reaches it is the client's decision, not this package's: go-redis v9
// passes context.Background() to the read unless the process built the client
// with ContextTimeoutEnabled, in which case the deadline below reaches the
// socket and this is the bound. Without it the bound is the client's own
// ReadTimeout and MaxRetries, and a client that ignores contexts *and* disables
// its read deadline has no bound this package can reach at all.
//
// So this is the tightest bound available from here, not a promise about every
// client. Two things follow, and both are load-bearing. The process that builds
// its own client turns ContextTimeoutEnabled on, which is why its own
// acquisitions and renewals are bounded by these numbers. And because a caller
// may hand in a client that honours none of it, the deadline is not the only
// thing standing between a late reply and a lease: [Lifetime.HandOut] re-checks
// the elapsed time against the same budget after the backend has answered.
func (lt *Lifetime) Call(ctx context.Context) (context.Context, context.CancelFunc) {
	callCtx, cancel := context.WithTimeout(ctx, lt.acquireBudget())
	stop := context.AfterFunc(lt.ctx, cancel)
	return callCtx, func() {
		stop()
		cancel()
	}
}

// Interrupted reports what a failed backend call owes its caller when this
// coordinator, rather than the caller, is what ended it: [ErrClosed] when Close
// cut it short, [ErrUnavailable] when it ran out of the acquire budget.
//
// It reports nil when the caller's own context ended the call. The three look
// identical to the backend and mean different things to the caller: a caller
// whose context is still fine must not be handed back a cancellation it never
// asked for, and a coordination outage is a 503 rather than a 409.
func (lt *Lifetime) Interrupted(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled) && lt.ctx.Err() != nil:
		return ErrClosed
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: backend did not answer within %s",
			ErrUnavailable, lt.acquireBudget())
	}
	return nil
}

// HandOut turns a lock the backend has just granted into a [Lease], and
// registers its renewal loop so Close waits for it.
//
// It is also the last fail-closed check of an acquisition, and the only one
// that can be made after the backend has spoken. [Lifetime.Call] bounds the
// command, but a client is free to ignore the context it is handed — go-redis
// v9 does exactly that unless it was built with ContextTimeoutEnabled — so "the
// backend said yes" and "the backend said yes in time" are different facts, and
// this is where the second one is established rather than assumed. A backend
// implemented outside this package cannot opt out of the check by forgetting
// it, which is the reason it lives here and not in each Acquire.
//
// Three things make a granted lock undeliverable. They are kept apart because
// they mean different things to the caller, and they are reported in this
// order:
//
//   - the coordinator was closed: [ErrClosed];
//   - the caller's own context ended: that context's error, unwrapped, because
//     "the client went away" is not "coordination is down";
//   - the confirmation arrived past [Lifetime.acquireBudget]: [ErrUnavailable],
//     because the renewal loop would give such a lease up on its first tick
//     without ever renewing it, and a caller cannot tell a lease with no safety
//     budget left from a live one until its run is cancelled underneath it.
//
// In all three the lock is given straight back rather than left to expire.
// These are the only places a lock is deleted outside an explicit
// [Lease.Release], and it is not the exception it looks like: none of these
// acquisitions ever became a run, so there are no tail writes for a TTL to
// cover, and leaving the lock would keep the next Worker out of the Session for
// a full TTL for nothing. The return is owner-matched and best effort; see
// [Lifetime.abandon].
//
// acquiredAt must have been read before the acquire command was issued; see
// [LeaseParams].
func (lt *Lifetime) HandOut(
	ctx context.Context,
	fence uint64,
	acquiredAt time.Time,
	holder Holder,
) (Lease, error) {
	lt.mu.Lock()
	if err := lt.undeliverable(ctx, acquiredAt); err != nil {
		lt.mu.Unlock()
		lt.abandon(ctx, holder)
		return nil, err
	}
	// Both the registration and the lease itself happen under the lock, so
	// there is no window in which Close could see a quiet coordinator while a
	// renewal loop was on its way to existing.
	lt.active.Add(1)
	lease := NewLease(LeaseParams{
		RunCtx:     ctx,
		CoordCtx:   lt.ctx,
		Config:     lt.cfg,
		Fence:      fence,
		Holder:     holder,
		AcquiredAt: acquiredAt,
		OnStop:     lt.active.Done,
	})
	lt.mu.Unlock()
	return lease, nil
}

// undeliverable reports why a lock the backend granted cannot become a lease,
// or nil when it can. It must be called with lt.mu held: the closed check has to
// be atomic with the registration that follows it, or a Close could slip
// between the two and return while a renewal loop was still being started.
//
// A nil ctx is tolerated rather than rejected. [ValidateAcquire] has already
// refused one for both backends in this package, and refusing it a second time
// here would turn a lock that was legitimately taken into a lock left behind.
func (lt *Lifetime) undeliverable(ctx context.Context, acquiredAt time.Time) error {
	switch {
	case lt.closed:
		return ErrClosed
	case ctx != nil && ctx.Err() != nil:
		return ctx.Err()
	}
	// The reply's age, not the reply itself. A lock is dated from before the
	// command went out, so a confirmation that took a full acquireBudget leaves
	// the renewal loop nothing: it first wakes RenewInterval later and finds the
	// safety margin already gone. Refusing here is the same decision the loop
	// would make one tick later, made before a caller has been told it may run.
	if late := time.Since(acquiredAt); late >= lt.acquireBudget() {
		return fmt.Errorf(
			"%w: the backend confirmed this lock %s after it was asked for, which "+
				"leaves none of the %s a lease needs to renew itself",
			ErrUnavailable, late.Round(time.Millisecond), lt.acquireBudget())
	}
	return nil
}

// abandon gives back a lock that was granted but cannot be handed out.
//
// The context drops the caller's cancellation deliberately: two of the three
// reasons for being here are that a cancel is already travelling or has already
// arrived, and inheriting it would turn every one of those into a lock left
// standing for a full TTL — the precise outcome this method exists to avoid. It
// takes a short deadline of its own instead, and the caller's Begin
// registration is still outstanding, so Close waits for this rather than racing
// it to the connection.
//
// It cannot disturb anybody else's lock: [Holder.Release] deletes only while
// this holder still owns it, so a return that lands after a takeover is a no-op
// by contract rather than by timing.
func (lt *Lifetime) abandon(ctx context.Context, holder Holder) {
	if ctx == nil {
		// Matching undeliverable: a nil context is not a reason to leave a lock
		// standing, and WithoutCancel would panic on one.
		ctx = context.Background()
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abandonTimeout)
	defer cancel()
	// Best effort by construction: if it fails the lock expires by TTL, which
	// is the outcome this method exists to improve on, not to guarantee.
	_ = holder.Release(releaseCtx)
}

// Close stops every renewal, refuses every later Acquire, and returns only once
// nothing this coordinator started is still running.
//
// It is idempotent, and every caller waits: a second Close returning early
// would let the caller that made it close the connection while the first one
// was still draining.
//
// Everything it waits for is something Close itself has just cancelled: a
// renewal loop through [Lifetime.Context], an acquisition through
// [Lifetime.Call]. The one thing cancelling cannot reach is a command already
// on the wire, and Close waits for that one rather than abandoning it. How long
// it may wait is the client's property, not this package's: on a client that
// honours contexts it is [Lifetime.acquireBudget] for an acquisition and the
// call timeout the renewal loop sets, on a client that ignores them it is that
// client's own read timeout and retry count, and on a client that ignores them
// *and* disables its read deadline it is not bounded from here at all. See
// [Lifetime.Call]. The process that owns this package's client turns
// ContextTimeoutEnabled on so the first of those applies to it; a caller that
// supplies its own client owns that choice and its consequences.
//
// What it does not wait for is a [Lease.Release] a caller is still running:
// that call is the caller's, made on the caller's context, and blocking
// shutdown on it would trade this race for a worse one. A process that releases
// its leases before closing its coordinator — which is the order the run path
// already uses — has nothing left to sequence.
func (lt *Lifetime) Close() error {
	if lt == nil {
		return nil
	}
	lt.mu.Lock()
	first := !lt.closed
	lt.closed = true
	lt.mu.Unlock()
	if first {
		lt.cancel()
	}
	lt.active.Wait()
	return nil
}
