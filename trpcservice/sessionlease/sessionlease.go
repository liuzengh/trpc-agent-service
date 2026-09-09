// Package sessionlease coordinates which Worker is currently allowed to run a
// Session.
//
// # What this package is
//
// A lease is scoped to a full [sessiondir.Key] — tenant, app, principal,
// session and epoch. While one Worker holds the lease for a key, an Acquire for
// the same key from any other Worker fails with [ErrSessionBusy]. The holder
// keeps the lease alive by renewing it; if the holder stops renewing (crash,
// pause, partition, process shutdown) the lease expires after its TTL and
// another Worker may take over.
//
// # What this package is not
//
// This is a *cooperative* lease. Every participant that respects it stays out
// of a Session another Worker is running, but nothing in the storage layer
// enforces that. [Lease.Fence] returns a monotonically increasing token per
// key, and it exists only so that observation and future work have a stable
// handle: it does **not** participate in Session write admission today, and
// calling it "fencing" in the enforcement sense would be wrong. The upstream
// session.Service interface this service builds on has no fence or CAS
// parameter on AppendEvent, and the Postgres/Redis AppendEventHook is not
// atomic with the write it precedes, so a writer that has lost its lease cannot
// be rejected at the storage layer. Concretely, the following writes are still
// possible and are not prevented here:
//
//   - a paused or partitioned holder that resumes inside its TTL still writes;
//   - an upstream Runner whose context was cancelled still emits terminal
//     events for roughly a second afterwards, deliberately, through
//     context.WithoutCancel.
//
// Cancellation is therefore best effort and eventual: losing a lease closes
// [Lease.Done], the caller cancels its run, and the tail of that run is covered
// by leaving the lock to expire by TTL rather than deleting it eagerly.
//
// # Backends
//
// [MemoryCoordinator] is the reference implementation and the local default. It
// coordinates one process only. The Redis backend in the redis subpackage is
// the multi-Worker implementation. Both are held to the same contract by the
// shared suite in the sessionleasetest subpackage.
//
// The process configuration checks the one direction it can: it refuses Redis
// coordination over in-process sessions, because a shared lock over unshared
// state protects nothing. It does not refuse in-process coordination over a
// shared Session store, because that is a legitimate single-Worker deployment
// on a persistent store. Nothing can tell the two apart from inside one
// process, so "only one Worker writes to this store" stays an operator's
// promise rather than a validated configuration.
package sessionlease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Stable errors every backend reports for the same situations. Callers match
// them with errors.Is; the HTTP layer maps ErrSessionBusy to 409 and everything
// else to 503.
var (
	// ErrSessionBusy reports that another holder currently owns the lease for
	// this key. It is the only "the system is working, come back later" error
	// in this package.
	ErrSessionBusy = errors.New("sessionlease: session is busy")

	// ErrUnavailable reports that the coordination backend could not be
	// reached, or answered something this package refuses to interpret. Acquire
	// fails closed: the caller must not run.
	ErrUnavailable = errors.New("sessionlease: coordination backend unavailable")

	// ErrLeaseLost reports that a lease this process held is no longer held by
	// it. It is delivered through [Lease.Done] and returned by a late
	// [Lease.Release].
	ErrLeaseLost = errors.New("sessionlease: lease lost")

	// ErrClosed reports that the coordinator has been closed.
	ErrClosed = errors.New("sessionlease: coordinator closed")

	// ErrInvalidConfig reports a configuration that cannot be used.
	ErrInvalidConfig = errors.New("sessionlease: invalid configuration")
)

// Coordinator hands out run leases. Implementations are safe for concurrent
// use.
type Coordinator interface {
	// Acquire takes the lease for key, or fails.
	//
	// The returned lease is renewed in the background until it is released,
	// until ctx is done, or until the coordinator is closed. None of those
	// three delete the lock eagerly except an explicit [Lease.Release]; ctx
	// ending or the coordinator closing leaves the lock to expire by TTL, which
	// is what covers a cancelled Runner's tail writes.
	//
	// Acquire fails closed. A backend answer that cannot be interpreted is
	// reported as [ErrUnavailable] and the caller must not run, even though a
	// lock may have been left behind for the TTL to clean up. Context
	// cancellation is reported as the context's own error, unwrapped, so
	// callers can tell "the client went away" from "coordination is down".
	Acquire(ctx context.Context, key sessiondir.Key) (Lease, error)

	// Close stops renewing every lease this coordinator handed out and refuses
	// further Acquire calls with [ErrClosed]. It does not delete the locks it
	// holds: they expire by TTL. Close is idempotent, and every caller of it
	// waits.
	//
	// It returns only once nothing this coordinator started is still running:
	// no renewal, and no acquisition, is on its way to the backend or on its
	// way back. That is what makes it safe to close a connection the
	// coordinator was borrowing as soon as Close returns, and it is the reason
	// the order is close the coordinator, then close the client.
	//
	// The one exception is an acquisition that was admitted and then finished
	// after Close: it is refused with [ErrClosed] and its lock is given back
	// rather than left to the TTL, because no run ever started under it and
	// there are no tail writes to cover.
	//
	// Close does not wait for a [Lease.Release] a caller is still running. That
	// call is the caller's, on the caller's context; release before closing.
	Close() error
}

// Lease is a held run lease. Implementations are safe for concurrent use.
type Lease interface {
	// Fence returns the monotonically increasing token this acquisition was
	// given for its key.
	//
	// It is an observation handle, not an admission control token. Nothing in
	// the Session write path consults it, so a stale writer is not rejected. Do
	// not describe it as enforcement fencing, and do not expose it to clients.
	Fence() uint64

	// Done is closed as soon as this process can no longer claim to hold the
	// lease: it was released, renewal could not be confirmed within the safety
	// budget, another holder took over, the acquiring context ended, or the
	// coordinator closed. Callers watch it and cancel the run it protects.
	//
	// A closed Done does not mean the previous holder has stopped writing. See
	// the package documentation.
	Done() <-chan struct{}

	// Release gives the lease back and deletes the lock, but only while this
	// holder still owns it: a release that arrives after another holder took
	// over must not disturb the new owner.
	//
	// Release always stops renewal, even when the delete fails. Call it only on
	// a clean finish, with an independent short-timeout context; after a client
	// disconnect, a lost lease or a shutdown cancel, leave the lock to the TTL
	// instead so the cancelled Runner's tail writes stay inside the window this
	// process still owns.
	//
	// Releasing twice is a no-op that returns nil. Releasing a lease that was
	// already lost returns [ErrLeaseLost] and deletes nothing.
	Release(ctx context.Context) error
}

// Default lease timings. A holder renews every DefaultRenewInterval; if a
// renewal cannot be confirmed it retries until DefaultSafetyMargin before the
// lock would expire and then declares the lease lost, so the holder gives up
// strictly before another Worker can take over.
const (
	DefaultTTL           = 15 * time.Second
	DefaultRenewInterval = 5 * time.Second
	DefaultSafetyMargin  = 2 * time.Second
)

// Config is the lease timing shared by every backend. The zero value means the
// defaults; tests inject much shorter timings.
type Config struct {
	// TTL is how long a lock survives without a renewal.
	TTL time.Duration
	// RenewInterval is how often a holder renews.
	RenewInterval time.Duration
	// SafetyMargin is how much of the TTL a holder refuses to use up while
	// retrying an unconfirmed renewal.
	SafetyMargin time.Duration
}

// WithDefaults fills unset fields. A non-positive field means "unset".
func (c Config) WithDefaults() Config {
	if c.TTL <= 0 {
		c.TTL = DefaultTTL
	}
	if c.RenewInterval <= 0 {
		c.RenewInterval = DefaultRenewInterval
	}
	if c.SafetyMargin <= 0 {
		c.SafetyMargin = DefaultSafetyMargin
	}
	return c
}

// Validate applies the defaults and reports whether the result can be used.
func (c Config) Validate() error {
	c = c.WithDefaults()
	if c.RenewInterval >= c.TTL {
		return fmt.Errorf("%w: renew interval %s must be shorter than ttl %s",
			ErrInvalidConfig, c.RenewInterval, c.TTL)
	}
	if c.SafetyMargin >= c.TTL {
		return fmt.Errorf("%w: safety margin %s must be shorter than ttl %s",
			ErrInvalidConfig, c.SafetyMargin, c.TTL)
	}
	// A renewal that fails must leave room for at least one retry before the
	// holder has to give up, otherwise the safety budget is decorative.
	if c.RenewInterval+c.SafetyMargin >= c.TTL {
		return fmt.Errorf("%w: renew interval %s plus safety margin %s must be shorter than ttl %s",
			ErrInvalidConfig, c.RenewInterval, c.SafetyMargin, c.TTL)
	}
	return nil
}

// ValidateAcquire runs the argument checks every backend owes its callers
// before it touches a network: a malformed key must be rejected the same way by
// every implementation, and it must never become a lookup.
//
// Implementations of [Coordinator] call this; callers of one do not.
func ValidateAcquire(ctx context.Context, key sessiondir.Key) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", tenant.ErrInvalidArgument)
	}
	if err := key.Validate(); err != nil {
		return err
	}
	return ctx.Err()
}
