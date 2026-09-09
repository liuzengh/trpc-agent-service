package sessionlease

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Holder is one acquired lock as its backend sees it. It exists so that every
// backend shares one renewal loop: the rule that an unconfirmed renewal never
// counts as success is the whole safety argument of this package, and it would
// be worth very little if each backend re-derived it.
//
// Implement this to add a backend. Callers of [Coordinator] never see it.
type Holder interface {
	// Renew extends the lock's expiry by the configured TTL.
	//
	// It reports (true, nil) only when the backend confirmed that this holder
	// still owns the lock and the expiry was extended. It reports (false, nil)
	// only for the definitive answer "you do not own this lock any more".
	// Anything else — a transport failure, a timeout, a reply that cannot be
	// interpreted — is (false, err), which the renewal loop treats as unknown
	// and retries.
	//
	// It must return promptly once ctx is done, without waiting for a reply it
	// is no longer going to use. [Lifetime.Close] waits for the renewal loop to
	// come back before it reports that the coordinator has stopped, so a Renew
	// that ignores cancellation is a Renew that hangs a shutdown.
	Renew(ctx context.Context) (bool, error)

	// Release deletes the lock if and only if this holder still owns it. It
	// must leave a lock another holder has taken over untouched, and it must
	// not delete the fence counter, whose monotonicity outlives any single
	// acquisition.
	Release(ctx context.Context) error
}

// leaseState is the single-winner outcome of a lease.
type leaseState int

const (
	leaseHeld leaseState = iota
	leaseReleased
	leaseLost
)

// LeaseParams is everything a backend hands the shared renewal loop when it has
// just taken a lock.
type LeaseParams struct {
	// RunCtx is the caller's context: when it ends the loop stops renewing and
	// the lock is abandoned to its TTL, deliberately without deleting it.
	RunCtx context.Context

	// CoordCtx is the coordinator's lifetime and behaves the same way, so
	// closing a coordinator never yanks a lock out from under a run that is
	// still winding down.
	CoordCtx context.Context

	// Config is the lease timing. The zero value means the package defaults.
	Config Config

	// Fence is the token the backend's acquisition was given.
	Fence uint64

	// Holder is this acquisition's view of the backend.
	Holder Holder

	// AcquiredAt must be read *before* the command that took the lock was
	// issued, never after its reply arrived. See [managedLease.run]: the whole
	// safety argument is that this process gives up strictly before the backend
	// would let anybody else in, and the backend started counting the TTL when
	// it ran the command, not when this process heard about it.
	//
	// A zero value is not rejected here, because there is nothing safe to
	// substitute for it: it makes the lease expire immediately, which is the
	// fail-safe direction and loud enough to find in a test. A backend that goes
	// through [Lifetime.HandOut], as both of this package's do, never gets that
	// far — the same budget check that catches a late confirmation catches a
	// stamp from 1970 and refuses the acquisition outright.
	AcquiredAt time.Time

	// OnStop is called once, after the renewal loop has returned and therefore
	// after the last call this lease will ever make to its Holder.Renew has
	// come back. [Lifetime] uses it to know when a coordinator has gone quiet.
	// Optional.
	OnStop func()
}

// managedLease is the renewal loop and the state machine shared by all
// backends.
type managedLease struct {
	cfg    Config
	fence  uint64
	holder Holder
	onStop func()

	done       chan struct{}
	doneOnce   sync.Once
	stopped    chan struct{}
	cancelLoop context.CancelFunc
	stopRun    func() bool
	stopCoord  func() bool

	mu    sync.Mutex
	state leaseState
	// expires is when the backend would let another holder in. It is always
	// anchored to the moment before the command that set it was issued.
	expires time.Time
}

// NewLease starts the renewal loop for a lock a backend has just taken and
// returns it as a [Lease].
//
// Implementations of [Coordinator] call this; callers of one do not.
func NewLease(p LeaseParams) Lease {
	cfg := p.Config.WithDefaults()
	lease := &managedLease{
		cfg:     cfg,
		fence:   p.Fence,
		holder:  p.Holder,
		onStop:  p.OnStop,
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
		expires: p.AcquiredAt.Add(cfg.TTL),
	}
	// The loop deliberately does not inherit RunCtx: a renewal must not be
	// bound to a request deadline. It is cancelled from RunCtx and CoordCtx
	// instead, which is a stop signal rather than an inherited deadline.
	loopCtx, cancel := context.WithCancel(context.Background())
	lease.cancelLoop = cancel
	if p.RunCtx != nil {
		lease.stopRun = context.AfterFunc(p.RunCtx, cancel)
	}
	if p.CoordCtx != nil {
		lease.stopCoord = context.AfterFunc(p.CoordCtx, cancel)
	}
	go lease.run(loopCtx)
	return lease
}

func (l *managedLease) Fence() uint64 { return l.fence }

func (l *managedLease) Done() <-chan struct{} { return l.done }

func (l *managedLease) Release(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", tenant.ErrInvalidArgument)
	}
	l.mu.Lock()
	switch l.state {
	case leaseReleased:
		l.mu.Unlock()
		return nil
	case leaseLost:
		l.mu.Unlock()
		return ErrLeaseLost
	}
	l.state = leaseReleased
	l.mu.Unlock()

	// Stop renewing before the delete. A renewal that is already in flight can
	// still land after it, which is harmless: it is owner-matched, and the
	// delete removed the owner.
	l.finish()
	return l.holder.Release(ctx)
}

// lose records that this process can no longer claim the lease. It is a no-op
// once the lease has been released, so a renewal that loses a race with
// Release cannot rewrite the outcome.
func (l *managedLease) lose() {
	l.mu.Lock()
	if l.state == leaseHeld {
		l.state = leaseLost
	}
	l.mu.Unlock()
	l.finish()
}

func (l *managedLease) finish() {
	l.doneOnce.Do(func() {
		if l.stopRun != nil {
			l.stopRun()
		}
		if l.stopCoord != nil {
			l.stopCoord()
		}
		l.cancelLoop()
		close(l.done)
	})
}

// deadline is when the backend would let another holder in, as conservatively
// as this process can know it.
func (l *managedLease) deadline() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expires
}

// extend records a confirmed renewal. sentAt must be the moment *before* the
// renew command was issued.
//
// Anchoring to the reply instead would be the one arithmetic mistake this
// package cannot afford. Redis starts the PEXPIRE clock when the script runs,
// and the reply can come back arbitrarily later — the loop below allows a
// single renewal to take up to RenewInterval, which the defaults set to more
// than twice SafetyMargin. A holder that dated its lock from the reply would
// therefore keep claiming a lock that had already expired, for as long as the
// reply was late, and "give up strictly before another Worker may take over"
// would be false exactly when the network is bad enough for it to matter.
func (l *managedLease) extend(sentAt time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expires = sentAt.Add(l.cfg.TTL)
}

// run renews until the lease ends. It never treats an unconfirmed renewal as a
// success: an answer it cannot interpret is retried until the safety margin
// before the lock would expire, and then the lease is declared lost. That way
// this process stops believing it holds the lock strictly before another Worker
// is allowed to take it.
func (l *managedLease) run(ctx context.Context) {
	defer func() {
		close(l.stopped)
		if l.onStop != nil {
			l.onStop()
		}
	}()

	timer := time.NewTimer(l.cfg.RenewInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// Released, the caller's run ended, or the coordinator closed.
			// Abandon the lock to its TTL rather than deleting it.
			l.lose()
			return
		case <-timer.C:
		}
		// A tick that comes due in the same moment the loop is cancelled leaves
		// the select above with two ready cases, and Go picks between them at
		// random. The window is tiny, but the losing side would renew a lock
		// whose run is already over and push its expiry out by a further TTL.
		// The end of the run is the end of renewal, so re-check it here rather
		// than rely on a select that is allowed to choose either way.
		if ctx.Err() != nil {
			l.lose()
			return
		}

		expires := l.deadline()
		budget := time.Until(expires.Add(-l.cfg.SafetyMargin))
		if budget <= 0 {
			l.lose()
			return
		}
		// Read before the command goes out, so a reply that takes its time can
		// only ever make this process give up early, never late.
		sentAt := time.Now()
		callCtx, cancel := context.WithTimeout(ctx, min(budget, l.cfg.RenewInterval))
		renewed, err := l.holder.Renew(callCtx)
		cancel()

		switch {
		case err == nil && renewed:
			l.extend(sentAt)
			timer.Reset(l.cfg.RenewInterval)
		case err == nil:
			// Definitive: somebody else owns the lock now.
			l.lose()
			return
		default:
			remaining := time.Until(expires.Add(-l.cfg.SafetyMargin))
			if remaining <= 0 {
				l.lose()
				return
			}
			timer.Reset(retryDelay(l.cfg, remaining))
		}
	}
}

// retryDelay spaces out retries of an unconfirmed renewal without ever pushing
// the next attempt past the safety budget.
func retryDelay(cfg Config, remaining time.Duration) time.Duration {
	delay := cfg.RenewInterval / 4
	if delay < time.Millisecond {
		delay = time.Millisecond
	}
	return min(delay, remaining)
}
