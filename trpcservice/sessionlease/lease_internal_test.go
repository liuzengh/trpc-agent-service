package sessionlease

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These assertions are below the Lease interface because the thing they are
// about — which moment the loop dates a lock from — is deliberately not
// observable through it. Timing it from the outside would make the difference
// between "anchored to the command" and "anchored to the reply" a matter of
// milliseconds and a busy machine; from in here it is exact.

// gatedHolder answers a renewal only when the test lets it, and reports the
// moment each call started. It is how the gap between a backend running a
// command and this process hearing the reply is made exact rather than timed.
type gatedHolder struct {
	started chan time.Time
	reply   chan bool
}

func newGatedHolder() *gatedHolder {
	return &gatedHolder{started: make(chan time.Time, 1), reply: make(chan bool)}
}

func (h *gatedHolder) Renew(ctx context.Context) (bool, error) {
	select {
	case h.started <- time.Now():
	case <-ctx.Done():
		return false, ctx.Err()
	}
	select {
	case renewed := <-h.reply:
		return renewed, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (h *gatedHolder) Release(context.Context) error { return nil }

// anchorTimings leave a renewal a call budget several times longer than the
// reply delay the test injects, so a machine slow enough to blow that budget
// fails the "the renewal was confirmed" assertion rather than quietly passing
// the one that matters.
func anchorTimings() Config {
	return Config{
		TTL:           3 * time.Second,
		RenewInterval: 500 * time.Millisecond,
		SafetyMargin:  300 * time.Millisecond,
	}
}

func TestAConfirmedRenewalIsDatedFromTheCommandAndNotTheReply(t *testing.T) {
	t.Parallel()

	const replyDelay = 150 * time.Millisecond
	cfg := anchorTimings()
	require.NoError(t, cfg.Validate())

	holder := newGatedHolder()
	acquiredAt := time.Now()
	lease := NewLease(LeaseParams{
		RunCtx:     t.Context(),
		CoordCtx:   context.Background(),
		Config:     cfg,
		Fence:      1,
		Holder:     holder,
		AcquiredAt: acquiredAt,
	}).(*managedLease)

	// The renewal goes out, and its reply takes its time coming back. A backend
	// that starts its expiry clock when it runs the command — Redis PEXPIRE does
	// exactly that — will let somebody else in at issued+TTL, not at reply+TTL.
	issued := <-holder.started
	time.Sleep(replyDelay)
	holder.reply <- true

	// The next renewal starting is proof the previous one's result was applied,
	// and it is parked at its own first line, so nothing can rewrite the answer
	// while it is read.
	<-holder.started
	deadline := lease.deadline()

	require.True(t, deadline.After(acquiredAt.Add(cfg.TTL)),
		"the renewal has to have been confirmed for this test to mean anything")
	require.False(t, deadline.After(issued.Add(cfg.TTL)),
		"a holder must date its lock from the moment it asked for the extension, "+
			"not from the moment it heard back: a reply that took %s longer than "+
			"the safety margin would otherwise leave it claiming a lock the "+
			"backend had already given away", replyDelay)
}

func TestALeaseIsDatedFromItsAcquisitionAndNotFromWhenTheLoopStarts(t *testing.T) {
	t.Parallel()

	cfg := anchorTimings()
	// An acquire whose reply came back long after the backend took the lock.
	// The lock is already gone; a loop that dated the lease from its own first
	// instruction would renew happily on top of another Worker's.
	acquiredAt := time.Now().Add(-time.Hour)

	holder := &scriptedLoopHolder{answer: func(int) (bool, error) { return true, nil }}
	lease := NewLease(LeaseParams{
		RunCtx:     t.Context(),
		CoordCtx:   context.Background(),
		Config:     cfg,
		Fence:      1,
		Holder:     holder,
		AcquiredAt: acquiredAt,
	}).(*managedLease)

	require.Equal(t, acquiredAt.Add(cfg.TTL), lease.deadline())

	select {
	case <-lease.Done():
	case <-time.After(cfg.RenewInterval + 2*time.Second):
		t.Fatal("a lease whose lock had already expired when it was handed over " +
			"has to give up, however willing the backend is to renew it")
	}
	require.Zero(t, holder.calls(),
		"there was no budget left to renew inside; asking anyway would extend a "+
			"lock this process no longer owns")
}

func TestTheLoopReportsItHasStoppedOnlyAfterItsLastRenewalReturned(t *testing.T) {
	t.Parallel()

	cfg := anchorTimings()
	holder := &stuckHolder{entered: make(chan struct{}, 1), reply: make(chan struct{})}
	coordCtx, closeCoordinator := context.WithCancel(context.Background())
	stopped := make(chan struct{})

	NewLease(LeaseParams{
		RunCtx:     t.Context(),
		CoordCtx:   coordCtx,
		Config:     cfg,
		Fence:      1,
		Holder:     holder,
		AcquiredAt: time.Now(),
		OnStop:     func() { close(stopped) },
	})

	<-holder.entered
	closeCoordinator()

	// Cancelling the coordinator tells the loop to stop; it does not unsend the
	// command that is already on the wire. Reporting a stopped coordinator here
	// is what would let the process close the connection underneath it.
	select {
	case <-stopped:
		t.Fatal("the loop reported it had stopped while a renewal was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	close(holder.reply)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop never reported it had stopped")
	}
}

// scriptedLoopHolder is the in-package twin of the external suite's scripted
// holder, for tests that need to reach a lease's internals.
type scriptedLoopHolder struct {
	answer  func(attempt int) (bool, error)
	renewed atomic.Int64
}

func (h *scriptedLoopHolder) Renew(context.Context) (bool, error) {
	return h.answer(int(h.renewed.Add(1)))
}

func (h *scriptedLoopHolder) Release(context.Context) error { return nil }

func (h *scriptedLoopHolder) calls() int64 { return h.renewed.Load() }

// stuckHolder models a command that has already been sent: it deliberately
// ignores cancellation, because a reply travelling back over a socket does not
// stop travelling when the caller loses interest.
type stuckHolder struct {
	entered chan struct{}
	reply   chan struct{}
}

func (h *stuckHolder) Renew(context.Context) (bool, error) {
	h.entered <- struct{}{}
	<-h.reply
	return true, nil
}

func (h *stuckHolder) Release(context.Context) error { return nil }
