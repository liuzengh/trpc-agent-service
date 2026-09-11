package worker

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/health"
)

// TestRetryConsumeReconnectsAfterTransientFailure is the regression for the
// outage this platform actually hit: a 20 second Redis stop made ConsumeInbound
// return, the worker logged "worker stopped" and never consumed another message
// again — while publishing recovered, so every new message was accepted and
// silently never processed (and /healthz still said OK).
func TestRetryConsumeReconnectsAfterTransientFailure(t *testing.T) {
	var calls int32
	consume := func(ctx context.Context) error {
		if atomic.AddInt32(&calls, 1) <= 2 {
			return errors.New("bus: xreadgroup: dial tcp: connect: connection refused")
		}
		// Third call succeeds and behaves like a live consumer until the
		// context is cancelled.
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- retryConsume(ctx, consume) }()

	// Two failures then a third attempt: the loop must reconnect on its own
	// rather than return.
	deadline := time.After(8 * time.Second)
	for atomic.LoadInt32(&calls) < 3 {
		select {
		case <-deadline:
			t.Fatalf("consumer made %d attempt(s), want it to keep reconnecting", atomic.LoadInt32(&calls))
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("retryConsume returned %v on shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retryConsume did not stop when the context was cancelled")
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("attempts = %d, want 3 (reconnect once, then stay attached)", got)
	}
}

// TestRetryConsumeExitsCleanlyOnCancel keeps shutdown honest: a cancelled
// context is not a bus failure and must not be retried.
func TestRetryConsumeExitsCleanlyOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	if err := retryConsume(ctx, func(context.Context) error {
		calls++
		return errors.New("connection refused")
	}); err != nil {
		t.Errorf("retryConsume returned %v, want nil", err)
	}
	if calls != 0 {
		t.Errorf("consume was called %d time(s) with a cancelled context, want 0", calls)
	}
}

// TestRetryConsumeMarksTheNodeDegraded: a dead consumer must not be invisible.
// The health probe is what an orchestrator and an on-call engineer look at, so
// a node that accepts HTTP but consumes nothing has to report degraded.
func TestRetryConsumeMarksTheNodeDegraded(t *testing.T) {
	health.Resolve(consumerSupervisor.Name)
	t.Cleanup(func() { health.Resolve(consumerSupervisor.Name) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = retryConsume(ctx, func(context.Context) error {
			return errors.New("connection refused")
		})
	}()

	// consumeDegradeAfter failures happen within a few hundred ms (1s + 2s of
	// backoff at most), so a bounded wait is enough.
	deadline := time.After(10 * time.Second)
	for !isDegraded() {
		select {
		case <-deadline:
			t.Fatal("repeated consumer failure never marked the node degraded")
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if isDegraded() {
		t.Error("shutdown must clear the degraded flag")
	}
}

// isDegraded reports whether *this* consumer's outage is on the probe. It reads
// the reason rather than the boolean because the probe aggregates independent
// producers (MySQL, the consumer, the IM follower) and a test must not pass
// because some other issue happened to be open.
func isDegraded() bool {
	_, reason := health.Degraded()
	return strings.Contains(reason, consumerSupervisor.Name)
}
