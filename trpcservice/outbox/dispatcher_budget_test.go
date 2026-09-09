package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/capacity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type fakeBudgetGuard struct {
	err error

	mu        sync.Mutex
	acquired  int
	released  int
	acquiredW chan struct{}
}

func (g *fakeBudgetGuard) AcquireScope(ctx context.Context, tenantID, scope, ownerID string, ttl time.Duration) (func(), error) {
	if g.err != nil {
		return nil, g.err
	}
	g.mu.Lock()
	g.acquired++
	if g.acquiredW != nil {
		close(g.acquiredW)
	}
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		g.released++
		g.mu.Unlock()
	}, nil
}

func (g *fakeBudgetGuard) counts() (acquired, released int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.acquired, g.released
}

// TestDispatcherSenderBudgetExhaustedSkipsWithoutSend proves exhaustion
// skips the item before Send: no send call, no durable transition, and the
// skip is counted. The claim lock expires and the message is redelivered.
func TestDispatcherSenderBudgetExhaustedSkipsWithoutSend(t *testing.T) {
	tc := testTenant("tenant-budget-full")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 4)}
	enqueueTestMessage(t, repository, tc, "outbox-budget-full")
	sender := &scriptedSender{fn: func(context.Context, storage.OutboxMessage) SenderOutcome {
		t.Fatal("sender must not be called when the budget is exhausted")
		return SenderOutcome{Class: Delivered}
	}}
	guard := &fakeBudgetGuard{err: capacity.ErrCapacityFull}
	config := testConfig(tc, "dispatcher-budget-full")
	config.Budget = guard
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()

	// Wait for the skip to be observable, then stop.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if dispatcher.Stats().SkippedBudgetExhausted > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	stopCleanly(t, dispatcher)
	<-runDone

	if got := dispatcher.Stats().SendCalls; got != 0 {
		t.Fatalf("SendCalls=%d, want 0 (no unaccounted send)", got)
	}
	if got := dispatcher.Stats().SkippedBudgetExhausted; got != 1 {
		t.Fatalf("SkippedBudgetExhausted=%d, want 1", got)
	}
	if transitions := repository.snapshot(); len(transitions) != 0 {
		t.Fatalf("durable transitions=%v, want none (item stays claimed for redelivery)", transitions)
	}
}

// TestDispatcherSenderBudgetFailureIsFailClosed proves a budget backend
// failure also skips the send (never an unaccounted send) and surfaces the
// error through LastErrorCategory.
func TestDispatcherSenderBudgetFailureIsFailClosed(t *testing.T) {
	tc := testTenant("tenant-budget-fail")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 4)}
	enqueueTestMessage(t, repository, tc, "outbox-budget-fail")
	sender := &scriptedSender{fn: func(context.Context, storage.OutboxMessage) SenderOutcome {
		t.Fatal("sender must not be called when the budget backend fails")
		return SenderOutcome{Class: Delivered}
	}}
	guard := &fakeBudgetGuard{err: errors.New("budget backend unavailable")}
	config := testConfig(tc, "dispatcher-budget-fail")
	config.Budget = guard
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if dispatcher.Stats().SkippedBudgetExhausted > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Stop surfaces the recorded budget error (fail-closed observability);
	// that is the dispatcher's documented lastErr behavior.
	if stopErr := dispatcher.Stop(context.Background()); stopErr == nil {
		t.Fatalf("Stop error=nil, want the recorded budget backend failure")
	}
	<-runDone

	if got := dispatcher.Stats().SendCalls; got != 0 {
		t.Fatalf("SendCalls=%d, want 0 (fail closed)", got)
	}
	if got := dispatcher.Stats().LastErrorCategory; got != "sender budget acquire" {
		t.Fatalf("LastErrorCategory=%q, want sender budget acquire", got)
	}
}

// TestDispatcherSenderBudgetHeldAcrossSendAndReleased proves the happy
// path: one slot is acquired per send and released after the outcome is
// classified, and the message completes normally.
func TestDispatcherSenderBudgetHeldAcrossSendAndReleased(t *testing.T) {
	tc := testTenant("tenant-budget-ok")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 4)}
	enqueueTestMessage(t, repository, tc, "outbox-budget-ok")
	sender := &scriptedSender{fn: func(context.Context, storage.OutboxMessage) SenderOutcome { return SenderOutcome{Class: Delivered} }}
	guard := &fakeBudgetGuard{}
	config := testConfig(tc, "dispatcher-budget-ok")
	config.Budget = guard
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	waitForTransition(t, repository, "complete")
	stopCleanly(t, dispatcher)
	<-runDone

	acquired, released := guard.counts()
	if acquired != 1 || released != 1 {
		t.Fatalf("acquired=%d released=%d, want 1/1", acquired, released)
	}
	if got := dispatcher.Stats().Completed; got != 1 {
		t.Fatalf("Completed=%d, want 1", got)
	}
}
