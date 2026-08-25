package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	frameworkevent "trpc.group/trpc-go/trpc-agent-go/event"
)

func TestExecutionPumpCompletesBeforeDrainReturns(t *testing.T) {
	source := make(chan *frameworkevent.Event, 2)
	source <- &frameworkevent.Event{Author: "assistant"}
	source <- &frameworkevent.Event{Author: "assistant"}
	close(source)
	exec := newExecution(source, nil)
	result, err := drainExecution(context.Background(), exec, time.Second, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !exec.pumpCompleted.Load() || !exec.frameworkChannelClosed.Load() {
		t.Fatal("framework and pump completion were not observed")
	}
	select {
	case <-exec.done:
	default:
		t.Fatal("pump done was not closed before drain returned")
	}
	if len(result.Events) != 2 || result.Events[0].Sequence != 1 || result.Events[1].Sequence != 2 {
		t.Fatalf("unexpected events: %+v", result.Events)
	}
}

func TestExecutionKeepsFrameworkRunErrorChain(t *testing.T) {
	sentinel := errors.New("framework run sentinel")
	source := make(chan *frameworkevent.Event, 1)
	source <- &frameworkevent.Event{Author: "assistant"}
	close(source)
	exec := newExecution(source, sentinel)
	result, err := drainExecution(context.Background(), exec, time.Second, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 1 {
		t.Fatalf("expected event to drain, got %+v", result.Events)
	}
	frameworkErr := classifyFrameworkError(exec.frameworkError())
	if !errors.Is(frameworkErr, sentinel) || !errors.Is(frameworkErr, ErrFrameworkFailure) {
		t.Fatalf("framework error chain was lost: %v", frameworkErr)
	}
}

func TestExecutionReportsIncompleteProducer(t *testing.T) {
	source := make(chan *frameworkevent.Event)
	exec := newExecution(source, nil)
	_, err := drainExecution(context.Background(), exec, 20*time.Millisecond, 10*time.Millisecond, nil)
	if !errors.Is(err, ErrDrainTimeout) || !errors.Is(err, ErrProducerIncomplete) {
		t.Fatalf("expected bounded incomplete producer error, got %v", err)
	}
	if !exec.pumpCompleted.Load() {
		t.Fatal("adapter pump did not complete after cancellation")
	}
	if exec.frameworkChannelClosed.Load() {
		t.Fatal("canceled pump incorrectly marked framework channel closed")
	}
	close(source)
	select {
	case <-exec.done:
	case <-time.After(time.Second):
		t.Fatal("test producer did not converge after source close")
	}
}

func TestExecutionDrainTimeoutPreservesFrameworkError(t *testing.T) {
	sentinel := errors.New("framework timeout sentinel")
	source := make(chan *frameworkevent.Event)
	exec := newExecution(source, sentinel)
	_, err := drainExecution(context.Background(), exec, 20*time.Millisecond, 10*time.Millisecond, nil)
	if !errors.Is(err, sentinel) || !errors.Is(err, ErrFrameworkFailure) || !errors.Is(err, ErrDrainTimeout) || !errors.Is(err, ErrProducerIncomplete) {
		t.Fatalf("framework error chain was lost on drain timeout: %v", err)
	}
	if !exec.waitPump(time.Second) {
		t.Fatal("adapter pump did not stop after drain timeout")
	}
	close(source)
}

func TestExecutionBackpressureCancellationIsBounded(t *testing.T) {
	source := make(chan *frameworkevent.Event, 64)
	for i := 0; i < 32; i++ {
		source <- &frameworkevent.Event{Author: "assistant"}
	}
	exec := newExecution(source, nil)
	started := time.Now()
	_, err := drainExecution(context.Background(), exec, 20*time.Millisecond, 10*time.Millisecond, nil)
	if !errors.Is(err, ErrDrainTimeout) || !errors.Is(err, ErrProducerIncomplete) {
		t.Fatalf("expected bounded backpressure termination, got %v", err)
	}
	if time.Since(started) > time.Second || !exec.waitPump(time.Second) {
		t.Fatal("adapter pump did not terminate within the cleanup bound")
	}
	if exec.frameworkChannelClosed.Load() {
		t.Fatal("timeout incorrectly marked framework channel closed")
	}
	close(source)
}

func TestInvokeToolPreservesFailureCause(t *testing.T) {
	sentinel := errors.New("tool callback sentinel")
	_, events, err := invokeTool(context.Background(), ToolInvokerFunc(func(context.Context, ToolRequest) (ToolResult, error) {
		return ToolResult{}, sentinel
	}), tenantContext{}, validSpec(), "lookup", nil)
	if !errors.Is(err, ErrToolFailure) || !errors.Is(err, sentinel) {
		t.Fatalf("tool error chain was lost: %v", err)
	}
	if len(events) != 2 || events[0].Type != "tool.started" || events[1].Type != "tool.failed" {
		t.Fatalf("unexpected tool failure events: %+v", events)
	}
}

func TestInvokeToolPreservesDeadlineCause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, events, err := invokeTool(ctx, ToolInvokerFunc(func(ctx context.Context, _ ToolRequest) (ToolResult, error) {
		return ToolResult{}, ctx.Err()
	}), tenantContext{}, validSpec(), "lookup", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation cause was lost: %v", err)
	}
	if len(events) != 2 || events[1].ErrorType != "canceled" {
		t.Fatalf("unexpected canceled tool events: %+v", events)
	}
}

func TestDrainStopTimeoutIsBounded(t *testing.T) {
	source := make(chan *frameworkevent.Event)
	release := make(chan struct{})
	exec := newExecution(source, nil)
	started := time.Now()
	_, err := drainExecution(cancelledContext(), exec, 30*time.Millisecond, 10*time.Millisecond, func(context.Context) error {
		<-release
		return nil
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("expected cancellation and drain timeout, got %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("stop timeout was not bounded")
	}
	close(release)
	close(source)
	select {
	case <-exec.done:
	case <-time.After(time.Second):
		t.Fatal("blocked stop test producer did not converge")
	}
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
