package worker

import (
	"context"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestModelTimerAccumulatesAcrossCalls(t *testing.T) {
	timer := newModelTimer()
	if timer.Name() != "platform-model-timer" {
		t.Errorf("unexpected plugin name %q", timer.Name())
	}
	for i := 0; i < 3; i++ {
		if _, err := timer.beforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
			t.Fatalf("beforeModel: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
		if _, err := timer.afterModel(context.Background(), &model.AfterModelArgs{}); err != nil {
			t.Fatalf("afterModel: %v", err)
		}
	}
	if d := timer.Duration(); d < 3*time.Millisecond {
		t.Errorf("expected >=3ms accumulated model latency, got %v", d)
	}
}

func TestModelTimerUnbracketedCall(t *testing.T) {
	// An afterModel without a matching beforeModel must not panic or corrupt
	// the total (guards against a framework skip path).
	timer := newModelTimer()
	if _, err := timer.afterModel(context.Background(), &model.AfterModelArgs{}); err != nil {
		t.Fatalf("afterModel alone: %v", err)
	}
	if d := timer.Duration(); d != 0 {
		t.Errorf("expected zero duration for unbracketed call, got %v", d)
	}
}

func TestModelTimerOverlappingCalls(t *testing.T) {
	// Two overlapping brackets must measure the union of the windows (first
	// beforeModel to last afterModel), not reset on the inner call and
	// under-count. This is the parallel/cycle-agent path.
	timer := newModelTimer()
	if _, err := timer.beforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("beforeModel 1: %v", err)
	}
	if _, err := timer.beforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("beforeModel 2: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	if _, err := timer.afterModel(context.Background(), &model.AfterModelArgs{}); err != nil {
		t.Fatalf("afterModel 1: %v", err)
	}
	if _, err := timer.afterModel(context.Background(), &model.AfterModelArgs{}); err != nil {
		t.Fatalf("afterModel 2: %v", err)
	}
	if d := timer.Duration(); d < 3*time.Millisecond {
		t.Errorf("overlapping window undercounted: got %v, want >=3ms", d)
	}
}
