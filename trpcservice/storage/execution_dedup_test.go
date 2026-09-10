package storage

import (
	"context"
	"testing"
	"time"
)

func TestMemoryExecutionDedupLifecycle(t *testing.T) {
	t.Parallel()

	store := NewMemoryExecutionDedupStore()
	ctx := context.Background()
	begin := func(messageID, traceID string) BeginResult {
		t.Helper()
		state, err := store.Begin(ctx, "tenant-a", "telegram", "bot-a", messageID, traceID, time.Minute)
		if err != nil {
			t.Fatalf("Begin(%q) error = %v", messageID, err)
		}
		return state
	}

	if state := begin("m-1", "trace-1"); state != ExecutionFresh {
		t.Fatalf("first Begin state = %q, want fresh", state)
	}
	if state := begin("m-1", "trace-2"); state != ExecutionInProgress {
		t.Fatalf("live duplicate Begin state = %q, want in_progress", state)
	}

	store.MarkCompleted("tenant-a", "telegram", "bot-a", "m-1")
	if state := begin("m-1", "trace-3"); state != ExecutionCompleted {
		t.Fatalf("completed duplicate Begin state = %q, want completed", state)
	}

	if state := begin("m-2", "trace-4"); state != ExecutionFresh {
		t.Fatalf("second message Begin state = %q, want fresh", state)
	}
	if err := store.Abort(ctx, "tenant-a", "telegram", "bot-a", "m-2", "trace-4"); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if state := begin("m-2", "trace-5"); state != ExecutionFresh {
		t.Fatalf("Begin after abort state = %q, want fresh", state)
	}

	// The trace fence must prevent an old owner from deleting a takeover.
	if state := begin("m-3", "trace-6"); state != ExecutionFresh {
		t.Fatalf("third message Begin state = %q, want fresh", state)
	}
	if err := store.Abort(ctx, "tenant-a", "telegram", "bot-a", "m-3", "wrong-trace"); err != nil {
		t.Fatalf("Abort(wrong trace) error = %v", err)
	}
	if state := begin("m-3", "trace-7"); state != ExecutionInProgress {
		t.Fatalf("Begin after wrong-trace abort state = %q, want in_progress", state)
	}
}

func TestMemoryExecutionDedupTakeoverAfterWindow(t *testing.T) {
	t.Parallel()

	store := NewMemoryExecutionDedupStore()
	ctx := context.Background()
	clock := time.Now()
	store.now = func() time.Time { return clock }

	if state, err := store.Begin(ctx, "tenant-a", "telegram", "bot-a", "m-1", "trace-1", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("first Begin state = %q, error = %v, want fresh", state, err)
	}
	if state, err := store.Begin(ctx, "tenant-a", "telegram", "bot-a", "m-1", "trace-2", time.Minute); err != nil || state != ExecutionInProgress {
		t.Fatalf("live duplicate Begin state = %q, error = %v, want in_progress", state, err)
	}
	// The staleness window expires, so a second node may take over.
	clock = clock.Add(2 * time.Minute)
	if state, err := store.Begin(ctx, "tenant-a", "telegram", "bot-a", "m-1", "trace-2", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("takeover Begin state = %q, error = %v, want fresh", state, err)
	}
}

func TestMemoryExecutionDedupValidation(t *testing.T) {
	t.Parallel()

	store := NewMemoryExecutionDedupStore()
	ctx := context.Background()

	if _, err := store.Begin(ctx, "", "telegram", "bot-a", "m-1", "trace-1", time.Minute); err == nil {
		t.Fatal("Begin() with empty tenant error = nil, want failure")
	}
	if _, err := store.Begin(ctx, "tenant-a", "telegram", "bot-a", "m-1", "trace-1", 0); err == nil {
		t.Fatal("Begin() with zero window error = nil, want failure")
	}
}
