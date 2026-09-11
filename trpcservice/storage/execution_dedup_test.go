package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryExecutionDedupLifecycle(t *testing.T) {
	t.Parallel()

	store := NewMemoryExecutionDedupStore()
	ctx := context.Background()
	begin := func(messageID, traceID string) BeginResult {
		t.Helper()
		state, err := store.Begin(ctx, "tenant-a", "support", "telegram", "bot-a", messageID, traceID, time.Minute)
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
	if err := store.Fail(ctx, "tenant-a", "telegram", "bot-a", "m-2", "trace-4"); err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	failed := store.claims[memoryClaimKey("tenant-a", "telegram", "bot-a", "m-2")]
	if failed.status != "failed" || failed.traceID != "trace-4" {
		t.Fatalf("failed claim = %+v", failed)
	}
	if state := begin("m-2", "trace-5"); state != ExecutionFresh {
		t.Fatalf("Begin after failure state = %q, want fresh retry", state)
	}
	if err := store.Abort(ctx, "tenant-a", "telegram", "bot-a", "m-2", "trace-5"); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if state := begin("m-2", "trace-6"); state != ExecutionFresh {
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

	if state, err := store.Begin(ctx, "tenant-a", "support", "telegram", "bot-a", "m-1", "trace-1", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("first Begin state = %q, error = %v, want fresh", state, err)
	}
	if state, err := store.Begin(ctx, "tenant-a", "support", "telegram", "bot-a", "m-1", "trace-2", time.Minute); err != nil || state != ExecutionInProgress {
		t.Fatalf("live duplicate Begin state = %q, error = %v, want in_progress", state, err)
	}
	// The staleness window expires, so a second node may take over.
	clock = clock.Add(2 * time.Minute)
	if state, err := store.Begin(ctx, "tenant-a", "support", "telegram", "bot-a", "m-1", "trace-2", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("takeover Begin state = %q, error = %v, want fresh", state, err)
	}
}

func TestMemoryExecutionDedupValidation(t *testing.T) {
	t.Parallel()

	store := NewMemoryExecutionDedupStore()
	ctx := context.Background()

	if _, err := store.Begin(ctx, "", "support", "telegram", "bot-a", "m-1", "trace-1", time.Minute); err == nil {
		t.Fatal("Begin() with empty tenant error = nil, want failure")
	}
	if _, err := store.Begin(ctx, "tenant-a", "", "telegram", "bot-a", "m-1", "trace-1", time.Minute); err == nil {
		t.Fatal("Begin() with empty app code error = nil, want failure")
	}
	if _, err := store.Begin(ctx, "tenant-a", "support", "telegram", "bot-a", "m-1", "trace-1", 0); err == nil {
		t.Fatal("Begin() with zero window error = nil, want failure")
	}
}

func TestMemoryExecutionDedupValidatesAllIdentityFields(t *testing.T) {
	t.Parallel()
	store := NewMemoryExecutionDedupStore()
	base := []string{"tenant-a", "telegram", "support-bot", "message-1", "trace-1"}
	for index, name := range []string{"tenant", "channel", "binding", "message", "trace"} {
		t.Run(name, func(t *testing.T) {
			args := append([]string(nil), base...)
			args[index] = " "
			if _, err := store.Begin(context.Background(), args[0], "support", args[1], args[2], args[3], args[4], time.Minute); err == nil {
				t.Fatal("Begin() error = nil")
			}
			if err := store.Renew(context.Background(), args[0], args[1], args[2], args[3], args[4]); err == nil {
				t.Fatal("Renew() error = nil")
			}
			if err := store.Fail(context.Background(), args[0], args[1], args[2], args[3], args[4]); err == nil {
				t.Fatal("Fail() error = nil")
			}
		})
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Begin(cancelled, base[0], "support", base[1], base[2], base[3], base[4], time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Begin() error = %v", err)
	}
	if err := store.Renew(cancelled, base[0], base[1], base[2], base[3], base[4]); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Renew() error = %v", err)
	}
}

func TestMemoryExecutionDedupRenewPreservesOwnershipAndFreshness(t *testing.T) {
	t.Parallel()
	store := NewMemoryExecutionDedupStore()
	clock := time.Date(2026, time.September, 11, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	ctx := context.Background()
	if state, err := store.Begin(ctx, "tenant-a", "support", "telegram", "support-bot", "message-1", "trace-a", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("Begin() = %q, %v", state, err)
	}
	clock = clock.Add(45 * time.Second)
	if err := store.Renew(ctx, "tenant-a", "telegram", "support-bot", "message-1", "trace-a"); err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	clock = clock.Add(30 * time.Second)
	if state, err := store.Begin(ctx, "tenant-a", "support", "telegram", "support-bot", "message-1", "trace-b", time.Minute); err != nil || state != ExecutionInProgress {
		t.Fatalf("Begin() after renewal = %q, %v; want in_progress", state, err)
	}
	if err := store.Renew(ctx, "tenant-a", "telegram", "support-bot", "message-1", "trace-b"); err == nil {
		t.Fatal("foreign trace renewed claim")
	}
	store.MarkCompleted("tenant-a", "telegram", "support-bot", "message-1")
	if err := store.Renew(ctx, "tenant-a", "telegram", "support-bot", "message-1", "trace-a"); err == nil {
		t.Fatal("completed claim was renewed")
	}
	if err := store.Abort(ctx, "tenant-a", "telegram", "support-bot", "missing", "trace-a"); err != nil {
		t.Fatalf("Abort(missing) error = %v", err)
	}
}
