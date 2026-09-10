package storage

import (
	"context"
	"testing"
)

func TestMemoryRetryTrackerLifecycleAndIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tracker := NewMemoryRetryTracker()

	if got, err := tracker.ListAttempts(ctx, "support", "session-a", "event-a"); err != nil || got != 0 {
		t.Fatalf("ListAttempts(empty) = %d, %v", got, err)
	}
	for want := 1; want <= 3; want++ {
		got, err := tracker.Increment(ctx, "support", "session-a", "event-a")
		if err != nil || got != want {
			t.Fatalf("Increment() = %d, %v; want %d", got, err, want)
		}
	}
	if got, _ := tracker.Increment(ctx, "support", "session-a", "event-b"); got != 1 {
		t.Fatalf("other event attempt = %d", got)
	}
	if got, _ := tracker.Increment(ctx, "support", "session-b", "event-a"); got != 1 {
		t.Fatalf("other session attempt = %d", got)
	}
	if got, _ := tracker.Increment(ctx, "other", "session-a", "event-a"); got != 1 {
		t.Fatalf("other tenant attempt = %d", got)
	}
	if err := tracker.Clear(ctx, "support", "session-a", "event-a"); err != nil {
		t.Fatal(err)
	}
	if got, _ := tracker.ListAttempts(ctx, "support", "session-a", "event-a"); got != 0 {
		t.Fatalf("attempt after clear = %d", got)
	}
}

func TestNewPostgresRetryTrackerRequiresDatabase(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresRetryTracker(nil); err == nil {
		t.Fatal("NewPostgresRetryTracker(nil) error = nil")
	}
}
