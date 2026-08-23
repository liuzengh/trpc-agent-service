package queue

import (
	"context"
	"testing"
	"time"
)

func TestMemoryLockerSerializesAndFences(t *testing.T) {
	locker := NewMemoryLocker()
	first, err := locker.Acquire(context.Background(), "session", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := locker.Acquire(ctx, "session", time.Second); err == nil {
		t.Fatal("second acquisition should time out")
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := locker.Acquire(context.Background(), "session", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release(context.Background())
	if second.Fence() <= first.Fence() {
		t.Fatalf("fencing token did not increase: first=%d second=%d", first.Fence(), second.Fence())
	}
}
