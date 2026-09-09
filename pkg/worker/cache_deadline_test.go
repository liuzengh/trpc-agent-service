package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCacheCloseDeadlineBoundsIdleProcessorCleanup(t *testing.T) {
	cache := NewCache(CacheOptions{MaxEntries: 1, IdleTTL: time.Hour})
	started := make(chan struct{})
	unblock := make(chan struct{})
	_, release, err := cache.Acquire(context.Background(), testCacheKey("idle-close-deadline"), func(context.Context) (Processor, error) {
		return &blockingProcessor{started: started, release: unblock, closeErr: errors.New("provider secret")}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cache.Close(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(unblock)
		t.Fatal("idle processor cleanup did not start")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			close(unblock)
			t.Fatalf("Close error = %v, want deadline exceeded", err)
		}
	case <-time.After(200 * time.Millisecond):
		close(unblock)
		<-done
		t.Fatal("Close ignored its deadline while idle processor cleanup was blocked")
	}
	close(unblock)
	finalCtx, finalCancel := context.WithTimeout(context.Background(), time.Second)
	defer finalCancel()
	if err := cache.Close(finalCtx); !errors.Is(err, ErrProcessorCloseFailed) {
		t.Fatalf("Close after cleanup error = %v, want retained sanitized cleanup error", err)
	}
}
