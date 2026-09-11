package coordination

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLocalCoordinatorSerializesSameSession(t *testing.T) {
	coordinator := NewLocalCoordinator()
	t.Cleanup(func() {
		if err := coordinator.Close(); err != nil {
			t.Fatalf("close coordinator: %v", err)
		}
	})
	key := testKey("same-session")
	first, err := coordinator.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := coordinator.Acquire(waitCtx, key); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire error = %v, want deadline exceeded", err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatalf("release first lease: %v", err)
	}

	second, err := coordinator.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire second lease after release: %v", err)
	}
	if second.FencingToken() <= first.FencingToken() {
		t.Fatalf("fencing token did not increase: first=%d second=%d", first.FencingToken(), second.FencingToken())
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("release second lease: %v", err)
	}
}

func TestLocalCoordinatorAllowsDifferentSessions(t *testing.T) {
	coordinator := NewLocalCoordinator()
	t.Cleanup(func() { _ = coordinator.Close() })

	first, err := coordinator.Acquire(context.Background(), testKey("session-a"))
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	defer func() { _ = first.Release(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	second, err := coordinator.Acquire(ctx, testKey("session-b"))
	if err != nil {
		t.Fatalf("different Session was unexpectedly blocked: %v", err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("release second lease: %v", err)
	}
}

func TestLocalCoordinatorCloseCancelsLease(t *testing.T) {
	coordinator := NewLocalCoordinator()
	lease, err := coordinator.Acquire(context.Background(), testKey("close"))
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatalf("close coordinator: %v", err)
	}

	select {
	case <-lease.Context().Done():
		if !errors.Is(context.Cause(lease.Context()), ErrCoordinatorClosed) {
			t.Fatalf("lease cause = %v", context.Cause(lease.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("lease context was not cancelled")
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("release cancelled lease: %v", err)
	}
	if _, err := coordinator.Acquire(context.Background(), testKey("new")); !errors.Is(err, ErrCoordinatorClosed) {
		t.Fatalf("acquire after close error = %v", err)
	}
}

func TestCoordinatorKeyValidation(t *testing.T) {
	coordinator := NewLocalCoordinator()
	t.Cleanup(func() { _ = coordinator.Close() })
	if _, err := coordinator.Acquire(context.Background(), Key{}); err == nil {
		t.Fatal("expected invalid key error")
	}
}

func TestFencingTokenContext(t *testing.T) {
	ctx := ContextWithFencingToken(context.Background(), 42)
	if token, ok := FencingTokenFromContext(ctx); !ok || token != 42 {
		t.Fatalf("fencing token = %d, ok = %t", token, ok)
	}
	if _, ok := FencingTokenFromContext(context.Background()); ok {
		t.Fatal("unexpected fencing token")
	}
}

func testKey(sessionID string) Key {
	return Key{AppName: "tutorial-app", UserID: "alice", SessionID: sessionID}
}
