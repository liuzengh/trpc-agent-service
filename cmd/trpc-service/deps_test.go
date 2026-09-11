package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRetryUntilSucceedsAfterTransientFailures covers the boot-order race the
// warm-up exists for: the dependency is not listening yet, then it is.
func TestRetryUntilSucceedsAfterTransientFailures(t *testing.T) {
	attempts := 0
	err := retryUntil(context.Background(), time.Second, time.Millisecond, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryUntil: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

// TestRetryUntilReportsTheLastError keeps the degraded path honest: when the
// dependency never comes up the caller must learn why, and the wait must end.
func TestRetryUntilReportsTheLastError(t *testing.T) {
	attempts := 0
	err := retryUntil(context.Background(), 20*time.Millisecond, time.Millisecond, func() error {
		attempts++
		return errors.New("still down")
	})
	if err == nil {
		t.Fatal("expected the last error to be reported")
	}
	if attempts < 2 {
		t.Errorf("attempts = %d, want repeated retries", attempts)
	}
}

// TestRetryUntilStopsOnContextCancel covers shutdown during the wait: a node
// killed while waiting for its database must not keep polling.
func TestRetryUntilStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := retryUntil(ctx, time.Minute, time.Millisecond, func() error {
		return errors.New("down")
	})
	if err == nil {
		t.Fatal("expected an error when the context is already cancelled")
	}
}
