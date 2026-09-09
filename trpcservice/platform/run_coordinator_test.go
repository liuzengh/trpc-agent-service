package platform

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestInMemoryRunCoordinatorSuppressesDuplicateAndPromotesQueue(t *testing.T) {
	c := NewInMemoryRunCoordinator("gateway-a")
	key := RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "request-a"}
	first, disposition, err := c.Claim(context.Background(), key, "hello")
	if err != nil || disposition != Claimed {
		t.Fatalf("first claim = %v, %v", disposition, err)
	}
	if _, disposition, err := c.Claim(context.Background(), key, "hello"); err != nil || disposition != ClaimAlreadyRun {
		t.Fatalf("duplicate claim = %v, %v", disposition, err)
	}
	if _, _, err := c.Claim(context.Background(), key, "different"); !errors.Is(err, ErrIdempotencyKeyReused) {
		t.Fatalf("reused request error = %v", err)
	}
	secondKey := RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "request-b"}
	second, disposition, err := c.Claim(context.Background(), secondKey, "world")
	if err != nil || disposition != ClaimQueued {
		t.Fatalf("queued claim = %v, %v", disposition, err)
	}
	if err := first.Finish(context.Background(), RunTerminal{Type: "completed"}); err != nil {
		t.Fatal(err)
	}
	waiter := second.(interface{ Wait(context.Context) error })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waiter.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if second.FencingToken() <= first.FencingToken() {
		t.Fatalf("fence did not advance: first=%d second=%d", first.FencingToken(), second.FencingToken())
	}
	if err := second.Finish(context.Background(), RunTerminal{Type: "completed"}); err != nil {
		t.Fatal(err)
	}
}

func TestInMemoryRunCoordinatorCancellationIsSharedAndIdempotent(t *testing.T) {
	c := NewInMemoryRunCoordinator("gateway-a")
	key := RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "request-a"}
	permit, _, err := c.Claim(context.Background(), key, "hello")
	if err != nil {
		t.Fatal(err)
	}
	var dispositions []CancelDisposition
	var mu sync.Mutex
	for i := 0; i < 2; i++ {
		go func() {
			disposition, cancelErr := c.RequestCancel(context.Background(), key)
			if cancelErr != nil {
				t.Error(cancelErr)
			}
			mu.Lock()
			dispositions = append(dispositions, disposition)
			mu.Unlock()
		}()
	}
	deadline := time.After(time.Second)
	for {
		select {
		case <-permit.CancelRequested():
			goto cancelled
		case <-deadline:
			t.Fatal("permit did not observe cancellation")
		}
	}
cancelled:
	if err := permit.Finish(context.Background(), RunTerminal{Type: "cancelled"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RequestCancel(context.Background(), key); err != nil {
		t.Fatal(err)
	}
}

func TestInMemoryRunCoordinatorDifferentSessionsOverlap(t *testing.T) {
	c := NewInMemoryRunCoordinator("gateway-a")
	a, _, err := c.Claim(context.Background(), RunKey{TenantID: "tenant-a", SessionID: "a", RequestID: "one"}, "one")
	if err != nil {
		t.Fatal(err)
	}
	b, disposition, err := c.Claim(context.Background(), RunKey{TenantID: "tenant-a", SessionID: "b", RequestID: "two"}, "two")
	if err != nil || disposition != Claimed || b.FencingToken() == 0 {
		t.Fatalf("second session claim = %v, %v", disposition, err)
	}
	_ = a.Finish(context.Background(), RunTerminal{Type: "completed"})
	_ = b.Finish(context.Background(), RunTerminal{Type: "completed"})
}

func TestInMemoryRunCoordinatorQueuedCancellationUnblocksWaiter(t *testing.T) {
	c := NewInMemoryRunCoordinator("gateway-a")
	first, _, err := c.Claim(context.Background(), RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "first"}, "first")
	if err != nil {
		t.Fatal(err)
	}
	second, disposition, err := c.Claim(context.Background(), RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "second"}, "second")
	if err != nil || disposition != ClaimQueued {
		t.Fatalf("second claim = %v, %v", disposition, err)
	}
	if _, err := c.RequestCancel(context.Background(), RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "second"}); err != nil {
		t.Fatal(err)
	}
	waiter := second.(interface{ Wait(context.Context) error })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waiter.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued waiter error = %v", err)
	}
	if err := first.Finish(context.Background(), RunTerminal{Type: "completed"}); err != nil {
		t.Fatal(err)
	}
}

func TestInMemoryRunCoordinatorOwnerExpiryProducesUnknownAndPromotes(t *testing.T) {
	c, err := NewTimedInMemoryRunCoordinator("gateway-a", 30*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := c.Claim(context.Background(), RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "first"}, "first")
	if err != nil {
		t.Fatal(err)
	}
	second, disposition, err := c.Claim(context.Background(), RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "second"}, "second")
	if err != nil || disposition != ClaimQueued {
		t.Fatalf("second claim = %v, %v", disposition, err)
	}
	if err := c.Expire(RunKey{TenantID: "tenant-a", SessionID: "session-a", RequestID: "first"}); err != nil {
		t.Fatal(err)
	}
	waiter := second.(interface{ Wait(context.Context) error })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waiter.Wait(ctx); err != nil {
		t.Fatalf("expired owner did not promote: %v", err)
	}
	if second.FencingToken() <= first.FencingToken() {
		t.Fatalf("fence did not advance: first=%d second=%d", first.FencingToken(), second.FencingToken())
	}
	if err := second.Finish(context.Background(), RunTerminal{Type: "completed"}); err != nil {
		t.Fatal(err)
	}
}
