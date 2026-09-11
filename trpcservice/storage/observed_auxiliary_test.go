package storage

import (
	"context"
	"testing"
	"time"
)

type claimListingDedupStore struct {
	*MemoryExecutionDedupStore
}

func (*claimListingDedupStore) ListClaims(context.Context, string, string, int) ([]Claim, error) {
	return []Claim{{TraceID: "trace-1"}}, nil
}

func TestObservedExecutionDedupStoreLifecycle(t *testing.T) {
	observer := &recordingStoreObserver{}
	store, err := NewObservedExecutionDedupStore(&claimListingDedupStore{
		MemoryExecutionDedupStore: NewMemoryExecutionDedupStore(),
	}, observer, "memory")
	if err != nil {
		t.Fatalf("NewObservedExecutionDedupStore() error = %v", err)
	}
	ctx := context.Background()
	result, err := store.Begin(ctx, "tenant-a", "support", "web", "console", "message-1", "trace-1", time.Minute)
	if err != nil || result != ExecutionFresh {
		t.Fatalf("Begin() = %q, %v", result, err)
	}
	if err := store.Renew(ctx, "tenant-a", "web", "console", "message-1", "trace-1"); err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	claims, err := store.ListClaims(ctx, "tenant-a", "", 10)
	if err != nil || len(claims) != 1 || claims[0].TraceID != "trace-1" {
		t.Fatalf("ListClaims() = %#v, %v", claims, err)
	}
	if err := store.Abort(ctx, "tenant-a", "web", "console", "message-1", "trace-1"); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	assertObservedOperations(t, observer, "claim_execution", "renew_execution_claim", "list_execution_claims", "abort_execution_claim")
}

func TestObservedRetryTrackerLifecycle(t *testing.T) {
	observer := &recordingStoreObserver{}
	store, err := NewObservedRetryTracker(NewMemoryRetryTracker(), observer, "memory")
	if err != nil {
		t.Fatalf("NewObservedRetryTracker() error = %v", err)
	}
	ctx := context.Background()
	if attempt, err := store.Increment(ctx, "tenant-a", "session-1", "event-1"); err != nil || attempt != 1 {
		t.Fatalf("Increment() = %d, %v", attempt, err)
	}
	if attempt, err := store.ListAttempts(ctx, "tenant-a", "session-1", "event-1"); err != nil || attempt != 1 {
		t.Fatalf("ListAttempts() = %d, %v", attempt, err)
	}
	if err := store.Clear(ctx, "tenant-a", "session-1", "event-1"); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}
	assertObservedOperations(t, observer, "increment_retry", "list_retry_attempts", "clear_retry")
}

func TestObservedIdempotencyStoreLifecycle(t *testing.T) {
	observer := &recordingStoreObserver{}
	store, err := NewObservedIdempotencyStore(NewMemoryIdempotencyStore(), observer, "memory")
	if err != nil {
		t.Fatalf("NewObservedIdempotencyStore() error = %v", err)
	}
	ctx := context.Background()
	acquired, err := store.Acquire(ctx, "key-1", time.Minute)
	if err != nil || acquired.State != LeaseAcquired {
		t.Fatalf("Acquire() = %#v, %v", acquired, err)
	}
	if err := store.Renew(ctx, acquired.Lease, time.Minute); err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if err := store.Complete(ctx, acquired.Lease, time.Minute); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	repeated, err := store.Acquire(ctx, "key-1", time.Minute)
	if err != nil || repeated.State != LeaseAlreadyCompleted {
		t.Fatalf("repeated Acquire() = %#v, %v", repeated, err)
	}
	releasable, err := store.Acquire(ctx, "key-2", time.Minute)
	if err != nil {
		t.Fatalf("second Acquire() error = %v", err)
	}
	if err := store.Release(ctx, releasable.Lease); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	assertObservedOperations(t, observer, "acquire_idempotency_lease", "renew_idempotency_lease", "complete_idempotency_lease", "release_idempotency_lease")
}

func TestObservedAuxiliaryStoreConstructorsRejectIncompleteDependencies(t *testing.T) {
	t.Parallel()
	observer := &recordingStoreObserver{}
	if _, err := NewObservedExecutionDedupStore(nil, observer, "memory"); err == nil {
		t.Fatal("dedup wrapper accepted nil delegate")
	}
	if _, err := NewObservedRetryTracker(NewMemoryRetryTracker(), nil, "memory"); err == nil {
		t.Fatal("retry wrapper accepted nil observer")
	}
	if _, err := NewObservedIdempotencyStore(NewMemoryIdempotencyStore(), observer, ""); err == nil {
		t.Fatal("idempotency wrapper accepted empty backend")
	}
}

func assertObservedOperations(t *testing.T, observer *recordingStoreObserver, operations ...string) {
	t.Helper()
	seen := make(map[string]bool)
	for _, attributes := range observer.attributes {
		seen[attributes.Operation] = true
	}
	for _, operation := range operations {
		if !seen[operation] {
			t.Fatalf("observer did not record %q: %#v", operation, observer.attributes)
		}
	}
}
