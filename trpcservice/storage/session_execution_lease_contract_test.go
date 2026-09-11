package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemorySessionExecutionLeaseValidatesIdentityAndTTL(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	for _, test := range []struct {
		name       string
		tenantID   string
		sessionKey string
		ownerID    string
		ttl        time.Duration
	}{
		{name: "tenant", sessionKey: "tenant-a/support/session/1", ownerID: "run-1", ttl: time.Minute},
		{name: "session", tenantID: "tenant-a", ownerID: "run-1", ttl: time.Minute},
		{name: "owner", tenantID: "tenant-a", sessionKey: "tenant-a/support/session/1", ttl: time.Minute},
		{name: "scope", tenantID: "tenant-a", sessionKey: "tenant-b/support/session/1", ownerID: "run-1", ttl: time.Minute},
		{name: "ttl", tenantID: "tenant-a", sessionKey: "tenant-a/support/session/1", ownerID: "run-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.AcquireSessionExecutionLease(context.Background(), test.tenantID, test.sessionKey, test.ownerID, test.ttl); err == nil {
				t.Fatal("AcquireSessionExecutionLease() error = nil")
			}
		})
	}
}

func TestMemorySessionExecutionLeaseRenewReleaseAndTakeover(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	clock := time.Date(2026, time.September, 11, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	ctx := context.Background()
	first, err := store.AcquireSessionExecutionLease(ctx, "tenant-a", "tenant-a/support/session/1", "run-a", time.Minute)
	if err != nil || first.FencingToken != 1 {
		t.Fatalf("first lease = %+v, %v", first, err)
	}
	clock = clock.Add(30 * time.Second)
	renewed, err := store.RenewSessionExecutionLease(ctx, first, time.Minute)
	if err != nil || renewed.FencingToken != first.FencingToken || !renewed.LeaseUntil.Equal(clock.Add(time.Minute)) {
		t.Fatalf("renewed lease = %+v, %v", renewed, err)
	}
	foreign := renewed
	foreign.OwnerID = "run-b"
	if _, err := store.RenewSessionExecutionLease(ctx, foreign, time.Minute); !errors.Is(err, ErrSessionExecutionLeaseLost) {
		t.Fatalf("foreign renew error = %v", err)
	}
	if err := store.ReleaseSessionExecutionLease(ctx, foreign); !errors.Is(err, ErrSessionExecutionLeaseLost) {
		t.Fatalf("foreign release error = %v", err)
	}
	if err := store.ReleaseSessionExecutionLease(ctx, renewed); err != nil {
		t.Fatalf("release error = %v", err)
	}
	second, err := store.AcquireSessionExecutionLease(ctx, "tenant-a", "tenant-a/support/session/1", "run-b", time.Minute)
	if err != nil || second.FencingToken != 2 || second.OwnerID != "run-b" {
		t.Fatalf("takeover lease = %+v, %v", second, err)
	}
	if _, err := store.RenewSessionExecutionLease(ctx, renewed, time.Minute); !errors.Is(err, ErrSessionExecutionLeaseLost) {
		t.Fatalf("stale renew error = %v", err)
	}
}

func TestMemorySessionExecutionLeaseHonorsContextCancellation(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	first, err := store.AcquireSessionExecutionLease(context.Background(), "tenant-a", "tenant-a/support/session/1", "run-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.AcquireSessionExecutionLease(cancelled, "tenant-a", "tenant-a/support/session/1", "run-b", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire error = %v", err)
	}
	if _, err := store.RenewSessionExecutionLease(cancelled, first, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled renew error = %v", err)
	}
	if err := store.ReleaseSessionExecutionLease(cancelled, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled release error = %v", err)
	}
}

func TestSessionExecutionLeaseRetryBackoffIsBounded(t *testing.T) {
	t.Parallel()
	delay := time.Duration(0)
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		2 * time.Second,
		2 * time.Second,
	}
	for index, expected := range want {
		delay = nextSessionLeaseRetryDelay(delay)
		if delay != expected {
			t.Fatalf("retry delay[%d] = %v, want %v", index, delay, expected)
		}
	}
}
