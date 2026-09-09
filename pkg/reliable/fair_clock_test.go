package reliable

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestMemoryFairQueueNewTenantCompetesWithoutHistoricalCatchUp(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	for _, tenantID := range []string{"tenant-a", "tenant-b", "never-active"} {
		if err := store.UpsertTenantQueuePolicy(ctx, TenantQueuePolicy{TenantID: tenantID, Weight: 1}); err != nil {
			t.Fatal(err)
		}
	}
	enqueueFairMessages(t, store, "tenant-a", 0, 20)
	for range 20 {
		claimAndCompleteFair(t, store)
	}
	enqueueFairMessages(t, store, "tenant-a", 20, 20)
	enqueueFairMessages(t, store, "tenant-b", 0, 20)
	counts := make(map[string]int)
	for range 10 {
		counts[claimAndCompleteFair(t, store).TenantID]++
	}
	if counts["tenant-a"] != 5 || counts["tenant-b"] != 5 {
		t.Fatalf("equal-weight active tenants received %v claims, want 5 each regardless of historical service", counts)
	}
}

func TestMemoryFairQueueIdleTenantResumesAtCurrentServiceTime(t *testing.T) {
	store := NewMemoryStore()
	enqueueFairMessages(t, store, "tenant-b", 0, 1)
	claimAndCompleteFair(t, store)
	enqueueFairMessages(t, store, "tenant-a", 0, 20)
	for range 20 {
		claimAndCompleteFair(t, store)
	}
	enqueueFairMessages(t, store, "tenant-a", 20, 20)
	enqueueFairMessages(t, store, "tenant-b", 1, 20)
	counts := make(map[string]int)
	for range 10 {
		counts[claimAndCompleteFair(t, store).TenantID]++
	}
	if counts["tenant-a"] != 5 || counts["tenant-b"] != 5 {
		t.Fatalf("resumed equal-weight tenants received %v claims, want 5 each without idle-time credit", counts)
	}
}

func TestMemoryFairQueuePolicyResetPreservesServiceDebt(t *testing.T) {
	store := NewMemoryStore()
	enqueueFairMessages(t, store, "tenant-a", 0, 3)
	enqueueFairMessages(t, store, "tenant-b", 0, 3)
	first := claimAndCompleteFair(t, store)
	if first.TenantID != "tenant-a" {
		t.Fatalf("first tenant=%s, want tenant-a", first.TenantID)
	}
	if err := store.DeleteTenantQueuePolicy(context.Background(), first.TenantID); err != nil {
		t.Fatal(err)
	}
	if next := claimAndCompleteFair(t, store); next.TenantID != "tenant-b" {
		t.Fatalf("policy reset let %s bypass the unserved tenant-b", next.TenantID)
	}
}

type fairTestStore interface {
	Store
	FairInboxClaimer
}

func enqueueFairMessages(t *testing.T, store Store, tenantID string, start, count int) {
	t.Helper()
	for i := start; i < start+count; i++ {
		msg := newTestInbox()
		msg.TenantID = tenantID
		msg.ExternalMessageID = fmt.Sprintf("message-%s-%d", tenantID, i)
		msg.SessionID = fmt.Sprintf("session-%s-%d", tenantID, i)
		if inserted, err := store.EnqueueInbox(context.Background(), msg); err != nil || !inserted {
			t.Fatalf("enqueue %s/%d inserted=%v error=%v", tenantID, i, inserted, err)
		}
	}
}

func claimAndCompleteFair(t *testing.T, store fairTestStore) *InboxMessage {
	t.Helper()
	ctx := context.Background()
	claim, err := store.ClaimInboxFair(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "reply"}); err != nil {
		t.Fatal(err)
	}
	return claim
}
