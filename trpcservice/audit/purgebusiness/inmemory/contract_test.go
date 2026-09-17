package inmemory

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness/contracttest"
)

func TestInMemoryBusinessAuditRetentionContract(t *testing.T) {
	contracttest.Suite(t, New())
}

func TestReconcilerDurablyQuarantinesExhaustedBatch(t *testing.T) {
	store := New()
	now := time.Now().UTC()
	batchID, err := store.Plan(context.Background(), purgebusiness.PlanInput{TenantID: "tenant", CutoffAt: now.Add(-24 * time.Hour),
		Actor: "test", Reason: "test", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	batch := store.batches[batchID]
	batch.State = purgebusiness.StateFailed
	batch.DeleteAttempt = 3
	batch.NotBefore = time.Time{}
	store.batches[batchID] = batch
	store.mu.Unlock()
	stats, err := (purgebusiness.Reconciler{Store: store, Owner: "test", MaxAttempts: 3, Now: func() time.Time { return now }}).ProcessOnce(context.Background())
	if err != nil || stats.Quarantined != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	batch, err = store.Get(context.Background(), "tenant", batchID)
	if err != nil || batch.State != purgebusiness.StateQuarantined {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}
