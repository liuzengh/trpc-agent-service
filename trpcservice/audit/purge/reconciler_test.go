package purge_test

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purge/inmemory"
)

func oldEvent(tenant, id string, at time.Time) audit.Event {
	return audit.Event{SchemaVersion: 1, AuditID: id, TenantID: tenant, Action: "usage.report",
		Decision: "recorded", OccurredAt: at}
}

func TestReconcilerDryRunDoesNotDestroy(t *testing.T) {
	store := inmemory.New(time.Hour)
	now := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	store.Seed(oldEvent("t1", "e1", now.Add(-2*time.Hour)))

	r := purge.Reconciler{Store: store, Owner: "recon", DryRun: true, RequireApproval: false, MaxAttempts: 3, Now: func() time.Time { return now }}
	stats, err := r.ProcessOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Skipped != 1 || stats.Executed != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if store.Certificates() != 0 {
		t.Fatal("dry-run must not destroy")
	}
}

func TestReconcilerRequireApprovalWaitsForOperator(t *testing.T) {
	store := inmemory.New(time.Hour)
	now := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	store.Seed(oldEvent("t1", "e1", now.Add(-2*time.Hour)))

	r := purge.Reconciler{Store: store, Owner: "recon", DryRun: false, RequireApproval: true, MaxAttempts: 3, Now: func() time.Time { return now }}
	if _, err := r.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.Certificates() != 0 {
		t.Fatal("require-approval must not auto-approve")
	}
}

func TestReconcilerExecutesBatchApprovedBetweenPasses(t *testing.T) {
	store := inmemory.New(time.Hour)
	now := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	store.Seed(oldEvent("t1", "e1", now.Add(-2*time.Hour)))
	r := purge.Reconciler{Store: store, Owner: "recon", DryRun: false, RequireApproval: true,
		MaxAttempts: 3, MaxBatchSize: 100, Now: func() time.Time { return now }}
	if _, err := r.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	batches, err := store.ActiveBatches(context.Background())
	if err != nil || len(batches) != 1 {
		t.Fatalf("active batches=%+v err=%v", batches, err)
	}
	if err := store.Approve(context.Background(), "t1", batches[0].BatchID, "operator", "reviewed"); err != nil {
		t.Fatal(err)
	}
	stats, err := r.ProcessOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Executed != 1 || store.Certificates() != 1 {
		t.Fatalf("approved batch was not resumed: stats=%+v certificates=%d", stats, store.Certificates())
	}
}

func TestInMemoryPlanMirrorsRetentionAndBatchLimit(t *testing.T) {
	store := inmemory.New(time.Hour)
	now := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	store.Seed(oldEvent("t1", "e1", now.Add(-2*time.Hour)), oldEvent("t1", "e2", now.Add(-3*time.Hour)))
	if _, err := store.Plan(context.Background(), purge.PlanInput{TenantID: "t1", Class: "billing",
		CutoffAt: now, Now: now, TTL: time.Hour, MaxBatchSize: 10}); err == nil {
		t.Fatal("too-recent cutoff was accepted")
	}
	if _, err := store.Plan(context.Background(), purge.PlanInput{TenantID: "t1", Class: "billing",
		CutoffAt: now.Add(-time.Hour), Now: now, TTL: time.Hour, MaxBatchSize: 1}); err == nil {
		t.Fatal("oversized batch was accepted")
	}
}

func TestReconcilerUsesWallClockWhenNowIsUnset(t *testing.T) {
	store := inmemory.New(time.Hour)
	r := purge.Reconciler{Store: store, Owner: "recon", DryRun: true, RequireApproval: true,
		MaxAttempts: 3, MaxBatchSize: 100}
	if _, err := r.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilerExecutesApprovedBatch(t *testing.T) {
	store := inmemory.New(time.Hour)
	now := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	store.Seed(oldEvent("t1", "e1", now.Add(-2*time.Hour)))

	r := purge.Reconciler{Store: store, Owner: "recon", DryRun: false, RequireApproval: false, MaxAttempts: 3, Now: func() time.Time { return now }}
	stats, err := r.ProcessOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Executed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if store.Certificates() != 1 {
		t.Fatal("expected one destruction certificate")
	}
	// A second pass finds no due candidates (events are gone).
	stats, err = r.ProcessOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Executed != 0 {
		t.Fatalf("second pass stats=%+v", stats)
	}
}

func TestReconcilerQuarantinesAfterRetries(t *testing.T) {
	store := inmemory.New(time.Hour)
	now := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	store.Seed(oldEvent("t1", "e1", now.Add(-2*time.Hour)))
	cutoff := now.Add(-time.Hour)
	batchID, err := store.Plan(context.Background(), purge.PlanInput{TenantID: "t1", Class: "billing",
		CutoffAt: cutoff, Actor: "op", Reason: "r", TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	// Stage the real batch as failed with attempts exhausted.
	store.SetBatch(purge.Batch{TenantID: "t1", BatchID: batchID, State: purge.StateFailed,
		Class: "billing", CutoffAt: cutoff, PlannedCount: 1, DeleteAttempt: 3, LastError: "divergence"})

	r := purge.Reconciler{Store: store, Owner: "recon", DryRun: false, RequireApproval: false, MaxAttempts: 3, Now: func() time.Time { return now }}
	stats, err := r.ProcessOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Quarantined != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}
