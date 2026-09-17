// Package contracttest runs the shared business audit retention contract
// against any purgebusiness.Store implementation.
package contracttest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

var base = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

type seeder interface {
	SeedEvent(string, string, time.Time)
	SeedOutbox(string, string, string, time.Time)
}

// Suite runs the retention contract against any store implementation. Each
// sub-test uses its own tenant so the durable batch-id derivation never
// collides across cases.
func Suite(t *testing.T, store purgebusiness.Store) {
	t.Helper()
	ctx := context.Background()
	seed, hasSeed := store.(seeder)

	plan := func(tenantID string, cutoff time.Time) string {
		t.Helper()
		id, err := store.Plan(ctx, purgebusiness.PlanInput{TenantID: tenantID, CutoffAt: cutoff,
			Actor: "contract", Reason: "contract", Now: base})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	t.Run("unexported outbox blocks deletion beyond watermark", func(t *testing.T) {
		if !hasSeed {
			t.Skip("store does not expose seed helpers")
		}
		const tenant = "bq-tenant-wm"
		seed.SeedEvent(tenant, "e1", base.Add(-48*time.Hour))
		seed.SeedEvent(tenant, "e2", base.Add(-24*time.Hour))
		seed.SeedOutbox(tenant, "o-unexported", "pending", base.Add(-25*time.Hour))

		batchID := plan(tenant, base.Add(-1*time.Hour))
		result, err := store.Execute(ctx, tenant, batchID, "contract", 1000)
		if err != nil || result != "completed" {
			t.Fatalf("execute result=%q err=%v", result, err)
		}
		batch, err := store.Get(ctx, tenant, batchID)
		if err != nil {
			t.Fatal(err)
		}
		if batch.DeletedEvents != 1 {
			t.Fatalf("deleted_events=%d, want 1 (only e1 before watermark)", batch.DeletedEvents)
		}
		if batch.DeletedOutbox != 0 {
			t.Fatalf("deleted_outbox=%d, want 0 (un-exported outbox retained)", batch.DeletedOutbox)
		}
		if batch.WatermarkAt.IsZero() {
			t.Fatal("watermark should be set to the un-exported outbox time")
		}
	})

	t.Run("fully exported facts are purged", func(t *testing.T) {
		if !hasSeed {
			t.Skip("store does not expose seed helpers")
		}
		const tenant = "bq-tenant-full"
		seed.SeedEvent(tenant, "e3", base.Add(-72*time.Hour))
		seed.SeedOutbox(tenant, "o-exported", "published", base.Add(-72*time.Hour))

		batchID := plan(tenant, base.Add(-1*time.Hour))
		result, err := store.Execute(ctx, tenant, batchID, "contract", 1000)
		if err != nil || result != "completed" {
			t.Fatalf("execute result=%q err=%v", result, err)
		}
		batch, err := store.Get(ctx, tenant, batchID)
		if err != nil {
			t.Fatal(err)
		}
		if batch.DeletedEvents != 1 || batch.DeletedOutbox != 1 {
			t.Fatalf("deleted events=%d outbox=%d, want 1/1", batch.DeletedEvents, batch.DeletedOutbox)
		}
	})

	t.Run("dead letter outbox blocks deletion", func(t *testing.T) {
		if !hasSeed {
			t.Skip("store does not expose seed helpers")
		}
		const tenant = "bq-tenant-dead-letter"
		seed.SeedEvent(tenant, "e-dead-letter", base.Add(-24*time.Hour))
		seed.SeedOutbox(tenant, "o-dead-letter", "dead_letter", base.Add(-25*time.Hour))

		batchID := plan(tenant, base.Add(-time.Hour))
		result, err := store.Execute(ctx, tenant, batchID, "contract", 1000)
		if err != nil || result != "completed" {
			t.Fatalf("execute result=%q err=%v", result, err)
		}
		batch, err := store.Get(ctx, tenant, batchID)
		if err != nil {
			t.Fatal(err)
		}
		if batch.DeletedEvents != 0 || batch.WatermarkAt.IsZero() {
			t.Fatalf("deleted=%d watermark=%v, want retained by dead letter", batch.DeletedEvents, batch.WatermarkAt)
		}
	})

	t.Run("plan is idempotent", func(t *testing.T) {
		const tenant = "bq-tenant-idem"
		first := plan(tenant, base.Add(-2*time.Hour))
		second := plan(tenant, base.Add(-2*time.Hour))
		if first != second {
			t.Fatalf("plan replay %q != %q", first, second)
		}
	})

	t.Run("missing batch is not found", func(t *testing.T) {
		if _, err := store.Get(ctx, "bq-tenant-missing", "missing"); !errors.Is(err, runtime.ErrNotFound) {
			t.Fatalf("missing batch: got %v", err)
		}
	})

	t.Run("invalid plan is rejected", func(t *testing.T) {
		if _, err := store.Plan(ctx, purgebusiness.PlanInput{TenantID: "", CutoffAt: base,
			Actor: "a", Reason: "r", Now: base}); !errors.Is(err, runtime.ErrInvariantViolation) {
			t.Fatalf("empty tenant: got %v", err)
		}
	})
}
