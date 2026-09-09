//go:build integration

package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
)

// seedReplayTenants builds the violation matrix tenants. Every session has a
// distinct stable category so assertions can pin per-category counts.
func seedReplayTenants(ctx context.Context, t *testing.T, lab *recoveryLab) {
	t.Helper()
	// Identity rows for the FK graph of the replay sessions.
	for _, tenant := range []string{"p202-replay", "p202-replay-two"} {
		if err := execTenantInsert(ctx, lab.ownerPool, tenant); err != nil {
			t.Fatal(err)
		}
		if _, err := lab.ownerPool.Exec(ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name)
			VALUES ($1,'app-replay','app-replay')
			ON CONFLICT (tenant_id, agent_app_id) DO NOTHING`, tenant); err != nil {
			t.Fatal(err)
		}
		if _, err := lab.ownerPool.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id)
			VALUES ($1,'lark','binding-replay',$1||'-ext')
			ON CONFLICT (tenant_id, channel, binding_id) DO NOTHING`, tenant); err != nil {
			t.Fatal(err)
		}
	}
	const tenant = "p202-replay"
	type fixture struct {
		session   string
		watermark int64
		events    []replayEventFixture
	}
	fixtures := []fixture{
		{session: "valid-1", watermark: 3, events: []replayEventFixture{
			{Sequence: 1, Type: "user.received", Payload: `{"text":"hello","meta":{"k":1}}`},
			{Sequence: 2, Type: "agent.started", Parent: "evt-valid-1-1", Payload: `{}`},
			{Sequence: 3, Type: "assistant.completed", Parent: "evt-valid-1-2", Payload: `{"done":true}`},
		}},
		{session: "empty-1", watermark: 0},
		{session: "gap-1", watermark: 4, events: []replayEventFixture{
			{Sequence: 1, Type: "user.received", Payload: `{}`},
			{Sequence: 2, Type: "agent.started", Parent: "evt-gap-1-1", Payload: `{}`},
			{Sequence: 4, Type: "assistant.completed", Payload: `{}`},
		}},
		{session: "unknown-1", watermark: 2, events: []replayEventFixture{
			{Sequence: 1, Type: "user.received", Payload: `{}`},
			{Sequence: 2, Type: "mystery.event", Payload: `{}`},
		}},
		{session: "parent-missing-1", watermark: 2, events: []replayEventFixture{
			{Sequence: 1, Type: "user.received", Payload: `{}`},
			{Sequence: 2, Type: "agent.started", Parent: "evt-does-not-exist", Payload: `{}`},
		}},
		{session: "parent-future-1", watermark: 3, events: []replayEventFixture{
			{Sequence: 1, Type: "user.received", Payload: `{}`},
			{Sequence: 2, Type: "agent.started", Parent: "evt-parent-future-1-3", Payload: `{}`},
			{Sequence: 3, Type: "assistant.completed", Payload: `{}`},
		}},
		{session: "payload-1", watermark: 1, events: []replayEventFixture{
			{Sequence: 1, Type: "user.received", Payload: `{"token":"leak-value","nested":{"api_key":"k"}}`},
		}},
		{session: "watermark-low-1", watermark: 2, events: []replayEventFixture{
			{Sequence: 1, Type: "user.received", Payload: `{}`},
			{Sequence: 2, Type: "agent.started", Parent: "evt-watermark-low-1-1", Payload: `{}`},
			{Sequence: 3, Type: "assistant.completed", Payload: `{}`},
		}},
		{session: "watermark-high-1", watermark: 5, events: []replayEventFixture{
			{Sequence: 1, Type: "user.received", Payload: `{}`},
			{Sequence: 2, Type: "assistant.completed", Payload: `{}`},
		}},
	}
	for _, f := range fixtures {
		if err := seedReplaySession(ctx, t, lab.runtimePool, tenant, f.session, f.watermark, f.events); err != nil {
			t.Fatalf("seed %s: %v", f.session, err)
		}
	}
	if err := seedReplaySession(ctx, t, lab.runtimePool, "p202-replay-two", "other-valid", 1, []replayEventFixture{
		{Sequence: 1, Type: "user.received", Payload: `{}`},
	}); err != nil {
		t.Fatal(err)
	}
	// A tenant with zero sessions must be handled without error.
	if err := execTenantInsert(ctx, lab.ownerPool, "p202-replay-empty"); err != nil {
		t.Fatal(err)
	}
}

// execTenantInsert creates a bare tenant row through the owner connection.
func execTenantInsert(ctx context.Context, pool *pgxpool.Pool, tenant string) error {
	_, err := pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status) VALUES ($1,$2,'active')`, tenant, tenant)
	return err
}

// TestReplayViolationMatrix runs the full bounded replay contract over the
// seeded violation tenants: category reporting, determinism, zero side
// effects, monotonic repair idempotency, repair gating, cancellation and
// tenant isolation.
func TestReplayViolationMatrix(t *testing.T) {
	lab := newRecoveryLab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	seedReplayTenants(ctx, t, lab)
	cfg := ReplayConfig{
		OwnerURL:   lab.ownerURL,
		RuntimeURL: lab.runtimeURL,
		Mode:       ReplayModeDryRun,
	}

	lowSession := "watermark-low-1"

	t.Run("dry_run_reports_all_categories", func(t *testing.T) {
		result, err := RunReplay(ctx, cfg)
		if !errors.Is(err, ErrReplayViolation) {
			t.Fatalf("dry-run category: %v", categoryForTest(err))
		}
		expected := map[string]int64{
			ViolationSequenceGap:      1,
			ViolationEventTypeUnknown: 1,
			ViolationParentMissing:    1,
			ViolationParentFuture:     1,
			ViolationPayloadInvalid:   1,
			ViolationWatermarkHigh:    1,
			ViolationWatermarkLow:     1,
		}
		if len(result.Violations) != len(expected) {
			t.Fatalf("violation categories: got %v want %v", result.Violations, expected)
		}
		for category, want := range expected {
			if result.Violations[category] != want {
				t.Fatalf("violation %s: got %d want %d", category, result.Violations[category], want)
			}
		}
		if result.Tenants != 3 || result.Sessions != 10 || result.Events != 20 {
			t.Fatalf("replay counts: tenants=%d sessions=%d events=%d", result.Tenants, result.Sessions, result.Events)
		}
		if result.Repaired != 0 {
			t.Fatalf("dry-run must not repair: %d", result.Repaired)
		}
	})

	t.Run("dry_run_deterministic_and_zero_writes", func(t *testing.T) {
		first, err := RunReplay(ctx, cfg)
		if !errors.Is(err, ErrReplayViolation) {
			t.Fatalf("dry-run one: %v", categoryForTest(err))
		}
		second, err := RunReplay(ctx, cfg)
		if !errors.Is(err, ErrReplayViolation) {
			t.Fatalf("dry-run two: %v", categoryForTest(err))
		}
		if first.Digest != second.Digest || first.Events != second.Events || first.Sessions != second.Sessions {
			t.Fatalf("replay not deterministic")
		}
		if first.Digest == "" {
			t.Fatalf("digest empty")
		}
		// Zero side effects: watermark untouched by dry-run.
		if got := sessionWatermark(ctx, t, lab.ownerPool, "p202-replay", lowSession); got != 2 {
			t.Fatalf("dry-run modified the watermark: %d", got)
		}
		var events int
		if err := lab.ownerPool.QueryRow(ctx, `SELECT count(*) FROM session_event`).Scan(&events); err != nil {
			t.Fatal(err)
		}
		if events != 20 {
			t.Fatalf("event rows changed during dry-run: %d", events)
		}
	})

	t.Run("repair_requires_explicit_gate", func(t *testing.T) {
		_, err := RunReplay(ctx, ReplayConfig{
			OwnerURL: lab.ownerURL, RuntimeURL: lab.runtimeURL, Mode: ReplayModeRepair,
		})
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("ungated repair category: %v", categoryForTest(err))
		}
	})

	t.Run("repair_monotonic_idempotent", func(t *testing.T) {
		before := readSessionSnapshot(ctx, t, lab.ownerPool, "p202-replay", lowSession)
		result, err := RunReplay(ctx, ReplayConfig{
			OwnerURL: lab.ownerURL, RuntimeURL: lab.runtimeURL,
			Mode: ReplayModeRepair, RepairAllowed: true,
		})
		if !errors.Is(err, ErrReplayViolation) {
			t.Fatalf("repair run category: %v", categoryForTest(err))
		}
		if result.Repaired != 1 {
			t.Fatalf("repaired sessions: got %d want 1", result.Repaired)
		}
		if got := sessionWatermark(ctx, t, lab.ownerPool, "p202-replay", lowSession); got != 3 {
			t.Fatalf("repair did not raise the watermark to 3: %d", got)
		}
		after := readSessionSnapshot(ctx, t, lab.ownerPool, "p202-replay", lowSession)
		if before.state != after.state || before.stateVersion != after.stateVersion || before.summaryVersion != after.summaryVersion || !before.updatedAt.Equal(after.updatedAt) {
			t.Fatalf("repair mutated fields beyond last_event_seq: %+v -> %+v", before, after)
		}
		// Watermark-high session must never be lowered.
		if got := sessionWatermark(ctx, t, lab.ownerPool, "p202-replay", "watermark-high-1"); got != 5 {
			t.Fatalf("watermark-high was modified: %d", got)
		}
		// Idempotent re-run: nothing left to repair.
		again, err := RunReplay(ctx, ReplayConfig{
			OwnerURL: lab.ownerURL, RuntimeURL: lab.runtimeURL,
			Mode: ReplayModeRepair, RepairAllowed: true,
		})
		if !errors.Is(err, ErrReplayViolation) {
			t.Fatalf("second repair category: %v", categoryForTest(err))
		}
		if again.Repaired != 0 {
			t.Fatalf("repair not idempotent: %d", again.Repaired)
		}
	})

	t.Run("cancel_is_safe", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := RunReplay(cancelCtx, cfg)
		if !errors.Is(err, ErrTimeoutOrCancelled) {
			t.Fatalf("cancelled replay category: %v", categoryForTest(err))
		}
		if got := sessionWatermark(ctx, t, lab.ownerPool, "p202-replay", lowSession); got != 3 {
			t.Fatalf("cancel modified data: %d", got)
		}
	})

	t.Run("runtime_role_isolation", func(t *testing.T) {
		// The replay runtime role sees nothing without the tenant GUC.
		var count int
		if err := lab.runtimePool.QueryRow(ctx, `SELECT count(*) FROM session`).Scan(&count); err != nil {
			t.Fatalf("runtime probe: %v", err)
		}
		if count != 0 {
			t.Fatalf("runtime role sees rows without tenant context: %d", count)
		}
		// With the GUC it sees exactly its own tenant rows.
		err := tenantctx.WithTenantContext(ctx, lab.runtimePool, "p202-replay", "probe", func(ctx context.Context, tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT count(*) FROM session`).Scan(&count)
		})
		if err != nil {
			t.Fatal(err)
		}
		if count != 9 {
			t.Fatalf("tenant scope rows: %d", count)
		}
	})
}
