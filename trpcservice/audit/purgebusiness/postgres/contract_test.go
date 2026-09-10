package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness"
)

const (
	seedTenant       = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	deadLetterTenant = "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

// TestBusinessAuditRetentionPostgreSQL16 proves the watermark gate on a real
// PostgreSQL 16 instance: an un-exported audit Outbox row must block deletion
// of any audit_event at or after its created_at, even when the retention
// cutoff is far in the past.
func TestBusinessAuditRetentionPostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var major int
	if err := db.QueryRow(`SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &major); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, major)
	}

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	clearTenant(t, ctx, db, seedTenant)

	seedEvent(t, ctx, db, seedTenant, "e1", now.Add(-48*time.Hour))
	seedEvent(t, ctx, db, seedTenant, "e2", now.Add(-24*time.Hour))
	seedOutbox(t, ctx, db, seedTenant, "o-unexported", "pending", now.Add(-25*time.Hour))

	store := New(db)
	batchID, err := store.Plan(ctx, purgebusiness.PlanInput{TenantID: seedTenant, CutoffAt: now.Add(-time.Hour),
		Actor: "contract", Reason: "contract", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Execute(ctx, seedTenant, batchID, "contract", 1000)
	if err != nil || result != "completed" {
		t.Fatalf("execute result=%q err=%v", result, err)
	}
	batch, err := store.Get(ctx, seedTenant, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if batch.DeletedEvents != 1 {
		t.Fatalf("deleted_events=%d, want 1 (only e1 before watermark)", batch.DeletedEvents)
	}
	if batch.DeletedOutbox != 0 {
		t.Fatalf("deleted_outbox=%d, want 0 (un-exported outbox retained)", batch.DeletedOutbox)
	}
	if batch.WatermarkAt.IsZero() || !batch.WatermarkAt.Equal(now.Add(-25*time.Hour)) {
		t.Fatalf("watermark=%v, want %v", batch.WatermarkAt, now.Add(-25*time.Hour))
	}
	if exists(t, ctx, db, seedTenant, "e1") {
		t.Fatal("e1 should have been deleted")
	}
	if !exists(t, ctx, db, seedTenant, "e2") {
		t.Fatal("e2 must be retained (after watermark)")
	}
	if !outboxExists(t, ctx, db, seedTenant, "o-unexported") {
		t.Fatal("un-exported outbox must be retained")
	}
	if _, err := db.ExecContext(ctx, `UPDATE business_audit_purge_certificate SET reason='forged' WHERE tenant_id=$1 AND batch_id=$2`, seedTenant, batchID); sqlState(err) != "55000" {
		t.Fatalf("certificate mutation SQLSTATE=%q err=%v", sqlState(err), err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM business_audit_purge_batch WHERE tenant_id=$1 AND batch_id=$2`, seedTenant, batchID); sqlState(err) != "55000" {
		t.Fatalf("batch delete SQLSTATE=%q err=%v", sqlState(err), err)
	}
}

func TestBusinessAuditRetentionDeadLetterBlocksPostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	ensureTenant(t, ctx, db, deadLetterTenant, "business-audit-retention-dead-letter")

	seedEvent(t, ctx, db, deadLetterTenant, "e-dead-letter", now.Add(-24*time.Hour))
	seedOutbox(t, ctx, db, deadLetterTenant, "o-dead-letter", "dead_letter", now.Add(-25*time.Hour))
	store := New(db)
	batchID, err := store.Plan(ctx, purgebusiness.PlanInput{TenantID: deadLetterTenant, CutoffAt: now.Add(-time.Hour),
		Actor: "contract", Reason: "dead-letter", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Execute(ctx, deadLetterTenant, batchID, "contract", 1000)
	if err != nil || result != "completed" {
		t.Fatalf("execute result=%q err=%v", result, err)
	}
	batch, err := store.Get(ctx, deadLetterTenant, batchID)
	if err != nil {
		t.Fatal(err)
	}
	if batch.DeletedEvents != 0 || batch.WatermarkAt.IsZero() {
		t.Fatalf("deleted=%d watermark=%v, want dead letter to block deletion", batch.DeletedEvents, batch.WatermarkAt)
	}
	if !exists(t, ctx, db, deadLetterTenant, "e-dead-letter") || !outboxExists(t, ctx, db, deadLetterTenant, "o-dead-letter") {
		t.Fatal("dead-letter source fact and outbox must be retained")
	}
}

func TestBusinessAuditRetentionAuthorizationBoundaryPostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	const principal = "business_audit_purge_contract"
	if _, err := db.ExecContext(ctx, `CREATE ROLE `+principal+` NOLOGIN`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP ROLE IF EXISTS `+principal) })
	ensureTenant(t, ctx, db, deadLetterTenant, "business-audit-retention-dead-letter")
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SET SESSION AUTHORIZATION `+principal); err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, `SELECT public.plan_business_audit_purge($1,$2,'contract','authorization',clock_timestamp())`, deadLetterTenant, time.Now().UTC().Add(-time.Hour))
	if sqlState(err) != "42501" {
		t.Fatalf("unprivileged plan SQLSTATE=%q err=%v", sqlState(err), err)
	}
	if _, err := conn.ExecContext(ctx, `RESET SESSION AUTHORIZATION`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `GRANT audit_retention_purger TO `+principal); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `REVOKE audit_retention_purger FROM `+principal) })
	if _, err := conn.ExecContext(ctx, `SET SESSION AUTHORIZATION `+principal); err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, `SELECT public.plan_business_audit_purge($1,$2,'contract','authorization',clock_timestamp())`, deadLetterTenant, time.Now().UTC().Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("granted plan: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `RESET SESSION AUTHORIZATION`); err != nil {
		t.Fatal(err)
	}
}

func seedEvent(t *testing.T, ctx context.Context, db *sql.DB, tenantID, auditID string, occurredAt time.Time) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO audit_event(tenant_id,audit_id,schema_version,action,decision,occurred_at,event_digest)
VALUES($1,$2,1,'retention.probe','recorded',$3,repeat('a',64))`, tenantID, auditID, occurredAt.UTC())
	if err != nil {
		t.Fatalf("seed event %s: %v", auditID, err)
	}
}

func seedOutbox(t *testing.T, ctx context.Context, db *sql.DB, tenantID, outboxID, state string, createdAt time.Time) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref,state,created_at)
VALUES($1,$2,'audit',$2,1,$2,$2,$3,$4)`, tenantID, outboxID, state, createdAt.UTC())
	if err != nil {
		t.Fatalf("seed outbox %s: %v", outboxID, err)
	}
}

func exists(t *testing.T, ctx context.Context, db *sql.DB, tenantID, auditID string) bool {
	t.Helper()
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM audit_event WHERE tenant_id=$1 AND audit_id=$2`, tenantID, auditID).Scan(&one)
	return err == nil
}

func outboxExists(t *testing.T, ctx context.Context, db *sql.DB, tenantID, outboxID string) bool {
	t.Helper()
	var one int
	err := db.QueryRowContext(ctx, `SELECT 1 FROM outbox WHERE tenant_id=$1 AND outbox_id=$2`, tenantID, outboxID).Scan(&one)
	return err == nil
}

func ensureTenant(t *testing.T, ctx context.Context, db *sql.DB, tenantID, tenantKey string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name)
VALUES($1,$2,'Business audit retention contract') ON CONFLICT (tenant_id) DO NOTHING`, tenantID, tenantKey); err != nil {
		t.Fatal(err)
	}
}

func clearTenant(t *testing.T, ctx context.Context, db *sql.DB, tenantID string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('audit.purge_authorized','on',true)`); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"audit_event", "outbox"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE tenant_id=$1`, tenantID); err != nil {
			t.Fatalf("clear %s: %v", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func sqlState(err error) string {
	var value *pgconn.PgError
	if errors.As(err, &value) {
		return value.Code
	}
	return ""
}
