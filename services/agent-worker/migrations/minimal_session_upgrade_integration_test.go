package migrations_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/migrations"
)

// Runs in a separate owned Worker database. Installs every published predecessor
// with its original digest, seeds existing history, then applies the real runner.
func TestMinimalIdentityUpgradePostgres(t *testing.T) {
	url := os.Getenv("WORKER_TEST_MIGRATION_URL")
	if url == "" {
		t.Skip("requires fresh dedicated Worker schema")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	entries, e := migrations.Files.ReadDir(".")
	if e != nil {
		t.Fatal(e)
	}
	tx, e := pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `CREATE TABLE worker_schema_migrations(version text PRIMARY KEY,digest text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); e != nil {
		t.Fatal(e)
	}
	count := 0
	for _, f := range entries {
		if !strings.HasSuffix(f.Name(), ".sql") || strings.HasPrefix(f.Name(), "0018_") {
			continue
		}
		raw, e := migrations.Files.ReadFile(f.Name())
		if e != nil {
			t.Fatal(e)
		}
		if _, e = tx.Exec(ctx, string(raw)); e != nil {
			t.Fatal(f.Name(), e)
		}
		h := sha256.Sum256(raw)
		if _, e = tx.Exec(ctx, `INSERT INTO worker_schema_migrations(version,digest) VALUES($1,$2)`, f.Name(), hex.EncodeToString(h[:])); e != nil {
			t.Fatal(e)
		}
		count++
	}
	// No synthetic policy authority is required for this upgrade. This is an old
	// accepted scope which must not be merged into a newly partitioned user scope.
	if _, e = tx.Exec(ctx, `INSERT INTO execution_sessions(tenant_id,session_id,scope_json) VALUES('upgrade-tenant','old-session','["old-shared-scope"]'); INSERT INTO execution_runs(tenant_id,run_id,admission_id,request_digest,request_json,session_id,session_sequence,status,policy_json,accepted_at,run_deadline,reply_deadline) VALUES('upgrade-tenant','old-run','old-admission','old-digest','{"Authorization":{"principal_id":"old-principal"}}','old-session',1,'QUEUED','{}',clock_timestamp(),clock_timestamp()+interval '1 hour',clock_timestamp()+interval '1 hour')`); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = migrations.ApplyForRuntime(ctx, pool, "worker_runtime"); e == nil || !strings.Contains(e.Error(), "MINIMAL_SESSION_CUTOVER_REQUIRES_GOVERNED_INPUT_DISPOSITION") {
		t.Fatal("outstanding governed input bypassed", e)
	}
	var exists bool
	if e = pool.QueryRow(ctx, `SELECT to_regclass('execution_social_identities') IS NOT NULL`).Scan(&exists); e != nil || exists {
		t.Fatal("failed cutover partially committed", exists, e)
	}
	// Explicit fixture disposition, not product migration behavior. Never mark
	// real Runs complete merely to pass the migration gate.
	if _, e = pool.Exec(ctx, `UPDATE execution_runs SET status='FAILED' WHERE run_id='old-run'`); e != nil {
		t.Fatal(e)
	}
	if e = migrations.ApplyForRuntime(ctx, pool, "worker_runtime"); e != nil {
		t.Fatal(e)
	}
	if e = migrations.ApplyForRuntime(ctx, pool, "worker_runtime"); e != nil {
		t.Fatal("reapply", e)
	}
	var rows int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM execution_sessions WHERE session_id='old-session' AND scope_json='["old-shared-scope"]'::jsonb`).Scan(&rows); e != nil || rows != 1 {
		t.Fatal("old history changed", e, rows)
	}
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM worker_schema_migrations`).Scan(&rows); e != nil || rows != count+1 {
		t.Fatal("migration ledger changed", e, rows)
	}
	if e = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname IN ('execution_attempt_authorization_guard','execution_partitioned_run_consumed'))`).Scan(&exists); e != nil || exists {
		t.Fatal("retired trigger survived", e)
	}
	if e = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='execution_runs' AND column_name='traceparent')`).Scan(&exists); e != nil || !exists {
		t.Fatal("tracing schema lost", e)
	}
	t.Log("MINIMAL_SESSION_UPGRADE=PASS published_digests=PRESERVED active_governed_input=BLOCKED failed_cutover=ATOMIC old_history=PRESERVED retired_triggers=REMOVED tracing=PRESERVED repeat_apply=PASS")
}
