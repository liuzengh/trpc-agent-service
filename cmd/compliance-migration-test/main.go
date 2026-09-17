// Command compliance-migration-test proves the guarded compliance retention
// path against a disposable PostgreSQL database that it creates and drops.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/compliancemigrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		return fmt.Errorf("refusing: set TRPC_MIGRATION_TEST=1")
	}
	adminDSN := os.Getenv("TRPC_POSTGRES_ADMIN_DSN")
	if adminDSN == "" {
		return fmt.Errorf("TRPC_POSTGRES_ADMIN_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return err
	}
	defer admin.Close()
	var major int
	if err := admin.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int/10000`).Scan(&major); err != nil {
		return err
	}
	if major != 16 {
		return fmt.Errorf("PostgreSQL 16 is required, found %d", major)
	}
	databaseName, err := randomDatabaseName()
	if err != nil {
		return err
	}
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+databaseName+`"`); err != nil {
		return fmt.Errorf("create test database: %w", err)
	}
	defer func() {
		_, _ = admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+databaseName+`" WITH (FORCE)`)
	}()
	testDSN, err := testDSNForDatabase(adminDSN, databaseName)
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	runner := compliancemigrations.Runner{DB: db}
	if err := runner.Up(ctx); err != nil {
		return fmt.Errorf("compliance migrations: %w", err)
	}
	if err := verifyGuardedPurge(ctx, db); err != nil {
		return fmt.Errorf("guarded purge: %w", err)
	}
	fmt.Printf("compliance migration matrix passed for %s\n", databaseName)
	return nil
}

func verifyGuardedPurge(ctx context.Context, db *sql.DB) error {
	const tenant = "t_purge"

	// 1. A row must exist so the per-row immutable trigger actually fires.
	cutoff := time.Now().UTC().Add(-11 * 365 * 24 * time.Hour)
	insertEvent(ctx, db, tenant, "e0", "usage.report", cutoff.Add(-time.Hour))

	if _, err := db.ExecContext(ctx, `DELETE FROM compliance.audit_event WHERE tenant_id=$1 AND audit_id='e0'`, tenant); err == nil {
		return fmt.Errorf("ad-hoc DELETE unexpectedly succeeded")
	}
	if _, err := db.ExecContext(ctx, `UPDATE compliance.audit_event SET event_digest=event_digest WHERE tenant_id=$1 AND audit_id='e0'`, tenant); err == nil {
		return fmt.Errorf("ad-hoc UPDATE unexpectedly succeeded")
	}

	// 2. A non-privileged, non-member session cannot gain delete authority by
	// setting the GUC itself, nor call the guarded execute function.
	if err := verifyAuthorizationBoundary(ctx, db, tenant); err != nil {
		return err
	}
	var unsafeBatch string
	if err := db.QueryRowContext(ctx, `SELECT compliance.plan_audit_purge_batch(
$1,'default',clock_timestamp(),'test','unsafe',interval '1 hour',1000)`, tenant).Scan(&unsafeBatch); err == nil {
		return fmt.Errorf("too-recent cutoff unexpectedly bypassed retention floor")
	}

	// 3. Purge an old billing-class event end to end.
	batchID := plan(ctx, db, tenant, "billing", cutoff)
	if err := approve(ctx, db, tenant, batchID); err != nil {
		return err
	}
	if code := execute(ctx, db, tenant, batchID); code != "completed" {
		return fmt.Errorf("execute returned %q", code)
	}
	if !exists(ctx, db, `SELECT 1 FROM compliance.audit_purge_certificate WHERE tenant_id=$1 AND batch_id=$2`, tenant, batchID) {
		return fmt.Errorf("destruction certificate missing")
	}
	if exists(ctx, db, `SELECT 1 FROM compliance.audit_event WHERE tenant_id=$1 AND audit_id='e0'`, tenant) {
		return fmt.Errorf("event e0 was not destroyed")
	}

	// 4. Unresolved quarantine alert blocks execution and persists failure.
	cutoff2 := cutoff.Add(-time.Minute)
	insertEvent(ctx, db, tenant, "e2", "usage.report", cutoff2.Add(-time.Hour))
	insertAlert(ctx, db, tenant, "e2")
	batchID2 := plan(ctx, db, tenant, "billing", cutoff2)
	if err := approve(ctx, db, tenant, batchID2); err != nil {
		return err
	}
	if code := execute(ctx, db, tenant, batchID2); code != "unresolved_quarantine" {
		return fmt.Errorf("unresolved quarantine execute returned %q", code)
	}
	if state(ctx, db, tenant, batchID2) != "failed" {
		return fmt.Errorf("unresolved quarantine batch did not persist failed")
	}

	// 5. Divergence fails closed and persists failure: plan, then insert a new
	// candidate before execute.
	cutoff3 := cutoff2.Add(-time.Minute)
	insertEvent(ctx, db, tenant, "e3", "usage.report", cutoff3.Add(-time.Hour))
	batchID3 := plan(ctx, db, tenant, "billing", cutoff3)
	insertEvent(ctx, db, tenant, "e4", "usage.report", cutoff3.Add(-time.Hour))
	if err := approve(ctx, db, tenant, batchID3); err != nil {
		return err
	}
	if code := execute(ctx, db, tenant, batchID3); code != "divergence" {
		return fmt.Errorf("diverged execute returned %q", code)
	}
	if state(ctx, db, tenant, batchID3) != "failed" {
		return fmt.Errorf("diverged batch did not persist failed")
	}

	// 6. A floor-version change after approval invalidates the preview.
	policyTenant := tenant + "_policy"
	policyCutoff := time.Now().UTC().Add(-181 * 24 * time.Hour)
	insertEvent(ctx, db, policyTenant, "e5", "ordinary.action", policyCutoff.Add(-time.Hour))
	policyBatch := plan(ctx, db, policyTenant, "default", policyCutoff)
	if err := approve(ctx, db, policyTenant, policyBatch); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `UPDATE compliance.audit_retention_floor
SET min_retention_seconds=200*86400,floor_version=floor_version+1,updated_at=clock_timestamp()
WHERE class='default'`); err != nil {
		return fmt.Errorf("raise retention floor: %w", err)
	}
	if code := execute(ctx, db, policyTenant, policyBatch); code != "retention_changed" {
		return fmt.Errorf("retention drift execute returned %q", code)
	}
	if state(ctx, db, policyTenant, policyBatch) != "failed" {
		return fmt.Errorf("retention drift batch did not persist failed")
	}

	// 7. Down refuses after any purge has completed.
	if err := (compliancemigrations.Runner{DB: db}).Down(ctx); err == nil {
		return fmt.Errorf("down after completed purge unexpectedly succeeded")
	}
	return nil
}

// verifyAuthorizationBoundary proves a session that is not a compliance_purger
// member cannot delete facts even after SETting the GUC, and cannot call the
// guarded execute function.
func verifyAuthorizationBoundary(ctx context.Context, db *sql.DB, tenant string) error {
	const role = "compliance_boundary_test"
	setup := []string{
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='compliance_boundary_test') THEN CREATE ROLE compliance_boundary_test NOLOGIN; END IF; END $$`,
		`GRANT USAGE ON SCHEMA compliance TO compliance_boundary_test`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA compliance TO compliance_boundary_test`,
		`GRANT EXECUTE ON FUNCTION compliance.execute_audit_purge_batch(text,text,text) TO compliance_boundary_test`,
	}
	for _, statement := range setup {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("boundary setup %q: %w", statement, err)
		}
	}
	if _, err := db.ExecContext(ctx, `SET SESSION AUTHORIZATION `+role); err != nil {
		return fmt.Errorf("SET SESSION AUTHORIZATION: %w", err)
	}
	if _, err := db.ExecContext(ctx, `SET compliance.purge_authorized = 'on'`); err != nil {
		_, _ = db.ExecContext(ctx, `RESET SESSION AUTHORIZATION`)
		return fmt.Errorf("SET purge_authorized: %w", err)
	}
	_, deleteErr := db.ExecContext(ctx, `DELETE FROM compliance.audit_event WHERE tenant_id=$1`, tenant)
	var ignored string
	callErr := db.QueryRowContext(ctx,
		`SELECT compliance.execute_audit_purge_batch($1,'missing','boundary')`, tenant).Scan(&ignored)
	_, _ = db.ExecContext(ctx, `RESET compliance.purge_authorized`)
	if _, err := db.ExecContext(ctx, `RESET SESSION AUTHORIZATION`); err != nil {
		return fmt.Errorf("RESET SESSION AUTHORIZATION: %w", err)
	}
	if deleteErr == nil {
		return fmt.Errorf("non-member DELETE with self-set GUC unexpectedly succeeded")
	}
	if callErr == nil {
		return fmt.Errorf("non-member guarded function call unexpectedly succeeded")
	}
	return nil
}

// plan/approve/execute helpers invoke the guarded SECURITY DEFINER functions.
func plan(ctx context.Context, db *sql.DB, tenant, class string, cutoff time.Time) string {
	var batchID string
	if err := db.QueryRowContext(ctx, `SELECT compliance.plan_audit_purge_batch($1,$2,$3,'test','contract',interval '1 hour',100000)`,
		tenant, class, cutoff).Scan(&batchID); err != nil {
		panic(err)
	}
	return batchID
}

func approve(ctx context.Context, db *sql.DB, tenant, batchID string) error {
	_, err := db.ExecContext(ctx, `SELECT compliance.approve_audit_purge_batch($1,$2,'test','contract')`, tenant, batchID)
	return err
}

func execute(ctx context.Context, db *sql.DB, tenant, batchID string) string {
	var code string
	if err := db.QueryRowContext(ctx, `SELECT compliance.execute_audit_purge_batch($1,$2,'test')`, tenant, batchID).Scan(&code); err != nil {
		panic(err)
	}
	return code
}

func state(ctx context.Context, db *sql.DB, tenant, batchID string) string {
	var s string
	if err := db.QueryRowContext(ctx, `SELECT state FROM compliance.audit_purge_batch WHERE tenant_id=$1 AND batch_id=$2`, tenant, batchID).Scan(&s); err != nil {
		panic(err)
	}
	return s
}

func exists(ctx context.Context, db *sql.DB, query string, args ...any) bool {
	var found bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(`+query+`)`, args...).Scan(&found)
	if err != nil {
		panic(err)
	}
	return found
}

func insertEvent(ctx context.Context, db *sql.DB, tenant, id, action string, at time.Time) {
	eventJSON := fmt.Sprintf(`{"schema_version":1,"audit_id":%q,"tenant_id":%q,"action":%q,"decision":"recorded","occurred_at":%q}`,
		id, tenant, action, at.Format(time.RFC3339Nano))
	if _, err := db.ExecContext(ctx, `INSERT INTO compliance.audit_event(tenant_id,audit_id,schema_version,event_json,event_digest,occurred_at)
VALUES($1,$2,1,$3::jsonb,$4,$5)`, tenant, id, eventJSON, strings.Repeat("a", 64), at); err != nil {
		panic(err)
	}
}

func insertAlert(ctx context.Context, db *sql.DB, tenant, id string) {
	now := time.Now().UTC()
	if _, err := db.ExecContext(ctx, `INSERT INTO compliance.quarantine_alert(
tenant_id,audit_id,resource_kind,artifact_id,resource_version,request_id,error_type,resource_ref,event_digest,occurred_at)
VALUES($1,$2,'upload','artifact-1',1,'r1','scan_failed','artifact-quarantine://`+tenant+`/upload/artifact-1/1',$3,$4)`,
		tenant, id, strings.Repeat("b", 64), now); err != nil {
		panic(err)
	}
}

func testDSNForDatabase(adminDSN, databaseName string) (string, error) {
	trimmed := strings.TrimSpace(adminDSN)
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		parsed.Path = "/" + databaseName
		return parsed.String(), nil
	}
	dsn := trimmed + " dbname=" + quoteConninfoValue(databaseName)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", err
	}
	if config.Database != databaseName {
		return "", fmt.Errorf("failed to target test database %q", databaseName)
	}
	return dsn, nil
}

func quoteConninfoValue(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`) + "'"
}

func randomDatabaseName() (string, error) {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	name := "trpc_agent_compliance_test_" + hex.EncodeToString(bytes)
	if !strings.HasPrefix(name, "trpc_agent_compliance_test_") {
		return "", fmt.Errorf("unsafe database name")
	}
	return name, nil
}
