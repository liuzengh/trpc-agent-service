package mysql

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// openTestDB connects to the DSN in MYSQL_TEST_DSN, or skips.
//
// A real MySQL is not optional here the way it was for the Redis session
// tests' "does it survive a restart" claim: migration locking, the checksum
// ledger, and the tenant-scoped composite foreign keys are MySQL 8.4
// behaviour. SQLite has no GET_LOCK, no composite foreign keys with the same
// NULL-skipping rule, and no utf8mb4 collation, so a mock that passes proves
// nothing about the thing this package relies on.
//
// Every package that integration-tests against MySQL gets its own DSN
// variable and its own database. These tests drop and recreate every table,
// and `go test ./...` runs package test binaries concurrently, so two
// packages sharing one database see each other's resets land mid-migration.
//
//	docker run -d --name tas-mysql -p 3307:3306 \
//	    -e MYSQL_ROOT_PASSWORD=rootpass -e MYSQL_DATABASE=tas_storage_test \
//	    -e MYSQL_USER=tas -e MYSQL_PASSWORD=taspw mysql:8
//	MYSQL_TEST_DSN='tas:taspw@tcp(127.0.0.1:3307)/tas_storage_test' go test ./trpcservice/storage/mysql/ -v
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN not set; skipping real-mysql integration test")
	}
	db, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// resetSchema drops everything a previous run may have left behind, so a
// failure's partial DDL cannot leak into the next test's assertions the way
// it would on a shared staging database.
func resetSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	tables := []string{
		"tool_call_attempts", "tool_calls", "artifacts", "memory_entries", "document_chunks", "documents",
		"knowledge_bindings", "knowledge_bases", "tool_bindings",
		"channel_identities", "channel_bindings",
		"agent_revisions", "agent_apps", "backend_profiles", "model_profiles",
		"tenant_users", "principals", "tenants", "schema_migrations",
	}
	if _, err := db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatalf("disable fk checks: %v", err)
	}
	for _, tbl := range tables {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	if _, err := db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatalf("re-enable fk checks: %v", err)
	}
}

func TestMigrateAppliesAndRecordsThenIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	resetSchema(t, db)
	ctx := context.Background()

	res, err := Migrate(ctx, db)
	if err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if len(res.Applied) == 0 {
		t.Fatal("first run applied nothing")
	}

	res2, err := Migrate(ctx, db)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if len(res2.Applied) != 0 {
		t.Fatalf("second run re-applied: %v", res2.Applied)
	}
	if len(res2.AlreadyKnown) != len(res.Applied) {
		t.Fatalf("already-known count %d != applied count %d", len(res2.AlreadyKnown), len(res.Applied))
	}

	applied, err := NewLedger(db).Applied(ctx)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	for _, a := range applied {
		if a.Checksum == "" {
			t.Fatalf("migration %s recorded without a checksum", a.Version)
		}
	}
}

func TestMigrateRejectsAnEditedAppliedFile(t *testing.T) {
	db := openTestDB(t)
	resetSchema(t, db)
	ctx := context.Background()

	if _, err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Simulate history being rewritten in place rather than superseded by a
	// new file — the exact mistake a checksum catches.
	if _, err := db.ExecContext(ctx,
		"UPDATE schema_migrations SET checksum = 'deadbeef' WHERE version = '0001_control.sql'"); err != nil {
		t.Fatalf("tamper with the ledger: %v", err)
	}

	_, err := Migrate(ctx, db)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("migrate after tampering: %v, want ErrChecksumMismatch", err)
	}
	if !strings.Contains(err.Error(), "0001_control.sql") {
		t.Fatalf("error must name the offending file, got: %v", err)
	}
}

func TestMigrateLockSerialisesConcurrentRuns(t *testing.T) {
	db := openTestDB(t)
	resetSchema(t, db)
	ctx := context.Background()

	// Two goroutines racing is a weak stand-in for two processes, but it does
	// exercise the real thing that matters: GET_LOCK is per-session, so each
	// goroutine needs its own pooled connection, and whichever loses must
	// wait rather than apply concurrently. Both must still report success —
	// one applies, one finds everything already known.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	results := make([]*MigrateResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = Migrate(ctx, db)
		}(i)
	}
	wg.Wait()

	appliedTotal := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent migrate %d: %v", i, err)
		}
		appliedTotal += len(results[i].Applied)
	}
	if appliedTotal == 0 {
		t.Fatal("neither concurrent run applied anything")
	}
	if appliedTotal > len(results[0].Applied)+len(results[1].Applied) {
		t.Fatal("impossible: applied counts exceed total files")
	}

	// Exactly one of the two must have done the real work; the other must
	// have observed the ledger already filled in under the lock.
	if (len(results[0].Applied) == 0) == (len(results[1].Applied) == 0) {
		t.Fatalf("expected exactly one concurrent run to apply migrations, got %v and %v",
			results[0].Applied, results[1].Applied)
	}
}

func TestOpenFailsFastOnUnreachableHost(t *testing.T) {
	// Port 1 is reserved and never served; a 2s dial timeout keeps this fast.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := Open(ctx, "nope:nope@tcp(127.0.0.1:1)/nope")
	if err == nil {
		t.Fatal("Open must fail against a host that is not there")
	}
	if !strings.Contains(err.Error(), "ping") {
		t.Fatalf("error should name the ping step, got: %v", err)
	}
}
