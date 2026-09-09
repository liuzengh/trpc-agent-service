//go:build integration

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/migrations"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/queuebench"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
)

func benchmarkTestDatabase(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL integration")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatal("TEST_DATABASE_URL must be a PostgreSQL URL")
	}
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal("open integration PostgreSQL")
	}
	t.Cleanup(func() { _ = admin.Close() })
	name := "queuebench_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+pq.QuoteIdentifier(name)); err != nil {
		t.Fatalf("create isolated benchmark database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP DATABASE "+pq.QuoteIdentifier(name)); err != nil {
			t.Errorf("drop isolated benchmark database: %v", err)
		}
	})
	parsed.Path = "/" + name
	db, err := sql.Open("postgres", parsed.String())
	if err != nil {
		t.Fatal("open isolated benchmark database")
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, parsed.String()
}

func TestRunPostgresCompletesMatrixAndCleansOwnedRows(t *testing.T) {
	db, dsn := benchmarkTestDatabase(t)
	output := filepath.Join(t.TempDir(), "results.json")
	if err := run(dsn, 16, output, 0, time.Minute); err != nil {
		t.Fatalf("run full benchmark matrix: %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Results []queuebench.Result `json:"results"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != 24 {
		t.Fatalf("benchmark cases = %d, want 24", len(report.Results))
	}
	seen := make(map[string]map[int]bool)
	for _, result := range report.Results {
		key := result.Scenario + "/" + result.Mode
		if seen[key] == nil {
			seen[key] = make(map[int]bool)
		}
		if seen[key][result.Consumers] {
			t.Fatalf("duplicate case %s/%d", key, result.Consumers)
		}
		seen[key][result.Consumers] = true
		if !result.Verified || result.Messages != 32 || result.Completed != 32 ||
			result.PersistedCompleted != 32 || result.PersistedReplies != 32 ||
			result.PersistedInflight != 0 || result.DuplicateClaims != 0 || result.DuplicateEnqueues != 2 {
			t.Fatalf("case failed durable or duplicate checks: %+v", result)
		}
	}
	for _, scenario := range []string{"uniform", "weighted", "weighted_hotspot"} {
		for _, mode := range []string{"ordinary", "fair"} {
			for _, consumers := range []int{1, 4, 8, 16} {
				if !seen[scenario+"/"+mode][consumers] {
					t.Fatalf("missing case %s/%s/%d", scenario, mode, consumers)
				}
			}
		}
	}
	for _, table := range []string{"tenants", "inbox_messages", "outbox_messages", "inbox_session_sequences", "tenant_queue_schedule"} {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM " + pq.QuoteIdentifier(table)).Scan(&count); err != nil || count != 0 {
			t.Errorf("after run %s rows = %d, err = %v", table, count, err)
		}
	}
}

func seedBenchmarkQueue(t *testing.T, db *sql.DB, ids []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, id := range ids {
		if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,$1,'active','{}')`, id); err != nil {
			t.Fatal(err)
		}
	}
	cfg := queuebench.DefaultConfig()
	cfg.MessagesPerTenant, cfg.WorkDuration = 16, 0
	if _, err := queuebench.Execute(ctx, reliable.NewPostgresStore(db), cfg, ids, "uniform", "ordinary", 1); err != nil {
		t.Fatalf("seed completed benchmark queue: %v", err)
	}
}

func assertBenchmarkRows(t *testing.T, db *sql.DB, ids []string, tenants, messages int) {
	t.Helper()
	for _, check := range []struct {
		table, column string
		want          int
	}{
		{"tenants", "id", tenants},
		{"inbox_messages", "tenant_id", messages},
		{"outbox_messages", "tenant_id", messages},
		{"inbox_session_sequences", "tenant_id", messages},
		{"tenant_queue_schedule", "tenant_id", min(tenants, messages)},
	} {
		var count int
		query := "SELECT count(*) FROM " + pq.QuoteIdentifier(check.table) + " WHERE " + check.column + " = ANY($1)"
		if err := db.QueryRow(query, pqArray(ids)).Scan(&count); err != nil || count != check.want {
			t.Errorf("%s scoped rows = %d, want %d; err = %v", check.table, count, check.want, err)
		}
	}
}

func TestResetPostgresIsScopedAndAtomic(t *testing.T) {
	for _, failure := range []string{"none", "delete", "recreate"} {
		t.Run(failure, func(t *testing.T) {
			db, _ := benchmarkTestDatabase(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := migrations.NewRunner(db).Up(ctx); err != nil {
				t.Fatal(err)
			}
			owned := []string{"queuebench-" + uuid.NewString(), "queuebench-" + uuid.NewString()}
			foreign := []string{"queuebench-" + uuid.NewString(), "queuebench-" + uuid.NewString()}
			seedBenchmarkQueue(t, db, owned)
			seedBenchmarkQueue(t, db, foreign)
			if failure == "delete" {
				if _, err := db.ExecContext(ctx, `CREATE TABLE reset_blocker (tenant_id TEXT REFERENCES tenants(id))`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `INSERT INTO reset_blocker VALUES($1)`, owned[1]); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "recreate" {
				if _, err := db.ExecContext(ctx, `CREATE FUNCTION reject_benchmark_recreate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN IF NEW.id = TG_ARGV[0] THEN RAISE EXCEPTION 'injected recreate failure'; END IF; RETURN NEW; END $$`); err != nil {
					t.Fatal(err)
				}
				statement := `CREATE TRIGGER reject_benchmark_recreate BEFORE INSERT ON tenants FOR EACH ROW EXECUTE FUNCTION reject_benchmark_recreate(` + pq.QuoteLiteral(owned[1]) + `)`
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			err := resetBenchmarkTenants(ctx, db, owned, true)
			if failure != "none" {
				if err == nil {
					t.Fatal("injected failure did not abort reset")
				}
				assertBenchmarkRows(t, db, owned, 2, 32)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertBenchmarkRows(t, db, owned, 2, 0)
				if err := resetBenchmarkTenants(ctx, db, owned, false); err != nil {
					t.Fatal(err)
				}
				assertBenchmarkRows(t, db, owned, 0, 0)
			}
			assertBenchmarkRows(t, db, foreign, 2, 32)
		})
	}
}

func TestBenchmarkPostgresLockUsesReservedConnection(t *testing.T) {
	db, _ := benchmarkTestDatabase(t)
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	holder, err := acquireBenchmarkLock(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if got := db.Stats().InUse; got != 1 {
		t.Fatalf("reserved lock connections = %d, want 1", got)
	}
	competitor, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer competitor.Close()
	tryLock := func(want bool) {
		t.Helper()
		var locked bool
		if err := competitor.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtextextended('trpc-agent-queuebench', 0))`).Scan(&locked); err != nil || locked != want {
			t.Fatalf("competing lock = %v, want %v; err = %v", locked, want, err)
		}
	}
	tryLock(false)
	if err := competitor.Close(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := db.ExecContext(ctx, `SELECT 1`); err != nil {
			t.Fatal(err)
		}
	}
	competitor, err = db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer competitor.Close()
	tryLock(false)
	if err := releaseBenchmarkLock(holder); err != nil {
		t.Fatal(err)
	}
	tryLock(true)
	if _, err := competitor.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtextextended('trpc-agent-queuebench', 0))`); err != nil {
		t.Fatal(err)
	}
	if err := competitor.Close(); err != nil {
		t.Fatal(err)
	}
	if got := db.Stats().InUse; got != 0 {
		t.Fatalf("connections retained after unlock = %d", got)
	}
}
