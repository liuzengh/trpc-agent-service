package tenant

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// schemaPool returns a pool bound to a fresh, empty schema so the store's
// missing-table / bad-shape paths can be exercised without touching the
// shared public tables. The schema is dropped on cleanup.
func schemaPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	admin := testenv.PG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	schema := fmt.Sprintf("tenant_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })

	cfg, err := pgxpool.ParseConfig(testenv.PGDSN())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// minimalTables holds the CREATE TABLE statements for a reduced-but-scannable
// version of the routing tables (column names and types as the store scans
// them).
var minimalTables = map[string]string{
	"tenant": `CREATE TABLE tenant (
		id text, name text, model_config jsonb, tool_policy jsonb, audit_policy jsonb,
		guardrail_policy jsonb, rate_policy jsonb, storage_config jsonb, status text)`,
	"agent_app": `CREATE TABLE agent_app (
		id text, tenant_id text, name text, agent_type text, config jsonb, version int, status text)`,
	"channel_binding": `CREATE TABLE channel_binding (
		id text, tenant_id text, channel text, app_id text, webhook_path text,
		token_ref text, aeskey_ref text, config jsonb, status text)`,
}

func createTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := pool.Exec(ctx, minimalTables[n]); err != nil {
			t.Fatal(err)
		}
	}
}

// LoadAll stops at the first table it cannot read, propagating the error.
func TestPGStoreLoadAllMissingTable(t *testing.T) {
	cases := []struct {
		name   string
		tables []string
	}{
		{"tenant missing", nil},
		{"agent_app missing", []string{"tenant"}},
		{"channel_binding missing", []string{"tenant", "agent_app"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := schemaPool(t)
			createTables(t, context.Background(), pool, tc.tables...)
			if _, err := NewPGStore(pool).LoadAll(context.Background()); err == nil {
				t.Fatal("a missing routing table must fail the load, not yield an empty snapshot")
			}
		})
	}
}

// loadMigrations: a missing storage_migration table (a schema predating it)
// yields an empty set instead of an error.
func TestLoadMigrationsUndefinedTableYieldsEmpty(t *testing.T) {
	pool := schemaPool(t)
	createTables(t, context.Background(), pool, "tenant", "agent_app", "channel_binding")

	var d Data
	if err := NewPGStore(pool).loadMigrations(context.Background(), &d); err != nil {
		t.Fatalf("missing storage_migration must degrade to an empty set, got %v", err)
	}
	if len(d.Migrations) != 0 {
		t.Fatalf("want no migrations, got %+v", d.Migrations)
	}
}

// A storage_migration table with an unexpected shape fails the query (the
// error is wrapped, not swallowed as undefined-table), and LoadAll surfaces it.
func TestLoadMigrationsBadShape(t *testing.T) {
	pool := schemaPool(t)
	ctx := context.Background()
	createTables(t, ctx, pool, "tenant", "agent_app", "channel_binding")
	if _, err := pool.Exec(ctx, "CREATE TABLE storage_migration (phase text)"); err != nil {
		t.Fatal(err)
	}

	s := NewPGStore(pool)
	var d Data
	if err := s.loadMigrations(ctx, &d); err == nil {
		t.Fatal("want an error for a table missing the expected columns")
	}
	if _, err := s.LoadAll(ctx); err == nil {
		t.Fatal("LoadAll must propagate the storage_migration failure")
	}
}

// isUndefinedTable matches exactly the PG 42P01 error and nothing else.
func TestIsUndefinedTable(t *testing.T) {
	pool := testenv.PG(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, "SELECT 1 FROM no_such_table_for_tenant_store_test")
	if err == nil {
		t.Fatal("want an undefined-table error")
	}
	if !isUndefinedTable(err) {
		t.Fatalf("42P01 must match: %v", err)
	}
	if isUndefinedTable(errors.New("something else")) {
		t.Fatal("a plain error must not match")
	}
	if isUndefinedTable(nil) {
		t.Fatal("nil must not match")
	}
}
