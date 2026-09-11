package migrations_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

// Exercise an actual three-migration database, not an empty fresh-install-only
// path. The old migration bytes/ledger identities must survive the new upgrade.
func TestIntegrationApplyLagMigrationUpgradesExistingDatabase(t *testing.T) {
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("GATEWAY_TEST_DATABASE_URL required")
	}
	for _, backlog := range []bool{false, true} {
		t.Run(fmt.Sprintf("backlog_%t", backlog), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			admin, err := pgxpool.New(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			schema := fmt.Sprintf("gateway_upgrade_%d", time.Now().UnixNano())
			if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				c, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				if _, e := admin.Exec(c, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); e != nil {
					t.Error(e)
				}
			}()
			cfg, err := pgxpool.ParseConfig(dsn)
			if err != nil {
				t.Fatal(err)
			}
			cfg.ConnConfig.RuntimeParams["search_path"] = schema
			pool, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				c, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				_ = tx.Rollback(c)
			}()
			if _, err = tx.Exec(ctx, `CREATE TABLE gateway_schema_migrations(version text PRIMARY KEY,digest text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
				t.Fatal(err)
			}
			old := []string{"0001_gateway.sql", "0002_admission_budget.sql", "0003_routing_replay.sql"}
			for _, name := range old {
				body, e := migrations.Files.ReadFile(name)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = tx.Exec(ctx, string(body)); e != nil {
					t.Fatal(e)
				}
				if _, e = tx.Exec(ctx, `INSERT INTO gateway_schema_migrations(version,digest) VALUES($1,$2)`, name, fmt.Sprintf("%x", sha256.Sum256(body))); e != nil {
					t.Fatal(e)
				}
			}
			highest := 1
			if backlog {
				highest = 2
			}
			if _, err = tx.Exec(ctx, `INSERT INTO gateway_route_replay_state(singleton,stream_name,stream_id,target_sequence,contiguous_sequence,highest_sequence) VALUES(true,'ROUTE_TEST','2026-09-05T00:00:00Z',1,1,$1)`, highest); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			before := time.Now()
			var wg sync.WaitGroup
			errs := make(chan error, 4)
			for range 4 {
				wg.Go(func() { errs <- migrations.Apply(ctx, pool) })
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			var n int
			if err = pool.QueryRow(ctx, `SELECT count(*) FROM gateway_schema_migrations`).Scan(&n); err != nil || n != embeddedMigrationCount(t) {
				t.Fatalf("migration count=%d err=%v", n, err)
			}
			for _, name := range old {
				body, _ := migrations.Files.ReadFile(name)
				var digest string
				if err = pool.QueryRow(ctx, `SELECT digest FROM gateway_schema_migrations WHERE version=$1`, name).Scan(&digest); err != nil || digest != fmt.Sprintf("%x", sha256.Sum256(body)) {
					t.Fatalf("old migration changed: %s err=%v", name, err)
				}
			}
			var since *time.Time
			var target, contiguous, gotHighest int
			if err = pool.QueryRow(ctx, `SELECT apply_lag_since,target_sequence,contiguous_sequence,highest_sequence FROM gateway_route_replay_state`).Scan(&since, &target, &contiguous, &gotHighest); err != nil {
				t.Fatal(err)
			}
			if target != 1 || contiguous != 1 || gotHighest != highest {
				t.Fatal("upgrade rewrote source watermarks")
			}
			if backlog {
				if since == nil || since.Before(before.Add(-time.Second)) || since.After(time.Now().Add(time.Second)) {
					t.Fatalf("upgrade did not conservatively begin backlog episode: %v", since)
				}
			} else if since != nil {
				t.Fatalf("caught-up source got false lag: %v", since)
			}
			if err = migrations.Apply(ctx, pool); err != nil {
				t.Fatal(err)
			}
			var again *time.Time
			if err = pool.QueryRow(ctx, `SELECT apply_lag_since FROM gateway_route_replay_state`).Scan(&again); err != nil {
				t.Fatal(err)
			}
			if (since == nil) != (again == nil) || (since != nil && !since.Equal(*again)) {
				t.Fatal("idempotent migration reset lag clock")
			}
			t.Log("UPGRADE_VERIFIED: existing 0001..0003 preserved; concurrent 0004 application; pending-only backfill; watermarks and episode retained")
		})
	}
}
