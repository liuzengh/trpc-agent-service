//go:build integration

package capacity

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testDBURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; capacity integration not verified")
	}
	return url
}

func setupBudgetSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS capacity_budget (
			tenant_id text NOT NULL, scope text NOT NULL,
			budget_limit bigint NOT NULL CHECK (budget_limit >= 0),
			active_count bigint NOT NULL DEFAULT 0 CHECK (active_count >= 0),
			config_version bigint NOT NULL DEFAULT 1,
			enabled boolean NOT NULL DEFAULT true,
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant_id, scope),
			CHECK (active_count <= budget_limit)
		)`,
		`CREATE TABLE IF NOT EXISTS capacity_reservation (
			reservation_id text NOT NULL, tenant_id text NOT NULL,
			scope text NOT NULL, owner_id text NOT NULL,
			budget_scope text NOT NULL DEFAULT 'ingress',
			state text NOT NULL DEFAULT 'active',
			created_at timestamptz NOT NULL DEFAULT now(),
			released_at timestamptz, expires_at timestamptz NOT NULL,
			PRIMARY KEY (tenant_id, reservation_id)
		)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("setup: %v", err)
		}
	}
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DROP TABLE IF EXISTS capacity_reservation")
		pool.Exec(context.Background(), "DROP TABLE IF EXISTS capacity_budget")
	})
}

func seedBudget(t *testing.T, pool *pgxpool.Pool, tenant, scope string, limit int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO capacity_budget (tenant_id, scope, budget_limit) VALUES ($1,$2,$3)
		 ON CONFLICT (tenant_id, scope) DO UPDATE SET budget_limit = $3, active_count = 0, enabled = true`,
		tenant, scope, limit); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicBudgetUnderConcurrency(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testDBURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	setupBudgetSchema(t, pool)

	const (
		tenant     = "cap-concurrent-tenant"
		scope      = "ingress"
		budget     = 5
		goroutines = 20
	)
	seedBudget(t, pool, tenant, scope, budget)

	var success, full, otherErr atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			tx, err := pool.Begin(ctx)
			if err != nil {
				otherErr.Add(1)
				return
			}
			_, err = Acquire(ctx, tx, tenant, scope, fmt.Sprintf("owner-%d", id), 60*time.Second)
			if err != nil {
				tx.Rollback(ctx)
				if strings.Contains(err.Error(), "budget exhausted") {
					full.Add(1)
				} else {
					otherErr.Add(1)
				}
				return
			}
			// Commit to actually consume the budget slot (held reservation)
			if err := tx.Commit(ctx); err != nil {
				otherErr.Add(1)
				return
			}
			success.Add(1)
		}(i)
	}
	wg.Wait()

	if success.Load() != int64(budget) {
		t.Fatalf("success: got %d want %d (full=%d other=%d)", success.Load(), budget, full.Load(), otherErr.Load())
	}
	if full.Load() != int64(goroutines-budget) {
		t.Fatalf("full: got %d want %d", full.Load(), goroutines-budget)
	}
	if otherErr.Load() != 0 {
		t.Fatalf("other errors: %d", otherErr.Load())
	}
}

func TestAtomicBudgetSustainedReservation(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testDBURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	setupBudgetSchema(t, pool)
	seedBudget(t, pool, "sustained-tenant", "ingress", 3)

	var resIDs []string
	for i := 0; i < 3; i++ {
		tx, err := pool.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		resID, err := Acquire(context.Background(), tx, "sustained-tenant", "ingress", "owner", 60*time.Second)
		if err != nil {
			tx.Rollback(context.Background())
			t.Fatalf("acquire %d: %v", i, err)
		}
		resIDs = append(resIDs, resID)
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var active int64
	pool.QueryRow(context.Background(),
		"SELECT active_count FROM capacity_budget WHERE tenant_id=$1 AND scope=$2", "sustained-tenant", "ingress").Scan(&active)
	if active != 3 {
		t.Fatalf("active count: got %d want 3", active)
	}
	for _, resID := range resIDs {
		tx, err := pool.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := Release(context.Background(), tx, "sustained-tenant", "ingress", resID, "owner"); err != nil {
			tx.Rollback(context.Background())
			t.Fatal(err)
		}
		tx.Commit(context.Background())
	}
	var finalActive int64
	pool.QueryRow(context.Background(),
		"SELECT active_count FROM capacity_budget WHERE tenant_id=$1 AND scope=$2", "sustained-tenant", "ingress").Scan(&finalActive)
	if finalActive != 0 {
		t.Fatalf("active count after all releases: %d", finalActive)
	}
}

func TestStaleReservationReconcile(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testDBURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	setupBudgetSchema(t, pool)

	_, err = pool.Exec(context.Background(),
		`INSERT INTO capacity_reservation (reservation_id, tenant_id, scope, owner_id, budget_scope, state, expires_at)
		 VALUES ('stale-res-' || to_char(now(), 'US'), 'stale-tenant', 'ingress', 'crashed-owner', 'ingress', 'active', now() - interval '1 hour')`)
	if err != nil {
		t.Fatal(err)
	}
	pool.Exec(context.Background(),
		`INSERT INTO capacity_budget (tenant_id, scope, budget_limit, active_count)
		 VALUES ('stale-tenant', 'ingress', 5, 1)
		 ON CONFLICT (tenant_id, scope) DO UPDATE SET active_count = 1`)

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	released, err := ReconcileStale(context.Background(), tx)
	if err != nil {
		t.Fatal(err)
	}
	tx.Commit(context.Background())
	if released < 1 {
		t.Fatalf("reconcile released %d, want >= 1", released)
	}
	var activeAfter int64
	pool.QueryRow(context.Background(),
		"SELECT active_count FROM capacity_budget WHERE tenant_id='stale-tenant' AND scope='ingress'").Scan(&activeAfter)
	if activeAfter != 0 {
		t.Fatalf("active_count after reconcile: %d, want 0", activeAfter)
	}
}
