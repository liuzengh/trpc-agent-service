//go:build integration

package capacity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func newGuardFixture(t *testing.T) (*pgxpool.Pool, *ScopeGuard) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testDBURL(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	setupBudgetSchema(t, pool)
	guard, err := NewScopeGuard(pool, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return pool, guard
}

func assertActiveCount(t *testing.T, pool *pgxpool.Pool, tenant, scope string, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(context.Background(),
		`SELECT active_count FROM capacity_budget WHERE tenant_id = $1 AND scope = $2`,
		tenant, scope).Scan(&got); err != nil {
		t.Fatalf("active count lookup: %v", err)
	}
	if got != want {
		t.Fatalf("active_count for %s/%s: got %d want %d", tenant, scope, got, want)
	}
}

func TestScopeGuardFailOpenWithoutBudgetRow(t *testing.T) {
	_, guard := newGuardFixture(t)
	release, err := guard.AcquireScope(context.Background(), "no-budget-tenant", ScopeIngress, "owner-1", time.Minute)
	if err != nil {
		t.Fatalf("fail-open acquire: %v", err)
	}
	if release != nil {
		t.Fatalf("no budget row means not enforced: want nil release, got %T", release)
	}
}

func TestScopeGuardEnforceAndRelease(t *testing.T) {
	pool, guard := newGuardFixture(t)
	seedBudget(t, pool, "guard-tenant", ScopeIngress, 1)

	release, err := guard.AcquireScope(context.Background(), "guard-tenant", ScopeIngress, "owner-1", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if release == nil {
		t.Fatal("enforced acquire returned nil release")
	}
	assertActiveCount(t, pool, "guard-tenant", ScopeIngress, 1)

	// The single slot is held: a second acquire is rejected.
	if _, err := guard.AcquireScope(context.Background(), "guard-tenant", ScopeIngress, "owner-2", time.Minute); !errors.Is(err, ErrCapacityFull) {
		t.Fatalf("second acquire: want ErrCapacityFull got %v", err)
	}

	// Release restores the slot; re-acquire succeeds.
	release()
	assertActiveCount(t, pool, "guard-tenant", ScopeIngress, 0)

	release2, err := guard.AcquireScope(context.Background(), "guard-tenant", ScopeIngress, "owner-3", time.Minute)
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}
	release2()
}

func TestScopeGuardDisabledRowNotEnforced(t *testing.T) {
	pool, guard := newGuardFixture(t)
	seedBudget(t, pool, "disabled-tenant", ScopeWorker, 1)
	if _, err := pool.Exec(context.Background(),
		`UPDATE capacity_budget SET enabled = false WHERE tenant_id = $1`, "disabled-tenant"); err != nil {
		t.Fatal(err)
	}
	release, err := guard.AcquireScope(context.Background(), "disabled-tenant", ScopeWorker, "owner-1", time.Minute)
	if err != nil {
		t.Fatalf("disabled-row acquire: %v", err)
	}
	if release != nil {
		t.Fatal("disabled budget row must not be enforced")
	}
}

func TestScopeGuardScopesAreIndependent(t *testing.T) {
	pool, guard := newGuardFixture(t)
	seedBudget(t, pool, "multi-tenant", ScopeIngress, 1)
	seedBudget(t, pool, "multi-tenant", ScopeSender, 2)

	rIngress, err := guard.AcquireScope(context.Background(), "multi-tenant", ScopeIngress, "owner-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guard.AcquireScope(context.Background(), "multi-tenant", ScopeIngress, "owner-2", time.Minute); !errors.Is(err, ErrCapacityFull) {
		t.Fatalf("ingress full: want ErrCapacityFull got %v", err)
	}
	rSender, err := guard.AcquireScope(context.Background(), "multi-tenant", ScopeSender, "owner-1", time.Minute)
	if err != nil {
		t.Fatalf("sender independent of ingress: %v", err)
	}
	rIngress()
	rSender()
	assertActiveCount(t, pool, "multi-tenant", ScopeIngress, 0)
	assertActiveCount(t, pool, "multi-tenant", ScopeSender, 0)
}

func TestScopeGuardInvalidArgs(t *testing.T) {
	_, guard := newGuardFixture(t)
	if _, err := guard.AcquireScope(context.Background(), "", ScopeIngress, "o", time.Minute); !errors.Is(err, ErrInvalidScopeRequest) {
		t.Fatalf("empty tenant: want ErrInvalidScopeRequest got %v", err)
	}
	if _, err := guard.AcquireScope(context.Background(), "t", "", "o", time.Minute); !errors.Is(err, ErrInvalidScopeRequest) {
		t.Fatalf("empty scope: want ErrInvalidScopeRequest got %v", err)
	}
	if _, err := guard.AcquireScope(context.Background(), "t", ScopeIngress, "", time.Minute); !errors.Is(err, ErrInvalidScopeRequest) {
		t.Fatalf("empty owner: want ErrInvalidScopeRequest got %v", err)
	}
	if _, err := NewScopeGuard(nil, time.Minute); !errors.Is(err, ErrInvalidScopeRequest) {
		t.Fatalf("nil pool: want ErrInvalidScopeRequest got %v", err)
	}
}
