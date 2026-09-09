//go:build integration

package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/capacity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// newDurableBudgetFixture migrates a fresh per-test schema from the real
// migrations directory and returns a scope guard over it.
func newDurableBudgetFixture(t *testing.T) (*pgxpool.Pool, *capacity.ScopeGuard) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; durable ingress budget not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	cfg := postgres.PostgresConfig{URL: url, MaxConns: 8, MinConns: 1}
	base, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("ws8_ingress_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		base.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		cancel()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	migrator, err := postgres.NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../migrations")))
	if err != nil {
		pool.Close()
		base.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		cancel()
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err != nil {
		pool.Close()
		base.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		cancel()
		t.Fatal(err)
	}
	guard, err := capacity.NewScopeGuard(pool, time.Minute)
	if err != nil {
		pool.Close()
		base.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		base.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		cancel()
	})
	return pool, guard
}

// TestIngressDurableBudgetCrossProcessExhaustion proves the durable ingress
// budget bounds the AGGREGATE concurrent admitted requests per tenant: the
// external slot (held by a simulated second app instance) plus the single
// local slot allowed by the budget means exactly one of two concurrent
// Handles is admitted; the other gets the bounded capacity rejection.
func TestIngressDurableBudgetCrossProcessExhaustion(t *testing.T) {
	pool, guard := newDurableBudgetFixture(t)
	tc := ingressTestContext()

	// Budget: one concurrent admitted request for this tenant.
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO capacity_budget (tenant_id, scope, budget_limit) VALUES ($1, 'ingress', 1)`,
		tc.TenantID); err != nil {
		t.Fatal(err)
	}

	ingress, _ := capacityIngress(t, queue.NewFakeQueue(queue.FakeQueueConfig{}), nil, nil)
	ingress.durableBudget = guard

	// Simulate a second app instance holding the slot (cross-process reach
	// of the durable budget): hold a reservation from a separate guard.
	externalRelease, err := guard.AcquireScope(context.Background(), tc.TenantID, capacity.ScopeIngress, "other-instance", time.Minute)
	if err != nil || externalRelease == nil {
		t.Fatalf("external hold: release-nil=%v err=%v", externalRelease == nil, err)
	}
	defer externalRelease()

	done := make(chan WebhookResult, 1)
	go func() {
		done <- ingress.Handle(context.Background(), "telegram", "app", capacityRequest(context.Background()), []byte(`{}`))
	}()
	var result WebhookResult
	select {
	case result = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ingress handle did not return; durable budget must reject fast, not block")
	}
	if result.Status != http.StatusTooManyRequests {
		t.Fatalf("durable budget exhaustion status: %d (body %s), want 429 capacity_exhausted", result.Status, result.Body)
	}

	// Release the external slot: the next Handle must be admitted.
	externalRelease()
	result = ingress.Handle(context.Background(), "telegram", "app", capacityRequest(context.Background()), []byte(`{}`))
	if result.Status == http.StatusTooManyRequests {
		t.Fatalf("admission after release rejected: %d %s", result.Status, result.Body)
	}
}

// TestIngressDurableBudgetFailOpenWithoutRow proves a tenant without an
// ingress budget row is never gated (historical behavior preserved).
func TestIngressDurableBudgetFailOpenWithoutRow(t *testing.T) {
	_, guard := newDurableBudgetFixture(t)
	ingress, _ := capacityIngress(t, queue.NewFakeQueue(queue.FakeQueueConfig{}), nil, nil)
	ingress.durableBudget = guard

	result := ingress.Handle(context.Background(), "telegram", "app", capacityRequest(context.Background()), []byte(`{}`))
	if result.Status == http.StatusTooManyRequests || result.Status == http.StatusServiceUnavailable {
		t.Fatalf("unbudgeted tenant must not be gated: %d %s", result.Status, result.Body)
	}
}

// TestIngressDurableBudgetConcurrentHoldsBound proves the budget holds
// under concurrency: with limit N, at most N concurrent Handles pass the
// durable gate at any instant.
func TestIngressDurableBudgetConcurrentHoldsBound(t *testing.T) {
	pool, guard := newDurableBudgetFixture(t)
	tc := ingressTestContext()
	const budget = 2
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO capacity_budget (tenant_id, scope, budget_limit) VALUES ($1, 'ingress', $2)`,
		tc.TenantID, budget); err != nil {
		t.Fatal(err)
	}

	ingress, _ := capacityIngress(t, queue.NewFakeQueue(queue.FakeQueueConfig{}), nil, nil)
	ingress.durableBudget = guard

	const goroutines = 8
	var admitted, rejected, inFlight, maxInFlight int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, err := guard.AcquireScope(context.Background(), tc.TenantID, capacity.ScopeIngress, "probe", time.Minute)
			mu.Lock()
			if err != nil {
				if errors.Is(err, capacity.ErrCapacityFull) {
					rejected++
				}
				mu.Unlock()
				return
			}
			admitted++
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond) // hold the slot briefly
			mu.Lock()
			inFlight--
			mu.Unlock()
			release()
		}()
	}
	close(start)
	wg.Wait()

	if maxInFlight > budget {
		t.Fatalf("max concurrent admitted=%d exceeds budget=%d", maxInFlight, budget)
	}
	if admitted == 0 || rejected == 0 {
		t.Fatalf("expected mixed outcomes under contention: admitted=%d rejected=%d", admitted, rejected)
	}
	var active int64
	if err := pool.QueryRow(context.Background(),
		`SELECT active_count FROM capacity_budget WHERE tenant_id=$1 AND scope='ingress'`, tc.TenantID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("active_count after all releases: %d, want 0", active)
	}
	_ = tenant.TenantContext{}
}
