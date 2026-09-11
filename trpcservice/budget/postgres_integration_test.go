package budget

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyl6/trpc-agent-service/migrations"
)

func newPostgresIntegrationLedger(t *testing.T) (*Postgres, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	schema := "budget_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create PostgreSQL integration schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatalf("open PostgreSQL schema pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL integration schema: %v", err)
		}
		admin.Close()
	})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyAll(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("apply runtime migrations: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	ledger, err := NewPostgres(pool)
	if err != nil {
		t.Fatal(err)
	}
	return ledger, pool
}

func TestPostgresLedgerConcurrentReservationsDoNotDoubleTheLimit(t *testing.T) {
	ledger, _ := newPostgresIntegrationLedger(t)
	other, err := NewPostgres(ledger.pool)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const contenders = 16
	results := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := testReserve("tenant-pg", "call-pg-"+uuid.NewString(), 100, 60)
			var reserveErr error
			if i%2 == 0 {
				_, reserveErr = ledger.Reserve(ctx, req)
			} else {
				_, reserveErr = other.Reserve(ctx, req)
			}
			results <- reserveErr
		}(i)
	}
	wg.Wait()
	close(results)
	passed := 0
	for err := range results {
		if err == nil {
			passed++
		} else if !errors.Is(err, ErrBudgetExceeded) {
			t.Fatalf("unexpected reservation error = %v", err)
		}
	}
	if passed != 1 {
		t.Fatalf("PostgreSQL admitted %d concurrent reservations, want 1", passed)
	}
	snapshot, err := ledger.Snapshot(ctx, "tenant-pg", time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || snapshot.ReservedUnits != 60 {
		t.Fatalf("period snapshot = %+v, %v", snapshot, err)
	}
}

func TestPostgresLedgerUnknownAndCrossMonthSettlement(t *testing.T) {
	ledger, _ := newPostgresIntegrationLedger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	reserveAt := time.Date(2026, 9, 30, 23, 59, 59, 0, time.UTC)
	unknownReq := testReserve("tenant-pg", "call-unknown", 100, 40)
	unknownReq.BillingPeriod = reserveAt
	if _, err := ledger.Reserve(ctx, unknownReq); err != nil {
		t.Fatal(err)
	}
	if err := ledger.MarkUnknown(ctx, UnknownRequest{TenantID: unknownReq.TenantID, CallID: unknownReq.CallID, ErrorType: "timeout"}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.MarkUnknown(ctx, UnknownRequest{TenantID: unknownReq.TenantID, CallID: unknownReq.CallID, ErrorType: "timeout"}); err != nil {
		t.Fatalf("idempotent MarkUnknown() = %v", err)
	}
	snapshot, err := ledger.Snapshot(ctx, unknownReq.TenantID, reserveAt)
	if err != nil || snapshot.UnknownUnits != 40 || snapshot.ReservedUnits != 0 {
		t.Fatalf("unknown snapshot = %+v, %v", snapshot, err)
	}
	if _, err := ledger.Reserve(ctx, testReserve("tenant-pg", "call-denied", 100, 61)); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("unknown charge was refunded, Reserve() error = %v", err)
	}

	settleReq := testReserve("tenant-pg", "call-cross-month", 200, 10)
	settleReq.BillingPeriod = reserveAt
	if _, err := ledger.Reserve(ctx, settleReq); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Settle(ctx, SettlementRequest{
		TenantID: settleReq.TenantID, CallID: settleReq.CallID, PromptTokens: 3,
		CompletionTokens: 4, ActualUnits: 25, SettledAt: reserveAt.Add(2 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	oldPeriod, err := ledger.Snapshot(ctx, settleReq.TenantID, reserveAt)
	if err != nil {
		t.Fatal(err)
	}
	newPeriod, err := ledger.Snapshot(ctx, settleReq.TenantID, reserveAt.Add(24*time.Hour))
	if err != nil {
		// The new period has no row, which is also proof settlement did not move.
		if !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	}
	if oldPeriod.SettledUnits != 25 || newPeriod.SettledUnits != 0 {
		t.Fatalf("cross-month settlement old=%+v new=%+v", oldPeriod, newPeriod)
	}
}
