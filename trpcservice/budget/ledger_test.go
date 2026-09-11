package budget

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testReserve(tenant, callID string, limit, estimate int64) ReserveRequest {
	return ReserveRequest{
		TenantID: tenant, AppNamespace: tenant + "/app", SessionID: "session",
		DedupKey: "message", RequestID: "request", RunID: "run-" + callID,
		CallID: callID, CallNo: 1, ModelName: "model",
		BillingPeriod:     time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		MonthlyLimitUnits: limit, EstimatedUnits: estimate,
		EstimatedPrompt: 10, EstimatedOutput: 20,
	}
}

func TestMemoryLedgerSettlesAndKeepsUnknownCost(t *testing.T) {
	ledger := NewMemory()
	ctx := context.Background()
	reserved, err := ledger.Reserve(ctx, testReserve("tenant-a", "call-1", 100, 60))
	if err != nil || reserved.State != StateReserved {
		t.Fatalf("Reserve() = %+v, %v", reserved, err)
	}
	if err := ledger.Settle(ctx, SettlementRequest{TenantID: "tenant-a", CallID: "call-1", PromptTokens: 2, CompletionTokens: 3, ActualUnits: 40}); err != nil {
		t.Fatal(err)
	}
	snapshot := ledger.Snapshot("tenant-a", reserved.BillingPeriod)
	if snapshot.ReservedUnits != 0 || snapshot.SettledUnits != 40 || snapshot.UnknownUnits != 0 {
		t.Fatalf("settled snapshot = %+v", snapshot)
	}

	unknown, err := ledger.Reserve(ctx, testReserve("tenant-a", "call-2", 100, 50))
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.MarkUnknown(ctx, UnknownRequest{TenantID: "tenant-a", CallID: unknown.CallID, ErrorType: "timeout"}); err != nil {
		t.Fatal(err)
	}
	snapshot = ledger.Snapshot("tenant-a", reserved.BillingPeriod)
	if snapshot.UnknownUnits != 50 || snapshot.ReservedUnits != 0 {
		t.Fatalf("unknown snapshot = %+v", snapshot)
	}
	if _, err := ledger.Reserve(ctx, testReserve("tenant-a", "call-3", 100, 11)); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("unknown cost was refunded, Reserve() error = %v", err)
	}
}

func TestMemoryLedgerIdempotencyAndRetryCallIdentity(t *testing.T) {
	ledger := NewMemory()
	ctx := context.Background()
	req := testReserve("tenant-a", "call-1", 100, 10)
	first, err := ledger.Reserve(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ledger.Reserve(ctx, req)
	if err != nil || !second.Existing || second.CallID != first.CallID {
		t.Fatalf("duplicate Reserve() = %+v, %v", second, err)
	}
	conflict := req
	conflict.EstimatedUnits++
	if _, err := ledger.Reserve(ctx, conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting call ID error = %v", err)
	}
	if err := ledger.Settle(ctx, SettlementRequest{TenantID: req.TenantID, CallID: req.CallID, ActualUnits: 7}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Settle(ctx, SettlementRequest{TenantID: req.TenantID, CallID: req.CallID, ActualUnits: 7}); err != nil {
		t.Fatalf("idempotent Settle() = %v", err)
	}
	if err := ledger.Settle(ctx, SettlementRequest{TenantID: req.TenantID, CallID: req.CallID, ActualUnits: 8}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Settle() = %v", err)
	}
	// A real retry has a fresh call ID even when it belongs to the same message.
	retry := req
	retry.CallID, retry.RunID = "call-2", "run-retry"
	if _, err := ledger.Reserve(ctx, retry); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryLedgerKeepsActualOverageAndRejectsConflictingTokens(t *testing.T) {
	ledger := NewMemory()
	ctx := context.Background()
	req := testReserve("tenant-a", "call-overage", 100, 10)
	if _, err := ledger.Reserve(ctx, req); err != nil {
		t.Fatal(err)
	}
	settlement := SettlementRequest{TenantID: req.TenantID, CallID: req.CallID,
		PromptTokens: 2, CompletionTokens: 3, ActualUnits: 25}
	if err := ledger.Settle(ctx, settlement); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Settle(ctx, settlement); err != nil {
		t.Fatalf("same settlement was not idempotent: %v", err)
	}
	conflict := settlement
	conflict.CompletionTokens++
	if err := ledger.Settle(ctx, conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting token settlement = %v", err)
	}
	snapshot := ledger.Snapshot(req.TenantID, req.BillingPeriod)
	if snapshot.SettledUnits != 25 || snapshot.ReservedUnits != 0 {
		t.Fatalf("actual overage was lost: %+v", snapshot)
	}
	if _, err := ledger.Reserve(ctx, testReserve(req.TenantID, "call-after-overage", 100, 76)); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("actual overage did not consume budget: %v", err)
	}
}

func TestMemoryLedgerConcurrentReservationsDoNotDoubleTheLimit(t *testing.T) {
	ledger := NewMemory()
	const contenders = 20
	results := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := testReserve("tenant-a", "call-"+string(rune('a'+i)), 100, 60)
			_, err := ledger.Reserve(context.Background(), req)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	passed := 0
	for err := range results {
		if err == nil {
			passed++
		} else if !errors.Is(err, ErrBudgetExceeded) {
			t.Fatalf("unexpected concurrent reservation error = %v", err)
		}
	}
	if passed != 1 {
		t.Fatalf("concurrent reservations admitted %d calls, want 1", passed)
	}
}

func TestMemoryLedgerCrossMonthSettlementUsesReservationPeriod(t *testing.T) {
	ledger := NewMemory()
	ctx := context.Background()
	reserveAt := time.Date(2026, 9, 30, 23, 59, 59, 0, time.UTC)
	request := testReserve("tenant-a", "call-cross-month", 100, 40)
	request.BillingPeriod = reserveAt
	reservation, err := ledger.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Settle(ctx, SettlementRequest{TenantID: request.TenantID, CallID: request.CallID, ActualUnits: 20, SettledAt: reserveAt.Add(2 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	old := ledger.Snapshot(request.TenantID, reserveAt)
	newPeriod := ledger.Snapshot(request.TenantID, reserveAt.Add(24*time.Hour))
	if old.SettledUnits != 20 || newPeriod.SettledUnits != 0 || !reservation.BillingPeriod.Equal(PeriodStart(reserveAt)) {
		t.Fatalf("cross-month snapshots old=%+v new=%+v reservation=%+v", old, newPeriod, reservation)
	}
}
