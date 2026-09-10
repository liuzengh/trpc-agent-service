package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

func TestSettlementAndReleasePropagateTransactionFailures(t *testing.T) {
	for _, operation := range []string{"settle", "release"} {
		for _, stage := range []string{"begin", "reservation query", "ledger query", "ledger update", "reservation update", "commit", "idempotent commit"} {
			t.Run(operation+"/"+stage, func(t *testing.T) {
				store, db, mock := newMockBudgetStore(t)
				t.Cleanup(func() { _ = db.Close() })
				cause := errors.New("database unavailable")
				expectBudgetTransactionFailure(mock, operation, stage, cause)
				var result budget.Reservation
				var err error
				if operation == "settle" {
					result, err = store.Settle(context.Background(), budgetTestTenantID, "request-1", budget.Usage{InputTokens: 4, OutputTokens: 6, SpendMinor: 7})
				} else {
					result, err = store.Release(context.Background(), budgetTestTenantID, "request-1")
				}
				if !errors.Is(err, ErrStorage) || !errors.Is(err, cause) || result.State != "" {
					t.Fatalf("result=%+v, error=%v; want storage failure preserving cause", result, err)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// Each failure must end the transaction before any later write is attempted.
func expectBudgetTransactionFailure(mock sqlmock.Sqlmock, operation, stage string, cause error) {
	begin := mock.ExpectBegin()
	if stage == "begin" {
		begin.WillReturnError(cause)
		return
	}
	if stage == "reservation query" {
		mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WillReturnError(cause)
		mock.ExpectRollback()
		return
	}
	state := budget.ReservationStateReserved
	if stage == "idempotent commit" {
		state = budget.ReservationStateSettled
	}
	period := normalizePeriod(budgetTestPeriod)
	expectReservationQuery(mock, budgetTestTenantID, "request-1", period, int64(100), int64(50), "USD", int64(30), int64(5), int64(10), int64(7), state)
	if stage == "idempotent commit" {
		mock.ExpectCommit().WillReturnError(cause)
		return
	}
	if stage == "ledger query" {
		mock.ExpectQuery("SELECT used_tokens, reserved_tokens, used_minor, reserved_minor").WillReturnError(cause)
		mock.ExpectRollback()
		return
	}
	expectLedgerQuery(mock, budgetTestTenantID, period, 0, 30, 0, 5)
	for _, table := range []string{"ledger", "reservation"} {
		update := mock.ExpectExec("UPDATE public\\.runtime_budget_" + table)
		if stage == table+" update" {
			update.WillReturnError(cause)
			mock.ExpectRollback()
			return
		}
		update.WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit().WillReturnError(cause)
}

func TestSettlementAndReleaseRejectCanceledContexts(t *testing.T) {
	store, db, mock := newMockBudgetStore(t)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Settle(ctx, budgetTestTenantID, "request-1", budget.Usage{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("settlement error = %v", err)
	}
	if _, err := store.Release(ctx, budgetTestTenantID, "request-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("release error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
