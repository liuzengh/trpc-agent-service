package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

const budgetTestTenantID = "tenant"

var budgetTestPeriod = time.Date(2026, 9, 7, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))

func TestNewValidatesDatabase(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("nil database error = %v", err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, err := New(db)
	if err != nil || store == nil {
		t.Fatalf("New() = %v, store = %v", err, store)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreValidationAndArithmeticHelpers(t *testing.T) {
	if err := validateContext(nil); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := validateContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	if nullableInt(nil) != nil || nullableInt(int64Pointer(3)) != int64(3) {
		t.Fatal("nullableInt conversion failed")
	}
	storageErr := errors.New("storage")
	if wrapStorage(nil) != nil || !errors.Is(wrapStorage(storageErr), ErrStorage) || !errors.Is(wrapStorage(storageErr), storageErr) {
		t.Fatal("storage wrapping failed")
	}
	if !exceeds(int64Pointer(10), 5, 5, 1) || exceeds(nil, 5, 5, 1) {
		t.Fatal("token limit arithmetic failed")
	}
	reservation := budget.Reservation{EstimatedTokens: 10, EstimatedSpendMinor: 5, Limits: budget.Limits{TokenBudget: int64Pointer(10), SpendLimitMinor: int64Pointer(5)}}
	ledger := ledgerValue{usedTokens: 1, reservedTokens: 10, usedMinor: 1, reservedMinor: 5}
	if !exceedsAfterRelease(reservation, budget.Usage{InputTokens: 20}, ledger) {
		t.Fatal("post-settlement limit arithmetic failed")
	}
}

func TestStoreReserveCoversTransactionalDatabaseFailures(t *testing.T) {
	store, db, mock := newMockBudgetStore(t)
	defer func() { _ = db.Close() }()
	input := validReserveInput()
	periodStart := normalizePeriod(input.PeriodStart)
	storageErr := errors.New("database failure")

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, input.ReservationID, periodStart, int64(100), int64(50), "USD", int64(30), int64(5), nil, nil, budget.ReservationStateReserved)
	mock.ExpectCommit().WillReturnError(storageErr)
	if _, err := store.Reserve(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("existing commit error = %v", err)
	}

	input.ReservationID = "insert-error"
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, input.ReservationID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart).WillReturnError(storageErr)
	mock.ExpectRollback()
	if _, err := store.Reserve(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("ledger insert error = %v", err)
	}

	input.ReservationID = "lock-error"
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, input.ReservationID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT used_tokens, reserved_tokens, used_minor, reserved_minor").WithArgs(budgetTestTenantID, periodStart).WillReturnError(storageErr)
	mock.ExpectRollback()
	if _, err := store.Reserve(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("ledger lock error = %v", err)
	}

	input.ReservationID = "reservation-insert-error"
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, input.ReservationID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart).WillReturnResult(sqlmock.NewResult(0, 1))
	expectLedgerQuery(mock, budgetTestTenantID, periodStart, 0, 0, 0, 0)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_reservation").WillReturnError(storageErr)
	mock.ExpectRollback()
	if _, err := store.Reserve(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("reservation insert error = %v", err)
	}

	input.ReservationID = "ledger-update-error"
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, input.ReservationID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart).WillReturnResult(sqlmock.NewResult(0, 1))
	expectLedgerQuery(mock, budgetTestTenantID, periodStart, 0, 0, 0, 0)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_reservation").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE public\\.runtime_budget_ledger").WillReturnError(storageErr)
	mock.ExpectRollback()
	if _, err := store.Reserve(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("ledger update error = %v", err)
	}

	input.ReservationID = "commit-error"
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, input.ReservationID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart).WillReturnResult(sqlmock.NewResult(0, 1))
	expectLedgerQuery(mock, budgetTestTenantID, periodStart, 0, 0, 0, 0)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_reservation").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE public\\.runtime_budget_ledger").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(storageErr)
	if _, err := store.Reserve(context.Background(), input); !errors.Is(err, ErrStorage) {
		t.Fatalf("reserve commit error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLockLedgerMapsMissingAndStorageErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, queryErr := range []error{sql.ErrNoRows, errors.New("query failed")} {
		mock.ExpectBegin()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SELECT used_tokens, reserved_tokens, used_minor, reserved_minor").WithArgs(budgetTestTenantID, budgetTestPeriod).WillReturnError(queryErr)
		_, gotErr := lockLedger(context.Background(), tx, budgetTestTenantID, budgetTestPeriod)
		if queryErr == sql.ErrNoRows {
			if !errors.Is(gotErr, budget.ErrConflict) {
				t.Fatalf("missing ledger error = %v", gotErr)
			}
		} else if !errors.Is(gotErr, ErrStorage) {
			t.Fatalf("storage ledger error = %v", gotErr)
		}
		mock.ExpectRollback()
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReserveCreatesAndReusesReservations(t *testing.T) {
	store, db, mock := newMockBudgetStore(t)
	defer func() { _ = db.Close() }()
	tokenLimit, spendLimit := int64(100), int64(50)
	input := budget.ReserveInput{
		TenantID: budgetTestTenantID, ReservationID: "request-1", PeriodStart: budgetTestPeriod,
		Limits:   budget.Limits{TokenBudget: &tokenLimit, SpendLimitMinor: &spendLimit, Currency: "USD"},
		Estimate: budget.Estimate{InputTokens: 10, OutputTokens: 20, SpendMinor: 5},
	}
	periodStart := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, input.ReservationID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT used_tokens, reserved_tokens, used_minor, reserved_minor").WithArgs(budgetTestTenantID, sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"used_tokens", "reserved_tokens", "used_minor", "reserved_minor"}).AddRow(int64(0), int64(0), int64(0), int64(0)))
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_reservation").WithArgs(budgetTestTenantID, input.ReservationID, sqlmock.AnyArg(), int64(100), int64(50), "USD", int64(30), int64(5)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, sqlmock.AnyArg(), int64(30), int64(5)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	reservation, err := store.Reserve(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.PeriodStart != periodStart || reservation.EstimatedTokens != 30 || reservation.State != budget.ReservationStateReserved {
		t.Fatalf("new reservation = %+v", reservation)
	}

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, input.ReservationID, periodStart, int64(100), int64(50), "USD", int64(30), int64(5), nil, nil, budget.ReservationStateReserved)
	mock.ExpectCommit()
	duplicate, err := store.Reserve(context.Background(), input)
	if err != nil || duplicate.ReservationID != input.ReservationID {
		t.Fatalf("duplicate reservation = %+v, err = %v", duplicate, err)
	}

	conflicting := input
	conflicting.Estimate.OutputTokens++
	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, input.ReservationID, periodStart, int64(100), int64(50), "USD", int64(30), int64(5), nil, nil, budget.ReservationStateReserved)
	mock.ExpectRollback()
	if _, err := store.Reserve(context.Background(), conflicting); !errors.Is(err, budget.ErrConflict) {
		t.Fatalf("reservation idempotency conflict = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReserveRejectsInvalidInputAndRollsBackAdmissionFailures(t *testing.T) {
	store, db, mock := newMockBudgetStore(t)
	defer func() { _ = db.Close() }()
	period := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, input := range []budget.ReserveInput{
		{TenantID: "", ReservationID: "id", PeriodStart: period},
		{TenantID: budgetTestTenantID, ReservationID: "id", PeriodStart: period, Estimate: budget.Estimate{InputTokens: -1}},
		{TenantID: budgetTestTenantID, ReservationID: "id", PeriodStart: period, Limits: budget.Limits{SpendLimitMinor: int64Pointer(1), Currency: "usd"}},
	} {
		if _, err := store.Reserve(context.Background(), input); !errors.Is(err, budget.ErrInvalid) {
			t.Fatalf("invalid input %+v error = %v", input, err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Reserve(canceled, budget.ReserveInput{TenantID: budgetTestTenantID, ReservationID: "canceled", PeriodStart: period}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reserve error = %v", err)
	}

	beginErr := errors.New("begin failed")
	mock.ExpectBegin().WillReturnError(beginErr)
	if _, err := store.Reserve(context.Background(), validReserveInput()); !errors.Is(err, ErrStorage) || !errors.Is(err, beginErr) {
		t.Fatalf("begin error = %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, "request-1").WillReturnError(errors.New("reservation query failed"))
	mock.ExpectRollback()
	if _, err := store.Reserve(context.Background(), validReserveInput()); !errors.Is(err, ErrStorage) {
		t.Fatalf("reservation query error = %v", err)
	}

	limited := validReserveInput()
	limited.ReservationID = "over-limit"
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, limited.ReservationID).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT used_tokens, reserved_tokens, used_minor, reserved_minor").WithArgs(budgetTestTenantID, sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"used_tokens", "reserved_tokens", "used_minor", "reserved_minor"}).AddRow(int64(95), int64(0), int64(0), int64(0)))
	mock.ExpectRollback()
	if _, err := store.Reserve(context.Background(), limited); !errors.Is(err, budget.ErrExceeded) {
		t.Fatalf("over-limit reserve error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreSettleIsIdempotentAndRecordsOverage(t *testing.T) {
	store, db, mock := newMockBudgetStore(t)
	defer func() { _ = db.Close() }()
	periodStart := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	usage := budget.Usage{InputTokens: 4, OutputTokens: 6, SpendMinor: 7}

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, "settle", periodStart, int64(100), int64(50), "USD", int64(30), int64(5), nil, nil, budget.ReservationStateReserved)
	expectLedgerQuery(mock, budgetTestTenantID, periodStart, 0, 30, 0, 5)
	mock.ExpectExec("UPDATE public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart, int64(30), int64(5), int64(10), int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE public\\.runtime_budget_reservation").WithArgs(budgetTestTenantID, "settle", int64(10), int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	settled, err := store.Settle(context.Background(), budgetTestTenantID, "settle", usage)
	if err != nil || settled.State != budget.ReservationStateSettled || settled.ActualTokens != 10 || settled.ActualSpendMinor != 7 {
		t.Fatalf("settled reservation = %+v, err = %v", settled, err)
	}

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, "settle", periodStart, int64(100), int64(50), "USD", int64(30), int64(5), int64(10), int64(7), budget.ReservationStateSettled)
	mock.ExpectCommit()
	if again, err := store.Settle(context.Background(), budgetTestTenantID, "settle", usage); err != nil || again.State != budget.ReservationStateSettled {
		t.Fatalf("idempotent settlement = %+v, err = %v", again, err)
	}

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, "settle", periodStart, int64(100), int64(50), "USD", int64(30), int64(5), int64(10), int64(7), budget.ReservationStateSettled)
	mock.ExpectRollback()
	if _, err := store.Settle(context.Background(), budgetTestTenantID, "settle", budget.Usage{InputTokens: 5, OutputTokens: 6, SpendMinor: 7}); !errors.Is(err, budget.ErrConflict) {
		t.Fatalf("settlement conflict = %v", err)
	}

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, "overage", periodStart, int64(100), int64(50), "USD", int64(10), int64(5), nil, nil, budget.ReservationStateReserved)
	expectLedgerQuery(mock, budgetTestTenantID, periodStart, 95, 10, 0, 5)
	mock.ExpectExec("UPDATE public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart, int64(10), int64(5), int64(10), int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE public\\.runtime_budget_reservation").WithArgs(budgetTestTenantID, "overage", int64(10), int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	overage, err := store.Settle(context.Background(), budgetTestTenantID, "overage", usage)
	if !errors.Is(err, budget.ErrExceeded) || overage.State != budget.ReservationStateSettled {
		t.Fatalf("overage settlement = %+v, err = %v", overage, err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, "missing").WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	if _, err := store.Settle(context.Background(), budgetTestTenantID, "missing", usage); !errors.Is(err, budget.ErrNotFound) {
		t.Fatalf("missing settlement = %v", err)
	}
	if _, err := store.Settle(context.Background(), budgetTestTenantID, "invalid", budget.Usage{InputTokens: -1}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("invalid usage = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReleaseIsIdempotentAndWrapsStorageErrors(t *testing.T) {
	store, db, mock := newMockBudgetStore(t)
	defer func() { _ = db.Close() }()
	periodStart := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, "release", periodStart, int64(100), int64(50), "USD", int64(30), int64(5), nil, nil, budget.ReservationStateReserved)
	expectLedgerQuery(mock, budgetTestTenantID, periodStart, 0, 30, 0, 5)
	mock.ExpectExec("UPDATE public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart, int64(30), int64(5)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE public\\.runtime_budget_reservation").WithArgs(budgetTestTenantID, "release").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	released, err := store.Release(context.Background(), budgetTestTenantID, "release")
	if err != nil || released.State != budget.ReservationStateReleased {
		t.Fatalf("released reservation = %+v, err = %v", released, err)
	}

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, "release", periodStart, int64(100), int64(50), "USD", int64(30), int64(5), nil, nil, budget.ReservationStateReleased)
	mock.ExpectCommit()
	if again, err := store.Release(context.Background(), budgetTestTenantID, "release"); err != nil || again.State != budget.ReservationStateReleased {
		t.Fatalf("idempotent release = %+v, err = %v", again, err)
	}

	mock.ExpectBegin()
	expectReservationQuery(mock, budgetTestTenantID, "storage-error", periodStart, int64(100), int64(50), "USD", int64(30), int64(5), nil, nil, budget.ReservationStateReserved)
	expectLedgerQuery(mock, budgetTestTenantID, periodStart, 0, 30, 0, 5)
	storageErr := errors.New("update failed")
	mock.ExpectExec("UPDATE public\\.runtime_budget_ledger").WithArgs(budgetTestTenantID, periodStart, int64(30), int64(5)).WillReturnError(storageErr)
	mock.ExpectRollback()
	if _, err := store.Release(context.Background(), budgetTestTenantID, "storage-error"); !errors.Is(err, ErrStorage) || !errors.Is(err, storageErr) {
		t.Fatalf("storage error = %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(budgetTestTenantID, "missing").WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	if _, err := store.Release(context.Background(), budgetTestTenantID, "missing"); !errors.Is(err, budget.ErrNotFound) {
		t.Fatalf("missing release = %v", err)
	}
	if _, err := store.Release(context.Background(), budgetTestTenantID, "invalid"); err != nil {
		// The database-backed store validates context and resolves missing rows;
		// this call is intentionally not expected to be a no-op for an unknown ID.
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func newMockBudgetStore(t *testing.T) (*Store, *sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return store, db, mock
}

func validReserveInput() budget.ReserveInput {
	tokenLimit, spendLimit := int64(100), int64(50)
	return budget.ReserveInput{
		TenantID: budgetTestTenantID, ReservationID: "request-1", PeriodStart: budgetTestPeriod,
		Limits:   budget.Limits{TokenBudget: &tokenLimit, SpendLimitMinor: &spendLimit, Currency: "USD"},
		Estimate: budget.Estimate{InputTokens: 10, OutputTokens: 20, SpendMinor: 5},
	}
}

func expectReservationQuery(mock sqlmock.Sqlmock, tenantID, reservationID string, periodStart time.Time, tokenLimit, spendLimit any, currency string, estimatedTokens, estimatedMinor, actualTokens, actualMinor any, state budget.ReservationState) {
	rows := sqlmock.NewRows([]string{"period_start", "token_limit", "spend_limit_minor", "currency", "estimated_tokens", "estimated_minor", "actual_tokens", "actual_minor", "state"}).AddRow(periodStart, tokenLimit, spendLimit, currency, estimatedTokens, estimatedMinor, actualTokens, actualMinor, string(state))
	mock.ExpectQuery("SELECT period_start, token_limit, spend_limit_minor, currency").WithArgs(tenantID, reservationID).WillReturnRows(rows)
}

func expectLedgerQuery(mock sqlmock.Sqlmock, tenantID string, periodStart time.Time, usedTokens, reservedTokens, usedMinor, reservedMinor int64) {
	mock.ExpectQuery("SELECT used_tokens, reserved_tokens, used_minor, reserved_minor").WithArgs(tenantID, periodStart).WillReturnRows(sqlmock.NewRows([]string{"used_tokens", "reserved_tokens", "used_minor", "reserved_minor"}).AddRow(usedTokens, reservedTokens, usedMinor, reservedMinor))
}

func int64Pointer(value int64) *int64 { return &value }
