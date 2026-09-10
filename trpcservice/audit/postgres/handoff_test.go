package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

func TestHandoffStoreScopeAndReserve(t *testing.T) {
	if _, err := NewHandoffStore(nil, ""); !errors.Is(err, audit.ErrInvalid) {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectBegin()
	now := time.Now().UTC()
	handoffRows := sqlmock.NewRows([]string{"tenant_id", "handoff_id", "request_id", "trace_id", "event_id", "state", "result", "error_type", "latency_ms", "created_at", "updated_at"})
	handoffRows.AddRow("t", "h", "r", "tr", "e", "pending", nil, nil, nil, now, now)
	mock.ExpectQuery("SELECT tenant_id,handoff_id,request_id").WillReturnRows(handoffRows)
	mock.ExpectCommit()
	store, _ := NewHandoffStore(db, "t")
	got, err := store.Reserve(context.Background(), audit.ExecutionHandoff{TenantID: "t", HandoffID: "h", RequestID: "r", State: audit.HandoffPending})
	if err != nil || got.State != audit.HandoffPending {
		t.Fatalf("reserve=%+v err=%v", got, err)
	}
	if _, err := store.Reserve(context.Background(), audit.ExecutionHandoff{TenantID: "other", HandoffID: "h", RequestID: "r", State: audit.HandoffPending}); !errors.Is(err, audit.ErrTenantScope) {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestHandoffStoreFinalizeAndGet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, _ := NewHandoffStore(db, "t")
	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{"tenant_id", "handoff_id", "request_id", "trace_id", "event_id", "state", "result", "error_type", "latency_ms", "created_at", "updated_at"}).AddRow("t", "h", "r", "tr", "e", "finalized", "success", nil, 12, now, now)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,handoff_id,request_id").WillReturnRows(rows)
	mock.ExpectCommit()
	got, err := store.Finalize(context.Background(), audit.ExecutionHandoff{TenantID: "t", HandoffID: "h", State: audit.HandoffFinalized, Result: audit.ResultSuccess})
	if err != nil || got.State != audit.HandoffFinalized {
		t.Fatalf("finalize=%+v err=%v", got, err)
	}
	getRows := sqlmock.NewRows([]string{"tenant_id", "handoff_id", "request_id", "trace_id", "event_id", "state", "result", "error_type", "latency_ms", "created_at", "updated_at"}).AddRow("t", "h", "r", "tr", "e", "finalized", "success", nil, 12, now, now)
	mock.ExpectQuery("SELECT tenant_id,handoff_id,request_id").WillReturnRows(getRows)
	if got, err := store.Get(context.Background(), "t", "h"); err != nil || got.State != audit.HandoffFinalized {
		t.Fatalf("get=%+v err=%v", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestHandoffStoreValidationAndDatabaseErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, _ := NewHandoffStore(db, "t")
	if _, err := store.Reserve(context.Background(), audit.ExecutionHandoff{TenantID: "t", HandoffID: "h", RequestID: "r", State: audit.HandoffFinalized}); !errors.Is(err, audit.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.Finalize(context.Background(), audit.ExecutionHandoff{TenantID: "t", HandoffID: "h", State: audit.HandoffPending}); !errors.Is(err, audit.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "other", "h"); !errors.Is(err, audit.ErrTenantScope) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(ctx, "t", "h"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, err := store.Reserve(context.Background(), audit.ExecutionHandoff{TenantID: "t", HandoffID: "h", RequestID: "r", State: audit.HandoffPending}); err == nil {
		t.Fatal("expected begin error")
	}
}

func TestHandoffStoreRemainingErrorBranches(t *testing.T) {
	if nullableTime(time.Time{}) != nil {
		t.Fatal("zero time should be nil")
	}
	if nullableTime(time.Unix(1, 0)) == nil {
		t.Fatal("non-zero time should be retained")
	}
	var nilStore *HandoffStore
	if _, err := nilStore.Get(context.Background(), "t", "h"); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, _ := NewHandoffStore(db, "t")
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,handoff_id,request_id").WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	if _, err := store.Finalize(context.Background(), audit.ExecutionHandoff{TenantID: "t", HandoffID: "h", State: audit.HandoffFinalized, Result: audit.ResultSuccess}); err == nil {
		t.Fatal("expected finalize query failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestHandoffScanConvertersRejectInvalidValues(t *testing.T) {
	if err := (scanState{out: &audit.ExecutionHandoff{}}).Scan(struct{}{}); err == nil {
		t.Fatal("scan state accepted invalid value")
	}
	if err := (scanResult{out: &audit.ExecutionHandoff{}}).Scan(struct{}{}); err == nil {
		t.Fatal("scan result accepted invalid value")
	}
	if err := (scanError{out: &audit.ExecutionHandoff{}}).Scan(struct{}{}); err == nil {
		t.Fatal("scan error accepted invalid value")
	}
	if err := (scanLatency{out: &audit.ExecutionHandoff{}}).Scan(struct{}{}); err == nil {
		t.Fatal("scan latency accepted invalid value")
	}
}

func TestHandoffGetQueryErrorBranches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, _ := NewHandoffStore(db, "t")
	if _, err := store.Get(context.Background(), "t", ""); !errors.Is(err, audit.ErrInvalid) {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT tenant_id,handoff_id,request_id").WillReturnError(sql.ErrNoRows)
	if _, err := store.Get(context.Background(), "t", "missing"); !errors.Is(err, audit.ErrHandoffNotFound) {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT tenant_id,handoff_id,request_id").WillReturnError(errors.New("query failed"))
	if _, err := store.Get(context.Background(), "t", "broken"); err == nil {
		t.Fatal("expected query failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
