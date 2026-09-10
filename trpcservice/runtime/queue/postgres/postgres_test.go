package postgres

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue"
)

func TestPostgresQueueLifecycle(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	columns := []string{"tenant_id", "task_id", "kind", "payload", "status", "attempts", "fencing_token", "lease_owner", "lease_expires_at", "next_attempt_at", "last_error_class", "created_at", "updated_at"}
	now := time.Now()
	row := sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "run", []byte("payload"), "queued", 0, 0, "", nil, now, "", now, now)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_execution_queue")).WillReturnRows(row)
	if task, duplicate, err := store.Enqueue(context.Background(), queue.TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte("payload")}); err != nil || duplicate || task.Status != queue.StatusQueued {
		t.Fatalf("enqueue = %+v duplicate=%v err=%v", task, duplicate, err)
	}
	getRow := sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "run", []byte("payload"), "queued", 0, 0, "", nil, now, "", now, now)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+columnsString(columns)+" FROM public.runtime_execution_queue")).WithArgs("tenant-a", "task-1").WillReturnRows(getRow)
	if _, err := store.Get(context.Background(), "tenant-a", "task-1"); err != nil {
		t.Fatal(err)
	}
	claimRow := sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "run", []byte("payload"), "leased", 1, 1, "worker", now.Add(time.Minute), now, "", now, now)
	mock.ExpectQuery(regexp.QuoteMeta("WITH candidate AS")).WithArgs("tenant-a", "worker", int64(60000)).WillReturnRows(claimRow)
	claimed, err := store.Claim(context.Background(), "tenant-a", "worker", time.Minute)
	if err != nil || claimed.FencingToken != 1 {
		t.Fatalf("claim = %+v err=%v", claimed, err)
	}
	completeRow := sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "run", []byte("payload"), "completed", 1, 1, "", nil, now, "", now, now)
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE public.runtime_execution_queue SET status=$4")).WillReturnRows(completeRow)
	if _, err := store.Complete(context.Background(), "tenant-a", "task-1", "worker", 1); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueMapsNoRowsAndValidatesInputs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, _ := New(db)
	if _, _, err := store.Enqueue(context.Background(), queue.TaskInput{}); !errors.Is(err, queue.ErrInvalid) {
		t.Fatalf("invalid enqueue = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + columnsString([]string{"tenant_id", "task_id", "kind", "payload", "status", "attempts", "fencing_token", "lease_owner", "lease_expires_at", "next_attempt_at", "last_error_class", "created_at", "updated_at"}) + " FROM public.runtime_execution_queue")).WillReturnError(sql.ErrNoRows)
	if _, err := store.Get(context.Background(), "tenant-a", "missing"); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("missing get = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueRetryFailAndClaimEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, _ := New(db)
	columns := []string{"tenant_id", "task_id", "kind", "payload", "status", "attempts", "fencing_token", "lease_owner", "lease_expires_at", "next_attempt_at", "last_error_class", "created_at", "updated_at"}
	mock.ExpectQuery(regexp.QuoteMeta("WITH candidate AS")).WillReturnError(sql.ErrNoRows)
	if _, err := store.Claim(context.Background(), "tenant-a", "worker", time.Second); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("empty claim = %v", err)
	}
	now := time.Now()
	row := func(status string) *sqlmock.Rows {
		return sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "run", []byte("payload"), status, 1, 2, "", nil, now, "temporary", now, now)
	}
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE public.runtime_execution_queue SET status=$4")).WithArgs("tenant-a", "task-1", "worker", string(queue.StatusRetryable), "temporary", now, int64(2)).WillReturnRows(row("retryable"))
	if _, err := store.Retry(context.Background(), "tenant-a", "task-1", "worker", 2, now, "temporary"); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE public.runtime_execution_queue SET status=$4")).WillReturnRows(row("failed"))
	if _, err := store.Fail(context.Background(), "tenant-a", "task-1", "worker", 2, "permanent"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueDuplicateAndConflictPaths(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, _ := New(db)
	columns := []string{"tenant_id", "task_id", "kind", "payload", "status", "attempts", "fencing_token", "lease_owner", "lease_expires_at", "next_attempt_at", "last_error_class", "created_at", "updated_at"}
	now := time.Now()
	row := sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "run", []byte("payload"), "queued", 0, 0, "", nil, now, "", now, now)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_execution_queue")).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+columnsString(columns)+" FROM public.runtime_execution_queue")).WithArgs("tenant-a", "task-1").WillReturnRows(row)
	if _, duplicate, err := store.Enqueue(context.Background(), queue.TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte("payload")}); err != nil || !duplicate {
		t.Fatalf("duplicate enqueue = duplicate=%v err=%v", duplicate, err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("WITH candidate AS")).WillReturnError(errors.New("database unavailable"))
	if _, err := store.Claim(context.Background(), "tenant-a", "worker", time.Second); err == nil {
		t.Fatal("claim database error was swallowed")
	}
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE public.runtime_execution_queue SET status=$4")).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+columnsString(columns)+" FROM public.runtime_execution_queue")).WithArgs("tenant-a", "task-1").WillReturnError(sql.ErrNoRows)
	if _, err := store.Complete(context.Background(), "tenant-a", "task-1", "worker", 1); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("transition missing = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueConstructorAndBoundaryInputs(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, queue.ErrInvalid) {
		t.Fatalf("nil constructor = %v", err)
	}
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, _ := New(db)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(context.Background(), "tenant-a", "worker", 0); !errors.Is(err, queue.ErrInvalid) {
		t.Fatalf("zero lease = %v", err)
	}
	if _, err := store.Complete(context.Background(), "tenant-a", "task", "", 1); !errors.Is(err, queue.ErrInvalid) {
		t.Fatalf("empty owner = %v", err)
	}
}

func TestPostgresClaimPreservesSubsecondLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, _ := New(db)
	columns := []string{"tenant_id", "task_id", "kind", "payload", "status", "attempts", "fencing_token", "lease_owner", "lease_expires_at", "next_attempt_at", "last_error_class", "created_at", "updated_at"}
	now := time.Now()
	mock.ExpectQuery(regexp.QuoteMeta("WITH candidate AS")).WithArgs("tenant-a", "worker", int64(1500)).WillReturnRows(sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "run", []byte("payload"), "leased", 1, 1, "worker", now.Add(1500*time.Millisecond), now, "", now, now))
	if _, err := store.Claim(context.Background(), "tenant-a", "worker", 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresQueueErrorBranches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, _ := New(db)
	input := queue.TaskInput{TenantID: "tenant-a", TaskID: "task-1", Kind: "run", Payload: []byte("payload")}
	if _, _, err := store.Enqueue(nil, input); !errors.Is(err, queue.ErrInvalid) {
		t.Fatalf("nil enqueue context = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_execution_queue")).WillReturnError(errors.New("insert failed"))
	if _, _, err := store.Enqueue(context.Background(), input); err == nil || err.Error() != "insert failed" {
		t.Fatalf("enqueue database error = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_execution_queue")).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+columnsString([]string{"tenant_id", "task_id", "kind", "payload", "status", "attempts", "fencing_token", "lease_owner", "lease_expires_at", "next_attempt_at", "last_error_class", "created_at", "updated_at"})+" FROM public.runtime_execution_queue")).WithArgs("tenant-a", "task-1").WillReturnError(errors.New("lookup failed"))
	if _, _, err := store.Enqueue(context.Background(), input); err == nil || err.Error() != "lookup failed" {
		t.Fatalf("enqueue lookup error = %v", err)
	}
	now := time.Now()
	columns := []string{"tenant_id", "task_id", "kind", "payload", "status", "attempts", "fencing_token", "lease_owner", "lease_expires_at", "next_attempt_at", "last_error_class", "created_at", "updated_at"}
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_execution_queue")).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+columnsString(columns)+" FROM public.runtime_execution_queue")).WithArgs("tenant-a", "task-1").WillReturnRows(sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "other", []byte("payload"), "queued", 0, 0, "", nil, now, "", now, now))
	if _, _, err := store.Enqueue(context.Background(), input); !errors.Is(err, queue.ErrConflict) {
		t.Fatalf("enqueue payload conflict = %v", err)
	}
	if _, err := store.Get(context.Background(), "", "task-1"); !errors.Is(err, queue.ErrInvalid) {
		t.Fatalf("invalid get = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+columnsString(columns)+" FROM public.runtime_execution_queue")).WithArgs("tenant-a", "task-1").WillReturnError(errors.New("select failed"))
	if _, err := store.Get(context.Background(), "tenant-a", "task-1"); err == nil || err.Error() != "select failed" {
		t.Fatalf("get database error = %v", err)
	}
	if _, err := store.Claim(nil, "tenant-a", "worker", time.Second); !errors.Is(err, queue.ErrInvalid) {
		t.Fatalf("nil claim context = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("WITH candidate AS")).WithArgs("tenant-a", "worker", int64(1)).WillReturnError(sql.ErrNoRows)
	if _, err := store.Claim(context.Background(), "tenant-a", "worker", 500*time.Microsecond); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("submillisecond claim = %v", err)
	}
	if _, err := store.Complete(context.Background(), "tenant-a", "task-1", "worker", 0); !errors.Is(err, queue.ErrInvalid) {
		t.Fatalf("invalid transition = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE public.runtime_execution_queue SET status=$4")).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+columnsString(columns)+" FROM public.runtime_execution_queue")).WithArgs("tenant-a", "task-1").WillReturnRows(sqlmock.NewRows(columns).AddRow("tenant-a", "task-1", "run", []byte("payload"), "leased", 1, 1, "worker", now.Add(time.Minute), now, "", now, now))
	if _, err := store.Complete(context.Background(), "tenant-a", "task-1", "worker", 1); !errors.Is(err, queue.ErrConflict) {
		t.Fatalf("stale transition = %v", err)
	}
	mock.ExpectQuery(regexp.QuoteMeta("UPDATE public.runtime_execution_queue SET status=$4")).WillReturnError(errors.New("update failed"))
	if _, err := store.Fail(context.Background(), "tenant-a", "task-1", "worker", 1, "failed"); err == nil || err.Error() != "update failed" {
		t.Fatalf("transition database error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func columnsString(columns []string) string {
	result := ""
	for i, column := range columns {
		if i > 0 {
			result += ","
		}
		result += column
	}
	return result
}
