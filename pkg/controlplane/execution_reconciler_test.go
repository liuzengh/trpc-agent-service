package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type reconcileResult struct {
	rows int64
	err  error
}

func (r reconcileResult) LastInsertId() (int64, error) { return 0, errors.New("unsupported") }
func (r reconcileResult) RowsAffected() (int64, error) { return r.rows, r.err }

func TestReconcileExpiredIsBoundedAndLeaseAware(t *testing.T) {
	var query string
	var arguments []interface{}
	recorder := &ExecutionRecorder{exec: func(_ context.Context, candidate string, args ...interface{}) (sql.Result, error) {
		query, arguments = candidate, args
		return reconcileResult{rows: 3}, nil
	}}
	cutoff := time.Unix(1000, 0)
	rows, err := recorder.ReconcileExpired(context.Background(), cutoff, 50)
	if err != nil || rows != 3 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	for _, required := range []string{
		"status='RUNNING'", "lease_until < $1", "status='ABANDONED'",
		"DISTINCT ON (e.tenant_id, e.agent_app_id, e.session_id)",
		"session_execution_guards", "g.status='BLOCKED'", "LIMIT $2",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("reconcile query missing %q: %s", required, query)
		}
	}
	if len(arguments) != 2 || !arguments[0].(time.Time).Equal(cutoff) || arguments[1] != 50 {
		t.Fatalf("arguments = %#v", arguments)
	}
}

func TestReconcileExpiredUsesGuardedOnePerSessionTransitions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cutoff := time.Unix(1000, 0).UTC()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT e.id, e.tenant_id, e.agent_app_id, e.session_id").
		WithArgs(cutoff, 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "agent_app_id", "session_id"}).
			AddRow(int64(11), "tenant-a", "app-a", "session-a").
			AddRow(int64(12), "tenant-a", "app-a", "session-a").
			AddRow(int64(21), "tenant-b", "app-b", "session-b"))
	mock.ExpectQuery("SELECT status, current_execution_id").
		WithArgs("tenant-a", "app-a", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).
			AddRow("RUNNING", int64(11)))
	mock.ExpectExec("UPDATE execution_records").
		WithArgs(int64(11), "tenant-a", cutoff).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT status, current_execution_id").
		WithArgs("tenant-b", "app-b", "session-b").
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).
			AddRow("BLOCKED", int64(20)))
	mock.ExpectExec("UPDATE session_execution_guards").
		WithArgs("tenant-b", "app-b", "session-b", int64(21)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE execution_records").
		WithArgs(int64(21), "tenant-b", cutoff).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	rows, err := NewExecutionRecorder(db).ReconcileExpired(context.Background(), cutoff, 10)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("reconciled rows = %d, want 2", rows)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileExpiredFailsClosedOnReadyGuardWithStaleExecution(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cutoff := time.Unix(1000, 0).UTC()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT e.id, e.tenant_id, e.agent_app_id, e.session_id").
		WithArgs(cutoff, 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "agent_app_id", "session_id"}).
			AddRow(int64(31), "tenant-a", "app-a", "session-a"))
	mock.ExpectQuery("SELECT status, current_execution_id").
		WithArgs("tenant-a", "app-a", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).
			AddRow("READY", nil))
	mock.ExpectRollback()

	rows, err := NewExecutionRecorder(db).ReconcileExpired(context.Background(), cutoff, 1)
	if err == nil || rows != 0 || !strings.Contains(err.Error(), "guard status") {
		t.Fatalf("rows=%d err=%v, want fail-closed guard status error", rows, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartRejectsLegacyUnfencedAPI(t *testing.T) {
	recorder := &ExecutionRecorder{exec: func(context.Context, string, ...interface{}) (sql.Result, error) {
		t.Fatal("legacy Start must not execute SQL")
		return nil, nil
	}}
	_, err := recorder.Start(context.Background(), "tenant-a", "session-a", &ResolvedDeployment{
		AgentAppID: "app-a", VersionID: "version-a", DeploymentID: "deployment-a",
	})
	if !errors.Is(err, ErrLegacyExecutionAPI) {
		t.Fatalf("Start error = %v, want ErrLegacyExecutionAPI", err)
	}
}

func TestReconcileExpiredValidatesBounds(t *testing.T) {
	recorder := &ExecutionRecorder{exec: func(context.Context, string, ...interface{}) (sql.Result, error) {
		t.Fatal("exec must not run")
		return nil, nil
	}}
	for _, limit := range []int{0, 10001} {
		if _, err := recorder.ReconcileExpired(context.Background(), time.Now(), limit); err == nil {
			t.Fatalf("limit %d accepted", limit)
		}
	}
}

func TestRunReconcilerContinuesAfterTransientError(t *testing.T) {
	var calls atomic.Int32
	recorder := &ExecutionRecorder{exec: func(context.Context, string, ...interface{}) (sql.Result, error) {
		call := calls.Add(1)
		if call == 1 {
			return nil, errors.New("temporary")
		}
		return reconcileResult{}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	errorsSeen := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- recorder.RunReconciler(ctx, time.Millisecond, 10, func(err error) {
			errorsSeen <- err
		})
	}()
	select {
	case <-errorsSeen:
	case <-time.After(time.Second):
		t.Fatal("transient error was not reported")
	}
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() < 2 {
		t.Fatalf("reconciler stopped after %d call", calls.Load())
	}
}

func TestRunReconcilerReportsSuccessfulRowCount(t *testing.T) {
	recorder := &ExecutionRecorder{exec: func(context.Context, string, ...interface{}) (sql.Result, error) {
		return reconcileResult{rows: 4}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	observed := make(chan int64, 1)
	done := make(chan error, 1)
	go func() {
		done <- recorder.RunReconcilerWithObserver(ctx, time.Millisecond, 10, func(rows int64) {
			observed <- rows
		}, nil)
	}()
	select {
	case rows := <-observed:
		if rows != 4 {
			t.Fatalf("observed rows = %d", rows)
		}
	case <-time.After(time.Second):
		t.Fatal("successful reconciliation was not observed")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
