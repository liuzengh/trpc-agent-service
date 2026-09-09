package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestStartWithRequestReturnsFencedHandle(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id").
		WithArgs("tenant-a", "inbox:1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "payload_hash", "agent_app_id", "agent_version_id", "deployment_id",
		}).AddRow("session-a", testExecutionPayloadHash, "app-1", "version-1", "deployment-1"))
	mock.ExpectQuery("FROM execution_records").
		WithArgs("tenant-a", "inbox:1").
		WillReturnError(sql.ErrNoRows)
	expectReadySessionGuard(mock)
	mock.ExpectQuery("INSERT INTO execution_records").
		WithArgs(
			"tenant-a", "session-a", "app-1", "version-1", "deployment-1",
			"inbox:1", testExecutionPayloadHash, int64(1), sqlmock.AnyArg(),
			int64(DefaultExecutionLeaseTTL/time.Millisecond),
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(41)))
	expectSessionGuardClaim(mock, int64(41), int64(1))
	mock.ExpectCommit()

	recorder := NewExecutionRecorder(db)
	handle, err := recorder.StartWithRequest(
		context.Background(),
		"tenant-a",
		"session-a",
		"inbox:1",
		testExecutionPayloadHash,
		&ResolvedDeployment{
			AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if handle.ID != 41 || handle.Token == "" || handle.Generation != 1 {
		t.Fatalf("execution handle = %#v, want ID 41 with token and generation 1", handle)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResolvePinnedWithPayloadRejectsDifferentSession(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("FROM invocation_bindings ib").
		WithArgs("tenant-a", "inbox:1").
		WillReturnRows(sqlmock.NewRows([]string{
			"app_id", "app_name", "version_id", "version_number",
			"deployment_id", "kind", "traffic_bps", "config_snapshot",
		}).AddRow(
			"app-1", "assistant", "version-1", int64(1),
			"deployment-1", "stable", 10000,
			[]byte(`{"agent":{"name":"assistant","defaultModel":"gpt"},"model":{"provider":"openai","modelName":"gpt"}}`),
		))
	mock.ExpectQuery("SELECT session_id, payload_hash FROM invocation_bindings").
		WithArgs("tenant-a", "inbox:1").
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "payload_hash"}).
			AddRow("session-old", testExecutionPayloadHash))
	mock.ExpectRollback()

	resolver := NewPostgresResolver(db)
	_, err = resolver.ResolvePinnedWithPayload(
		context.Background(),
		"tenant-a",
		"assistant",
		"session-new",
		"inbox:1",
		testExecutionPayloadHash,
	)
	if !errors.Is(err, ErrRequestIdentityConflict) {
		t.Fatalf("resolve error = %v, want ErrRequestIdentityConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartWithRequestRejectsUnsafeTerminalAttempts(t *testing.T) {
	tests := []struct {
		name      string
		status    string
		retrySafe bool
		want      error
	}{
		{name: "failed with possible side effects", status: "FAILED", want: ErrExecutionRetryUnsafe},
		{name: "abandoned outcome unknown", status: "ABANDONED", want: ErrExecutionOutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			mock.ExpectBegin()
			mock.ExpectQuery("SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id").
				WithArgs("tenant-a", "inbox:1").
				WillReturnRows(sqlmock.NewRows([]string{
					"session_id", "payload_hash", "agent_app_id", "agent_version_id", "deployment_id",
				}).AddRow("session-a", testExecutionPayloadHash, "app-1", "version-1", "deployment-1"))
			mock.ExpectQuery("FROM execution_records").
				WithArgs("tenant-a", "inbox:1").
				WillReturnRows(sqlmock.NewRows([]string{
					"id", "status", "session_id", "payload_hash", "agent_app_id",
					"agent_version_id", "deployment_id", "attempt_number", "retry_safe", "reconciled",
				}).AddRow(
					int64(40), test.status, "session-a", testExecutionPayloadHash, "app-1",
					"version-1", "deployment-1", int64(1), test.retrySafe, false,
				))
			mock.ExpectRollback()

			recorder := NewExecutionRecorder(db)
			_, err = recorder.StartWithRequest(
				context.Background(), "tenant-a", "session-a", "inbox:1", testExecutionPayloadHash,
				&ResolvedDeployment{
					AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1",
				},
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("start error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStartWithRequestRetriesExplicitlySafeFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id").
		WithArgs("tenant-a", "inbox:1").
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "payload_hash", "agent_app_id", "agent_version_id", "deployment_id",
		}).AddRow("session-a", testExecutionPayloadHash, "app-1", "version-1", "deployment-1"))
	mock.ExpectQuery("FROM execution_records").
		WithArgs("tenant-a", "inbox:1").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "status", "session_id", "payload_hash", "agent_app_id",
			"agent_version_id", "deployment_id", "attempt_number", "retry_safe", "reconciled",
		}).AddRow(
			int64(40), "FAILED", "session-a", testExecutionPayloadHash, "app-1",
			"version-1", "deployment-1", int64(1), true, false,
		))
	expectReadySessionGuard(mock)
	mock.ExpectQuery("INSERT INTO execution_records").
		WithArgs(
			"tenant-a", "session-a", "app-1", "version-1", "deployment-1",
			"inbox:1", testExecutionPayloadHash, int64(2), sqlmock.AnyArg(),
			int64(DefaultExecutionLeaseTTL/time.Millisecond),
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(41)))
	expectSessionGuardClaim(mock, int64(41), int64(2))
	mock.ExpectCommit()

	recorder := NewExecutionRecorder(db)
	handle, err := recorder.StartWithRequest(
		context.Background(), "tenant-a", "session-a", "inbox:1", testExecutionPayloadHash,
		&ResolvedDeployment{
			AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if handle.ID != 41 || handle.Token == "" || handle.Generation != 2 {
		t.Fatalf("retry handle = %#v", handle)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartWithRequestRejectsBindingAndTerminalConflicts(t *testing.T) {
	tests := []struct {
		name string
		want error
		mock func(sqlmock.Sqlmock)
	}{
		{
			name: "missing invocation binding",
			want: ErrInvocationBindingMissing,
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id").
					WithArgs("tenant-a", "inbox:1").
					WillReturnError(sql.ErrNoRows)
			},
		},
		{
			name: "binding payload conflict",
			want: ErrPayloadConflict,
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id").
					WithArgs("tenant-a", "inbox:1").
					WillReturnRows(sqlmock.NewRows([]string{
						"session_id", "payload_hash", "agent_app_id", "agent_version_id", "deployment_id",
					}).AddRow("session-a", strings.Repeat("b", 64), "app-1", "version-1", "deployment-1"))
			},
		},
		{
			name: "binding version conflict",
			want: ErrVersionBindingConflict,
			mock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id").
					WithArgs("tenant-a", "inbox:1").
					WillReturnRows(sqlmock.NewRows([]string{
						"session_id", "payload_hash", "agent_app_id", "agent_version_id", "deployment_id",
					}).AddRow("session-a", testExecutionPayloadHash, "app-1", "version-old", "deployment-old"))
			},
		},
		{
			name: "attempt already running",
			want: ErrExecutionInProgress,
			mock: func(mock sqlmock.Sqlmock) {
				expectMatchingExecutionBinding(mock)
				expectPreviousExecution(mock, "RUNNING", false)
			},
		},
		{
			name: "attempt already succeeded",
			want: ErrExecutionAlreadySucceeded,
			mock: func(mock sqlmock.Sqlmock) {
				expectMatchingExecutionBinding(mock)
				expectPreviousExecution(mock, "SUCCEEDED", false)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			test.mock(mock)
			mock.ExpectRollback()

			recorder := NewExecutionRecorder(db)
			_, err = recorder.StartWithRequest(
				context.Background(), "tenant-a", "session-a", "inbox:1", testExecutionPayloadHash,
				&ResolvedDeployment{
					AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1",
				},
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("start error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func expectMatchingExecutionBinding(mock sqlmock.Sqlmock) {
	expectMatchingExecutionBindingForKey(mock, "inbox:1")
}

func expectMatchingExecutionBindingForKey(mock sqlmock.Sqlmock, idempotencyKey string) {
	mock.ExpectQuery("SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id").
		WithArgs("tenant-a", idempotencyKey).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "payload_hash", "agent_app_id", "agent_version_id", "deployment_id",
		}).AddRow("session-a", testExecutionPayloadHash, "app-1", "version-1", "deployment-1"))
}

func expectPreviousExecution(mock sqlmock.Sqlmock, status string, retrySafe bool) {
	expectPreviousExecutionForKey(mock, "inbox:1", status, retrySafe)
}

func expectPreviousExecutionForKey(mock sqlmock.Sqlmock, idempotencyKey, status string, retrySafe bool) {
	mock.ExpectQuery("FROM execution_records").
		WithArgs("tenant-a", idempotencyKey).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "status", "session_id", "payload_hash", "agent_app_id",
			"agent_version_id", "deployment_id", "attempt_number", "retry_safe", "reconciled",
		}).AddRow(
			int64(40), status, "session-a", testExecutionPayloadHash, "app-1",
			"version-1", "deployment-1", int64(1), retrySafe, false,
		))
}

func expectReadySessionGuard(mock sqlmock.Sqlmock) {
	mock.ExpectExec("INSERT INTO session_execution_guards").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT status, current_execution_id").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).
			AddRow("READY", nil))
}

func expectSessionGuardClaim(mock sqlmock.Sqlmock, executionID, generation int64) {
	mock.ExpectQuery("UPDATE session_execution_guards").
		WithArgs("tenant-a", "app-1", "session-a", executionID).
		WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(generation))
}

func TestStartWithRequestRejectsAnotherRequestForRunningSession(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	expectMatchingExecutionBindingForKey(mock, "inbox:2")
	mock.ExpectQuery("FROM execution_records").
		WithArgs("tenant-a", "inbox:2").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO session_execution_guards").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT status, current_execution_id").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).
			AddRow("RUNNING", int64(40)))
	mock.ExpectRollback()

	recorder := NewExecutionRecorder(db)
	_, err = recorder.StartWithRequest(
		context.Background(), "tenant-a", "session-a", "inbox:2", testExecutionPayloadHash,
		&ResolvedDeployment{AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1"},
	)
	if !errors.Is(err, ErrSessionExecutionInProgress) {
		t.Fatalf("start error = %v, want ErrSessionExecutionInProgress", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartWithRequestRejectsBlockedSession(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	expectMatchingExecutionBindingForKey(mock, "inbox:2")
	mock.ExpectQuery("FROM execution_records").
		WithArgs("tenant-a", "inbox:2").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO session_execution_guards").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT status, current_execution_id").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).
			AddRow("BLOCKED", int64(40)))
	mock.ExpectRollback()

	recorder := NewExecutionRecorder(db)
	_, err = recorder.StartWithRequest(
		context.Background(), "tenant-a", "session-a", "inbox:2", testExecutionPayloadHash,
		&ResolvedDeployment{AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1"},
	)
	if !errors.Is(err, ErrSessionReconciliationRequired) {
		t.Fatalf("start error = %v, want ErrSessionReconciliationRequired", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartWithRequestRetriesReconciledAbandonedAttempt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	expectMatchingExecutionBinding(mock)
	mock.ExpectQuery("FROM execution_records").
		WithArgs("tenant-a", "inbox:1").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "status", "session_id", "payload_hash", "agent_app_id",
			"agent_version_id", "deployment_id", "attempt_number", "retry_safe", "reconciled",
		}).AddRow(
			int64(40), "ABANDONED", "session-a", testExecutionPayloadHash, "app-1",
			"version-1", "deployment-1", int64(1), false, true,
		))
	expectReadySessionGuard(mock)
	mock.ExpectQuery("INSERT INTO execution_records").
		WithArgs(
			"tenant-a", "session-a", "app-1", "version-1", "deployment-1",
			"inbox:1", testExecutionPayloadHash, int64(2), sqlmock.AnyArg(),
			int64(DefaultExecutionLeaseTTL/time.Millisecond),
		).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(41)))
	expectSessionGuardClaim(mock, int64(41), int64(2))
	mock.ExpectCommit()

	recorder := NewExecutionRecorder(db)
	handle, err := recorder.StartWithRequest(
		context.Background(), "tenant-a", "session-a", "inbox:1", testExecutionPayloadHash,
		&ResolvedDeployment{AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if handle.ID != 41 || handle.Generation != 2 {
		t.Fatalf("retry handle = %#v", handle)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileForRetryClearsGuardAndAuditsAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT session_id, agent_app_id, status, retry_safe").
		WithArgs("tenant-a", int64(40)).
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "agent_app_id", "status", "retry_safe", "idempotency_key"}).AddRow("session-a", "app-1", "ABANDONED", false, "inbox:1"))
	mock.ExpectQuery("SELECT decision, actor, reason, evidence").
		WithArgs("tenant-a", int64(40)).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO execution_reconciliations").
		WithArgs(int64(40), "tenant-a", "operator-a", "provider receipt verified", "receipt:abc").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO session_execution_guards").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT status, current_execution_id").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).AddRow("BLOCKED", int64(40)))
	mock.ExpectQuery("SELECT e.id, e.status").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status"}))
	mock.ExpectExec("UPDATE session_execution_guards").
		WithArgs("tenant-a", "app-1", "session-a", "READY", nil, "").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO control_plane_audit").
		WithArgs("tenant-a", "operator-a", "execution.reconcile", "execution", "40", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := NewExecutionRecorder(db).ReconcileForRetry(
		context.Background(), "tenant-a", 40, "operator-a", "provider receipt verified", "receipt:abc"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileForRetryIsIdempotentAndRejectsConflictingEvidence(t *testing.T) {
	for _, test := range []struct {
		name       string
		reason     string
		evidence   string
		want       error
		wantCommit bool
	}{
		{name: "same request", reason: "verified", evidence: "receipt:a", wantCommit: true},
		{name: "different evidence", reason: "verified", evidence: "receipt:b", want: ErrReconciliationConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT session_id, agent_app_id, status, retry_safe").
				WithArgs("tenant-a", int64(40)).
				WillReturnRows(sqlmock.NewRows([]string{"session_id", "agent_app_id", "status", "retry_safe", "idempotency_key"}).AddRow("session-a", "app-1", "ABANDONED", false, "inbox:1"))
			mock.ExpectQuery("SELECT decision, actor, reason, evidence").
				WithArgs("tenant-a", int64(40)).
				WillReturnRows(sqlmock.NewRows([]string{"decision", "actor", "reason", "evidence"}).
					AddRow("SAFE_TO_RETRY", "operator-a", "verified", "receipt:a"))
			if test.want != nil {
				mock.ExpectRollback()
			} else {
				mock.ExpectExec("INSERT INTO session_execution_guards").
					WithArgs("tenant-a", "app-1", "session-a").
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("SELECT status, current_execution_id").
					WithArgs("tenant-a", "app-1", "session-a").
					WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).AddRow("BLOCKED", int64(40)))
				mock.ExpectQuery("SELECT e.id, e.status").
					WithArgs("tenant-a", "app-1", "session-a").
					WillReturnRows(sqlmock.NewRows([]string{"id", "status"}))
				mock.ExpectExec("UPDATE session_execution_guards").
					WithArgs("tenant-a", "app-1", "session-a", "READY", nil, "").
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			}
			err = NewExecutionRecorder(db).ReconcileForRetry(
				context.Background(), "tenant-a", 40, "operator-a", test.reason, test.evidence)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReconcileForRetryKeepsRunningAttemptAsGuardOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT session_id, agent_app_id, status, retry_safe").
		WithArgs("tenant-a", int64(40)).
		WillReturnRows(sqlmock.NewRows([]string{"session_id", "agent_app_id", "status", "retry_safe", "idempotency_key"}).AddRow("session-a", "app-1", "ABANDONED", false, "inbox:1"))
	mock.ExpectQuery("SELECT decision, actor, reason, evidence").
		WithArgs("tenant-a", int64(40)).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO execution_reconciliations").
		WithArgs(int64(40), "tenant-a", "operator-a", "verified", "receipt:a").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO session_execution_guards").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT status, current_execution_id").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"status", "current_execution_id"}).AddRow("BLOCKED", int64(40)))
	mock.ExpectQuery("SELECT e.id, e.status").
		WithArgs("tenant-a", "app-1", "session-a").
		WillReturnRows(sqlmock.NewRows([]string{"id", "status"}).AddRow(int64(41), "RUNNING"))
	mock.ExpectExec("UPDATE session_execution_guards").
		WithArgs("tenant-a", "app-1", "session-a", "RUNNING", int64(41), "").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO control_plane_audit").
		WithArgs("tenant-a", "operator-a", "execution.reconcile", "execution", "40", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := NewExecutionRecorder(db).ReconcileForRetry(
		context.Background(), "tenant-a", 40, "operator-a", "verified", "receipt:a"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
