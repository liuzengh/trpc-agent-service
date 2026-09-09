package resultcache

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	testTenant      = "tenant-a"
	testKey         = "inbox:1"
	testPayloadHash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func testIdentity() Identity {
	return Identity{
		TenantID:       testTenant,
		IdempotencyKey: testKey,
		PayloadHash:    testPayloadHash,
		SessionID:      "session-a",
		AgentAppID:     "app-1",
		AgentVersionID: "version-1",
		DeploymentID:   "deployment-1",
	}
}

func TestGetScopedRejectsDifferentSession(t *testing.T) {
	store, mock, db := newMockStore(t)
	defer db.Close()

	mock.ExpectQuery("FROM invocation_results r").
		WithArgs(testTenant, testKey).
		WillReturnRows(sqlmock.NewRows([]string{
			"payload_hash", "response", "execution_id", "session_id",
			"agent_app_id", "agent_version_id", "deployment_id", "status",
		}).AddRow(
			testPayloadHash, []byte(`{"content":"stale"}`), int64(9), "session-old",
			"app-1", "version-1", "deployment-1", "SUCCEEDED",
		))

	_, _, err := store.GetScoped(context.Background(), testIdentity())
	if !errors.Is(err, ErrRequestIdentityConflict) {
		t.Fatalf("GetScoped error = %v, want ErrRequestIdentityConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetScopedRejectsNonSucceededProducer(t *testing.T) {
	store, mock, db := newMockStore(t)
	defer db.Close()
	identity := testIdentity()

	mock.ExpectQuery("FROM invocation_results r").
		WithArgs(testTenant, testKey).
		WillReturnRows(sqlmock.NewRows([]string{
			"payload_hash", "response", "execution_id", "session_id",
			"agent_app_id", "agent_version_id", "deployment_id", "status",
		}).AddRow(
			testPayloadHash, []byte(`{"content":"uncommitted"}`), int64(9), identity.SessionID,
			identity.AgentAppID, identity.AgentVersionID, identity.DeploymentID, "ABANDONED",
		))

	_, _, err := store.GetScoped(context.Background(), identity)
	if !errors.Is(err, ErrExecutionTerminal) {
		t.Fatalf("GetScoped error = %v, want ErrExecutionTerminal", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSuccessAtomicallyPersistsProducerResult(t *testing.T) {
	store, mock, db := newMockStore(t)
	defer db.Close()
	identity := testIdentity()

	mock.ExpectBegin()
	mock.ExpectQuery("FROM execution_records WHERE id=\\$1 FOR UPDATE").
		WithArgs(int64(41)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "session_id", "agent_app_id", "agent_version_id",
			"deployment_id", "idempotency_key", "payload_hash", "execution_token", "status", "lease_until",
		}).AddRow(
			identity.TenantID, identity.SessionID, identity.AgentAppID, identity.AgentVersionID,
			identity.DeploymentID, identity.IdempotencyKey, identity.PayloadHash, "attempt-41", "RUNNING",
			time.Now().Add(time.Minute),
		))
	mock.ExpectQuery("INSERT INTO invocation_results").
		WithArgs(
			identity.TenantID, identity.IdempotencyKey, identity.PayloadHash,
			[]byte(`{"content":"done"}`), int64(41),
		).
		WillReturnRows(sqlmock.NewRows([]string{"payload_hash", "execution_id"}).
			AddRow(identity.PayloadHash, int64(41)))
	mock.ExpectExec("UPDATE execution_records").
		WithArgs(int64(41), "attempt-41").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := store.CommitSuccess(
		context.Background(), identity, int64(41), "attempt-41", []byte(`{"content":"done"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func newMockStore(t *testing.T) (*Store, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create SQL mock: %v", err)
	}
	return New(db), mock, db
}

func TestGetScopedIgnoresExpiredResult(t *testing.T) {
	store, mock, db := newMockStore(t)
	defer db.Close()

	mock.ExpectQuery("FROM invocation_results r").
		WithArgs(testTenant, testKey).
		WillReturnRows(sqlmock.NewRows([]string{
			"payload_hash", "response", "execution_id", "session_id",
			"agent_app_id", "agent_version_id", "deployment_id", "status",
		}))

	entry, found, err := store.GetScoped(context.Background(), testIdentity())
	if err != nil {
		t.Fatalf("GetScoped: %v", err)
	}
	if found || entry.Response != nil {
		t.Fatalf("GetScoped returned expired result: found=%v entry=%#v", found, entry)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSuccessRejectsStaleExecutionToken(t *testing.T) {
	store, mock, db := newMockStore(t)
	defer db.Close()
	identity := testIdentity()

	mock.ExpectBegin()
	mock.ExpectQuery("FROM execution_records WHERE id=\\$1 FOR UPDATE").
		WithArgs(int64(41)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "session_id", "agent_app_id", "agent_version_id",
			"deployment_id", "idempotency_key", "payload_hash", "execution_token", "status", "lease_until",
		}).AddRow(
			identity.TenantID, identity.SessionID, identity.AgentAppID, identity.AgentVersionID,
			identity.DeploymentID, identity.IdempotencyKey, identity.PayloadHash, "attempt-new", "RUNNING",
			time.Now().Add(time.Minute),
		))
	mock.ExpectRollback()

	err := store.CommitSuccess(
		context.Background(), identity, 41, "attempt-old", []byte(`{"content":"stale"}`),
	)
	if !errors.Is(err, ErrExecutionFenceMismatch) {
		t.Fatalf("CommitSuccess error = %v, want ErrExecutionFenceMismatch", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSuccessRejectsExpiredExecutionLease(t *testing.T) {
	store, mock, db := newMockStore(t)
	defer db.Close()
	identity := testIdentity()

	mock.ExpectBegin()
	mock.ExpectQuery("FROM execution_records WHERE id=\\$1 FOR UPDATE").
		WithArgs(int64(41)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "session_id", "agent_app_id", "agent_version_id",
			"deployment_id", "idempotency_key", "payload_hash", "execution_token", "status", "lease_until",
		}).AddRow(
			identity.TenantID, identity.SessionID, identity.AgentAppID, identity.AgentVersionID,
			identity.DeploymentID, identity.IdempotencyKey, identity.PayloadHash, "attempt-41", "RUNNING",
			time.Now().Add(-time.Minute),
		))
	mock.ExpectQuery("INSERT INTO invocation_results").
		WithArgs(
			identity.TenantID, identity.IdempotencyKey, identity.PayloadHash,
			[]byte(`{"content":"late"}`), int64(41),
		).
		WillReturnRows(sqlmock.NewRows([]string{"payload_hash", "execution_id"}).
			AddRow(identity.PayloadHash, int64(41)))
	// sqlmock verifies the generated lease predicate only. A real PostgreSQL
	// lock-wait integration test is still needed to exercise clock behavior.
	mock.ExpectExec(`(?s)UPDATE execution_records.*lease_until > clock_timestamp\(\)`).
		WithArgs(int64(41), "attempt-41").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := store.CommitSuccess(
		context.Background(), identity, 41, "attempt-41", []byte(`{"content":"late"}`),
	)
	if !errors.Is(err, ErrExecutionLeaseExpired) {
		t.Fatalf("CommitSuccess error = %v, want ErrExecutionLeaseExpired", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSuccessRejectsResultFromAnotherExecution(t *testing.T) {
	store, mock, db := newMockStore(t)
	defer db.Close()
	identity := testIdentity()

	mock.ExpectBegin()
	mock.ExpectQuery("FROM execution_records WHERE id=\\$1 FOR UPDATE").
		WithArgs(int64(41)).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "session_id", "agent_app_id", "agent_version_id",
			"deployment_id", "idempotency_key", "payload_hash", "execution_token", "status", "lease_until",
		}).AddRow(
			identity.TenantID, identity.SessionID, identity.AgentAppID, identity.AgentVersionID,
			identity.DeploymentID, identity.IdempotencyKey, identity.PayloadHash, "attempt-41", "RUNNING",
			time.Now().Add(time.Minute),
		))
	mock.ExpectQuery("INSERT INTO invocation_results").
		WithArgs(
			identity.TenantID, identity.IdempotencyKey, identity.PayloadHash,
			[]byte(`{"content":"new"}`), int64(41),
		).
		WillReturnRows(sqlmock.NewRows([]string{"payload_hash", "execution_id"}))
	mock.ExpectQuery("SELECT payload_hash, COALESCE\\(execution_id,0\\)").
		WithArgs(testTenant, testKey).
		WillReturnRows(sqlmock.NewRows([]string{"payload_hash", "execution_id"}).
			AddRow(testPayloadHash, int64(40)))
	mock.ExpectRollback()

	err := store.CommitSuccess(
		context.Background(), identity, 41, "attempt-41", []byte(`{"content":"new"}`),
	)
	if !errors.Is(err, ErrResultProducerConflict) {
		t.Fatalf("CommitSuccess error = %v, want ErrResultProducerConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
