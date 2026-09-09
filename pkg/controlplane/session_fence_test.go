package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
)

func testFenceToken() fence.Token {
	return fence.Token{
		TenantID: "tenant-a", AgentAppID: "app-a", SessionID: "session-a",
		ExecutionID: 42, Generation: 9, Value: "execution-token",
	}
}

func TestPostgresSessionFenceReleaseRevalidatesAndIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	token := testFenceToken()
	mock.ExpectExec("SELECT pg_advisory_lock").WithArgs(token.Scope()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("clock_timestamp").
		WithArgs(token.TenantID, token.AgentAppID, token.SessionID).
		WillReturnRows(sqlmock.NewRows([]string{
			"status", "generation", "current_execution_id", "execution_token", "status", "lease_until", "heartbeat_at", "lease_valid",
		}).AddRow("RUNNING", token.Generation, token.ExecutionID, token.Value, "RUNNING", now.Add(10*time.Minute), now.Add(-5*time.Minute), true))

	authorizer := NewPostgresSessionFence(db)
	release, err := authorizer.Acquire(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT g.status, g.generation, g.current_execution_id").
		WithArgs(token.TenantID, token.AgentAppID, token.SessionID).
		WillReturnRows(sqlmock.NewRows([]string{
			"status", "generation", "current_execution_id", "execution_token", "lease_until", "lease_valid",
		}).AddRow("RUNNING", token.Generation, token.ExecutionID, token.Value, now.Add(10*time.Minute), true))
	mock.ExpectQuery("SELECT pg_advisory_unlock").WithArgs(token.Scope()).
		WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatalf("second release error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSessionFenceRejectsStaleGeneration(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	token := testFenceToken()
	mock.ExpectExec("SELECT pg_advisory_lock").WithArgs(token.Scope()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT g.status, g.generation, g.current_execution_id").
		WithArgs(token.TenantID, token.AgentAppID, token.SessionID).
		WillReturnRows(sqlmock.NewRows([]string{
			"status", "generation", "current_execution_id", "execution_token", "status", "lease_until", "heartbeat_at", "lease_valid",
		}).AddRow("RUNNING", token.Generation+1, token.ExecutionID, token.Value, "RUNNING", time.Now().Add(time.Minute), time.Now().Add(-time.Minute), true))
	mock.ExpectQuery("SELECT pg_advisory_unlock").WithArgs(token.Scope()).
		WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))
	_, err = NewPostgresSessionFence(db).Acquire(context.Background(), token)
	if !errors.Is(err, fence.ErrMismatch) {
		t.Fatalf("error=%v, want ErrMismatch", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSessionFenceUnlockFailureIsStickyAndDiscardsConnection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC()
	token := testFenceToken()
	mock.ExpectExec("SELECT pg_advisory_lock").WithArgs(token.Scope()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT g.status, g.generation, g.current_execution_id").
		WithArgs(token.TenantID, token.AgentAppID, token.SessionID).
		WillReturnRows(sqlmock.NewRows([]string{
			"status", "generation", "current_execution_id", "execution_token", "status", "lease_until", "heartbeat_at", "lease_valid",
		}).AddRow("RUNNING", token.Generation, token.ExecutionID, token.Value, "RUNNING", now.Add(10*time.Minute), now.Add(-5*time.Minute), true))

	release, err := NewPostgresSessionFence(db).Acquire(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT g.status, g.generation, g.current_execution_id").
		WithArgs(token.TenantID, token.AgentAppID, token.SessionID).
		WillReturnRows(sqlmock.NewRows([]string{
			"status", "generation", "current_execution_id", "execution_token", "lease_until", "lease_valid",
		}).AddRow("RUNNING", token.Generation, token.ExecutionID, token.Value, now.Add(10*time.Minute), true))
	mock.ExpectQuery("SELECT pg_advisory_unlock").WithArgs(token.Scope()).
		WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(false))
	firstErr := release()
	if firstErr == nil || !strings.Contains(firstErr.Error(), "lock was not held") {
		t.Fatalf("release error=%v, want failed unlock", firstErr)
	}
	secondErr := release()
	if secondErr == nil || secondErr.Error() != firstErr.Error() {
		t.Fatalf("second release error=%v, want sticky %v", secondErr, firstErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSessionFenceMissingGuardStillUnlocks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	token := testFenceToken()
	mock.ExpectExec("SELECT pg_advisory_lock").WithArgs(token.Scope()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT g.status, g.generation, g.current_execution_id").
		WithArgs(token.TenantID, token.AgentAppID, token.SessionID).
		WillReturnRows(sqlmock.NewRows([]string{
			"status", "generation", "current_execution_id", "execution_token", "status", "lease_until", "heartbeat_at", "lease_valid",
		}))
	mock.ExpectQuery("SELECT pg_advisory_unlock").WithArgs(token.Scope()).
		WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))

	_, err = NewPostgresSessionFence(db).Acquire(context.Background(), token)
	if !errors.Is(err, fence.ErrMismatch) {
		t.Fatalf("error=%v, want ErrMismatch", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionLeaseTTLUsesHeartbeatWindow(t *testing.T) {
	now := time.Now().UTC()
	heartbeatAt := now.Add(-5 * time.Minute)
	leaseUntil := now.Add(10 * time.Minute)

	got, err := executionLeaseTTL(leaseUntil, heartbeatAt)
	if err != nil {
		t.Fatal(err)
	}
	if got != 15*time.Minute {
		t.Fatalf("execution lease TTL=%s, want 15m", got)
	}
}

// Keep database/sql imported in this file's compile-time contract when the
// driver changes its nullable time implementation.
var _ sql.NullTime
