package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestResolveRequiresActiveTenantAndNormalizesNilContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery("(?s)"+regexp.QuoteMeta("FROM agent_apps aa")+".*"+regexp.QuoteMeta("t.status='active'")).
		WithArgs("tenant-a", "assistant").
		WillReturnRows(sqlmock.NewRows([]string{
			"app_id", "app_name", "version_id", "version_number",
			"deployment_id", "kind", "traffic_bps", "config_snapshot",
		}))

	_, err = NewPostgresResolver(db).Resolve(nil, "tenant-a", "assistant", "session-a")
	if !errors.Is(err, ErrNoActiveDeployment) {
		t.Fatalf("resolve error = %v, want ErrNoActiveDeployment", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResolvePinnedRequiresActiveTenantForExistingAndNewBindings(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("(?s)"+regexp.QuoteMeta("FROM invocation_bindings ib")+".*"+regexp.QuoteMeta("t.status='active'")).
		WithArgs("tenant-a", "inbox:1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("(?s)"+regexp.QuoteMeta("SELECT aa.id FROM agent_apps aa")+".*"+regexp.QuoteMeta("t.status='active'")).
		WithArgs("tenant-a", "assistant").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err = NewPostgresResolver(db).ResolvePinned(
		nil, "tenant-a", "assistant", "session-a", "inbox:1",
	)
	if !errors.Is(err, ErrNoActiveDeployment) {
		t.Fatalf("pinned resolve error = %v, want ErrNoActiveDeployment", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStartWithRequestRequiresActiveTenantAndNormalizesNilContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("(?s)"+regexp.QuoteMeta("SELECT session_id, payload_hash, agent_app_id, agent_version_id, deployment_id")+
		".*"+regexp.QuoteMeta("FROM invocation_bindings")+".*"+regexp.QuoteMeta("t.status='active'")).
		WithArgs("tenant-a", "inbox:1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err = NewExecutionRecorder(db).StartWithRequest(
		nil,
		"tenant-a",
		"session-a",
		"inbox:1",
		testExecutionPayloadHash,
		&ResolvedDeployment{AgentAppID: "app-1", VersionID: "version-1", DeploymentID: "deployment-1"},
	)
	if !errors.Is(err, ErrInvocationBindingMissing) {
		t.Fatalf("start error = %v, want ErrInvocationBindingMissing", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeContextPreservesNonNilContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if got := normalizeContext(ctx); got != ctx {
		t.Fatal("normalizeContext replaced a non-nil context")
	}
}
