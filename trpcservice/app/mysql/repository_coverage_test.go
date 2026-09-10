package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

func TestAgentMySQLMutationPreflightErrorBranches(t *testing.T) {
	t.Run("create maps insert error", func(t *testing.T) {
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO agent_app").WillReturnError(errors.New("insert"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).Create(context.Background(), appmodel.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", AppKey: "coverage-create", DisplayName: "Coverage"})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("Create error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("list revisions maps revision readback error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectQuery("SELECT revision FROM agent_app_revision").WithArgs(app.TenantID, app.AppID).
			WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(int64(1)))
		mock.ExpectQuery("SELECT tenant_id, app_id, revision").WithArgs(app.TenantID, app.AppID, int64(1)).
			WillReturnError(errors.New("revision readback"))
		_, _, err := NewAppRepository(db).ListRevisions(context.Background(), app.TenantID, app.AppID, "", "", "", 50)
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("ListRevisions error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("metadata rejects disabled app", func(t *testing.T) {
		app := newStoredAgentApp(t)
		app.Status = appmodel.StatusDisabled
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, DisplayName: "Updated", Description: app.Description})
		if !errors.Is(err, appmodel.ErrDisabled) {
			t.Fatalf("disabled metadata error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("metadata rejects stale version", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version + 1, DisplayName: "Updated", Description: app.Description})
		if !errors.Is(err, appmodel.ErrConflict) {
			t.Fatalf("stale metadata error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("metadata maps update error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectExec("UPDATE agent_app SET display_name").WillReturnError(errors.New("update"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, DisplayName: "Updated", Description: app.Description})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("metadata update error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("metadata maps readback error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectExec("UPDATE agent_app SET display_name").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("readback"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, DisplayName: "Updated", Description: app.Description})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("metadata readback error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("create draft maps app read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("app read"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).CreateDraft(context.Background(), mysqlCreateDraftInput(app))
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("CreateDraft app read error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("create draft maps revision number error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectQuery("COALESCE\\(MAX\\(revision\\)").WillReturnError(errors.New("revision number"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).CreateDraft(context.Background(), mysqlCreateDraftInput(app))
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("CreateDraft revision number error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("update draft maps app read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("app read"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateDraft(context.Background(), mysqlUpdateDraftInput(app, 1))
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("UpdateDraft app read error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("update draft maps revision read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("revision read"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateDraft(context.Background(), mysqlUpdateDraftInput(app, 1))
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("UpdateDraft revision read error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("update draft rejects stale draft version", func(t *testing.T) {
		app := newStoredAgentApp(t)
		draft := newStoredAgentRevision(t, app, 1, false)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		expectAgentRevision(t, mock, draft)
		mock.ExpectRollback()
		input := mysqlUpdateDraftInput(app, draft.Revision)
		input.ExpectedDraftVersion++
		_, err := NewAppRepository(db).UpdateDraft(context.Background(), input)
		if !errors.Is(err, appmodel.ErrConflict) {
			t.Fatalf("stale UpdateDraft error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("update draft maps rows affected conflict", func(t *testing.T) {
		app := newStoredAgentApp(t)
		draft := newStoredAgentRevision(t, app, 1, false)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		expectAgentRevision(t, mock, draft)
		mock.ExpectExec("UPDATE agent_app_revision SET").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateDraft(context.Background(), mysqlUpdateDraftInput(app, draft.Revision))
		if !errors.Is(err, appmodel.ErrConflict) {
			t.Fatalf("UpdateDraft rows conflict = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})
}

func TestAgentMySQLPublishReadbackErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	draft := newStoredAgentRevision(t, app, 1, false)
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "coverage", CorrelationID: "mysql-publish-coverage"}
	input := appmodel.PublishInput{TenantID: app.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true, Metadata: metadata}
	storedRevision := newStoredAgentRevision(t, app, draft.Revision, true)
	storedApp := app.Clone()
	storedApp.Status = appmodel.StatusActive
	storedApp.CurrentRevision = agentInt64(draft.Revision)
	storedApp.Version++
	storedApp.UpdatedAt = storedRevision.UpdatedAt

	setPrefix := func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT status FROM tenant").WithArgs(app.TenantID).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
		expectAgentApp(mock, app)
		expectAgentRevision(t, mock, draft)
	}
	setPersistence := func(mock sqlmock.Sqlmock) {
		mock.ExpectExec("UPDATE agent_app_revision SET").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
	}{
		{name: "persist error", setup: func(mock sqlmock.Sqlmock) {
			setPrefix(mock)
			mock.ExpectExec("UPDATE agent_app_revision SET").WillReturnError(errors.New("persist"))
			mock.ExpectRollback()
		}},
		{name: "app readback error", setup: func(mock sqlmock.Sqlmock) {
			setPrefix(mock)
			setPersistence(mock)
			mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("app readback"))
			mock.ExpectRollback()
		}},
		{name: "revision readback error", setup: func(mock sqlmock.Sqlmock) {
			setPrefix(mock)
			setPersistence(mock)
			expectAgentApp(mock, &storedApp)
			mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("revision readback"))
			mock.ExpectRollback()
		}},
		{name: "event readback error", setup: func(mock sqlmock.Sqlmock) {
			setPrefix(mock)
			setPersistence(mock)
			expectAgentApp(mock, &storedApp)
			expectAgentRevision(t, mock, storedRevision)
			mock.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("short"))
			mock.ExpectRollback()
		}},
		{name: "commit error", setup: func(mock sqlmock.Sqlmock) {
			setPrefix(mock)
			setPersistence(mock)
			expectAgentApp(mock, &storedApp)
			expectAgentRevision(t, mock, storedRevision)
			expectAgentEvent(mock, &storedApp, appmodel.ChangePublished, appmodel.StatusDraft, appmodel.StatusActive, nil, storedApp.CurrentRevision, storedRevision.ContentDigest, app.Version, storedApp.Version, storedRevision.UpdatedAt)
			mock.ExpectCommit().WillReturnError(errors.New("commit"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newMySQLCoverageDB(t)
			tc.setup(mock)
			_, _, _, err := NewAppRepository(db).Publish(context.Background(), input)
			if !errors.Is(err, ErrStorage) {
				t.Fatalf("Publish error = %v", err)
			}
			assertMySQLCoverageExpectations(t, mock)
		})
	}
}

func TestAgentMySQLControlPlanePreflightErrorBranches(t *testing.T) {
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "coverage", CorrelationID: "mysql-control-coverage"}

	t.Run("canary begin error", func(t *testing.T) {
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin"))
		_, _, err := NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: "tenant", AppID: "app", TenantActive: true, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("SetCanary begin error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("canary app read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("app read"))
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, TenantActive: true, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("SetCanary app read error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("canary rejects disabled app", func(t *testing.T) {
		app := newStoredAgentApp(t)
		app.Status = appmodel.StatusDisabled
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, TenantActive: true, Metadata: metadata})
		if !errors.Is(err, appmodel.ErrDisabled) {
			t.Fatalf("SetCanary disabled app error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("canary rejects draft candidate", func(t *testing.T) {
		app := newStoredAgentApp(t)
		currentRevision := int64(1)
		app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 2
		candidate := newStoredAgentRevision(t, app, 2, false)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		expectAgentRevision(t, mock, candidate)
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: app.TenantID, AppID: app.AppID, CandidateRevision: agentInt64(candidate.Revision), ExpectedAppVersion: app.Version, TenantActive: true, Metadata: metadata})
		if !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("draft canary candidate error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("rollback app read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("app read"))
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).Rollback(context.Background(), appmodel.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: 1, ExpectedAppVersion: app.Version, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("Rollback app read error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("rollback rejects disabled app", func(t *testing.T) {
		app := newStoredAgentApp(t)
		app.Status = appmodel.StatusDisabled
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).Rollback(context.Background(), appmodel.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: 1, ExpectedAppVersion: app.Version, Metadata: metadata})
		if !errors.Is(err, appmodel.ErrDisabled) {
			t.Fatalf("Rollback disabled app error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("rollback target read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		currentRevision := int64(2)
		app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 3
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("target read"))
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).Rollback(context.Background(), appmodel.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: 1, ExpectedAppVersion: app.Version, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("Rollback target read error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("transition app read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("app read"))
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusDisabled, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("TransitionStatus app read error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("transition rejects disabled app", func(t *testing.T) {
		app := newStoredAgentApp(t)
		app.Status = appmodel.StatusDisabled
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusActive, Metadata: metadata})
		if !errors.Is(err, appmodel.ErrDisabled) {
			t.Fatalf("TransitionStatus disabled app error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("transition rejects invalid status transition", func(t *testing.T) {
		app := newStoredAgentApp(t)
		currentRevision := int64(1)
		app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 2
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusDraft, Metadata: metadata})
		if !errors.Is(err, appmodel.ErrInvalidTransition) {
			t.Fatalf("invalid transition error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})

	t.Run("transition current revision read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		currentRevision := int64(1)
		app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 2
		db, mock := newMySQLCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("current revision read"))
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusSuspended, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("TransitionStatus revision read error = %v", err)
		}
		assertMySQLCoverageExpectations(t, mock)
	})
}

func newMySQLCoverageDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func assertMySQLCoverageExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func mysqlCreateDraftInput(app *appmodel.App) appmodel.CreateDraftInput {
	return appmodel.CreateDraftInput{
		TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
		Configuration: appmodel.DraftConfiguration{Instruction: "coverage", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: appmodel.DefaultRuntimePolicy()},
	}
}

func mysqlUpdateDraftInput(app *appmodel.App, revision int64) appmodel.UpdateDraftInput {
	return appmodel.UpdateDraftInput{
		TenantID: app.TenantID, AppID: app.AppID, Revision: revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: 1,
		Configuration: appmodel.DraftConfiguration{Instruction: "updated coverage", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: appmodel.DefaultRuntimePolicy()},
	}
}
