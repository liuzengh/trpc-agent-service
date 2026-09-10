package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

func TestAgentPostgresMutationPreflightErrorBranches(t *testing.T) {
	t.Run("get maps missing app", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WithArgs(app.TenantID, app.AppID).WillReturnError(sql.ErrNoRows)
		_, err := NewAppRepository(db).Get(context.Background(), app.TenantID, app.AppID)
		if !errors.Is(err, appmodel.ErrNotFound) {
			t.Fatalf("Get missing app error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("metadata rejects stale version", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateMetadata(context.Background(), appmodel.UpdateMetadataInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version + 1, DisplayName: "Updated", Description: app.Description})
		if !errors.Is(err, appmodel.ErrConflict) {
			t.Fatalf("stale metadata error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("create draft maps revision number error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectQuery("COALESCE\\(MAX\\(revision\\)").WillReturnError(errors.New("revision number"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).CreateDraft(context.Background(), postgresCreateDraftInput(app))
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("CreateDraft revision number error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("update draft maps revision read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("revision read"))
		mock.ExpectRollback()
		_, err := NewAppRepository(db).UpdateDraft(context.Background(), postgresUpdateDraftInput(app, 1))
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("UpdateDraft revision read error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("transition checks already cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := NewAppRepository(nil).TransitionStatus(ctx, appmodel.TransitionStatusInput{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("TransitionStatus canceled error = %v", err)
		}
	})
}

func TestAgentPostgresPublishErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	draft := newStoredAgentRevision(t, app, 1, false)
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "coverage", CorrelationID: "postgres-publish-coverage"}
	input := appmodel.PublishInput{TenantID: app.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true, Metadata: metadata}
	storedRevision := newStoredAgentRevision(t, app, draft.Revision, true)
	storedApp := app.Clone()
	storedApp.Status = appmodel.StatusActive
	storedApp.CurrentRevision = agentInt64(draft.Revision)
	storedApp.Version++
	storedApp.UpdatedAt = storedRevision.UpdatedAt

	prefix := func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT status FROM public.tenant").WithArgs(app.TenantID).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
		expectAgentApp(mock, app)
		expectAgentRevision(t, mock, draft)
	}
	prefixBeforeRevision := func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT status FROM public.tenant").WithArgs(app.TenantID).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
		expectAgentApp(mock, app)
	}
	persist := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT public.control_plane_publish_agent_app").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(7)))
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{name: "begin error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin().WillReturnError(errors.New("begin"))
		}, want: ErrStorage},
		{name: "app read error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT status FROM public.tenant").WithArgs(app.TenantID).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
			mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("app read"))
			mock.ExpectRollback()
		}, want: ErrStorage},
		{name: "revision read error", setup: func(mock sqlmock.Sqlmock) {
			prefixBeforeRevision(mock)
			mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("revision read"))
			mock.ExpectRollback()
		}, want: ErrStorage},
		{name: "draft version conflict", setup: func(mock sqlmock.Sqlmock) {
			stale := draft.Clone()
			stale.DraftVersion++
			prefixBeforeRevision(mock)
			expectAgentRevision(t, mock, &stale)
			mock.ExpectRollback()
		}, want: appmodel.ErrConflict},
		{name: "control plane function error", setup: func(mock sqlmock.Sqlmock) {
			prefix(mock)
			mock.ExpectQuery("SELECT public.control_plane_publish_agent_app").WillReturnError(errors.New("publish function"))
			mock.ExpectRollback()
		}, want: ErrStorage},
		{name: "stored app readback error", setup: func(mock sqlmock.Sqlmock) {
			prefix(mock)
			persist(mock)
			mock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("app readback"))
			mock.ExpectRollback()
		}, want: ErrStorage},
		{name: "stored revision readback error", setup: func(mock sqlmock.Sqlmock) {
			prefix(mock)
			persist(mock)
			expectAgentApp(mock, &storedApp)
			mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("revision readback"))
			mock.ExpectRollback()
		}, want: ErrStorage},
		{name: "event readback error", setup: func(mock sqlmock.Sqlmock) {
			prefix(mock)
			persist(mock)
			expectAgentApp(mock, &storedApp)
			expectAgentRevision(t, mock, storedRevision)
			mock.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("short"))
			mock.ExpectRollback()
		}, want: ErrStorage},
		{name: "commit error", setup: func(mock sqlmock.Sqlmock) {
			prefix(mock)
			persist(mock)
			expectAgentApp(mock, &storedApp)
			expectAgentRevision(t, mock, storedRevision)
			expectAgentEvent(mock, &storedApp, appmodel.ChangePublished, appmodel.StatusDraft, appmodel.StatusActive, nil, storedApp.CurrentRevision, storedRevision.ContentDigest, app.Version, storedApp.Version, storedRevision.UpdatedAt)
			mock.ExpectCommit().WillReturnError(errors.New("commit"))
		}, want: ErrStorage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresCoverageDB(t)
			tc.setup(mock)
			_, _, _, err := NewAppRepository(db).Publish(context.Background(), input)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Publish error = %v, want %v", err, tc.want)
			}
			assertPostgresCoverageExpectations(t, mock)
		})
	}
}

func TestAgentPostgresRollbackErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(2)
	app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 3
	target := newStoredAgentRevision(t, app, 1, true)
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "coverage", CorrelationID: "postgres-rollback-coverage"}
	input := appmodel.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: target.Revision, ExpectedAppVersion: app.Version, Metadata: metadata}
	stored := app.Clone()
	stored.CurrentRevision = agentInt64(target.Revision)
	stored.Version++
	stored.UpdatedAt = target.UpdatedAt

	prefix := func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		expectAgentRevision(t, mock, target)
	}
	persist := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT public.control_plane_rollback_agent_app").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(8)))
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{name: "begin error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin().WillReturnError(errors.New("begin"))
		}},
		{name: "disabled app", setup: func(mock sqlmock.Sqlmock) {
			disabled := app.Clone()
			disabled.Status = appmodel.StatusDisabled
			disabled.CurrentRevision = nil
			mock.ExpectBegin()
			expectAgentApp(mock, &disabled)
			mock.ExpectRollback()
		}, want: appmodel.ErrDisabled},
		{name: "target read error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectBegin()
			expectAgentApp(mock, app)
			mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("target read"))
			mock.ExpectRollback()
		}, want: ErrStorage},
		{name: "control plane function error", setup: func(mock sqlmock.Sqlmock) {
			prefix(mock)
			mock.ExpectQuery("SELECT public.control_plane_rollback_agent_app").WillReturnError(errors.New("rollback function"))
			mock.ExpectRollback()
		}},
		{name: "event readback error", setup: func(mock sqlmock.Sqlmock) {
			prefix(mock)
			persist(mock)
			expectAgentApp(mock, &stored)
			mock.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("short"))
			mock.ExpectRollback()
		}},
		{name: "commit error", setup: func(mock sqlmock.Sqlmock) {
			prefix(mock)
			persist(mock)
			expectAgentApp(mock, &stored)
			expectAgentEvent(mock, &stored, appmodel.ChangeRolledBack, appmodel.StatusActive, appmodel.StatusActive, app.CurrentRevision, stored.CurrentRevision, target.ContentDigest, app.Version, stored.Version, stored.UpdatedAt)
			mock.ExpectCommit().WillReturnError(errors.New("commit"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newPostgresCoverageDB(t)
			tc.setup(mock)
			_, _, err := NewAppRepository(db).Rollback(context.Background(), input)
			want := tc.want
			if want == nil {
				want = ErrStorage
			}
			if !errors.Is(err, want) {
				t.Fatalf("Rollback error = %v", err)
			}
			assertPostgresCoverageExpectations(t, mock)
		})
	}
}

func TestAgentPostgresSetCanaryAndTransitionErrorBranches(t *testing.T) {
	metadata := appmodel.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "coverage", CorrelationID: "postgres-control-coverage"}

	t.Run("canary rejects inactive tenant", func(t *testing.T) {
		db, mock := newPostgresCoverageDB(t)
		_, _, err := NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: "tenant", AppID: "app", TenantActive: false, Metadata: metadata})
		if !errors.Is(err, appmodel.ErrInvalid) {
			t.Fatalf("inactive tenant error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("canary begin error", func(t *testing.T) {
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin"))
		_, _, err := NewAppRepository(db).SetCanary(context.Background(), appmodel.SetCanaryInput{TenantID: "tenant", AppID: "app", TenantActive: true, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("canary begin error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("transition begin error", func(t *testing.T) {
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin"))
		_, _, err := NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: "tenant", AppID: "app", ExpectedVersion: 1, NextStatus: appmodel.StatusDisabled, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("transition begin error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("transition current revision read error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		currentRevision := int64(1)
		app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 2
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("revision read"))
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusSuspended, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("transition revision read error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("transition event readback error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		currentRevision := int64(1)
		app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 2
		revision := newStoredAgentRevision(t, app, 1, true)
		stored := app.Clone()
		stored.Status = appmodel.StatusSuspended
		stored.Version++
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		expectAgentRevision(t, mock, revision)
		mock.ExpectQuery("SELECT public.control_plane_transition_agent_app_status").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(9)))
		expectAgentApp(mock, &stored)
		mock.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("short"))
		mock.ExpectRollback()
		_, _, err := NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusSuspended, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("transition event error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})

	t.Run("transition commit error", func(t *testing.T) {
		app := newStoredAgentApp(t)
		currentRevision := int64(1)
		app.Status, app.CurrentRevision, app.Version = appmodel.StatusActive, &currentRevision, 2
		revision := newStoredAgentRevision(t, app, 1, true)
		stored := app.Clone()
		stored.Status = appmodel.StatusSuspended
		stored.Version++
		db, mock := newPostgresCoverageDB(t)
		mock.ExpectBegin()
		expectAgentApp(mock, app)
		expectAgentRevision(t, mock, revision)
		mock.ExpectQuery("SELECT public.control_plane_transition_agent_app_status").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(9)))
		expectAgentApp(mock, &stored)
		expectAgentEvent(mock, &stored, appmodel.ChangeSuspended, appmodel.StatusActive, appmodel.StatusSuspended, app.CurrentRevision, stored.CurrentRevision, revision.ContentDigest, app.Version, stored.Version, stored.UpdatedAt)
		mock.ExpectCommit().WillReturnError(errors.New("commit"))
		_, _, err := NewAppRepository(db).TransitionStatus(context.Background(), appmodel.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: appmodel.StatusSuspended, Metadata: metadata})
		if !errors.Is(err, ErrStorage) {
			t.Fatalf("transition commit error = %v", err)
		}
		assertPostgresCoverageExpectations(t, mock)
	})
}

func TestAgentPostgresLoadRevisionDecodesTools(t *testing.T) {
	app := newStoredAgentApp(t)
	revision := newStoredAgentRevision(t, app, 1, false)
	revision.Tools = []appmodel.ToolAuthorization{{ToolID: "tool-a", Required: true}}
	generation, runtime, _, err := encodeAgentRevisionParts(*revision)
	if err != nil {
		t.Fatal(err)
	}
	db, mock := newPostgresCoverageDB(t)
	mock.ExpectQuery("SELECT tenant_id, app_id, revision").WithArgs(app.TenantID, app.AppID, revision.Revision).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "revision", "state", "draft_version", "agent_kind", "schema_version", "description", "instruction", "global_instruction", "model_profile_id", "generation_config", "runtime_policy", "content_digest", "published_at", "created_at", "updated_at",
	}).AddRow(revision.TenantID, revision.AppID, revision.Revision, string(revision.State), revision.DraftVersion, string(revision.Kind), revision.SchemaVersion, revision.Description, revision.Instruction, revision.GlobalInstruction, revision.ModelProfileID, generation, runtime, nil, nil, revision.CreatedAt, revision.UpdatedAt))
	mock.ExpectQuery("SELECT tool_id, required").WithArgs(app.TenantID, app.AppID, revision.Revision).WillReturnRows(sqlmock.NewRows([]string{"tool_id", "required"}).AddRow("tool-a", true))
	loaded, err := NewAppRepository(db).GetRevision(context.Background(), app.TenantID, app.AppID, revision.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Tools) != 1 || loaded.Tools[0].ToolID != "tool-a" || !loaded.Tools[0].Required {
		t.Fatalf("decoded tools = %+v", loaded.Tools)
	}
	assertPostgresCoverageExpectations(t, mock)
}

func newPostgresCoverageDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func assertPostgresCoverageExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func postgresCreateDraftInput(app *appmodel.App) appmodel.CreateDraftInput {
	return appmodel.CreateDraftInput{
		TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
		Configuration: appmodel.DraftConfiguration{Instruction: "coverage", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: appmodel.DefaultRuntimePolicy()},
	}
}

func postgresUpdateDraftInput(app *appmodel.App, revision int64) appmodel.UpdateDraftInput {
	return appmodel.UpdateDraftInput{
		TenantID: app.TenantID, AppID: app.AppID, Revision: revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: 1,
		Configuration: appmodel.DraftConfiguration{Instruction: "updated coverage", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: appmodel.DefaultRuntimePolicy()},
	}
}
