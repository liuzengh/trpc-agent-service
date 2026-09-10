package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestPostgreSQLRepositoriesRollbackWhenReadAfterControlledWriteFails(t *testing.T) {
	ctx := context.Background()
	modelCatalog, backendCatalog := mockCatalogs(t)
	draftApp := mockApp(t, 1)
	draft := mockRevision(t, draftApp, 2, false)
	rollbackApp := mockApp(t, 2)
	rollbackTarget := mockRevision(t, rollbackApp, 1, true)
	transitionApp := mockApp(t, 1)
	transitionRevision := mockRevision(t, transitionApp, 1, true)
	readFailure := errors.New("read after write failure")

	t.Run("tenant create", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewTenantRepository(db)
		mock.ExpectBegin()
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, err := repo.Create(ctx, tenant.CreateInput{
			TenantKey: "postwrite-tenant", DisplayName: "Postwrite Tenant", Status: tenant.StatusActive,
			AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("tenant update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewTenantRepository(db)
		current := mockTenant(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockTenantRows(current))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"next_version"}).AddRow(current.Version + 1))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, err := repo.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
			TenantID: current.TenantID, ExpectedVersion: current.Version, DisplayName: "Updated Tenant",
			AuditRetentionDays: current.AuditRetentionDays, LogMaskingLevel: current.LogMaskingLevel, TraceSamplingRate: current.TraceSamplingRate,
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("tenant transition", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewTenantRepository(db)
		current := mockTenant(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockTenantRows(current))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, tenant.TransitionStatusInput{
			TenantID: current.TenantID, ExpectedVersion: current.Version, NextStatus: tenant.StatusSuspended, Metadata: mockTenantMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("model create", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewModelRepository(db, modelCatalog)
		current := mockModel(t, modelCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.Create(ctx, model.CreateInput{
			TenantID: current.TenantID, ProfileKey: "postwrite-model", DisplayName: "Postwrite Model", Status: model.StatusActive,
			Configuration: current.Configuration, Metadata: mockModelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("model create event read", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewModelRepository(db, modelCatalog)
		current := mockModel(t, modelCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnRows(mockModelRows(t, current))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.Create(ctx, model.CreateInput{
			TenantID: current.TenantID, ProfileKey: "postwrite-model-event", DisplayName: "Postwrite Model Event", Status: model.StatusActive,
			Configuration: current.Configuration, Metadata: mockModelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("model update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewModelRepository(db, modelCatalog)
		current := mockModel(t, modelCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockModelRows(t, current))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.UpdateConfiguration(ctx, model.UpdateConfigurationInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			DisplayName: "Updated Model", SchemaVersion: current.SchemaVersion, Configuration: current.Configuration, Metadata: mockModelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("model transition", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewModelRepository(db, modelCatalog)
		current := mockModel(t, modelCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockModelRows(t, current))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, model.TransitionStatusInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			NextStatus: model.StatusSuspended, Metadata: mockModelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("backend create", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewBackendRepository(db, backendCatalog)
		current := mockBackend(t, backendCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.Create(ctx, backend.CreateInput{
			TenantID: current.TenantID, ProfileKey: "postwrite-backend", DisplayName: "Postwrite Backend", Status: backend.StatusActive,
			Bindings: current.Bindings, Metadata: mockBackendMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("backend create event read", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewBackendRepository(db, backendCatalog)
		current := mockBackend(t, backendCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnRows(mockBackendRootRows(current))
		mock.ExpectQuery(".*").WillReturnRows(mockBackendBindingRows(current))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.Create(ctx, backend.CreateInput{
			TenantID: current.TenantID, ProfileKey: "postwrite-backend-event", DisplayName: "Postwrite Backend Event", Status: backend.StatusActive,
			Bindings: current.Bindings, Metadata: mockBackendMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("backend update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewBackendRepository(db, backendCatalog)
		current := mockBackend(t, backendCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockBackendRootRows(current))
		mock.ExpectQuery(".*").WillReturnRows(mockBackendBindingRows(current))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			DisplayName: "Updated Backend", SchemaVersion: current.SchemaVersion, Bindings: current.Bindings, Metadata: mockBackendMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("backend transition", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewBackendRepository(db, backendCatalog)
		current := mockBackend(t, backendCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockBackendRootRows(current))
		mock.ExpectQuery(".*").WillReturnRows(mockBackendBindingRows(current))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"next_version"}).AddRow(current.Version + 1))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, backend.TransitionStatusInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			NextStatus: backend.StatusSuspended, Metadata: mockBackendMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("channel create", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewChannelRepository(db)
		current := mockBinding(t, draftApp.AppID)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.Create(ctx, channels.CreateInput{
			TenantID: current.TenantID, BindingKey: "postwrite-binding", Channel: current.Channel,
			ProviderAccountID: current.ProviderAccountID, PublicRouteKeyDigest: current.PublicRouteKeyDigest, AppID: current.AppID,
			SecretRef: current.SecretRef, Protocol: current.Protocol, Status: channels.StatusActive, Metadata: mockChannelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("channel create event read", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewChannelRepository(db)
		current := mockBinding(t, draftApp.AppID)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnRows(mockBindingRows(current))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.Create(ctx, channels.CreateInput{
			TenantID: current.TenantID, BindingKey: "postwrite-binding-event", Channel: current.Channel,
			ProviderAccountID: current.ProviderAccountID, PublicRouteKeyDigest: current.PublicRouteKeyDigest, AppID: current.AppID,
			SecretRef: current.SecretRef, Protocol: current.Protocol, Status: channels.StatusActive, Metadata: mockChannelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("channel update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewChannelRepository(db)
		current := mockBinding(t, draftApp.AppID)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockBindingRows(current))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{
			TenantID: current.TenantID, BindingID: current.BindingID, ExpectedVersion: current.Version,
			ProviderAccountID: current.ProviderAccountID, PublicRouteKeyDigest: current.PublicRouteKeyDigest,
			AppID: current.AppID, SecretRef: current.SecretRef, Protocol: current.Protocol, Metadata: mockChannelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("channel transition", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewChannelRepository(db)
		current := mockBinding(t, draftApp.AppID)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockBindingRows(current))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, channels.TransitionStatusInput{
			TenantID: current.TenantID, BindingID: current.BindingID, ExpectedVersion: current.Version,
			NextStatus: channels.StatusSuspended, Metadata: mockChannelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("agent create", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(".*").WillReturnError(readFailure)
		mock.ExpectRollback()
		_, err := repo.Create(ctx, appmodel.CreateInput{TenantID: draftApp.TenantID, AppKey: "postwrite-app", DisplayName: "Postwrite App"})
		assertStorageFailure(t, err, mock)
	})

	t.Run("agent metadata", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		expectAgentApp(mock, draftApp)
		mock.ExpectExec(".*").WithArgs(agentWriterArgs(6, draftApp.TenantID, draftApp.AppID, draftApp.Version)...).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(".*").WithArgs(draftApp.TenantID, draftApp.AppID).WillReturnError(readFailure)
		mock.ExpectRollback()
		_, err := repo.UpdateMetadata(ctx, appmodel.UpdateMetadataInput{
			TenantID: draftApp.TenantID, AppID: draftApp.AppID, ExpectedVersion: draftApp.Version, DisplayName: "Updated Agent", Description: "updated",
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("agent draft creation", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		expectAgentApp(mock, draftApp)
		mock.ExpectQuery(".*").WithArgs(draftApp.TenantID, draftApp.AppID).WillReturnRows(sqlmock.NewRows([]string{"next_revision"}).AddRow(int64(3)))
		mock.ExpectExec(".*").WithArgs(agentWriterArgs(16, draftApp.TenantID, draftApp.AppID, int64(3), int64(1))...).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(".*").WithArgs(draftApp.TenantID, draftApp.AppID, int64(3)).WillReturnError(readFailure)
		mock.ExpectRollback()
		_, err := repo.CreateDraft(ctx, appmodel.CreateDraftInput{
			TenantID: draftApp.TenantID, AppID: draftApp.AppID, ExpectedAppVersion: draftApp.Version,
			Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
			Configuration: appmodel.DraftConfiguration{Instruction: "draft", ModelProfileID: mockModelID, Runtime: appmodel.DefaultRuntimePolicy()},
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("agent draft update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		expectAgentApp(mock, draftApp)
		expectAgentRevision(mock, draft)
		mock.ExpectExec(".*").WithArgs(agentWriterArgs(16, draftApp.TenantID, draftApp.AppID, draft.Revision, draft.DraftVersion)...).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(".*").WithArgs(draftApp.TenantID, draftApp.AppID, draft.Revision).WillReturnError(readFailure)
		mock.ExpectRollback()
		_, err := repo.UpdateDraft(ctx, appmodel.UpdateDraftInput{
			TenantID: draftApp.TenantID, AppID: draftApp.AppID, Revision: draft.Revision,
			ExpectedAppVersion: draftApp.Version, ExpectedDraftVersion: draft.DraftVersion,
			Configuration: appmodel.DraftConfiguration{Instruction: "updated", ModelProfileID: mockModelID, Runtime: appmodel.DefaultRuntimePolicy()},
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("agent publication", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WithArgs(draftApp.TenantID).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
		expectAgentApp(mock, draftApp)
		expectAgentRevision(mock, draft)
		mock.ExpectQuery(".*").WithArgs(agentWriterArgs(20, draftApp.TenantID, draftApp.AppID, draft.Revision, draftApp.Version, draft.DraftVersion)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WithArgs(draftApp.TenantID, draftApp.AppID).WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, _, err := repo.Publish(ctx, appmodel.PublishInput{
			TenantID: draftApp.TenantID, AppID: draftApp.AppID, Revision: draft.Revision,
			ExpectedAppVersion: draftApp.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true, Metadata: mockAgentMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("agent rollback", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		expectAgentApp(mock, rollbackApp)
		expectAgentRevision(mock, rollbackTarget)
		mock.ExpectQuery(".*").WithArgs(agentWriterArgs(16, rollbackApp.TenantID, rollbackApp.AppID, rollbackTarget.Revision, rollbackApp.Version)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WithArgs(rollbackApp.TenantID, rollbackApp.AppID).WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.Rollback(ctx, appmodel.RollbackInput{
			TenantID: rollbackApp.TenantID, AppID: rollbackApp.AppID, TargetRevision: rollbackTarget.Revision, ExpectedAppVersion: rollbackApp.Version, Metadata: mockAgentMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("agent transition", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		expectAgentApp(mock, transitionApp)
		expectAgentRevision(mock, transitionRevision)
		mock.ExpectQuery(".*").WithArgs(agentWriterArgs(12, transitionApp.TenantID, transitionApp.AppID, transitionApp.Version)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery(".*").WithArgs(transitionApp.TenantID, transitionApp.AppID).WillReturnError(readFailure)
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, appmodel.TransitionStatusInput{
			TenantID: transitionApp.TenantID, AppID: transitionApp.AppID, ExpectedVersion: transitionApp.Version,
			NextStatus: appmodel.StatusSuspended, Metadata: mockAgentMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})
}

func TestPostgreSQLControlledCreatesCommitAfterCompleteReadback(t *testing.T) {
	ctx := context.Background()
	modelCatalog, backendCatalog := mockCatalogs(t)

	t.Run("tenant", func(t *testing.T) {
		db, mock := newSQLMock(t)
		stored := mockTenant(t)
		mock.ExpectBegin()
		mock.ExpectExec("control_plane_create_tenant").WithArgs(agentWriterArgs(17)...).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("FROM public\\.tenant").WithArgs(sqlmock.AnyArg()).WillReturnRows(mockTenantRows(stored))
		mock.ExpectCommit()
		if _, err := NewTenantRepository(db).Create(ctx, tenant.CreateInput{
			TenantKey: "committed-tenant", DisplayName: "Committed Tenant", Status: tenant.StatusActive,
			AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
		}); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("agent", func(t *testing.T) {
		db, mock := newSQLMock(t)
		stored := mockApp(t, 1)
		mock.ExpectBegin()
		mock.ExpectExec("control_plane_create_agent_app").WithArgs(agentWriterArgs(9)...).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery("FROM public\\.agent_app").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockAgentAppRows(stored))
		mock.ExpectCommit()
		if _, err := NewAgentRepository(db).Create(ctx, appmodel.CreateInput{TenantID: stored.TenantID, AppKey: "committed-app", DisplayName: "Committed App"}); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("model", func(t *testing.T) {
		db, mock := newSQLMock(t)
		stored := mockModel(t, modelCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery("control_plane_create_model_profile").WithArgs(agentWriterArgs(21)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery("FROM public\\.model_profile").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockModelRows(t, stored))
		mock.ExpectQuery("FROM public\\.model_profile_change_outbox").WithArgs(int64(1)).WillReturnRows(mockCreatedChangeEventRows(stored.TenantID, stored.ProfileID, stored.ContentDigest, stored.CreatedAt))
		mock.ExpectCommit()
		if _, _, err := NewModelRepository(db, modelCatalog).Create(ctx, model.CreateInput{
			TenantID: stored.TenantID, ProfileKey: "committed-model", DisplayName: "Committed Model", Status: model.StatusActive,
			Configuration: stored.Configuration, Metadata: mockModelMetadata(),
		}); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("backend", func(t *testing.T) {
		db, mock := newSQLMock(t)
		stored := mockBackend(t, backendCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery("control_plane_create_backend_profile").WithArgs(agentWriterArgs(21)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery("FROM public\\.backend_profile").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockBackendRootRows(stored))
		mock.ExpectQuery("FROM public\\.backend_profile_binding").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockBackendBindingRows(stored))
		mock.ExpectQuery("FROM public\\.backend_profile_change_outbox").WithArgs(int64(1)).WillReturnRows(mockCreatedChangeEventRows(stored.TenantID, stored.ProfileID, stored.ContentDigest, stored.CreatedAt))
		mock.ExpectCommit()
		if _, _, err := NewBackendRepository(db, backendCatalog).Create(ctx, backend.CreateInput{
			TenantID: stored.TenantID, ProfileKey: "committed-backend", DisplayName: "Committed Backend", Status: backend.StatusActive,
			Bindings: stored.Bindings, Metadata: mockBackendMetadata(),
		}); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("channel", func(t *testing.T) {
		db, mock := newSQLMock(t)
		stored := mockBinding(t, mockAppID)
		mock.ExpectBegin()
		mock.ExpectQuery("control_plane_create_channel_binding").WithArgs(agentWriterArgs(24)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
		mock.ExpectQuery("FROM public\\.channel_binding").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockBindingRows(stored))
		mock.ExpectQuery("FROM public\\.channel_binding_change_outbox").WithArgs(int64(1)).WillReturnRows(mockCreatedChangeEventRows(stored.TenantID, stored.BindingID, stored.ConfigDigest, stored.CreatedAt))
		mock.ExpectCommit()
		if _, _, err := NewChannelRepository(db).Create(ctx, channels.CreateInput{
			TenantID: stored.TenantID, BindingKey: "committed-binding", Channel: stored.Channel,
			ProviderAccountID: stored.ProviderAccountID, PublicRouteKeyDigest: stored.PublicRouteKeyDigest, AppID: stored.AppID,
			SecretRef: stored.SecretRef, Protocol: stored.Protocol, Status: channels.StatusActive, Metadata: mockChannelMetadata(),
		}); err != nil {
			t.Fatal(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPostgreSQLControlledCreateTransactionFailureBoundaries(t *testing.T) {
	ctx := context.Background()
	modelCatalog, backendCatalog := mockCatalogs(t)
	tenantValue := mockTenant(t)
	appValue := mockApp(t, 1)
	modelValue := mockModel(t, modelCatalog)
	backendValue := mockBackend(t, backendCatalog)
	channelValue := mockBinding(t, mockAppID)
	writerFailure := errors.New("controlled writer failure")
	commitFailure := errors.New("commit failure")

	type createCase struct {
		name                string
		create              func(*sql.DB) error
		expectWriterFailure func(sqlmock.Sqlmock)
		expectCommitFailure func(sqlmock.Sqlmock)
	}
	cases := []createCase{
		{
			name: "tenant",
			create: func(db *sql.DB) error {
				_, err := NewTenantRepository(db).Create(ctx, tenant.CreateInput{TenantKey: "transaction-tenant", DisplayName: "Transaction Tenant", Status: tenant.StatusActive, AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
				return err
			},
			expectWriterFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("control_plane_create_tenant").WithArgs(agentWriterArgs(17)...).WillReturnError(writerFailure)
			},
			expectCommitFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("control_plane_create_tenant").WithArgs(agentWriterArgs(17)...).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("FROM public\\.tenant").WithArgs(sqlmock.AnyArg()).WillReturnRows(mockTenantRows(tenantValue))
			},
		},
		{
			name: "agent",
			create: func(db *sql.DB) error {
				_, err := NewAgentRepository(db).Create(ctx, appmodel.CreateInput{TenantID: appValue.TenantID, AppKey: "transaction-app", DisplayName: "Transaction App"})
				return err
			},
			expectWriterFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("control_plane_create_agent_app").WithArgs(agentWriterArgs(9)...).WillReturnError(writerFailure)
			},
			expectCommitFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("control_plane_create_agent_app").WithArgs(agentWriterArgs(9)...).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("FROM public\\.agent_app").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockAgentAppRows(appValue))
			},
		},
		{
			name: "model",
			create: func(db *sql.DB) error {
				_, _, err := NewModelRepository(db, modelCatalog).Create(ctx, model.CreateInput{TenantID: modelValue.TenantID, ProfileKey: "transaction-model", DisplayName: "Transaction Model", Status: model.StatusActive, Configuration: modelValue.Configuration, Metadata: mockModelMetadata()})
				return err
			},
			expectWriterFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("control_plane_create_model_profile").WithArgs(agentWriterArgs(21)...).WillReturnError(writerFailure)
			},
			expectCommitFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("control_plane_create_model_profile").WithArgs(agentWriterArgs(21)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
				mock.ExpectQuery("FROM public\\.model_profile").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockModelRows(t, modelValue))
				mock.ExpectQuery("FROM public\\.model_profile_change_outbox").WithArgs(int64(1)).WillReturnRows(mockCreatedChangeEventRows(modelValue.TenantID, modelValue.ProfileID, modelValue.ContentDigest, modelValue.CreatedAt))
			},
		},
		{
			name: "backend",
			create: func(db *sql.DB) error {
				_, _, err := NewBackendRepository(db, backendCatalog).Create(ctx, backend.CreateInput{TenantID: backendValue.TenantID, ProfileKey: "transaction-backend", DisplayName: "Transaction Backend", Status: backend.StatusActive, Bindings: backendValue.Bindings, Metadata: mockBackendMetadata()})
				return err
			},
			expectWriterFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("control_plane_create_backend_profile").WithArgs(agentWriterArgs(21)...).WillReturnError(writerFailure)
			},
			expectCommitFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("control_plane_create_backend_profile").WithArgs(agentWriterArgs(21)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
				mock.ExpectQuery("FROM public\\.backend_profile").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockBackendRootRows(backendValue))
				mock.ExpectQuery("FROM public\\.backend_profile_binding").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockBackendBindingRows(backendValue))
				mock.ExpectQuery("FROM public\\.backend_profile_change_outbox").WithArgs(int64(1)).WillReturnRows(mockCreatedChangeEventRows(backendValue.TenantID, backendValue.ProfileID, backendValue.ContentDigest, backendValue.CreatedAt))
			},
		},
		{
			name: "channel",
			create: func(db *sql.DB) error {
				_, _, err := NewChannelRepository(db).Create(ctx, channels.CreateInput{TenantID: channelValue.TenantID, BindingKey: "transaction-channel", Channel: channelValue.Channel, ProviderAccountID: channelValue.ProviderAccountID, PublicRouteKeyDigest: channelValue.PublicRouteKeyDigest, AppID: channelValue.AppID, SecretRef: channelValue.SecretRef, Protocol: channelValue.Protocol, Status: channels.StatusActive, Metadata: mockChannelMetadata()})
				return err
			},
			expectWriterFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("control_plane_create_channel_binding").WithArgs(agentWriterArgs(24)...).WillReturnError(writerFailure)
			},
			expectCommitFailure: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("control_plane_create_channel_binding").WithArgs(agentWriterArgs(24)...).WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(1)))
				mock.ExpectQuery("FROM public\\.channel_binding").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(mockBindingRows(channelValue))
				mock.ExpectQuery("FROM public\\.channel_binding_change_outbox").WithArgs(int64(1)).WillReturnRows(mockCreatedChangeEventRows(channelValue.TenantID, channelValue.BindingID, channelValue.ConfigDigest, channelValue.CreatedAt))
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name+" begin", func(t *testing.T) {
			db, mock := newSQLMock(t)
			mock.ExpectBegin().WillReturnError(writerFailure)
			assertStorageFailure(t, testCase.create(db), mock)
		})
		t.Run(testCase.name+" writer", func(t *testing.T) {
			db, mock := newSQLMock(t)
			mock.ExpectBegin()
			testCase.expectWriterFailure(mock)
			mock.ExpectRollback()
			assertStorageFailure(t, testCase.create(db), mock)
		})
		t.Run(testCase.name+" commit", func(t *testing.T) {
			db, mock := newSQLMock(t)
			mock.ExpectBegin()
			testCase.expectCommitFailure(mock)
			mock.ExpectCommit().WillReturnError(commitFailure)
			assertStorageFailure(t, testCase.create(db), mock)
		})
	}
}

func mockCreatedChangeEventRows(tenantID, objectID, digest string, occurredAt time.Time) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"event_type", "tenant_id", "object_id", "previous_status", "current_status", "previous_digest", "current_digest",
		"actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow("created", tenantID, objectID, nil, "active", nil, digest, "test", "mock", "test", "mock", int64(0), int64(1), occurredAt)
}
