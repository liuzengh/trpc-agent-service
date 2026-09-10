package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
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

const (
	mockTenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	mockAppID    = "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	mockModelID  = "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestPostgreSQLRepositoriesRollbackAfterControlledWriterFailure(t *testing.T) {
	ctx := context.Background()
	modelCatalog, backendCatalog := mockCatalogs(t)
	draftApp := mockApp(t, 1)
	draft := mockRevision(t, draftApp, 2, false)
	rollbackApp := mockApp(t, 2)
	rollbackTarget := mockRevision(t, rollbackApp, 1, true)
	transitionApp := mockApp(t, 1)
	transitionRevision := mockRevision(t, transitionApp, 1, true)

	t.Run("tenant update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewTenantRepository(db)
		current := mockTenant(t)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockTenantRows(current))
		mock.ExpectQuery(".*").WillReturnError(errors.New("writer failure"))
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
		mock.ExpectQuery(".*").WillReturnError(errors.New("writer failure"))
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, tenant.TransitionStatusInput{
			TenantID: current.TenantID, ExpectedVersion: current.Version, NextStatus: tenant.StatusSuspended, Metadata: mockTenantMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("model update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewModelRepository(db, modelCatalog)
		current := mockModel(t, modelCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockModelRows(t, current))
		mock.ExpectQuery(".*").WillReturnError(errors.New("writer failure"))
		mock.ExpectRollback()
		_, _, err := repo.UpdateConfiguration(ctx, model.UpdateConfigurationInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			DisplayName: "Updated Model", SchemaVersion: current.SchemaVersion, Configuration: current.Configuration,
			Metadata: mockModelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("model transition", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewModelRepository(db, modelCatalog)
		current := mockModel(t, modelCatalog)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockModelRows(t, current))
		mock.ExpectQuery(".*").WillReturnError(errors.New("writer failure"))
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, model.TransitionStatusInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			NextStatus: model.StatusSuspended, Metadata: mockModelMetadata(),
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
		mock.ExpectQuery(".*").WillReturnError(errors.New("writer failure"))
		mock.ExpectRollback()
		_, _, err := repo.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			DisplayName: "Updated Backend", SchemaVersion: current.SchemaVersion, Bindings: current.Bindings,
			Metadata: mockBackendMetadata(),
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
		mock.ExpectQuery(".*").WillReturnError(errors.New("writer failure"))
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, backend.TransitionStatusInput{
			TenantID: current.TenantID, ProfileID: current.ProfileID, ExpectedVersion: current.Version,
			NextStatus: backend.StatusSuspended, Metadata: mockBackendMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("channel update", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewChannelRepository(db)
		current := mockBinding(t, draftApp.AppID)
		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(mockBindingRows(current))
		mock.ExpectQuery(".*").WillReturnError(errors.New("writer failure"))
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
		mock.ExpectQuery(".*").WillReturnError(errors.New("writer failure"))
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, channels.TransitionStatusInput{
			TenantID: current.TenantID, BindingID: current.BindingID, ExpectedVersion: current.Version,
			NextStatus: channels.StatusSuspended, Metadata: mockChannelMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})

	t.Run("agent metadata", func(t *testing.T) {
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		expectAgentApp(mock, draftApp)
		mock.ExpectExec(".*").WithArgs(agentWriterArgs(6, draftApp.TenantID, draftApp.AppID, draftApp.Version)...).WillReturnError(errors.New("writer failure"))
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
		mock.ExpectExec(".*").WithArgs(agentWriterArgs(16, draftApp.TenantID, draftApp.AppID, int64(3), int64(1))...).WillReturnError(errors.New("writer failure"))
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
		mock.ExpectExec(".*").WithArgs(agentWriterArgs(16, draftApp.TenantID, draftApp.AppID, draft.Revision, draft.DraftVersion)...).WillReturnError(errors.New("writer failure"))
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
		mock.ExpectQuery(".*").WithArgs(agentWriterArgs(20, draftApp.TenantID, draftApp.AppID, draft.Revision, draftApp.Version, draft.DraftVersion)...).WillReturnError(errors.New("writer failure"))
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
		mock.ExpectQuery(".*").WithArgs(agentWriterArgs(16, rollbackApp.TenantID, rollbackApp.AppID, rollbackTarget.Revision, rollbackApp.Version)...).WillReturnError(errors.New("writer failure"))
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
		mock.ExpectQuery(".*").WithArgs(agentWriterArgs(12, transitionApp.TenantID, transitionApp.AppID, transitionApp.Version)...).WillReturnError(errors.New("writer failure"))
		mock.ExpectRollback()
		_, _, err := repo.TransitionStatus(ctx, appmodel.TransitionStatusInput{
			TenantID: transitionApp.TenantID, AppID: transitionApp.AppID, ExpectedVersion: transitionApp.Version,
			NextStatus: appmodel.StatusSuspended, Metadata: mockAgentMetadata(),
		})
		assertStorageFailure(t, err, mock)
	})
}

func newSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func assertStorageFailure(t *testing.T, err error, mock sqlmock.Sqlmock) {
	t.Helper()
	if !errors.Is(err, ErrStorage) {
		t.Fatalf("repository error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func mockCatalogs(t *testing.T) (*model.ProviderCatalog, *backend.ProviderCatalog) {
	t.Helper()
	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	return modelCatalog, backendCatalog
}

func mockTenant(t *testing.T) *tenant.Tenant {
	t.Helper()
	value, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "mock-tenant", DisplayName: "Mock Tenant", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mockTenantRows(value *tenant.Tenant) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"tenant_id", "tenant_key", "display_name", "status", "rate_limit_rpm", "max_concurrent_executions",
		"monthly_token_budget", "monthly_spend_limit_minor", "billing_currency", "audit_retention_days",
		"log_masking_level", "trace_sampling_rate", "default_agent_app_id", "default_backend_profile_id", "version", "created_at", "updated_at",
	}).AddRow(value.TenantID, value.TenantKey, value.DisplayName, string(value.Status), value.RateLimitRPM,
		value.MaxConcurrentExecutions, value.MonthlyTokenBudget, value.MonthlySpendLimitMinor, value.BillingCurrency,
		value.AuditRetentionDays, string(value.LogMaskingLevel), value.TraceSamplingRate, value.DefaultAgentAppID,
		value.DefaultBackendProfileID, value.Version, value.CreatedAt, value.UpdatedAt)
}

func mockApp(t *testing.T, currentRevision int64) *appmodel.App {
	t.Helper()
	now := time.Now().UTC()
	value := &appmodel.App{
		TenantID: mockTenantID, AppID: mockAppID, AppKey: "mock-app", DisplayName: "Mock App", Status: appmodel.StatusActive,
		CurrentRevision: &currentRevision, Version: 3, CreatedAt: now, UpdatedAt: now,
	}
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	return value
}

func mockAgentAppRows(value *appmodel.App) *sqlmock.Rows {
	var current any
	if value.CurrentRevision != nil {
		current = *value.CurrentRevision
	}
	return sqlmock.NewRows([]string{"tenant_id", "app_id", "app_key", "display_name", "description", "status", "current_revision", "canary_revision", "version", "created_at", "updated_at"}).
		AddRow(value.TenantID, value.AppID, value.AppKey, value.DisplayName, value.Description, string(value.Status), current, nil, value.Version, value.CreatedAt, value.UpdatedAt)
}

func expectAgentApp(mock sqlmock.Sqlmock, value *appmodel.App) {
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID).WillReturnRows(mockAgentAppRows(value))
}

func mockRevision(t *testing.T, appRoot *appmodel.App, revision int64, published bool) *appmodel.Revision {
	t.Helper()
	value, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: appRoot.TenantID, AppID: appRoot.AppID, Revision: revision, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
		Configuration: appmodel.DraftConfiguration{Instruction: "Answer", ModelProfileID: mockModelID, Runtime: appmodel.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if published {
		publishedValue, err := value.Publish(time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		value = &publishedValue
	}
	return value
}

func expectAgentRevision(mock sqlmock.Sqlmock, value *appmodel.Revision) {
	generation, runtime, _, err := encodeAgentRevisionParts(*value)
	if err != nil {
		panic(err)
	}
	var digest, publishedAt any
	if value.ContentDigest != "" {
		digest = value.ContentDigest
	}
	if value.PublishedAt != nil {
		publishedAt = *value.PublishedAt
	}
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID, value.Revision).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "revision", "state", "draft_version", "agent_kind", "schema_version", "description",
		"instruction", "global_instruction", "model_profile_id", "generation_config", "runtime_policy", "content_digest", "published_at", "created_at", "updated_at",
	}).AddRow(value.TenantID, value.AppID, value.Revision, string(value.State), value.DraftVersion, string(value.Kind),
		value.SchemaVersion, value.Description, value.Instruction, value.GlobalInstruction, value.ModelProfileID, generation,
		runtime, digest, publishedAt, value.CreatedAt, value.UpdatedAt))
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID, value.Revision).WillReturnRows(sqlmock.NewRows([]string{"tool_id", "required"}))
}

func agentWriterArgs(count int, prefix ...driver.Value) []driver.Value {
	args := make([]driver.Value, count)
	copy(args, prefix)
	for index := len(prefix); index < count; index++ {
		args[index] = sqlmock.AnyArg()
	}
	return args
}

func mockModel(t *testing.T, catalog *model.ProviderCatalog) *model.Profile {
	t.Helper()
	value, err := model.NewProfile(model.CreateInput{
		TenantID: mockTenantID, ProfileKey: "mock-model", DisplayName: "Mock Model", Status: model.StatusActive,
		Configuration: model.Configuration{Provider: "public", Model: "chat"}, Metadata: mockModelMetadata(),
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mockModelRows(t *testing.T, value *model.Profile) *sqlmock.Rows {
	t.Helper()
	options, generation, err := encodeModelJSON(value.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	return sqlmock.NewRows([]string{
		"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "provider", "model", "endpoint", "options", "secret_ref", "generation", "content_digest", "version", "created_at", "updated_at",
	}).AddRow(value.TenantID, value.ProfileID, value.ProfileKey, value.DisplayName, value.Description, string(value.Status),
		value.SchemaVersion, value.Configuration.Provider, value.Configuration.Model, value.Configuration.Endpoint, options,
		value.Configuration.SecretRef, generation, value.ContentDigest, value.Version, value.CreatedAt, value.UpdatedAt)
}

func mockBackend(t *testing.T, catalog *backend.ProviderCatalog) *backend.Profile {
	t.Helper()
	value, err := backend.NewProfile(backend.CreateInput{
		TenantID: mockTenantID, ProfileKey: "mock-backend", DisplayName: "Mock Backend", Status: backend.StatusActive,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Metadata: mockBackendMetadata(),
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mockBackendRootRows(value *backend.Profile) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "content_digest", "version", "created_at", "updated_at"}).
		AddRow(value.TenantID, value.ProfileID, value.ProfileKey, value.DisplayName, value.Description, string(value.Status), value.SchemaVersion, value.ContentDigest, value.Version, value.CreatedAt, value.UpdatedAt)
}

func mockBackendBindingRows(value *backend.Profile) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"capability", "provider", "endpoint", "options", "secret_ref"})
	for _, binding := range value.Bindings {
		rows.AddRow(string(binding.Capability), binding.Provider, binding.Endpoint, []byte(`{}`), binding.SecretRef)
	}
	return rows
}

func mockBinding(t *testing.T, appID string) *channels.Binding {
	t.Helper()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "mock-binding")
	if err != nil {
		t.Fatal(err)
	}
	value, err := channels.NewBinding(channels.CreateInput{
		TenantID: mockTenantID, BindingKey: "mock-binding", Channel: channels.ChannelTelegram, ProviderAccountID: "account",
		PublicRouteKeyDigest: routeDigest, AppID: appID, SecretRef: "secret://mock",
		Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}}, Status: channels.StatusActive, Metadata: mockChannelMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mockBindingRows(value *channels.Binding) *sqlmock.Rows {
	protocol, err := encodeProtocol(value.Protocol)
	if err != nil {
		panic(err)
	}
	return sqlmock.NewRows([]string{"tenant_id", "binding_id", "binding_key", "channel", "provider_account_id", "public_route_key_digest", "app_id", "secret_ref", "protocol_config", "schema_version", "status", "version", "config_digest", "created_at", "updated_at"}).
		AddRow(value.TenantID, value.BindingID, value.BindingKey, string(value.Channel), value.ProviderAccountID, value.PublicRouteKeyDigest, value.AppID, value.SecretRef, protocol, channels.SchemaVersionV1, string(value.Status), value.Version, value.ConfigDigest, value.CreatedAt, value.UpdatedAt)
}

func mockTenantMetadata() tenant.TransitionMetadata {
	return tenant.TransitionMetadata{ActorType: "test", ActorID: "mock", Reason: "test", CorrelationID: "mock"}
}

func mockAgentMetadata() appmodel.ChangeMetadata {
	return appmodel.ChangeMetadata{ActorType: "test", ActorID: "mock", Reason: "test", CorrelationID: "mock"}
}

func mockModelMetadata() model.ChangeMetadata {
	return model.ChangeMetadata{ActorType: "test", ActorID: "mock", Reason: "test", CorrelationID: "mock"}
}

func mockBackendMetadata() backend.ChangeMetadata {
	return backend.ChangeMetadata{ActorType: "test", ActorID: "mock", Reason: "test", CorrelationID: "mock"}
}

func mockChannelMetadata() channels.ChangeMetadata {
	return channels.ChangeMetadata{ActorType: "test", ActorID: "mock", Reason: "test", CorrelationID: "mock"}
}
