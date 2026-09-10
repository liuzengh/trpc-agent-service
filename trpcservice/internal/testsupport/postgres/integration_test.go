package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/jackc/pgx/v5"
)

func TestPostgreSQLRepositories(t *testing.T) {
	dsn := os.Getenv("POSTGRES_REPOSITORY_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_REPOSITORY_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := runRepositoryMigrations(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	db, err := storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("close repository database: %v", err)
		}
	}()

	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"},
		SecretRefPolicy: model.FieldOptional,
		Options:         map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}

	tenants := NewTenantRepository(db)
	root, activeTenant := testPostgreSQLTenantLifecycle(t, ctx, tenants)

	models := NewModelRepository(db, modelCatalog)
	profile := testPostgreSQLModelLifecycle(t, ctx, models, root.TenantID)

	apps := NewAgentRepository(db)
	app, draft, updatedDraft := testPostgreSQLAgentLifecycle(t, ctx, apps, root.TenantID, profile.ProfileID)

	backends := NewBackendRepository(db, backendCatalog)
	backendProfile := testPostgreSQLBackendLifecycle(t, ctx, backends, root.TenantID)

	channelRepo := NewChannelRepository(db)
	activeBinding := testPostgreSQLChannelLifecycle(t, ctx, channelRepo, root.TenantID, app.AppID)
	testPostgreSQLDisabledPaths(t, ctx, tenants, models, apps, backends, channelRepo, root, activeTenant, profile, app, draft, updatedDraft, backendProfile, activeBinding)
}

func testPostgreSQLTenantLifecycle(t *testing.T, ctx context.Context, tenants *TenantRepository) (*tenant.Tenant, *tenant.Tenant) {
	t.Helper()
	metadata := tenant.TransitionMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "repo-test"}
	root, err := tenants.Create(ctx, tenant.CreateInput{
		TenantKey: "postgres-repo", DisplayName: "Postgres Repository", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := tenants.Get(ctx, root.TenantID); err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	updatedTenant, err := tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: "Postgres Repository Updated",
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatalf("update tenant: %v", err)
	}
	if updatedTenant.Version != root.Version+1 {
		t.Fatalf("tenant version = %d, want %d", updatedTenant.Version, root.Version+1)
	}
	if _, err := tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: "Stale Tenant",
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	}); !errors.Is(err, tenant.ErrConflict) {
		t.Fatalf("stale tenant update error = %v", err)
	}
	suspendedTenant, _, err := tenants.TransitionStatus(ctx, tenant.TransitionStatusInput{
		TenantID: root.TenantID, ExpectedVersion: updatedTenant.Version,
		NextStatus: tenant.StatusSuspended, Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("suspend tenant: %v", err)
	}
	activeTenant, _, err := tenants.TransitionStatus(ctx, tenant.TransitionStatusInput{
		TenantID: root.TenantID, ExpectedVersion: suspendedTenant.Version,
		NextStatus: tenant.StatusActive, Metadata: metadata,
	})
	if err != nil {
		t.Fatalf("resume tenant: %v", err)
	}
	return root, activeTenant
}

func testPostgreSQLModelLifecycle(t *testing.T, ctx context.Context, models *ModelRepository, tenantID string) *model.Profile {
	t.Helper()
	profile, _, err := models.Create(ctx, model.CreateInput{
		TenantID: tenantID, ProfileKey: "primary", DisplayName: "Primary Model", Status: model.StatusActive,
		Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
		Metadata:      model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "model-test"},
	})
	if err != nil {
		t.Fatalf("create model profile: %v", err)
	}
	loadedProfile, err := models.Get(ctx, tenantID, profile.ProfileID)
	if err != nil {
		t.Fatalf("get model profile: %v", err)
	}
	if loadedProfile.TenantID != tenantID || loadedProfile.ProfileID != profile.ProfileID || loadedProfile.Configuration.Options["mode"] != "safe" {
		t.Fatalf("loaded model scope = %s/%s", loadedProfile.TenantID, loadedProfile.ProfileID)
	}
	if _, err := models.Get(ctx, tenantID, "mp_01ARZ3NDEKTSV4RRFFQ69G5FAW"); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("missing model profile error = %v", err)
	}
	loadedProfile.Configuration.Options["mode"] = "mutated"
	if again, err := models.Get(ctx, tenantID, profile.ProfileID); err != nil || again.Configuration.Options["mode"] != "safe" {
		t.Fatalf("model defensive copy = %+v, err=%v", again, err)
	}
	if _, _, err := models.UpdateConfiguration(ctx, model.UpdateConfigurationInput{
		TenantID: tenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version - 1,
		DisplayName: "Stale Model", SchemaVersion: profile.SchemaVersion,
		Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
		Metadata:      model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "stale", CorrelationID: "model-stale"},
	}); !errors.Is(err, model.ErrConflict) {
		t.Fatalf("stale model profile update error = %v", err)
	}
	updatedProfile, _, err := models.UpdateConfiguration(ctx, model.UpdateConfigurationInput{
		TenantID: tenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		DisplayName: "Primary Model Updated", SchemaVersion: profile.SchemaVersion,
		Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
		Metadata:      model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "model-update"},
	})
	if err != nil {
		t.Fatalf("update model profile: %v", err)
	}
	profile = updatedProfile
	profile, _, err = models.TransitionStatus(ctx, model.TransitionStatusInput{
		TenantID: tenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version,
		NextStatus: model.StatusSuspended,
		Metadata:   model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "model-status"},
	})
	if err != nil {
		t.Fatalf("suspend model profile: %v", err)
	}
	return profile
}

func testPostgreSQLAgentLifecycle(t *testing.T, ctx context.Context, apps *AgentRepository, tenantID, profileID string) (*appmodel.App, *appmodel.Revision, *appmodel.Revision) {
	t.Helper()
	appRoot, err := apps.Create(ctx, appmodel.CreateInput{TenantID: tenantID, AppKey: "primary-app", DisplayName: "Primary App"})
	if err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	draft, err := apps.CreateDraft(ctx, appmodel.CreateDraftInput{
		TenantID: tenantID, AppID: appRoot.AppID, ExpectedAppVersion: appRoot.Version, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
		Configuration: appmodel.DraftConfiguration{Instruction: "Answer briefly", ModelProfileID: profileID, Runtime: appmodel.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatalf("create agent draft: %v", err)
	}
	publishedApp, publishedRevision, event, err := apps.Publish(ctx, appmodel.PublishInput{
		TenantID: tenantID, AppID: appRoot.AppID, Revision: draft.Revision, ExpectedAppVersion: appRoot.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-publish"},
	})
	if err != nil {
		t.Fatalf("publish agent: %v", err)
	}
	if publishedApp.CurrentRevision == nil || *publishedApp.CurrentRevision != publishedRevision.Revision || event.EventType != appmodel.ChangePublished {
		t.Fatalf("publication result = app=%+v revision=%+v event=%+v", publishedApp, publishedRevision, event)
	}
	if loadedRevision, err := apps.GetRevision(ctx, tenantID, appRoot.AppID, draft.Revision); err != nil || loadedRevision.State != appmodel.RevisionStatePublished {
		t.Fatalf("get published revision = %+v, err=%v", loadedRevision, err)
	}
	if _, err := apps.Get(ctx, tenantID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"); !errors.Is(err, appmodel.ErrNotFound) {
		t.Fatalf("missing agent app error = %v", err)
	}
	if _, err := apps.UpdateMetadata(ctx, appmodel.UpdateMetadataInput{TenantID: tenantID, AppID: appRoot.AppID, ExpectedVersion: publishedApp.Version - 1, DisplayName: "Stale App", Description: "stale"}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("stale agent metadata error = %v", err)
	}
	appRoot, err = apps.UpdateMetadata(ctx, appmodel.UpdateMetadataInput{TenantID: tenantID, AppID: appRoot.AppID, ExpectedVersion: publishedApp.Version, DisplayName: "Primary App Updated", Description: "integration app"})
	if err != nil {
		t.Fatalf("update agent metadata: %v", err)
	}
	appRoot, updatedDraft := testPostgreSQLAgentRevisionLifecycle(t, ctx, apps, tenantID, profileID, appRoot, draft)
	return appRoot, draft, updatedDraft
}

func testPostgreSQLAgentRevisionLifecycle(t *testing.T, ctx context.Context, apps *AgentRepository, tenantID, profileID string, appRoot *appmodel.App, draft *appmodel.Revision) (*appmodel.App, *appmodel.Revision) {
	t.Helper()
	if _, err := apps.CreateDraft(ctx, appmodel.CreateDraftInput{TenantID: tenantID, AppID: appRoot.AppID, ExpectedAppVersion: appRoot.Version - 1, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Configuration: appmodel.DraftConfiguration{Instruction: "stale", ModelProfileID: profileID, Runtime: appmodel.DefaultRuntimePolicy()}}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("stale agent draft error = %v", err)
	}
	secondDraft, err := apps.CreateDraft(ctx, appmodel.CreateDraftInput{TenantID: tenantID, AppID: appRoot.AppID, ExpectedAppVersion: appRoot.Version, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Configuration: appmodel.DraftConfiguration{Instruction: "Answer with more detail", ModelProfileID: profileID, Runtime: appmodel.DefaultRuntimePolicy()}})
	if err != nil {
		t.Fatalf("create second agent draft: %v", err)
	}
	if _, err := apps.UpdateDraft(ctx, appmodel.UpdateDraftInput{TenantID: tenantID, AppID: appRoot.AppID, Revision: secondDraft.Revision, ExpectedAppVersion: appRoot.Version, ExpectedDraftVersion: secondDraft.DraftVersion - 1, Configuration: appmodel.DraftConfiguration{Instruction: "stale", ModelProfileID: profileID, Runtime: appmodel.DefaultRuntimePolicy()}}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("stale agent draft update error = %v", err)
	}
	updatedDraft, err := apps.UpdateDraft(ctx, appmodel.UpdateDraftInput{TenantID: tenantID, AppID: appRoot.AppID, Revision: secondDraft.Revision, ExpectedAppVersion: appRoot.Version, ExpectedDraftVersion: secondDraft.DraftVersion, Configuration: appmodel.DraftConfiguration{Instruction: "Answer with detail", ModelProfileID: profileID, Runtime: appmodel.DefaultRuntimePolicy()}})
	if err != nil {
		t.Fatalf("update second agent draft: %v", err)
	}
	if _, _, _, err := apps.Publish(ctx, appmodel.PublishInput{TenantID: tenantID, AppID: appRoot.AppID, Revision: updatedDraft.Revision, ExpectedAppVersion: appRoot.Version, ExpectedDraftVersion: updatedDraft.DraftVersion - 1, TenantActive: true, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "stale", CorrelationID: "agent-publish-stale"}}); !errors.Is(err, appmodel.ErrConflict) {
		t.Fatalf("stale agent publish error = %v", err)
	}
	publishedApp, _, _, err := apps.Publish(ctx, appmodel.PublishInput{TenantID: tenantID, AppID: appRoot.AppID, Revision: updatedDraft.Revision, ExpectedAppVersion: appRoot.Version, ExpectedDraftVersion: updatedDraft.DraftVersion, TenantActive: true, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-publish-two"}})
	if err != nil {
		t.Fatalf("publish second agent revision: %v", err)
	}
	appRoot = publishedApp
	rolledBackApp, rollbackEvent, err := apps.Rollback(ctx, appmodel.RollbackInput{TenantID: tenantID, AppID: appRoot.AppID, TargetRevision: draft.Revision, ExpectedAppVersion: appRoot.Version, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-rollback"}})
	if err != nil {
		t.Fatalf("rollback agent revision: %v", err)
	}
	if rolledBackApp.CurrentRevision == nil || *rolledBackApp.CurrentRevision != draft.Revision || rollbackEvent.EventType != appmodel.ChangeRolledBack {
		t.Fatalf("rollback result = app=%+v event=%+v", rolledBackApp, rollbackEvent)
	}
	appRoot, _, err = apps.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: tenantID, AppID: rolledBackApp.AppID, ExpectedVersion: rolledBackApp.Version, NextStatus: appmodel.StatusSuspended, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-suspend"}})
	if err != nil {
		t.Fatalf("suspend agent app: %v", err)
	}
	appRoot, _, err = apps.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: tenantID, AppID: appRoot.AppID, ExpectedVersion: appRoot.Version, NextStatus: appmodel.StatusActive, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-resume"}})
	if err != nil {
		t.Fatalf("resume agent app: %v", err)
	}
	return appRoot, updatedDraft
}

func testPostgreSQLBackendLifecycle(t *testing.T, ctx context.Context, backends *BackendRepository, tenantID string) *backend.Profile {
	t.Helper()
	backendProfile, _, err := backends.Create(ctx, backend.CreateInput{
		TenantID: tenantID, ProfileKey: "primary-backend", DisplayName: "Primary Backend", Status: backend.StatusActive,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "primary"}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-test"},
	})
	if err != nil {
		t.Fatalf("create backend profile: %v", err)
	}
	if loaded, err := backends.Get(ctx, tenantID, backendProfile.ProfileID); err != nil || len(loaded.Bindings) != 1 || loaded.Bindings[0].Options["namespace"] != "primary" {
		t.Fatalf("get backend profile = %+v, err=%v", loaded, err)
	}
	if _, err := backends.Get(ctx, tenantID, "bp_01ARZ3NDEKTSV4RRFFQ69G5FAV"); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("missing backend profile error = %v", err)
	}
	if _, _, err := backends.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{
		TenantID: tenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version - 1, DisplayName: "Stale Backend", SchemaVersion: backendProfile.SchemaVersion,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "stale"}}}, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "stale", CorrelationID: "backend-stale"},
	}); !errors.Is(err, backend.ErrConflict) {
		t.Fatalf("stale backend update error = %v", err)
	}
	updatedBackend, _, err := backends.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{
		TenantID: tenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version, DisplayName: "Primary Backend Updated", SchemaVersion: backendProfile.SchemaVersion,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "updated"}}}, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-update"},
	})
	if err != nil {
		t.Fatalf("update backend profile: %v", err)
	}
	backendProfile = updatedBackend
	backendProfile, _, err = backends.TransitionStatus(ctx, backend.TransitionStatusInput{TenantID: tenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version, NextStatus: backend.StatusSuspended, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-suspend"}})
	if err != nil {
		t.Fatalf("suspend backend profile: %v", err)
	}
	backendProfile, _, err = backends.TransitionStatus(ctx, backend.TransitionStatusInput{TenantID: tenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version, NextStatus: backend.StatusActive, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-resume"}})
	if err != nil {
		t.Fatalf("resume backend profile: %v", err)
	}
	return backendProfile
}

func testPostgreSQLChannelLifecycle(t *testing.T, ctx context.Context, channelRepo *ChannelRepository, tenantID, appID string) *channels.Binding {
	t.Helper()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "repo-test-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err := channelRepo.Create(ctx, channels.CreateInput{
		TenantID: tenantID, BindingKey: "primary-channel", Channel: channels.ChannelTelegram, ProviderAccountID: "repo-account", PublicRouteKeyDigest: routeDigest, AppID: appID,
		SecretRef: "secret://repo-test", Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}}, Status: channels.StatusDraft,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-test"},
	})
	if err != nil {
		t.Fatalf("create channel binding: %v", err)
	}
	if _, err := channelRepo.Get(ctx, tenantID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAV"); !errors.Is(err, channels.ErrNotFound) {
		t.Fatalf("missing channel binding error = %v", err)
	}
	activeBinding, _, err := channelRepo.Activate(ctx, channels.TransitionStatusInput{TenantID: tenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-activate"}})
	if err != nil {
		t.Fatalf("activate channel binding: %v", err)
	}
	if _, _, err := channelRepo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{TenantID: tenantID, BindingID: binding.BindingID, ExpectedVersion: activeBinding.Version - 1, ProviderAccountID: "repo-account", PublicRouteKeyDigest: routeDigest, AppID: appID, SecretRef: "secret://repo-test", Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}}, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "stale", CorrelationID: "channel-stale"}}); !errors.Is(err, channels.ErrConflict) {
		t.Fatalf("stale channel update error = %v", err)
	}
	updatedBinding, _, err := channelRepo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{TenantID: tenantID, BindingID: binding.BindingID, ExpectedVersion: activeBinding.Version, ProviderAccountID: "repo-account", PublicRouteKeyDigest: routeDigest, AppID: appID, SecretRef: "secret://repo-test", Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}}, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-update"}})
	if err != nil {
		t.Fatalf("update channel binding: %v", err)
	}
	activeBinding = updatedBinding
	if _, _, err := channelRepo.Suspend(ctx, channels.TransitionStatusInput{TenantID: tenantID, BindingID: binding.BindingID, ExpectedVersion: activeBinding.Version, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-suspend"}}); err != nil {
		t.Fatalf("suspend channel binding: %v", err)
	}
	activeBinding, _, err = channelRepo.Resume(ctx, channels.TransitionStatusInput{TenantID: tenantID, BindingID: binding.BindingID, ExpectedVersion: activeBinding.Version + 1, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-resume"}})
	if err != nil {
		t.Fatalf("resume channel binding: %v", err)
	}
	candidates, err := channelRepo.LookupCandidates(ctx, channels.ChannelTelegram, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("lookup candidates = %d, err=%v", len(candidates), err)
	}
	consumed, err := channelRepo.ConsumeCandidate(ctx, candidates[0])
	if err != nil {
		t.Fatalf("consume candidate: %v", err)
	}
	if consumed.BindingID != activeBinding.BindingID || consumed.Version != activeBinding.Version {
		t.Fatalf("consumed binding = %+v, active = %+v", consumed, activeBinding)
	}
	if _, err := channelRepo.ConsumeCandidate(ctx, candidates[0]); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("candidate replay error = %v", err)
	}
	return activeBinding
}

func testPostgreSQLDisabledPaths(t *testing.T, ctx context.Context, tenants *TenantRepository, models *ModelRepository, apps *AgentRepository, backends *BackendRepository, channelRepo *ChannelRepository, root, activeTenant *tenant.Tenant, profile *model.Profile, app *appmodel.App, draft, updatedDraft *appmodel.Revision, backendProfile *backend.Profile, activeBinding *channels.Binding) {
	t.Helper()
	testPostgreSQLDisabledChannel(t, ctx, channelRepo, root.TenantID, activeBinding)
	testPostgreSQLDisabledAgent(t, ctx, apps, root.TenantID, app, profile.ProfileID, draft.Revision, updatedDraft)
	testPostgreSQLDisabledModel(t, ctx, models, root.TenantID, profile)
	testPostgreSQLDisabledBackend(t, ctx, backends, root.TenantID, backendProfile)
	testPostgreSQLDisabledTenant(t, ctx, tenants, root.TenantID, activeTenant)
	canceled, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	if _, err := models.Get(canceled, root.TenantID, profile.ProfileID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled model get error = %v", err)
	}
}

func testPostgreSQLDisabledChannel(t *testing.T, ctx context.Context, channelRepo *ChannelRepository, tenantID string, activeBinding *channels.Binding) {
	t.Helper()
	disabledBinding, _, err := channelRepo.Disable(ctx, channels.TransitionStatusInput{TenantID: tenantID, BindingID: activeBinding.BindingID, ExpectedVersion: activeBinding.Version, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "channel-disable"}})
	if err != nil {
		t.Fatalf("disable channel binding: %v", err)
	}
	if _, _, err := channelRepo.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{TenantID: tenantID, BindingID: disabledBinding.BindingID, ExpectedVersion: disabledBinding.Version, ProviderAccountID: disabledBinding.ProviderAccountID, PublicRouteKeyDigest: disabledBinding.PublicRouteKeyDigest, AppID: disabledBinding.AppID, SecretRef: disabledBinding.SecretRef, Protocol: disabledBinding.Protocol, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "channel-disabled-update"}}); !errors.Is(err, channels.ErrDisabled) {
		t.Fatalf("disabled channel update error = %v", err)
	}
	if _, _, err := channelRepo.TransitionStatus(ctx, channels.TransitionStatusInput{TenantID: tenantID, BindingID: disabledBinding.BindingID, ExpectedVersion: disabledBinding.Version, NextStatus: channels.StatusActive, Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "channel-disabled-transition"}}); !errors.Is(err, channels.ErrDisabled) {
		t.Fatalf("disabled channel transition error = %v", err)
	}
}

func testPostgreSQLDisabledAgent(t *testing.T, ctx context.Context, apps *AgentRepository, tenantID string, appRoot *appmodel.App, profileID string, draftRevision int64, updatedDraft *appmodel.Revision) {
	t.Helper()
	disabledApp, _, err := apps.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: tenantID, AppID: appRoot.AppID, ExpectedVersion: appRoot.Version, NextStatus: appmodel.StatusDisabled, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "agent-disable"}})
	if err != nil {
		t.Fatalf("disable agent app: %v", err)
	}
	if _, err := apps.UpdateMetadata(ctx, appmodel.UpdateMetadataInput{TenantID: tenantID, AppID: disabledApp.AppID, ExpectedVersion: disabledApp.Version, DisplayName: disabledApp.DisplayName, Description: disabledApp.Description}); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("disabled agent metadata error = %v", err)
	}
	if _, err := apps.CreateDraft(ctx, appmodel.CreateDraftInput{TenantID: tenantID, AppID: disabledApp.AppID, ExpectedAppVersion: disabledApp.Version, Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Configuration: appmodel.DraftConfiguration{Instruction: "disabled", ModelProfileID: profileID, Runtime: appmodel.DefaultRuntimePolicy()}}); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("disabled agent draft error = %v", err)
	}
	if _, err := apps.UpdateDraft(ctx, appmodel.UpdateDraftInput{TenantID: tenantID, AppID: disabledApp.AppID, Revision: updatedDraft.Revision, ExpectedAppVersion: disabledApp.Version, ExpectedDraftVersion: updatedDraft.DraftVersion, Configuration: appmodel.DraftConfiguration{Instruction: "disabled", ModelProfileID: profileID, Runtime: appmodel.DefaultRuntimePolicy()}}); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("disabled agent draft update error = %v", err)
	}
	if _, _, err := apps.Rollback(ctx, appmodel.RollbackInput{TenantID: tenantID, AppID: disabledApp.AppID, TargetRevision: draftRevision, ExpectedAppVersion: disabledApp.Version, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "agent-disabled-rollback"}}); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("disabled agent rollback error = %v", err)
	}
	if _, _, err := apps.TransitionStatus(ctx, appmodel.TransitionStatusInput{TenantID: tenantID, AppID: disabledApp.AppID, ExpectedVersion: disabledApp.Version, NextStatus: appmodel.StatusActive, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "agent-disabled-transition"}}); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("disabled agent transition error = %v", err)
	}
	if _, _, _, err := apps.Publish(ctx, appmodel.PublishInput{TenantID: tenantID, AppID: disabledApp.AppID, Revision: updatedDraft.Revision, ExpectedAppVersion: disabledApp.Version, ExpectedDraftVersion: updatedDraft.DraftVersion, TenantActive: true, Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "agent-disabled-publish"}}); !errors.Is(err, appmodel.ErrDisabled) {
		t.Fatalf("disabled agent publish error = %v", err)
	}
}

func testPostgreSQLDisabledModel(t *testing.T, ctx context.Context, models *ModelRepository, tenantID string, profile *model.Profile) {
	t.Helper()
	disabledProfile, _, err := models.TransitionStatus(ctx, model.TransitionStatusInput{TenantID: tenantID, ProfileID: profile.ProfileID, ExpectedVersion: profile.Version, NextStatus: model.StatusDisabled, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "model-disable"}})
	if err != nil {
		t.Fatalf("disable model profile: %v", err)
	}
	if _, _, err := models.UpdateConfiguration(ctx, model.UpdateConfigurationInput{TenantID: tenantID, ProfileID: disabledProfile.ProfileID, ExpectedVersion: disabledProfile.Version, DisplayName: disabledProfile.DisplayName, Description: disabledProfile.Description, SchemaVersion: disabledProfile.SchemaVersion, Configuration: disabledProfile.Configuration, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "model-disabled-update"}}); !errors.Is(err, model.ErrDisabled) {
		t.Fatalf("disabled model update error = %v", err)
	}
	if _, _, err := models.TransitionStatus(ctx, model.TransitionStatusInput{TenantID: tenantID, ProfileID: disabledProfile.ProfileID, ExpectedVersion: disabledProfile.Version, NextStatus: model.StatusActive, Metadata: model.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "model-disabled-transition"}}); !errors.Is(err, model.ErrDisabled) {
		t.Fatalf("disabled model transition error = %v", err)
	}
}

func testPostgreSQLDisabledBackend(t *testing.T, ctx context.Context, backends *BackendRepository, tenantID string, backendProfile *backend.Profile) {
	t.Helper()
	disabledBackend, _, err := backends.TransitionStatus(ctx, backend.TransitionStatusInput{TenantID: tenantID, ProfileID: backendProfile.ProfileID, ExpectedVersion: backendProfile.Version, NextStatus: backend.StatusDisabled, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "backend-disable"}})
	if err != nil {
		t.Fatalf("disable backend profile: %v", err)
	}
	if _, _, err := backends.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{TenantID: tenantID, ProfileID: disabledBackend.ProfileID, ExpectedVersion: disabledBackend.Version, DisplayName: disabledBackend.DisplayName, Description: disabledBackend.Description, SchemaVersion: disabledBackend.SchemaVersion, Bindings: disabledBackend.Bindings, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "backend-disabled-update"}}); !errors.Is(err, backend.ErrDisabled) {
		t.Fatalf("disabled backend update error = %v", err)
	}
	if _, _, err := backends.TransitionStatus(ctx, backend.TransitionStatusInput{TenantID: tenantID, ProfileID: disabledBackend.ProfileID, ExpectedVersion: disabledBackend.Version, NextStatus: backend.StatusActive, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "postgres", Reason: "disabled", CorrelationID: "backend-disabled-transition"}}); !errors.Is(err, backend.ErrDisabled) {
		t.Fatalf("disabled backend transition error = %v", err)
	}
}

func testPostgreSQLDisabledTenant(t *testing.T, ctx context.Context, tenants *TenantRepository, tenantID string, activeTenant *tenant.Tenant) {
	t.Helper()
	metadata := tenant.TransitionMetadata{ActorType: "test", ActorID: "postgres", Reason: "integration", CorrelationID: "repo-test"}
	disabledTenant, _, err := tenants.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: tenantID, ExpectedVersion: activeTenant.Version, NextStatus: tenant.StatusDisabled, Metadata: metadata})
	if err != nil {
		t.Fatalf("disable tenant: %v", err)
	}
	if _, err := tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{TenantID: disabledTenant.TenantID, ExpectedVersion: disabledTenant.Version, DisplayName: disabledTenant.DisplayName, AuditRetentionDays: disabledTenant.AuditRetentionDays, LogMaskingLevel: disabledTenant.LogMaskingLevel, TraceSamplingRate: disabledTenant.TraceSamplingRate}); !errors.Is(err, tenant.ErrDisabled) {
		t.Fatalf("disabled tenant update error = %v", err)
	}
	if _, _, err := tenants.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: disabledTenant.TenantID, ExpectedVersion: disabledTenant.Version, NextStatus: tenant.StatusActive, Metadata: metadata}); !errors.Is(err, tenant.ErrDisabled) {
		t.Fatalf("disabled tenant transition error = %v", err)
	}
}

func runRepositoryMigrations(ctx context.Context, dsn string) error {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("locate repository migration test source")
	}
	migrationDir := filepath.Join(filepath.Dir(sourceFile), "..", "..", "..", "..", "migrations")
	for _, name := range []string{
		"0001_control_plane.up.sql",
		"0002_control_plane_repository_functions.up.sql",
		"0003_runtime_storage.up.sql",
		"0004_runtime_session_delete_cascade.up.sql",
		"0005_runtime_event_history.up.sql",
		"0006_audit_usage.up.sql",
		"0007_execution_audit_handoff.up.sql",
		"0008_runtime_reply_target.up.sql",
		"0009_runtime_reply_correlation.up.sql",
		"0010_agent_app_canary.up.sql",
	} {
		contents, err := os.ReadFile(filepath.Join(migrationDir, name)) // #nosec G304 -- names are fixed migration files under the repository root.
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(contents)); err != nil {
			return err
		}
	}
	return nil
}
