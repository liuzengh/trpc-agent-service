package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/backend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"gopkg.in/yaml.v3"
)

func postgresStorageProfile() tenant.StorageProfile {
	postgres := tenant.BackendConfig{Type: tenant.BackendPostgres, Endpoint: "postgres://postgres:5432/trpc_agent"}
	return tenant.StorageProfile{Session: postgres, Memory: postgres, Summary: postgres, Artifact: postgres, Knowledge: postgres, Audit: postgres}
}

func TestPositiveEnvIntBoundsWorkerConcurrency(t *testing.T) {
	t.Setenv(workerConcurrencyEnv, "")
	if value, err := positiveEnvInt(workerConcurrencyEnv, 8, 256); err != nil || value != 8 {
		t.Fatalf("default value=%d err=%v", value, err)
	}
	for _, invalid := range []string{"0", "-1", "257", "many"} {
		t.Run(invalid, func(t *testing.T) {
			t.Setenv(workerConcurrencyEnv, invalid)
			if _, err := positiveEnvInt(workerConcurrencyEnv, 8, 256); err == nil {
				t.Fatalf("accepted %q", invalid)
			}
		})
	}
	t.Setenv(workerConcurrencyEnv, "32")
	if value, err := positiveEnvInt(workerConcurrencyEnv, 8, 256); err != nil || value != 32 {
		t.Fatalf("value=%d err=%v", value, err)
	}
}

func TestPositiveEnvDurationBoundsShutdown(t *testing.T) {
	t.Setenv(shutdownTimeoutEnv, "")
	if value, err := positiveEnvDuration(shutdownTimeoutEnv, 10*time.Second, time.Second, 10*time.Minute); err != nil || value != 10*time.Second {
		t.Fatalf("default value=%s err=%v", value, err)
	}
	for _, invalid := range []string{"0s", "500ms", "-1s", "11m", "forever"} {
		t.Run(invalid, func(t *testing.T) {
			t.Setenv(shutdownTimeoutEnv, invalid)
			if _, err := positiveEnvDuration(shutdownTimeoutEnv, 10*time.Second, time.Second, 10*time.Minute); err == nil {
				t.Fatalf("accepted %q", invalid)
			}
		})
	}
	t.Setenv(shutdownTimeoutEnv, "100s")
	if value, err := positiveEnvDuration(shutdownTimeoutEnv, 10*time.Second, time.Second, 10*time.Minute); err != nil || value != 100*time.Second {
		t.Fatalf("value=%s err=%v", value, err)
	}
}

func publishedWeComYAML(t *testing.T, version int, agentID, secretKey string) []byte {
	t.Helper()
	file := &config.File{SchemaVersion: 1, Tenants: []tenant.Tenant{{
		ID: "tenant-a", Name: "Tenant A", Enabled: true, ConfigVersion: tenant.ConfigVersion(version),
		Audit: tenant.AuditPolicy{Enabled: true, RetentionDays: 30},
		Apps: []tenant.AgentApp{{
			ID: "assistant", Name: "Assistant", Enabled: true,
			Config: tenant.AppConfig{Instruction: "Help."}, Model: tenant.ModelProfile{Provider: "deepseek", Name: "deepseek-chat", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_TEST_MODEL_KEY"}},
			Tools: tenant.ToolPolicy{Allow: []string{"echo"}}, Storage: postgresStorageProfile(),
			Channels: []tenant.ChannelBinding{{ID: "wecom", Type: tenant.ChannelTypeWeCom, ProviderAccountID: "corp", ProviderAppID: agentID, Enabled: true,
				Token: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_WECOM_TOKEN"}, Secret: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: secretKey}, EncryptionKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_WECOM_AES"}}},
		}},
	}}}
	payload, err := yaml.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestPublishedDeliveryRoutesPinSenderVersion(t *testing.T) {
	t.Setenv("PR9_WECOM_SECRET_V1", "secret-v1")
	t.Setenv("PR9_WECOM_SECRET_V2", "secret-v2")
	store := repository.NewMemoryStore()
	ctx := context.Background()
	for version, payload := range [][]byte{publishedWeComYAML(t, 1, "1001", "PR9_WECOM_SECRET_V1"), publishedWeComYAML(t, 2, "2002", "PR9_WECOM_SECRET_V2")} {
		if _, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: "tenant-a", Payload: payload, SHA256: "fixture"}, tenant.ConfigVersion(version)); err != nil {
			t.Fatal(err)
		}
	}
	published, err := config.NewPublishedCache(store)
	if err != nil {
		t.Fatal(err)
	}
	routes := &publishedDeliveryRoutes{published: published, senders: make(map[deliverySenderKey]channels.TextSender)}
	resolve := func(version tenant.ConfigVersion) *wecom.Sender {
		sender, err := routes.Resolve(gateway.OutboundMessage{TenantID: "tenant-a", AppID: "assistant", BindingID: "wecom", ConfigVersion: version})
		if err != nil {
			t.Fatal(err)
		}
		result, ok := sender.(*wecom.Sender)
		if !ok {
			t.Fatalf("sender type %T", sender)
		}
		return result
	}
	v1, v2 := resolve(1), resolve(2)
	if v1.AgentID != 1001 || v2.AgentID != 2002 || v1 == v2 {
		t.Fatalf("versioned senders v1=%+v v2=%+v", v1, v2)
	}
}

func inmemoryStorageProfile() tenant.StorageProfile {
	inmemory := tenant.BackendConfig{Type: tenant.BackendInMemory}
	return tenant.StorageProfile{Session: inmemory, Memory: inmemory, Summary: inmemory, Artifact: inmemory, Knowledge: inmemory, Audit: inmemory}
}

func persistentTenantYAML(id string, version int) []byte {
	file := &config.File{SchemaVersion: 1, Tenants: []tenant.Tenant{{
		ID: id, Name: id, Enabled: true, ConfigVersion: tenant.ConfigVersion(version),
		Audit: tenant.AuditPolicy{Enabled: true, RetentionDays: 30},
		Apps: []tenant.AgentApp{{
			ID: "assistant", Name: "Assistant", Enabled: true,
			Config:   tenant.AppConfig{Instruction: "Help the user."},
			Model:    tenant.ModelProfile{Provider: "deepseek", Name: "deepseek-chat", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_TEST_MODEL_KEY"}},
			Tools:    tenant.ToolPolicy{Allow: []string{"echo"}},
			Channels: []tenant.ChannelBinding{{ID: "http", Type: tenant.ChannelTypeHTTP, ProviderAccountID: "local", Enabled: true}},
			Storage:  postgresStorageProfile(),
		}},
	}}}
	payload, err := yaml.Marshal(file)
	if err != nil {
		panic(err)
	}
	return payload
}

// A production startup must fail fast when the shared PostgreSQL backend is
// not configured; silently falling back to any InMemory backend is forbidden.
func TestProductionStartupFailsWithoutPostgresDSN(t *testing.T) {
	t.Setenv(postgresDSNEnv, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := backend.OpenPostgres(ctx, ""); err == nil {
		t.Fatal("empty PostgreSQL DSN must fail fast")
	}
}

// The persistent profile gate rejects InMemory storage and mock models, so a
// production config can never silently run on the framework's InMemory
// defaults. The same gate guards the Admin publish path.
func TestProductionProfileRejectsInMemoryAndMock(t *testing.T) {
	t.Setenv("PR9_TEST_MODEL_KEY", "fixture-key")
	for _, test := range []struct {
		name    string
		app     tenant.AgentApp
		wantErr string
	}{
		{name: "inmemory storage", app: tenant.AgentApp{ID: "app", Enabled: true, Model: tenant.ModelProfile{Provider: "deepseek", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_TEST_MODEL_KEY"}}, Storage: inmemoryStorageProfile()}, wantErr: "postgres"},
		{name: "mock model", app: tenant.AgentApp{ID: "app", Enabled: true, Model: tenant.ModelProfile{Provider: "mock"}, Storage: postgresStorageProfile()}, wantErr: "test-only"},
		{name: "missing model credential", app: tenant.AgentApp{ID: "app", Enabled: true, Model: tenant.ModelProfile{Provider: "deepseek", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_UNSET_MODEL_KEY"}}, Storage: postgresStorageProfile()}, wantErr: "credential"},
		{name: "missing knowledge credential", app: tenant.AgentApp{ID: "app", Enabled: true, Model: tenant.ModelProfile{Provider: "deepseek", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_TEST_MODEL_KEY"}}, Storage: func() tenant.StorageProfile {
			profile := postgresStorageProfile()
			profile.Knowledge = tenant.BackendConfig{Type: tenant.BackendQdrant, Endpoint: "https://vectors.example", Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_UNSET_QDRANT_KEY"}}
			return profile
		}()}, wantErr: "knowledge storage credential"},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := &config.File{Tenants: []tenant.Tenant{{ID: "tenant", Enabled: true, Apps: []tenant.AgentApp{test.app}}}}
			err := validatePersistentProfiles(file)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error=%v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestProductionProfileAcceptsNamespacedRedisRunnerSession(t *testing.T) {
	t.Setenv("PR9_TEST_MODEL_KEY", "fixture-key")
	profile := postgresStorageProfile()
	redisSession := tenant.BackendConfig{Type: tenant.BackendRedis, Endpoint: "redis://redis:6379/0", Namespace: "runner-session"}
	profile.Session, profile.Summary = redisSession, redisSession
	file := &config.File{Tenants: []tenant.Tenant{{ID: "tenant", Enabled: true, Apps: []tenant.AgentApp{{
		ID: "app", Enabled: true, Model: tenant.ModelProfile{Provider: "deepseek", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR9_TEST_MODEL_KEY"}}, Storage: profile,
	}}}}}
	if err := validatePersistentProfiles(file); err != nil {
		t.Fatalf("Redis Runner Session profile rejected: %v", err)
	}
}

// Admin publishes run through the same gate: an InMemory production config is
// rejected at validate/publish time instead of failing at Runtime build.
func TestAdminPublishRejectsNonPersistentProfile(t *testing.T) {
	t.Setenv("PR9_TEST_MODEL_KEY", "fixture-key")
	service, err := admin.NewService(repository.NewMemoryStore(), admin.WithProfileValidator(validatePersistentProfiles))
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`schema_version: 1
tenants:
- tenant_id: tenant-a
  name: tenant-a
  enabled: true
  config_version: 1
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: true
    config: {instruction: Help the user.}
    model:
      provider: deepseek
      name: deepseek-chat
      api_key: {provider: env, key: PR9_TEST_MODEL_KEY}
    tools: {allow: [echo], deny: [], require_approval: []}
    channels:
    - {binding_id: http, type: http, provider_account_id: local, enabled: true}
    storage:
      session: {type: inmemory}
      memory: {type: inmemory}
      summary: {type: inmemory}
      artifact: {type: inmemory}
      knowledge: {type: inmemory}
      audit: {type: inmemory}
`)
	if _, err := service.Validate(payload); err == nil || !strings.Contains(err.Error(), "postgres") {
		t.Fatalf("validate err=%v", err)
	}
	if _, err := service.Publish(context.Background(), "tenant-a", 0, payload); err == nil {
		t.Fatal("publish must reject an InMemory production profile")
	}
}

// After an Admin API publish the database head is ahead of the startup file;
// a restart must accept that instead of forcing an environment rebuild.
func TestBootstrapToleratesDatabaseAheadOfFile(t *testing.T) {
	t.Setenv("PR9_TEST_MODEL_KEY", "fixture-key")
	store := repository.NewMemoryStore()
	ctx := context.Background()
	file := loadPayload(t, persistentTenantYAML("tenant-a", 1))
	if err := bootstrapConfig(ctx, store, file); err != nil {
		t.Fatalf("first boot: %v", err)
	}
	// An operator publishes v2 through the Admin API.
	service, err := admin.NewService(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Publish(admin.WithActor(ctx, "ops"), "tenant-a", 1, persistentTenantYAML("tenant-a", 2)); err != nil {
		t.Fatal(err)
	}
	// Restarting with the original v1 file keeps the published head.
	if err := bootstrapConfig(ctx, store, file); err != nil {
		t.Fatalf("restart after publish: %v", err)
	}
	current, err := store.GetCurrentConfig(ctx, "tenant-a")
	if err != nil || current.Version != 2 {
		t.Fatalf("current=%+v err=%v", current, err)
	}
	if current.CreatedBy == "" {
		t.Fatal("created_by must be recorded")
	}
	// A file ahead of the database is ambiguous and must fail fast.
	ahead := loadPayload(t, persistentTenantYAML("tenant-a", 5))
	if err := bootstrapConfig(ctx, store, ahead); err == nil || !strings.Contains(err.Error(), "ahead") {
		t.Fatalf("file ahead err=%v", err)
	}
	// A brand-new tenant in the file still seeds at version 1.
	fileNew := loadPayload(t, persistentTenantYAML("tenant-b", 1))
	if err := bootstrapConfig(ctx, store, fileNew); err != nil {
		t.Fatalf("seed new tenant: %v", err)
	}
	if _, err := store.GetCurrentConfig(ctx, "tenant-b"); err != nil {
		t.Fatalf("tenant-b not seeded: %v", err)
	}
	// Bootstrap seeds are attributed.
	seeded, err := store.GetCurrentConfig(ctx, "tenant-b")
	if err != nil || seeded.CreatedBy != "bootstrap" {
		t.Fatalf("seeded=%+v err=%v", seeded, err)
	}
}

func loadPayload(t *testing.T, payload []byte) *config.File {
	t.Helper()
	file, err := config.Load(strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	return file
}
