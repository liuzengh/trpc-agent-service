package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestLoadPlatformCatalog(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "model-secret")
	t.Setenv("TENANT_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	path := writeCatalogFile(t, validCatalogJSON())
	t.Setenv("PLATFORM_CONFIG_FILE", path)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CatalogPath != path || len(cfg.Catalog.Tenants) != 1 || cfg.ModelName != "" {
		t.Fatalf("unexpected catalog config: %#v", cfg)
	}
	catalog, credentials, err := cfg.RuntimeCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if catalog.ConfigVersions[0].Model.RequestTimeout != 5*time.Second {
		t.Fatalf("request timeout = %s", catalog.ConfigVersions[0].Model.RequestTimeout)
	}
	if value, err := credentials.Resolve("env:TENANT_MODEL_KEY"); err != nil || value != "model-secret" {
		t.Fatalf("credential resolve = (%q, %v)", value, err)
	}
}

func TestLoadPlatformCatalogStrictFailures(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "model-secret")
	t.Setenv("TENANT_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	tests := []struct {
		name string
		data []byte
	}{
		{name: "unknown field", data: []byte(strings.Replace(validCatalogJSON(), `"schema_version": 1`, `"schema_version": 1, "plaintext_api_key": "must-reject"`, 1))},
		{name: "binding credential ref", data: []byte(strings.Replace(validCatalogJSON(), `"agent_app_id":"assistant","enabled":true}]`, `"agent_app_id":"assistant","credential_ref":"env:TENANT_MODEL_KEY","enabled":true}]`, 1))},
		{name: "multiple objects", data: []byte(validCatalogJSON() + `{}`)},
		{name: "unsupported schema", data: []byte(strings.Replace(validCatalogJSON(), `"schema_version": 1`, `"schema_version": 2`, 1))},
		{name: "oversized", data: bytes.Repeat([]byte("x"), MaxCatalogBytes+1)},
		{name: "model URL userinfo", data: []byte(strings.Replace(validCatalogJSON(), "https://example.test", "https://secret@example.test", 1))},
		{name: "model URL query", data: []byte(strings.Replace(validCatalogJSON(), "https://example.test", "https://example.test?api_key=secret", 1))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeCatalogBytes(t, test.data)
			t.Setenv("PLATFORM_CONFIG_FILE", path)
			if _, err := Load(); err == nil {
				t.Fatal("expected strict catalog error")
			}
		})
	}
}

func TestPhase2ExampleCatalogParses(t *testing.T) {
	catalog, err := loadCatalogFile(filepath.Join("..", "..", "configs", "phase2.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Tenants) != 2 || len(catalog.StorageProfiles) != 2 || len(catalog.ChannelBindings) != 2 {
		t.Fatalf("unexpected example catalog: %#v", catalog)
	}
}

func TestPhase3ExampleConfigParses(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_TENANT_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_MODEL_KEY", "model-secret")
	t.Setenv("PLATFORM_CONFIG_FILE", filepath.Join("..", "..", "configs", "phase3.example.json"))
	cfg, err := LoadForRole(RoleServe)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Messaging == nil || len(cfg.Catalog.Tenants) != 2 {
		t.Fatalf("unexpected Phase 3 config: %#v", cfg)
	}
}

func TestPhase4ExampleConfigParses(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("PHASE4_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE4_MODEL_KEY", "model-secret")
	t.Setenv("PLATFORM_CONFIG_FILE", filepath.Join("..", "..", "configs", "phase4.example.json"))
	cfg, err := LoadForRole(RoleServe)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Messaging == nil || cfg.Messaging.SessionFencing != "strong" ||
		cfg.Messaging.MaxTurnEvents != 512 || cfg.Messaging.MaxTurnBytes != 2<<20 ||
		len(cfg.Catalog.StorageProfiles) != 1 || cfg.Catalog.StorageProfiles[0].Kind != tenant.StorageKindRedis {
		t.Fatalf("unexpected Phase 4 config: %#v", cfg)
	}
}

func TestPhase55ExampleConfigParses(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("PHASE55_REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("PHASE55_POSTGRES_DSN", "postgres://user:secret@localhost:5432/app?sslmode=disable")
	t.Setenv("PHASE55_MYSQL_DSN", "user:secret@tcp(localhost:3306)/app?parseTime=true&charset=utf8mb4&loc=UTC")
	t.Setenv("PHASE55_MODEL_KEY", "model-secret")
	t.Setenv("PLATFORM_CONFIG_FILE", filepath.Join("..", "..", "configs", "phase5.5.example.json"))
	cfg, err := LoadForRole(RoleServe)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Messaging == nil || cfg.Messaging.PersistenceMaxAttempts != 5 || cfg.Messaging.PersistencePayloadMaxBytes != 4<<20 || len(cfg.Catalog.StorageProfiles) != 3 ||
		cfg.Catalog.StorageProfiles[0].Kind != tenant.StorageKindRedis || cfg.Catalog.StorageProfiles[1].Kind != tenant.StorageKindPostgres || cfg.Catalog.StorageProfiles[2].Kind != tenant.StorageKindMySQL {
		t.Fatalf("unexpected Phase 5.5 config: %#v", cfg)
	}
	formatted := fmt.Sprintf("%+v", cfg)
	for _, secret := range []string{"user:secret", "localhost:5432", "localhost:3306", "/app"} {
		if strings.Contains(formatted, secret) {
			t.Fatalf("Phase 5.5 formatted config leaked SQL credential material %q: %s", secret, formatted)
		}
	}
}

func TestMessagingTurnLimitsDefaultsAndJSONOverrides(t *testing.T) {
	resolver := NewStaticCredentialResolver(map[string]string{"env:REDIS_URL": "redis://localhost:6379/0"})
	defaults, err := parseMessagingFile(&messagingConfigFile{RedisCredentialRef: "env:REDIS_URL", KeyPrefix: "phase4-defaults"}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.MaxTurnEvents != 512 || defaults.MaxTurnBytes != 2<<20 {
		t.Fatalf("default turn limits=(%d,%d)", defaults.MaxTurnEvents, defaults.MaxTurnBytes)
	}
	overridden, err := parseMessagingFile(&messagingConfigFile{
		RedisCredentialRef: "env:REDIS_URL", KeyPrefix: "phase4-overrides",
		MaxTurnEvents: 7, MaxTurnBytes: 4096,
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if overridden.MaxTurnEvents != 7 || overridden.MaxTurnBytes != 4096 {
		t.Fatalf("overridden turn limits=(%d,%d)", overridden.MaxTurnEvents, overridden.MaxTurnBytes)
	}
}

func TestMessagingTurnLimitsMustBePositive(t *testing.T) {
	resolver := NewStaticCredentialResolver(map[string]string{"env:REDIS_URL": "redis://localhost:6379/0"})
	for _, raw := range []*messagingConfigFile{
		{RedisCredentialRef: "env:REDIS_URL", KeyPrefix: "phase4-invalid-events", MaxTurnEvents: -1},
		{RedisCredentialRef: "env:REDIS_URL", KeyPrefix: "phase4-invalid-bytes", MaxTurnBytes: -1},
	} {
		if _, err := parseMessagingFile(raw, resolver); err == nil {
			t.Fatalf("parseMessagingFile(%#v) unexpectedly succeeded", raw)
		}
	}
}

func TestLoadPlatformCatalogValidatesEveryEnabledVersionCredential(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "model-secret")
	t.Setenv("TENANT_REDIS_URL", "localhost:6379")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	secondVersion := `,
    {
      "tenant_id":"tenant-a","agent_app_id":"assistant","version":"v2",
      "storage_profile_id":"redis-v1","instruction":"v2",
      "model":{"name":"model","base_url":"https://example.test","credential_ref":"env:MISSING_MODEL_KEY","request_timeout":"5s","max_output_tokens":128}
    }`
	data := strings.Replace(validCatalogJSON(), "\n  }],\n  \"channel_bindings\"", "\n  }"+secondVersion+"\n  ],\n  \"channel_bindings\"", 1)
	t.Setenv("PLATFORM_CONFIG_FILE", writeCatalogFile(t, data))
	_, err := Load()
	if err == nil {
		t.Fatal("expected missing inactive version credential to reject startup")
	}
	if strings.Contains(err.Error(), "MISSING_MODEL_KEY") {
		t.Fatalf("credential reference leaked: %v", err)
	}
}

func writeCatalogFile(t *testing.T, data string) string {
	t.Helper()
	return writeCatalogBytes(t, []byte(data))
}

func writeCatalogBytes(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "platform.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMessagingPersistenceDefaultsAndOverrides(t *testing.T) {
	resolver := NewStaticCredentialResolver(map[string]string{"env:REDIS_URL": "redis://localhost:6379/0"})
	defaults, err := parseMessagingFile(&messagingConfigFile{RedisCredentialRef: "env:REDIS_URL", KeyPrefix: "phase55-defaults"}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.PersistenceTimeout != DefaultPersistenceTimeout || defaults.PersistenceMaxAttempts != DefaultPersistenceMaxAttempts ||
		defaults.PersistenceInitialBackoff != DefaultPersistenceInitialBackoff || defaults.PersistenceMaxBackoff != DefaultPersistenceMaxBackoff ||
		defaults.PersistencePayloadMaxBytes != DefaultPersistencePayloadMaxBytes {
		t.Fatalf("unexpected persistence defaults: %#v", defaults)
	}
	overrides, err := parseMessagingFile(&messagingConfigFile{
		RedisCredentialRef: "env:REDIS_URL", KeyPrefix: "phase55-overrides",
		MaxTurnBytes: 4096, PersistenceTimeout: "3s", PersistenceMaxAttempts: 4,
		PersistenceInitialBackoff: "25ms", PersistenceMaxBackoff: "2s", PersistencePayloadMaxBytes: 8192,
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if overrides.PersistenceTimeout != 3*time.Second || overrides.PersistenceMaxAttempts != 4 ||
		overrides.PersistenceInitialBackoff != 25*time.Millisecond || overrides.PersistenceMaxBackoff != 2*time.Second ||
		overrides.PersistencePayloadMaxBytes != 8192 {
		t.Fatalf("unexpected persistence overrides: %#v", overrides)
	}
}

func TestMessagingPersistenceValidation(t *testing.T) {
	base := MessagingConfig{RedisURL: "redis://localhost:6379/0", KeyPrefix: "phase55", LeaseDuration: time.Second,
		HeartbeatInterval: 100 * time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: time.Second,
		MaxAttempts: 3, InboxRetention: time.Second, ReplyWaitTimeout: time.Second, SessionFencing: "strong",
		SessionLockDuration: time.Second, SessionWaitBackoff: time.Millisecond, SessionWaitMaxBackoff: time.Second,
		MaxTurnEvents: 10, MaxTurnBytes: 4096, ShutdownTimeout: time.Second, OutboundMaxAttempts: 3,
		OutboundInitialBackoff: time.Millisecond, OutboundMaxBackoff: time.Second, OutboundSendTimeout: time.Second,
		OutboundClaimIdle: time.Second, PersistenceTimeout: time.Second, PersistenceMaxAttempts: 3,
		PersistenceInitialBackoff: time.Millisecond, PersistenceMaxBackoff: time.Second, PersistencePayloadMaxBytes: 4096}
	tests := []func(*MessagingConfig){
		func(c *MessagingConfig) { c.PersistenceMaxAttempts = 11 },
		func(c *MessagingConfig) { c.PersistencePayloadMaxBytes = 2048 },
		func(c *MessagingConfig) { c.PersistenceInitialBackoff = 2 * time.Second },
	}
	for _, mutate := range tests {
		cfg := base
		mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Fatalf("invalid persistence config unexpectedly passed: %#v", cfg)
		}
	}
}

func validCatalogJSON() string {
	return `{
  "schema_version": 1,
	"messaging": {"redis_credential_ref":"env:PHASE3_MESSAGING_REDIS_URL","key_prefix":"test-messaging"},
  "tenants": [{"id":"tenant-a","enabled":true}],
  "storage_profiles": [{"tenant_id":"tenant-a","id":"redis-v1","kind":"redis","credential_ref":"env:TENANT_REDIS_URL","key_prefix":"test"}],
  "agent_apps": [{"tenant_id":"tenant-a","id":"assistant","enabled":true,"active_config_version":"v1"}],
  "config_versions": [{
    "tenant_id":"tenant-a","agent_app_id":"assistant","version":"v1",
    "storage_profile_id":"redis-v1","instruction":"test",
    "model":{"name":"model","base_url":"https://example.test","credential_ref":"env:TENANT_MODEL_KEY","request_timeout":"5s","max_output_tokens":128}
  }],
  "channel_bindings": [{"id":"binding-a","channel":"demo","external_account_id":"binding-a","tenant_id":"tenant-a","agent_app_id":"assistant","enabled":true}]
}`
}

func TestNormalizeRedisURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "host shorthand", in: "localhost:6379", want: "redis://localhost:6379/0"},
		{name: "host shorthand with db", in: "localhost:6379/2", want: "redis://localhost:6379/2"},
		{name: "full uri", in: "redis://user:pass@localhost:6379/3", want: "redis://user:pass@localhost:6379/3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeRedisURL(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("NormalizeRedisURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeRedisURLRejectsUnsupportedScheme(t *testing.T) {
	for _, input := range []string{
		"http://localhost:6379",
		"redis://localhost",
		"redis://localhost:bad/0",
		"redis://localhost:6379/not-a-db",
		"redis://localhost:6379/0/extra",
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := NormalizeRedisURL(input); err == nil {
				t.Fatalf("NormalizeRedisURL(%q) unexpectedly succeeded", input)
			}
		})
	}
}

func TestLoadDefaultsAndAPIKeyIndirection(t *testing.T) {
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test/v1/")
	t.Setenv("MODEL_API_KEY_ENV", "TEST_MODEL_KEY")
	t.Setenv("TEST_MODEL_KEY", "secret-value")
	t.Setenv("IDENTITY_SECRET", strings.Repeat("x", 32))
	t.Setenv("REDIS_URL", "localhost:6379")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModelRequestTimeout != 60*time.Second || cfg.ModelMaxOutput != 1024 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
	if cfg.ModelBaseURL != "https://example.test/v1" || cfg.RedisURL != "redis://localhost:6379/0" {
		t.Fatalf("unexpected normalized config: %#v", cfg)
	}
}

func TestLoadRejectsShortIdentitySecret(t *testing.T) {
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test")
	t.Setenv("MODEL_API_KEY_ENV", "TEST_MODEL_KEY")
	t.Setenv("TEST_MODEL_KEY", "secret-value")
	t.Setenv("IDENTITY_SECRET", "short")
	t.Setenv("REDIS_URL", "localhost:6379")
	if _, err := Load(); err == nil {
		t.Fatal("expected short identity secret error")
	}
}

func TestLoadRejectsMissingReferencedAPIKey(t *testing.T) {
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test")
	t.Setenv("MODEL_API_KEY_ENV", "potentialsecret")
	t.Setenv("potentialsecret", "")
	t.Setenv("IDENTITY_SECRET", strings.Repeat("x", 32))
	t.Setenv("REDIS_URL", "localhost:6379")
	_, err := Load()
	if err == nil {
		t.Fatal("expected missing referenced API key error")
	}
	if strings.Contains(err.Error(), "potentialsecret") {
		t.Fatalf("configuration error echoed MODEL_API_KEY_ENV value: %v", err)
	}
}

func TestLoadRejectsInvalidAPIKeyEnvironmentNameWithoutEchoingIt(t *testing.T) {
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test")
	t.Setenv("MODEL_API_KEY_ENV", "sk-must-not-be-echoed")
	t.Setenv("IDENTITY_SECRET", strings.Repeat("x", 32))
	t.Setenv("REDIS_URL", "localhost:6379")
	_, err := Load()
	if err == nil {
		t.Fatal("expected invalid API key environment name error")
	}
	if strings.Contains(err.Error(), "sk-must-not-be-echoed") {
		t.Fatalf("configuration error echoed a possible API key: %v", err)
	}
}

func TestConfigFormattingRedactsSecrets(t *testing.T) {
	messaging := MessagingConfig{
		RedisCredentialRef: "env:SENSITIVE_MESSAGING_REF",
		RedisURL:           "redis://message-user:message-password@sensitive-messaging-host:6379/0",
	}
	cfg := Config{
		ModelAPIKey:    "model-secret-value",
		ModelBaseURL:   "https://model-user:model-url-secret@example.test/v1?api_key=query-secret",
		IdentitySecret: []byte("identity-secret-value"),
		RedisURL:       "redis://user:redis-secret-value@localhost:6379/0",
		Messaging:      &messaging,
	}
	formatted := fmt.Sprintf("%v %#v", cfg, cfg)
	for _, secret := range []string{
		"model-secret-value", "model-url-secret", "query-secret", "identity-secret-value", "redis-secret-value",
		"SENSITIVE_MESSAGING_REF", "message-password", "sensitive-messaging-host",
	} {
		if strings.Contains(formatted, secret) {
			t.Fatalf("formatted config leaked %q: %s", secret, formatted)
		}
	}
}

func TestLegacyGatewayDoesNotReadModelKeyValue(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("MODEL_NAME", "model")
	t.Setenv("MODEL_BASE_URL", "https://example.test/v1")
	t.Setenv("MODEL_API_KEY_ENV", "LEGACY_MISSING_MODEL_KEY")
	t.Setenv("LEGACY_MISSING_MODEL_KEY", "legacy-model-secret")
	t.Setenv("REDIS_URL", "localhost:6379")
	cfg, err := LoadForRole(RoleGateway)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ModelAPIKey != "" || cfg.Messaging == nil || cfg.Messaging.KeyPrefix != DefaultRedisPrefix+":messaging" {
		t.Fatalf("unexpected legacy Gateway config: %#v", cfg)
	}
	t.Setenv("LEGACY_MISSING_MODEL_KEY", "")
	if _, err := LoadForRole(RoleWorker); err == nil {
		t.Fatal("legacy Worker unexpectedly accepted missing model key")
	}
}

func TestGatewayRoleDoesNotResolveModelCredential(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", strings.Repeat("i", 32))
	t.Setenv("TENANT_MODEL_KEY", "")
	t.Setenv("TENANT_REDIS_URL", "")
	t.Setenv("PHASE3_MESSAGING_REDIS_URL", "localhost:6379")
	t.Setenv("PLATFORM_CONFIG_FILE", writeCatalogFile(t, validCatalogJSON()))
	cfg, err := LoadForRole(RoleGateway)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Messaging == nil || cfg.Messaging.RedisURL != "redis://localhost:6379/0" {
		t.Fatalf("unexpected Gateway messaging config: %#v", cfg.Messaging)
	}
	if _, err := LoadForRole(RoleWorker); err == nil {
		t.Fatal("Worker unexpectedly accepted missing model/storage credentials")
	}
}
