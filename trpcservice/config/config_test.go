package config

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckedInServiceConfigsValidate(t *testing.T) {
	for _, relative := range []string{"config/example.yaml", "deploy/service-config.yaml", "deploy/production-config.yaml"} {
		t.Run(relative, func(t *testing.T) {
			path := filepath.Join("..", "..", filepath.FromSlash(relative))
			if _, err := Load(path); err != nil {
				t.Fatalf("checked-in config %s is invalid: %v", relative, err)
			}
		})
	}
}

const validYAML = `
server: {address: ":0"}
coordination: {backend: inmemory}
telemetry: {service_name: test}
tenants:
  - tenant_id: tenant-a
    version: v1
    enabled: true
    app: {name: assistant, agent_name: chat-agent}
    model: {provider: mock, name: mock}
    tools: {allow: [calculator], deny: [], side_effects: []}
    channels:
      - {type: telegram, binding_id: telegram-main, enabled: true, token_env: TG_TOKEN, signing_secret_env: TG_SECRET}
      - {type: slack, binding_id: slack-main, enabled: true, token_env: SLACK_TOKEN, signing_secret_env: SLACK_SECRET, workspace_id: T_TEST, application_id: A_TEST}
    data:
      session: {type: inmemory, namespace: ta}
      memory: {type: inmemory}
      summary: {type: inmemory}
      artifact: {type: inmemory}
      knowledge: {type: disabled}
      audit_log: {type: stdout}
    audit: {enabled: true, sink: stdout}
    budget: {requests_per_minute: 10, max_input_chars: 2000}
`

func TestDecodeDefaultsAndSecretReferences(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.QueueSize == 0 || cfg.Coordination.DedupTTL == 0 {
		t.Fatal("defaults were not applied")
	}
	names := strings.Join(cfg.SecretEnvNames(), ",")
	for _, want := range []string{"TG_TOKEN", "TG_SECRET", "SLACK_TOKEN", "SLACK_SECRET"} {
		if !strings.Contains(names, want) {
			t.Fatalf("missing secret reference %s in %s", want, names)
		}
	}
}

func TestValidateWeComAIBotCredentials(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	channel := &cfg.Tenants[0].Channels[0]
	*channel = ChannelConfig{
		Type: "wecom-aibot", BindingID: "wecom-bot", Enabled: true,
		BotIDEnv: "WECOM_AIBOT_ID", BotSecretEnv: "WECOM_AIBOT_SECRET",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid WeCom intelligent bot rejected: %v", err)
	}
	channel.BotSecretEnv = ""
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "bot_id_env and bot_secret_env") {
		t.Fatalf("missing bot secret validation = %v", err)
	}

	aibotYAML := strings.Replace(validYAML,
		"{type: telegram, binding_id: telegram-main, enabled: true, token_env: TG_TOKEN, signing_secret_env: TG_SECRET}",
		"{type: wecom-aibot, binding_id: wecom-bot, enabled: true, bot_id_env: WECOM_AIBOT_ID, bot_secret_env: WECOM_AIBOT_SECRET}", 1)
	parsed, err := Decode(strings.NewReader(aibotYAML))
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.Tenants[0].Channels[0].MaxMessageLength; got != 20480 {
		t.Fatalf("default message limit = %d, want 20480", got)
	}
	names := strings.Join(parsed.SecretEnvNames(), ",")
	if !strings.Contains(names, "WECOM_AIBOT_ID") || !strings.Contains(names, "WECOM_AIBOT_SECRET") {
		t.Fatalf("missing bot credential references in %s", names)
	}
}

func TestDecodeRejectsUnknownField(t *testing.T) {
	_, err := Decode(strings.NewReader(validYAML + "unknown: true\n"))
	if err == nil || !strings.Contains(err.Error(), "field unknown") {
		t.Fatalf("expected strict YAML error, got %v", err)
	}
}

func TestValidateModelFallbackRequiresNonStreamingOpenAIPrimary(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	modelConfig := &cfg.Tenants[0].Model
	modelConfig.Provider = "openai"
	modelConfig.Name = "qwen-test"
	modelConfig.APIKeyEnv = "MODEL_KEY"
	modelConfig.FallbackProvider = "mock"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid primary/fallback config rejected: %v", err)
	}
	modelConfig.Streaming = true
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "streaming must be false") {
		t.Fatalf("streaming fallback validation = %v", err)
	}
	modelConfig.Streaming = false
	modelConfig.FallbackProvider = "openai"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "mock fallback") {
		t.Fatalf("unsupported fallback validation = %v", err)
	}
}

func TestValidateRejectsCrossTenantBindingCollision(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	duplicate := cfg.Tenants[0]
	duplicate.TenantID = "tenant-b"
	cfg.Tenants = append(cfg.Tenants, duplicate)
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "shared by tenants") {
		t.Fatalf("expected binding collision, got %v", err)
	}
}

func TestValidateToolSideEffectsAreExplicitAndAllowed(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	policy := &cfg.Tenants[0].Tools
	policy.Allow = append(policy.Allow, "tenant_admin_action")
	policy.RequireConfirm = []string{"tenant_admin_action"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "must be declared in side_effects") {
		t.Fatalf("confirmation without side-effect declaration = %v", err)
	}
	policy.SideEffects = []string{"tenant_admin_action"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid side-effect policy rejected: %v", err)
	}
	policy.SideEffects = append(policy.SideEffects, "not-allowed")
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "side-effect tool") {
		t.Fatalf("unallowed side-effect tool validation = %v", err)
	}
}

func TestValidateToolPolicyRejectsUnsafeAndDuplicateNamesAtStartup(t *testing.T) {
	for name, mutate := range map[string]func(*ToolPolicy){
		"unsafe side effect": func(policy *ToolPolicy) {
			policy.Allow = append(policy.Allow, "calendar create")
			policy.SideEffects = []string{"calendar create"}
		},
		"duplicate side effect": func(policy *ToolPolicy) {
			policy.Allow = append(policy.Allow, "calendar_create")
			policy.SideEffects = []string{"calendar_create", "calendar_create"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := Decode(strings.NewReader(validYAML))
			if err != nil {
				t.Fatal(err)
			}
			mutate(&cfg.Tenants[0].Tools)
			if err := cfg.Validate(); err == nil {
				t.Fatal("invalid tool policy was accepted")
			}
		})
	}
}

func TestValidateRequiresNumericWeComAgentID(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	channel := &cfg.Tenants[0].Channels[0]
	channel.Type = "wecom"
	channel.WorkspaceID = "wwCorpId123"
	channel.ApplicationID = "not-numeric"
	channel.EncryptionKeyEnv = "WECOM_AES_KEY"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "application_id (agentid)") {
		t.Fatalf("non-numeric WeCom agentid validation = %v", err)
	}
	channel.ApplicationID = "1000002"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("numeric WeCom agentid rejected: %v", err)
	}
}

func TestValidateRejectsDeclaredButUncompiledMemoryBackend(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tenants[0].Data.Memory = BackendConfig{Type: "sql", DSNEnv: "SQL_DSN"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "runnable memory backend") {
		t.Fatalf("expected fail-closed memory backend validation, got %v", err)
	}
}

func TestValidateAllowsSQLSessionWithSummaryDisabled(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	tenant := &cfg.Tenants[0]
	tenant.Data.Session = BackendConfig{Type: "sql", DSNEnv: "POSTGRES_SESSION_DSN"}
	tenant.Data.Summary = BackendConfig{Type: "disabled"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("sql session backend rejected: %v", err)
	}
}

func TestValidatePostgresQueueRequiresSQLSessionOnSameAtomicDSN(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Queue.Backend = "postgres"
	cfg.Queue.DSNEnv = "QUEUE_DATABASE_DSN"
	tenant := &cfg.Tenants[0]
	tenant.Data.Session = BackendConfig{Type: "sql", DSNEnv: "SESSION_DATABASE_DSN"}
	tenant.Data.Summary = BackendConfig{Type: "disabled"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "must match queue.dsn_env for atomic turn commit") {
		t.Fatalf("different queue/session DSNs validation = %v", err)
	}
	tenant.Data.Session.DSNEnv = cfg.Queue.DSNEnv
	if err := cfg.Validate(); err != nil {
		t.Fatalf("shared queue/session atomic DSN rejected: %v", err)
	}
}

func TestValidateSQLSessionRequiresDSNEnv(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	tenant := &cfg.Tenants[0]
	tenant.Data.Session = BackendConfig{Type: "sql"}
	tenant.Data.Summary = BackendConfig{Type: "disabled"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "data.session requires dsn_env") {
		t.Fatalf("sql session without dsn_env validation = %v", err)
	}
}

func TestValidateSQLSessionSupportsPersistentSummary(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	tenant := &cfg.Tenants[0]
	tenant.Data.Session = BackendConfig{Type: "sql", DSNEnv: "POSTGRES_SESSION_DSN"}
	tenant.Data.Summary = BackendConfig{Type: "inmemory"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "runnable summary backend") {
		t.Fatalf("sql session with non-persistent summary validation = %v", err)
	}
	tenant.Data.Summary = BackendConfig{Type: "sql", DSNEnv: "POSTGRES_SESSION_DSN"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("sql session with persistent summary validation = %v", err)
	}
}

func TestValidateProductionDataBackendRequirements(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*TenantConfig)
		want   string
	}{
		{
			name: "artifact object",
			mutate: func(tenant *TenantConfig) {
				tenant.Data.Artifact = BackendConfig{Type: "object", Bucket: "artifacts"}
			},
			want: "object artifact requires provider=s3 and bucket",
		},
		{
			name: "knowledge vector",
			mutate: func(tenant *TenantConfig) {
				tenant.Data.Knowledge = BackendConfig{Type: "vector"}
			},
			want: "data.knowledge requires dsn_env",
		},
		{
			name: "independent summary",
			mutate: func(tenant *TenantConfig) {
				tenant.Data.Summary = BackendConfig{Type: "external", DSNEnv: "SUMMARY_DSN"}
			},
			want: "runnable summary backend",
		},
		{
			name: "audit declaration drift",
			mutate: func(tenant *TenantConfig) {
				tenant.Data.AuditLog = BackendConfig{Type: "file"}
			},
			want: "must match effective audit sink",
		},
		{
			name: "sql audit requires durable queue",
			mutate: func(tenant *TenantConfig) {
				tenant.Audit.Sink = "sql"
				tenant.Data.AuditLog = BackendConfig{Type: "sql", DSNEnv: "AUDIT_DSN"}
			},
			want: "requires queue.backend=postgres",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := Decode(strings.NewReader(validYAML))
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(&cfg.Tenants[0])
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateAllowsConstructedProductionDataBackends(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	tenant := &cfg.Tenants[0]
	tenant.Data.Artifact = BackendConfig{
		Type: "object", Provider: "s3", Bucket: "tenant-artifacts",
		DSNEnv: "ARTIFACT_RUNTIME_DSN", MigrationDSNEnv: "ARTIFACT_MIGRATION_DSN",
		Endpoint: "https://s3.example.test", Region: "ap-guangzhou",
		AccessKeyEnv: "S3_ACCESS_KEY", SecretKeyEnv: "S3_SECRET_KEY",
	}
	tenant.Data.Knowledge = BackendConfig{
		Type: "vector", Provider: "pgvector", DSNEnv: "KNOWLEDGE_RUNTIME_DSN",
		MigrationDSNEnv: "KNOWLEDGE_MIGRATION_DSN", Namespace: "knowledge_1536",
		APIKeyEnv: "EMBEDDING_API_KEY", EmbeddingModel: "text-embedding-3-small",
		EmbeddingDimension: 1536,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("production data backends rejected: %v", err)
	}
	names := strings.Join(cfg.SecretEnvNames(), ",")
	for _, want := range []string{"S3_ACCESS_KEY", "S3_SECRET_KEY", "ARTIFACT_RUNTIME_DSN", "ARTIFACT_MIGRATION_DSN", "KNOWLEDGE_RUNTIME_DSN", "KNOWLEDGE_MIGRATION_DSN", "EMBEDDING_API_KEY"} {
		if !strings.Contains(names, want) {
			t.Fatalf("secret reference %s missing from %s", want, names)
		}
	}
}

func TestValidateObjectArtifactRequiresRuntimeAndMigrationMetadataDSNs(t *testing.T) {
	for name, backend := range map[string]BackendConfig{
		"both missing":      {Type: "object", Provider: "s3", Bucket: "artifacts"},
		"migration missing": {Type: "object", Provider: "s3", Bucket: "artifacts", DSNEnv: "ARTIFACT_RUNTIME_DSN"},
		"runtime missing":   {Type: "object", Provider: "s3", Bucket: "artifacts", MigrationDSNEnv: "ARTIFACT_MIGRATION_DSN"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := Decode(strings.NewReader(validYAML))
			if err != nil {
				t.Fatal(err)
			}
			cfg.Tenants[0].Data.Artifact = backend
			err = cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), "metadata ledger") {
				t.Fatalf("validation error = %v", err)
			}
		})
	}
}

func TestValidateAllowsExternalMem0ReadAndIngest(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	tenant := &cfg.Tenants[0]
	tenant.Tools.Allow = append(tenant.Tools.Allow, "memory_search", "memory_load")
	tenant.Data.Memory = BackendConfig{
		Type: "external", Provider: "mem0", Endpoint: "https://memory.example.test",
		Mode: "cloud", APIKeyEnv: "MEM0_API_KEY",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("external memory rejected: %v", err)
	}
	tenant.Tools.Allow = append(tenant.Tools.Allow, "memory_delete")
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "read-only tools") {
		t.Fatalf("mutating external memory tool validation = %v", err)
	}
}

func TestValidateRejectsCredentialsEmbeddedInBackendEndpoint(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tenants[0].Data.Artifact = BackendConfig{
		Type: "object", Provider: "s3", Bucket: "artifacts",
		Endpoint: "https://user:canary-secret@s3.example.test",
	}
	err = cfg.Validate()
	if err == nil || strings.Contains(err.Error(), "canary-secret") {
		t.Fatalf("embedded endpoint credential validation = %v", err)
	}
}

func TestValidateSummaryMustUseSessionBackend(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	tenant := &cfg.Tenants[0]
	tenant.Data.Session = BackendConfig{Type: "redis", DSNEnv: "REDIS_URL", Namespace: "session-prefix"}
	tenant.Data.Summary = BackendConfig{Type: "redis", DSNEnv: "REDIS_URL", Namespace: "other-prefix"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "summary namespace must match session namespace") {
		t.Fatalf("mismatched summary validation = %v", err)
	}
	tenant.Data.Summary.Namespace = tenant.Data.Session.Namespace
	if err := cfg.Validate(); err != nil {
		t.Fatalf("matching session/summary backend rejected: %v", err)
	}
}

func TestValidateRejectsLiteralSecretInEnvironmentReference(t *testing.T) {
	for _, literal := range []string{"fixture-channel-credential", "redis://fixture-user:fixture-credential@redis:6379/0"} {
		cfg, err := Decode(strings.NewReader(validYAML))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Tenants[0].Channels[0].TokenEnv = literal
		err = cfg.Validate()
		if err == nil {
			t.Fatalf("literal secret was accepted as an env reference")
		}
		if strings.Contains(err.Error(), "fixture") {
			t.Fatalf("validation error echoed literal secret: %v", err)
		}
	}
}

func TestSecretRejectsLiteralWithoutEchoingIt(t *testing.T) {
	const literal = "redis://fixture-user:fixture-credential@redis:6379/0"
	_, err := Secret(literal)
	if err == nil || strings.Contains(err.Error(), "fixture-credential") {
		t.Fatalf("unsafe secret reference error = %v", err)
	}
}

func TestSkillPolicyValidationAndLegacyJSON(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(cfg.Tenants[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"skills"`) {
		t.Fatal("absent skills changed legacy revision JSON")
	}
	for _, names := range [][]string{{""}, {"*"}, {"../other"}, {"greeting", "greeting"}} {
		cfg.Tenants[0].Skills.Allow = names
		if err := cfg.Validate(); err == nil {
			t.Fatalf("accepted invalid skill grants: %v", names)
		}
	}
	cfg.Tenants[0].Skills.Allow = []string{"greeting", "tenant-a-private"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(cfg.Tenants[0])
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip TenantConfig
	if err := json.Unmarshal(data, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if len(roundtrip.Skills.Allow) != 2 || roundtrip.Skills.Allow[1] != "tenant-a-private" {
		t.Fatal("skill grants missing from persisted task snapshot")
	}
	decoded, err := Decode(strings.NewReader(strings.Replace(validYAML, "    tools:", "    skills: {allow: [greeting]}\n    tools:", 1)))
	if err != nil || len(decoded.Tenants[0].Skills.Allow) != 1 {
		t.Fatalf("skill YAML failed: %v", err)
	}
}

func TestPrivacyModesAndLegacyRevisionEncoding(t *testing.T) {
	cfg, err := Decode(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	original, err := json.Marshal(cfg.Tenants[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(original), `"privacy"`) {
		t.Fatal("empty policy changed legacy revision encoding")
	}
	for _, mode := range []string{"off", "redact", "block"} {
		cfg.Tenants[0].Privacy = PrivacyPolicy{Input: mode, Output: mode}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(cfg.Tenants[0])
		var restored TenantConfig
		if json.Unmarshal(encoded, &restored) != nil || restored.Privacy != cfg.Tenants[0].Privacy {
			t.Fatal("revision dropped privacy policy")
		}
	}
	cfg.Tenants[0].Privacy.Input = "redcat"
	if err := cfg.Validate(); err == nil {
		t.Fatal("typo silently disabled policy")
	}
}
