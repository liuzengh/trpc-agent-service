package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const validYAML = `schema_version: 1
tenants:
  - tenant_id: tenant-a
    name: Tenant A
    enabled: true
    config_version: 3
    audit:
      enabled: true
      retention_days: 30
      store_content: false
      redact_fields: [authorization]
    runtime:
      max_concurrent_runs: 4
    apps:
      - app_id: support
        name: Support Agent
        enabled: true
        config:
          instruction: Help the user.
        model:
          provider: openai
          name: example-model
          base_url: https://models.example.com/v1
          api_key:
            provider: env
            key: MODEL_API_KEY
          temperature: 0.2
          max_tokens: 2048
          pricing:
            version: price-2026-09
            input_micros_per_million: 1000000
            output_micros_per_million: 2000000
        tools:
          allow: [calculator, search]
          deny: [shell]
          require_approval: [search]
          request_token_budget: 10000
          monthly_cost_budget_cents: 100000
        channels:
          - binding_id: http-a
            type: http
            provider_account_id: local
            enabled: true
          - binding_id: feishu-a
            type: feishu
            provider_account_id: bot-a
            token:
              provider: env
              key: FEISHU_VERIFICATION_TOKEN
            secret:
              provider: env
              key: FEISHU_APP_SECRET
            enabled: true
        storage:
          session:
            type: inmemory
            namespace: tenant-a-support
          memory:
            type: inmemory
          summary:
            type: inmemory
          artifact:
            type: local
            endpoint: ./data/artifacts
          knowledge:
            type: inmemory
          audit:
            type: sqlite
            endpoint: ./data/audit.db
`

func TestLoadValidConfiguration(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(file.Tenants) != 1 || len(file.Tenants[0].Apps) != 1 {
		t.Fatalf("unexpected config: %+v", file)
	}
	if file.Tenants[0].ConfigVersion != 3 {
		t.Fatalf("config version = %d", file.Tenants[0].ConfigVersion)
	}
	if file.Tenants[0].Runtime.ConcurrentRunLimit() != 4 {
		t.Fatalf("runtime quota = %d", file.Tenants[0].Runtime.ConcurrentRunLimit())
	}
	snapshot, err := file.Snapshot("tenant-a", "support")
	if err != nil {
		t.Fatal(err)
	}
	file.Tenants[0].Runtime.MaxConcurrentRuns = 1
	if snapshot.Runtime().ConcurrentRunLimit() != 4 {
		t.Fatalf("snapshot runtime policy was mutated: %d", snapshot.Runtime().ConcurrentRunLimit())
	}
}

func TestRuntimeConcurrencyQuotaValidationAndDefault(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	file.Tenants[0].Runtime.MaxConcurrentRuns = 0
	if err := file.Validate(); err != nil || file.Tenants[0].Runtime.ConcurrentRunLimit() != 8 {
		t.Fatalf("legacy default limit=%d err=%v", file.Tenants[0].Runtime.ConcurrentRunLimit(), err)
	}
	file.Tenants[0].Runtime.MaxConcurrentRuns = 257
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "max_concurrent_runs") {
		t.Fatalf("invalid runtime quota error=%v", err)
	}
}

func TestWorkflowValidation(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	app := &file.Tenants[0].Apps[0]
	app.Workflow = tenant.WorkflowConfig{Type: tenant.WorkflowGraph, Nodes: []tenant.WorkflowNode{{ID: "research", Instruction: "Research."}, {ID: "answer", Instruction: "Answer."}}, Edges: []tenant.WorkflowEdge{{From: "research", To: "answer"}}, Entry: "research", Finish: "answer", MaxConcurrency: 2}
	if err := file.Validate(); err != nil {
		t.Fatalf("valid graph workflow: %v", err)
	}
	app.Workflow.Edges[0].To = "missing"
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "to is unknown") {
		t.Fatalf("invalid graph edge error=%v", err)
	}
	app.Workflow = tenant.WorkflowConfig{Type: tenant.WorkflowParallel, Nodes: []tenant.WorkflowNode{{ID: "branch", Instruction: "Branch."}}}
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "requires an aggregator") {
		t.Fatalf("parallel without aggregator error=%v", err)
	}
}

func TestDisabledFeishuBindingMayAwaitCredentials(t *testing.T) {
	payload := strings.Replace(validYAML, "            token:\n              provider: env\n              key: FEISHU_VERIFICATION_TOKEN\n            secret:\n              provider: env\n              key: FEISHU_APP_SECRET\n            enabled: true", "            enabled: false", 1)
	if _, err := Load(strings.NewReader(payload)); err != nil {
		t.Fatalf("disabled Feishu binding should permit deferred credentials: %v", err)
	}
	payload = strings.Replace(payload, "            enabled: false", "            enabled: true", 1)
	if _, err := Load(strings.NewReader(payload)); err == nil || !strings.Contains(err.Error(), "token is required") {
		t.Fatalf("enabled Feishu binding without credentials error = %v", err)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "unknown field",
			yaml: validYAML + "unknown_field: true\n",
			want: "field unknown_field not found",
		},
		{
			name: "empty tenant",
			yaml: strings.Replace(validYAML, "tenant_id: tenant-a", "tenant_id: ''", 1),
			want: "tenant_id must be non-empty",
		},
		{
			name: "duplicate binding",
			yaml: strings.Replace(validYAML, "binding_id: feishu-a", "binding_id: http-a", 1),
			want: "duplicate binding_id",
		},
		{
			name: "duplicate allowed user",
			yaml: strings.Replace(validYAML, "provider_account_id: bot-a", "provider_account_id: bot-a\n            allowed_users: [alice, alice]", 1),
			want: "allowed_users contains duplicate",
		},
		{
			name: "untrimmed allowed chat",
			yaml: strings.Replace(validYAML, "provider_account_id: bot-a", "provider_account_id: bot-a\n            allowed_chats: [' room-a']", 1),
			want: "allowed_chats entries must be non-empty and trimmed",
		},
		{
			name: "invalid backend",
			yaml: strings.Replace(validYAML, "type: inmemory\n            namespace", "type: invalid\n            namespace", 1),
			want: "storage.session.type",
		},
		{
			name: "budget without pricing version",
			yaml: strings.Replace(validYAML, "version: price-2026-09", "version: ''", 1),
			want: "pricing.version is required",
		},
		{
			name: "budget without output pricing",
			yaml: strings.Replace(validYAML, "output_micros_per_million: 2000000", "output_micros_per_million: 0", 1),
			want: "input and output rates must be positive",
		},
		{
			name: "credentials in endpoint",
			yaml: strings.Replace(validYAML, "endpoint: ./data/artifacts", "endpoint: https://user:pass@example.com", 1),
			want: "must not contain credentials",
		},
		{
			name: "secret-like query in endpoint",
			yaml: strings.Replace(validYAML, "endpoint: ./data/artifacts", "endpoint: https://example.com?token=secret", 1),
			want: "must not contain query parameters",
		},
		{
			name: "secret-like fragment in endpoint",
			yaml: strings.Replace(validYAML, "endpoint: ./data/artifacts", "endpoint: https://example.com/#token=secret", 1),
			want: "must not contain fragments",
		},
		{
			name: "secret-like fragment in webhook",
			yaml: strings.Replace(validYAML, "provider_account_id: local", "provider_account_id: local\n            webhook_url: 'https://example.com/#token=secret'", 1),
			want: "must not contain fragments",
		},
		{
			name: "multiple documents",
			yaml: validYAML + "---\n{}\n",
			want: "multiple YAML documents",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(test.yaml))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadNeverResolvesOrLeaksSecretValue(t *testing.T) {
	const canary = "canary-super-secret-value"
	t.Setenv("MODEL_API_KEY", canary)

	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	encoded, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(encoded), canary) {
		t.Fatalf("serialized config leaked secret: %s", encoded)
	}
	if !strings.Contains(string(encoded), "MODEL_API_KEY") {
		t.Fatalf("serialized config lost secret reference: %s", encoded)
	}
}

func TestLoadRejectsInlineSecretValueWithoutEchoingIt(t *testing.T) {
	const canary = "inline-canary-secret"
	invalid := strings.Replace(
		validYAML,
		"key: MODEL_API_KEY",
		"key: MODEL_API_KEY\n            value: "+canary,
		1,
	)
	_, err := Load(strings.NewReader(invalid))
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
	if strings.Contains(err.Error(), canary) {
		t.Fatalf("Load() error leaked inline secret: %v", err)
	}
}

func TestLoadFromEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(validYAML), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv(EnvConfigPath, path)
	file, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if file.Tenants[0].ID != "tenant-a" {
		t.Fatalf("tenant ID = %q", file.Tenants[0].ID)
	}
}

func TestLoadFromEnvRequiresPath(t *testing.T) {
	t.Setenv(EnvConfigPath, "")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("LoadFromEnv() error = nil, want non-nil")
	}
}

func TestExampleConfigurationLoads(t *testing.T) {
	file, err := LoadFile("../../configs/example.yaml")
	if err != nil {
		t.Fatalf("LoadFile(example) error = %v", err)
	}
	if file.Tenants[0].ID != "demo" {
		t.Fatalf("example tenant ID = %q", file.Tenants[0].ID)
	}
}

func TestValidateWeComRequiresAppAndEncryptionReferences(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	binding := &file.Tenants[0].Apps[0].Channels[0]
	binding.Type = tenant.ChannelTypeWeCom
	binding.ProviderAccountID = "ww-corp"
	binding.Token = tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "WECOM_TOKEN"}
	binding.Secret = tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "WECOM_APP_SECRET"}
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "provider_app_id") {
		t.Fatalf("missing provider_app_id error=%v", err)
	}
	binding.ProviderAppID = "1000002"
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "encryption_key") {
		t.Fatalf("missing encryption_key error=%v", err)
	}
	binding.EncryptionKey = tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "WECOM_ENCODING_AES_KEY"}
	if err := file.Validate(); err != nil {
		t.Fatalf("valid WeCom binding error=%v", err)
	}
}

func TestValidateAllowsSameBindingIDAcrossTenants(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	second := file.Tenants[0].Clone()
	second.ID = "tenant-b"
	second.Name = "Tenant B"
	file.Tenants = append(file.Tenants, second)
	if err := file.Validate(); err != nil {
		t.Fatalf("Validate() cross-tenant binding error = %v", err)
	}
}

func TestValidateKnowledgeRequiresToolAndSafeEmbeddingConfig(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	app := &file.Tenants[0].Apps[0]
	app.Knowledge = tenant.KnowledgePolicy{Enabled: true, Embedding: tenant.EmbeddingProfile{Provider: "openai-compatible", Model: "text-embedding", BaseURL: "https://embedding.example/v1", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "EMBEDDING_API_KEY"}, Dimensions: 1536}}
	app.Storage.Knowledge = tenant.BackendConfig{Type: tenant.BackendQdrant, Endpoint: "grpcs://qdrant.example:6334", Namespace: "docs", Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "QDRANT_API_KEY"}}
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "knowledge_search") {
		t.Fatalf("missing knowledge tool error=%v", err)
	}
	app.Tools.Allow = append(app.Tools.Allow, "knowledge_search")
	if err := file.Validate(); err != nil {
		t.Fatalf("valid knowledge config error=%v", err)
	}
	app.Knowledge.Embedding.Dimensions = 0
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "dimensions") {
		t.Fatalf("invalid dimension error=%v", err)
	}
}

func TestValidateMigrationTargetIsOneLevelAndDifferent(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	route := &file.Tenants[0].Apps[0].Storage.Session
	target := route.Clone()
	route.MigrationTarget = &target
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("same target error=%v", err)
	}
	target.Endpoint = "postgres://target.example/runtime"
	nested := target.Clone()
	target.MigrationTarget = &nested
	if err := file.Validate(); err == nil || !strings.Contains(err.Error(), "cannot contain") {
		t.Fatalf("nested target error=%v", err)
	}
}
