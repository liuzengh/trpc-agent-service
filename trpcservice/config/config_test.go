package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadAcceptsPlatformConfiguration(t *testing.T) {
	configuration, err := Load(strings.NewReader(`{
  "service": {
    "listen_address": ":8080",
    "request_timeout": "15s",
    "max_inbound_bytes": 1048576,
    "docling_endpoint": "http://docling:5001",
    "document_extract_timeout": "5m"
  }
}`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := configuration.Service.ListenAddress, ":8080"; got != want {
		t.Errorf("ListenAddress = %q, want %q", got, want)
	}
	if configuration.Service.DoclingEndpoint != "http://docling:5001" || configuration.Service.DocumentExtractTimeout.Duration != 5*time.Minute {
		t.Fatalf("document extraction config = %#v", configuration.Service)
	}
	if got, want := configuration.Service.RequestTimeout.String(), "15s"; got != want {
		t.Errorf("RequestTimeout = %q, want %q", got, want)
	}
}

func TestLoadAcceptsManagedModelCapabilities(t *testing.T) {
	configuration, err := Load(strings.NewReader(`{
  "service": {
    "listen_address": ":8080",
    "request_timeout": "15s",
    "max_inbound_bytes": 1048576,
    "allowed_secret_refs": ["env:MODEL_KEY"]
  },
  "model_providers": [{
    "id": "primary",
    "base_url": "https://models.example/v1",
    "api_key_ref": "env:MODEL_KEY",
    "models": [{
      "name": "reasoning-model",
      "capabilities": {
        "reasoning_efforts": ["low", "high"],
        "thinking_toggle": true,
        "thinking_budget": true
      }
    }]
  }]
}`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	capabilities := configuration.ModelProviders[0].Models[0].Capabilities
	if capabilities == nil || len(capabilities.ReasoningEfforts) != 2 || !capabilities.ThinkingToggle || !capabilities.ThinkingBudget {
		t.Fatalf("model capabilities = %#v", capabilities)
	}
}

func TestLoadAcceptsFrameworkKnowledgeConfiguration(t *testing.T) {
	configuration, err := Load(strings.NewReader(`{
  "service": {
    "listen_address": ":8080",
    "request_timeout": "15s",
    "max_inbound_bytes": 1048576,
    "allowed_secret_refs": ["env:MODEL_KEY"],
    "knowledge": {
      "embedding_provider_id": "primary",
      "embedding_model": "text-embedding-3-small",
      "embedding_dimensions": 1536,
      "reranker": {"type": "cohere", "api_key_ref": "env:MODEL_KEY", "top_n": 7},
      "allowed_source_hosts": ["github.com"],
      "allowed_source_roots": ["/srv/knowledge"]
    }
  },
  "model_providers": [{
    "id": "primary",
    "type": "openai",
    "base_url": "https://models.example/v1",
    "api_key_ref": "env:MODEL_KEY"
  }]
}`))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := configuration.Service.Knowledge.Reranker.Type, "cohere"; got != want {
		t.Fatalf("knowledge reranker = %q, want %q", got, want)
	}
}

func TestLoadRejectsInvalidFrameworkKnowledgeConfiguration(t *testing.T) {
	_, err := Load(strings.NewReader(`{
  "service": {
    "listen_address": ":8080",
    "request_timeout": "15s",
    "max_inbound_bytes": 1048576,
    "allowed_secret_refs": ["env:MODEL_KEY"],
    "knowledge": {
      "embedding_provider_id": "primary",
      "embedding_model": "text-embedding-3-small",
      "embedding_dimensions": 768,
      "reranker": {"type": "custom"}
    }
  },
  "model_providers": [{"id": "primary", "base_url": "https://models.example/v1", "api_key_ref": "env:MODEL_KEY"}]
}`))
	if err == nil || !strings.Contains(err.Error(), "embedding_dimensions") {
		t.Fatalf("Load() error = %v, want invalid framework knowledge dimensions", err)
	}
}

func TestLoadRejectsTenantSnapshotsInStartupConfiguration(t *testing.T) {
	_, err := Load(strings.NewReader(`{
  "service": {"listen_address": ":8080", "request_timeout": "15s", "max_inbound_bytes": 1024},
  "tenants": [{"tenant_id":"acme","app_code":"support","status":"active","config_version":1}]
}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("Load() error = %v, want tenants rejected as an unknown startup field", err)
	}
}

func TestLoadRejectsInvalidPlatformConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		config string
		want   string
	}{
		{
			name: "unknown field",
			config: `{
  "service": {"listen_address": ":8080", "request_timeout": "15s", "max_inbound_bytes": 1024, "debug_everything": true}
}`,
			want: "unknown field",
		},
		{
			name: "invalid timeout",
			config: `{
  "service": {"listen_address": ":8080", "request_timeout": "never", "max_inbound_bytes": 1024}
}`,
			want: "request_timeout",
		},
		{
			name: "duplicate allowed secret",
			config: `{
  "service": {
    "listen_address": ":8080", "request_timeout": "15s", "max_inbound_bytes": 1024,
    "allowed_secret_refs": ["env:MODEL_KEY", "env:MODEL_KEY"]
  }
}`,
			want: "duplicate",
		},
		{
			name: "unmanaged channel credential",
			config: `{
  "service": {
    "listen_address": ":8080", "request_timeout": "15s", "max_inbound_bytes": 1024,
    "allowed_secret_refs": ["env:MODEL_KEY"],
    "channel_credential_refs": ["env:BOT_CONFIG"]
  }
}`,
			want: "not in allowed_secret_refs",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(test.config))
			if err == nil {
				t.Fatal("Load() error = nil, want an error")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Errorf("Load() error = %q, want substring %q", err, test.want)
			}
		})
	}
}

func validServiceConfigForTest() Config {
	return Config{Service: ServiceConfig{
		ListenAddress:   ":8080",
		RequestTimeout:  Duration{Duration: 15 * time.Second},
		MaxInboundBytes: 1024,
	}}
}

func TestConfigValidatesOptionalHTTPTimeouts(t *testing.T) {
	base := validServiceConfigForTest()
	base.Service.HTTPReadHeaderTimeout = Duration{Duration: 15 * time.Second}
	base.Service.HTTPReadTimeout = Duration{Duration: 5 * time.Minute}
	base.Service.HTTPIdleTimeout = Duration{Duration: 2 * time.Minute}
	if err := base.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := []struct {
		name string
		set  func(*ServiceConfig)
	}{
		{name: "read header", set: func(service *ServiceConfig) { service.HTTPReadHeaderTimeout.Duration = -time.Second }},
		{name: "read", set: func(service *ServiceConfig) { service.HTTPReadTimeout.Duration = -time.Second }},
		{name: "idle", set: func(service *ServiceConfig) { service.HTTPIdleTimeout.Duration = -time.Second }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validServiceConfigForTest()
			test.set(&cfg.Service)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() accepted negative HTTP timeout")
			}
		})
	}
}

func TestConfigValidateRejectsInvalidServiceSettings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"missing listen address", func(c *Config) { c.Service.ListenAddress = " " }, "listen_address"},
		{"zero request timeout", func(c *Config) { c.Service.RequestTimeout.Duration = 0 }, "request_timeout"},
		{"negative request timeout", func(c *Config) { c.Service.RequestTimeout.Duration = -time.Second }, "request_timeout"},
		{"zero inbound limit", func(c *Config) { c.Service.MaxInboundBytes = 0 }, "max_inbound_bytes"},
		{"negative archive age", func(c *Config) { c.Service.SessionIdleArchiveAge.Duration = -time.Second }, "session_idle_archive_age"},
		{"negative outbox retention age", func(c *Config) { c.Service.OutboxRetentionAge.Duration = -time.Second }, "outbox_retention_age"},
		{"negative document timeout", func(c *Config) { c.Service.DocumentExtractTimeout.Duration = -time.Second }, "document_extract_timeout"},
		{"invalid docling scheme", func(c *Config) { c.Service.DoclingEndpoint = "ftp://docling.example.test" }, "docling_endpoint"},
		{"docling without host", func(c *Config) { c.Service.DoclingEndpoint = "https:///missing-host" }, "docling_endpoint"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			configuration := validServiceConfigForTest()
			tt.edit(&configuration)
			if err := configuration.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want %q", err, tt.want)
			}
		})
	}

	valid := validServiceConfigForTest()
	valid.Service.DoclingEndpoint = " https://docling.example.test/api "
	valid.Service.SessionIdleArchiveAge.Duration = time.Hour
	valid.Service.DocumentExtractTimeout.Duration = time.Minute
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate(valid service) error = %v", err)
	}
}

func TestConfigValidateKnowledgePolicyMatrix(t *testing.T) {
	t.Parallel()
	base := validServiceConfigForTest()
	base.Service.AllowedSecretRefs = []string{"env:MODEL_KEY", "env:RERANK_KEY"}
	base.ModelProviders = []ModelProviderConfig{{ID: "embeddings", Type: ModelProviderOpenAI, BaseURL: "https://models.example.test/v1", APIKeyRef: "env:MODEL_KEY"}}
	base.Service.Knowledge = KnowledgeConfig{
		EmbeddingProviderID: "embeddings",
		EmbeddingModel:      "embedding-model",
		EmbeddingDimensions: 1536,
		Reranker:            RerankerConfig{Type: "topk", TopN: 5},
		AllowedSourceHosts:  []string{"docs.example.test"},
		AllowedSourceRoots:  []string{"/srv/support-knowledge"},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("base knowledge configuration error = %v", err)
	}

	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"provider required when model set", func(c *Config) { c.Service.Knowledge.EmbeddingProviderID = "" }, "embedding_provider_id"},
		{"embedding model required", func(c *Config) { c.Service.Knowledge.EmbeddingModel = "" }, "embedding_model"},
		{"embedding dimensions fixed", func(c *Config) { c.Service.Knowledge.EmbeddingDimensions = 768 }, "embedding_dimensions"},
		{"provider must exist", func(c *Config) { c.Service.Knowledge.EmbeddingProviderID = "missing" }, "not managed"},
		{"provider must be compatible", func(c *Config) { c.ModelProviders[0].Type = ModelProviderHuggingFace }, "OpenAI-compatible"},
		{"topk rejects endpoint", func(c *Config) { c.Service.Knowledge.Reranker.Endpoint = "https://rerank.example.test" }, "topk"},
		{"topk rejects key", func(c *Config) { c.Service.Knowledge.Reranker.APIKeyRef = "env:RERANK_KEY" }, "topk"},
		{"infinity requires endpoint", func(c *Config) { c.Service.Knowledge.Reranker.Type = "infinity" }, "endpoint is required"},
		{"reranker endpoint scheme", func(c *Config) {
			c.Service.Knowledge.Reranker.Type = "infinity"
			c.Service.Knowledge.Reranker.Endpoint = "ftp://rerank.example.test"
		}, "http(s) URL"},
		{"reranker endpoint credentials", func(c *Config) {
			c.Service.Knowledge.Reranker.Type = "infinity"
			c.Service.Knowledge.Reranker.Endpoint = "https://user:pass@rerank.example.test"
		}, "without credentials or fragment"},
		{"reranker endpoint fragment", func(c *Config) {
			c.Service.Knowledge.Reranker.Type = "cohere"
			c.Service.Knowledge.Reranker.Endpoint = "https://rerank.example.test/#fragment"
		}, "without credentials or fragment"},
		{"unsupported reranker", func(c *Config) { c.Service.Knowledge.Reranker.Type = "custom" }, "unsupported"},
		{"negative reranker top n", func(c *Config) { c.Service.Knowledge.Reranker.TopN = -1 }, "top_n"},
		{"unmanaged reranker key", func(c *Config) {
			c.Service.Knowledge.Reranker.Type = "cohere"
			c.Service.Knowledge.Reranker.APIKeyRef = "env:OTHER_KEY"
		}, "api_key_ref"},
		{"invalid source host path", func(c *Config) { c.Service.Knowledge.AllowedSourceHosts = []string{"docs.example.test/path"} }, "allowed_source_hosts"},
		{"invalid source host empty", func(c *Config) { c.Service.Knowledge.AllowedSourceHosts = []string{" "} }, "allowed_source_hosts"},
		{"relative source root", func(c *Config) { c.Service.Knowledge.AllowedSourceRoots = []string{"relative/path"} }, "allowed_source_roots"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			configuration := base
			configuration.Service.AllowedSecretRefs = append([]string(nil), base.Service.AllowedSecretRefs...)
			configuration.ModelProviders = append([]ModelProviderConfig(nil), base.ModelProviders...)
			configuration.Service.Knowledge.AllowedSourceHosts = append([]string(nil), base.Service.Knowledge.AllowedSourceHosts...)
			configuration.Service.Knowledge.AllowedSourceRoots = append([]string(nil), base.Service.Knowledge.AllowedSourceRoots...)
			tt.edit(&configuration)
			if err := configuration.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestValidateTenantStructureRejectsInvalidApplicationShape(t *testing.T) {
	t.Parallel()
	base := TenantConfig{
		TenantID: "support", AppCode: "assistant", Status: AgentActive, ConfigVersion: 1,
		Model:    ModelConfig{ProviderID: "primary", Name: "support-model"},
		Channels: []ChannelBinding{{Type: ChannelTelegram, BindingID: "support-main", CredentialRef: "env:SUPPORT_BOT"}},
	}
	if err := ValidateTenantStructure(base); err != nil {
		t.Fatalf("ValidateTenantStructure(valid) error = %v", err)
	}
	if base.AppName() != "support/assistant" {
		t.Fatalf("AppName() = %q", base.AppName())
	}
	if got := (ChannelBinding{}).EffectiveAccessPolicy(); got != ChannelAccessPublic {
		t.Fatalf("EffectiveAccessPolicy(default) = %q", got)
	}
	if got := (ChannelBinding{AccessPolicy: " allowlist "}).EffectiveAccessPolicy(); got != ChannelAccessAllowlist {
		t.Fatalf("EffectiveAccessPolicy(configured) = %q", got)
	}

	tests := []struct {
		name string
		edit func(*TenantConfig)
		want string
	}{
		{"missing tenant", func(c *TenantConfig) { c.TenantID = "" }, "tenant_id"},
		{"tenant separator", func(c *TenantConfig) { c.TenantID = "support/bad" }, "reserved separator"},
		{"missing app", func(c *TenantConfig) { c.AppCode = "" }, "app_code"},
		{"app separator", func(c *TenantConfig) { c.AppCode = `support\\bad` }, "reserved separator"},
		{"invalid status", func(c *TenantConfig) { c.Status = "paused" }, "status must be"},
		{"zero version", func(c *TenantConfig) { c.ConfigVersion = 0 }, "config_version"},
		{"partial model provider", func(c *TenantConfig) { c.Model.Name = "" }, "provider_id and name together"},
		{"partial model name", func(c *TenantConfig) { c.Model.ProviderID = "" }, "provider_id and name together"},
		{"partial failover", func(c *TenantConfig) { c.Model.FailoverCandidates = []ModelCandidate{{ProviderID: "backup"}} }, "failover_candidates"},
		{"unsupported channel", func(c *TenantConfig) { c.Channels[0].Type = "unknown" }, "unsupported channel"},
		{"empty binding", func(c *TenantConfig) { c.Channels[0].BindingID = "" }, "binding_id"},
		{"binding separator", func(c *TenantConfig) { c.Channels[0].BindingID = "bad/name" }, "binding_id"},
		{"trusted id on telegram", func(c *TenantConfig) { c.Channels[0].TrustedEnterpriseID = "trusted-boundary" }, "only valid for wecom"},
		{"credential reference", func(c *TenantConfig) { c.Channels[0].CredentialRef = "plain-secret" }, "credential_ref"},
		{"access policy", func(c *TenantConfig) { c.Channels[0].AccessPolicy = "private" }, "access_policy"},
		{"allowlist without policy", func(c *TenantConfig) { c.Channels[0].Allowlist = []string{"customer-1"} }, "allowlist requires"},
		{"empty allowlist item", func(c *TenantConfig) {
			c.Channels[0].AccessPolicy = ChannelAccessAllowlist
			c.Channels[0].Allowlist = []string{" "}
		}, "allowlist[0]"},
		{"duplicate allowlist item", func(c *TenantConfig) {
			c.Channels[0].AccessPolicy = ChannelAccessAllowlist
			c.Channels[0].Allowlist = []string{"customer-1", " customer-1 "}
		}, "duplicate identity"},
		{"duplicate binding", func(c *TenantConfig) { c.Channels = append(c.Channels, c.Channels[0]) }, "duplicate channel binding"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			configuration := base
			configuration.Channels = append([]ChannelBinding(nil), base.Channels...)
			tt.edit(&configuration)
			if err := ValidateTenantStructure(configuration); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateTenantStructure() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadRejectsMultipleJSONDocumentsAndNonStringDuration(t *testing.T) {
	t.Parallel()
	valid := `{"service":{"listen_address":":8080","request_timeout":"15s","max_inbound_bytes":1024}}`
	if _, err := Load(strings.NewReader(valid + ` {}`)); err == nil || !strings.Contains(err.Error(), "multiple JSON documents") {
		t.Fatalf("Load(multiple documents) error = %v", err)
	}
	if _, err := Load(strings.NewReader(`{"service":{"listen_address":":8080","request_timeout":15,"max_inbound_bytes":1024}}`)); err == nil || !strings.Contains(err.Error(), "duration string") {
		t.Fatalf("Load(non-string duration) error = %v", err)
	}
}
