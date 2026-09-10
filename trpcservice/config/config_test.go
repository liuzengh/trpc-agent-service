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
