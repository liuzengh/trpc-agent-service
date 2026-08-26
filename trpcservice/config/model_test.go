package config

import (
	"strings"
	"testing"
)

func TestLoadModelConfigFromEnvDefaultsToMock(t *testing.T) {
	clearModelEnvironment(t)

	config, err := LoadModelConfigFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.Provider != ModelProviderMock {
		t.Fatalf("provider = %q, want %q", config.Provider, ModelProviderMock)
	}
	if config.Stream {
		t.Fatal("mock model should default to non-streaming")
	}
}

func TestLoadModelConfigFromEnvOpenAI(t *testing.T) {
	clearModelEnvironment(t)
	t.Setenv("TRPC_AGENT_MODEL_PROVIDER", "openai")
	t.Setenv("TRPC_AGENT_MODEL_NAME", "example-model")
	t.Setenv("OPENAI_API_KEY", "test-secret")
	t.Setenv("OPENAI_BASE_URL", "https://example.com/v1")

	config, err := LoadModelConfigFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if config.Provider != ModelProviderOpenAI || config.Name != "example-model" {
		t.Fatalf("unexpected config: %+v", config)
	}
	if config.APIKey != "test-secret" || config.BaseURL != "https://example.com/v1" {
		t.Fatal("OpenAI credentials or base URL were not loaded")
	}
	if config.Stream {
		t.Fatal("OpenAI model should default to non-streaming for the JSON tutorial endpoint")
	}
}

func TestLoadModelConfigFromEnvRejectsInvalidOpenAIConfig(t *testing.T) {
	tests := []struct {
		name      string
		modelName string
		apiKey    string
		baseURL   string
		wantError string
	}{
		{
			name:      "missing model name",
			apiKey:    "test-secret",
			wantError: "TRPC_AGENT_MODEL_NAME",
		},
		{
			name:      "missing API key",
			modelName: "example-model",
			wantError: "OPENAI_API_KEY",
		},
		{
			name:      "invalid base URL",
			modelName: "example-model",
			apiKey:    "test-secret",
			baseURL:   "file:///tmp/model",
			wantError: "http or https",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearModelEnvironment(t)
			t.Setenv("TRPC_AGENT_MODEL_PROVIDER", "openai")
			t.Setenv("TRPC_AGENT_MODEL_NAME", test.modelName)
			t.Setenv("OPENAI_API_KEY", test.apiKey)
			t.Setenv("OPENAI_BASE_URL", test.baseURL)

			_, err := LoadModelConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestLoadModelConfigFromEnvRejectsUnknownProvider(t *testing.T) {
	clearModelEnvironment(t)
	t.Setenv("TRPC_AGENT_MODEL_PROVIDER", "unknown")

	_, err := LoadModelConfigFromEnv()
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error = %v, want unsupported provider error", err)
	}
}

func TestLoadModelConfigFromEnvParsesStreamOverride(t *testing.T) {
	clearModelEnvironment(t)
	t.Setenv("TRPC_AGENT_MODEL_STREAM", "true")

	config, err := LoadModelConfigFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !config.Stream {
		t.Fatal("stream override was not applied")
	}
}

func clearModelEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"TRPC_AGENT_MODEL_PROVIDER",
		"TRPC_AGENT_MODEL_NAME",
		"TRPC_AGENT_MODEL_STREAM",
		"OPENAI_API_KEY",
		"OPENAI_BASE_URL",
	} {
		t.Setenv(key, "")
	}
}
