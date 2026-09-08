package config

import (
	"strings"
	"testing"
)

func embeddingConfigFixture(t *testing.T) {
	t.Helper()
	t.Setenv("TRPC_AGENT_EMBEDDING_MODEL", "embedding-test")
	t.Setenv("TRPC_AGENT_EMBEDDING_BASE_URL", "http://127.0.0.1:9876/v1")
	t.Setenv("TRPC_AGENT_EMBEDDING_API_KEY", "embedding-private-canary")
	t.Setenv("TRPC_AGENT_EMBEDDING_DIMENSIONS", "3")
	// A chat model must never silently supply the missing embedding settings.
	t.Setenv("OPENAI_API_KEY", "chat-private-canary")
	t.Setenv("OPENAI_BASE_URL", "https://chat.invalid/v1")
	t.Setenv("TRPC_AGENT_MODEL_NAME", "chat-only")
}

func TestEmbeddingCheckConfigExplicitAndSeparate(t *testing.T) {
	embeddingConfigFixture(t)
	c, err := LoadEmbeddingCheckConfigFromEnv()
	if err != nil || c.Dimensions != 3 || c.Model != "embedding-test" || c.APIKey != "embedding-private-canary" {
		t.Fatal("explicit config not loaded")
	}
	for _, key := range []string{"TRPC_AGENT_EMBEDDING_MODEL", "TRPC_AGENT_EMBEDDING_BASE_URL", "TRPC_AGENT_EMBEDDING_API_KEY", "TRPC_AGENT_EMBEDDING_DIMENSIONS"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "")
			_, err := LoadEmbeddingCheckConfigFromEnv()
			if err == nil {
				t.Fatal("missing embedding setting inherited chat configuration")
			}
		})
	}
}

func TestEmbeddingCheckConfigRejectsUnsafeValuesWithoutEcho(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"TRPC_AGENT_EMBEDDING_BASE_URL", "https://user:private-canary@host/v1"},
		{"TRPC_AGENT_EMBEDDING_BASE_URL", "https://host/v1?key=private-canary"},
		{"TRPC_AGENT_EMBEDDING_BASE_URL", "https://host/v1#private-canary"},
		{"TRPC_AGENT_EMBEDDING_BASE_URL", "file:///private-canary"},
		{"TRPC_AGENT_EMBEDDING_BASE_URL", "https://host:99999/v1"},
		{"TRPC_AGENT_EMBEDDING_BASE_URL", "https://host/v1?"},
		{"TRPC_AGENT_EMBEDDING_MODEL", "private-canary\nforged-log"},
		{"TRPC_AGENT_EMBEDDING_API_KEY", "private-canary\nforged-header"},
		{"TRPC_AGENT_EMBEDDING_DIMENSIONS", "private-canary"},
		{"TRPC_AGENT_EMBEDDING_DIMENSIONS", "0"},
		{"TRPC_AGENT_EMBEDDING_DIMENSIONS", "65537"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			embeddingConfigFixture(t)
			t.Setenv(tc.key, tc.value)
			_, err := LoadEmbeddingCheckConfigFromEnv()
			if err == nil || strings.Contains(err.Error(), "private-canary") {
				t.Fatal("invalid configuration accepted or value exposed")
			}
		})
	}
}
