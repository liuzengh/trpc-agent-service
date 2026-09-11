package config

import (
	"encoding/json"
	"testing"
)

func TestDocsMCPConfigIsLoopbackAndOptIn(t *testing.T) {
	t.Setenv("TRPC_AGENT_DOCS_MCP_ENABLED", "false")
	if cfg, err := LoadDocsMCPConfigFromEnv(Roles{Worker: true}); err != nil || cfg.Enabled {
		t.Fatal("default should be off", err)
	}
	t.Setenv("TRPC_AGENT_DOCS_MCP_ENABLED", "true")
	t.Setenv("TRPC_AGENT_DOCS_MCP_ADDR", "127.0.0.1:18090")
	raw, _ := json.Marshal(map[string]string{"url": "http://127.0.0.1:18090/mcp", "bearer_token": "a-strong-test-token-0123456789abcdef"})
	t.Setenv("MCP_DOCS_SERVER", string(raw))
	if cfg, err := LoadDocsMCPConfigFromEnv(Roles{Worker: true}); err != nil || !cfg.Enabled {
		t.Fatal("valid local config rejected", err)
	}
	for _, addr := range []string{":18090", "0.0.0.0:18090", "localhost:18090", "127.0.0.1:0", "127.0.0.1:99999"} {
		t.Setenv("TRPC_AGENT_DOCS_MCP_ADDR", addr)
		if _, err := LoadDocsMCPConfigFromEnv(Roles{Worker: true}); err == nil {
			t.Fatal("unsafe address accepted")
		}
	}
	t.Setenv("MCP_DOCS_SERVER", "not valid JSON")
	if cfg, err := LoadDocsMCPConfigFromEnv(Roles{Admin: true}); err != nil || cfg.Enabled {
		t.Fatal("Admin must not read Worker server credential", err)
	}
}
