package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
)

func TestLoadAndDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	yaml := `
http_addr: 127.0.0.1:8080
model_defaults:
  provider: deepseek
  model: deepseek-v4-flash
  timeout: 45s
tenants:
  - id: tenant
    name: Tenant
    enabled: true
    agent: {id: assistant, version: "1"}
    channels: []
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	profile, ok := cfg.TenantByID("tenant")
	if !ok || profile.Model.Model != "deepseek-v4-flash" {
		t.Fatalf("defaults not applied: %+v", profile)
	}
}

func TestValidateRejectsDuplicateTenant(t *testing.T) {
	cfg, err := Load("../../configs/demo.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Tenants = append(cfg.Tenants, cfg.Tenants[0])
	if err := cfg.Validate(); err == nil {
		t.Fatal("duplicate tenant should fail")
	}
}

func TestEnvironmentOverridesAndLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := `http_addr: 127.0.0.1:1
database: {postgres_dsn: old, redis_addr: old}
tenants:
  - id: tenant
    agent: {id: assistant, version: "1"}
    model: {provider: deepseek, timeout: 1s}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRPC_HTTP_ADDR", "127.0.0.1:2")
	t.Setenv("TRPC_POSTGRES_DSN", "postgres")
	t.Setenv("TRPC_REDIS_ADDR", "redis")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:2" || cfg.Database.PostgresDSN != "postgres" || cfg.Database.RedisAddr != "redis" {
		t.Fatalf("environment overrides=%+v", cfg)
	}
	if _, ok := cfg.TenantByID("missing"); ok {
		t.Fatal("missing tenant should not resolve")
	}
}

func TestLoadAndValidationErrors(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing config should fail")
	}
	badYAML := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(badYAML, []byte("tenants: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(badYAML); err == nil {
		t.Fatal("invalid YAML should fail")
	}

	validTenant := tenant.Tenant{ID: "tenant", Agent: tenant.AgentProfile{ID: "assistant", Version: "1"}}
	cases := []Config{
		{Tenants: []tenant.Tenant{validTenant}},
		{HTTPAddr: "127.0.0.1:1"},
		{
			HTTPAddr: "127.0.0.1:1",
			Tenants: []tenant.Tenant{{
				ID: "tenant", Agent: tenant.AgentProfile{ID: "assistant", Version: "1"},
				Model: tenant.ModelProfile{Provider: "deepseek", Timeout: "invalid"},
			}},
		},
		{HTTPAddr: "127.0.0.1:1", Tenants: []tenant.Tenant{{}}},
	}
	for i, cfg := range cases {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("case %d should fail", i)
		}
	}
}
