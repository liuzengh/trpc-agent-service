package config

import "testing"

func TestLoadAdminConfig(t *testing.T) {
	t.Setenv("TRPC_AGENT_ADMIN_ENABLED", "true")
	t.Setenv("TRPC_AGENT_ADMIN_TOKEN", "123456789012345678901234")
	config, err := LoadAdminConfigFromEnv()
	if err != nil || !config.Enabled || config.Token == "" {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}

func TestLoadAdminConfigRejectsShortToken(t *testing.T) {
	t.Setenv("TRPC_AGENT_ADMIN_ENABLED", "true")
	t.Setenv("TRPC_AGENT_ADMIN_TOKEN", "short")
	if _, err := LoadAdminConfigFromEnv(); err == nil {
		t.Fatal("expected short token error")
	}
}

func TestLoadAdminConfigPrincipals(t *testing.T) {
	t.Setenv("TRPC_AGENT_ADMIN_ENABLED", "true")
	t.Setenv("TRPC_AGENT_ADMIN_TOKEN", "")
	t.Setenv("TRPC_AGENT_ADMIN_PRINCIPALS_JSON", `[{
        "name":"tenant-admin",
        "token":"123456789012345678901234",
        "role":"tenant_admin",
        "tenant_ids":["tenant-a"]
    }]`)
	config, err := LoadAdminConfigFromEnv()
	if err != nil || len(config.Principals) != 1 || config.Principals[0].Role != "tenant_admin" {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}
