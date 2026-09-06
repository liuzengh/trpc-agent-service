package config

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestHTTPAPIConfigFailsClosed(t *testing.T) {
	t.Setenv("TRPC_AGENT_HTTP_API_ENABLED", "")
	t.Setenv("TRPC_AGENT_HTTP_API_TOKEN", "")
	t.Setenv("TRPC_AGENT_HTTP_API_PRINCIPALS_JSON", "")
	cfg, err := LoadHTTPAPIConfigFromEnv()
	if err != nil || cfg.Enabled {
		t.Fatalf("default = %v, %v", cfg.Enabled, err)
	}
	t.Setenv("TRPC_AGENT_HTTP_API_ENABLED", "true")
	if _, err := LoadHTTPAPIConfigFromEnv(); err == nil {
		t.Fatal("enabled without principals")
	}
	valid := `[{"name":"client","token":"test-secret-with-more-than-32-characters","tenant_id":"tenant-a","binding_keys":["test-http"],"user_ids":["alice"]}]`
	for _, raw := range []string{valid, strings.Replace(valid, "test-http", "*", 1), strings.Replace(valid, "tenant_id", "typo", 1), valid + " {}", "[\"private-canary\"]"} {
		t.Setenv("TRPC_AGENT_HTTP_API_PRINCIPALS_JSON", raw)
		_, err := LoadHTTPAPIConfigFromEnv()
		if (raw == valid) != (err == nil) {
			t.Fatalf("valid=%t err=%v", raw == valid, err)
		}
		if err != nil && (strings.Contains(err.Error(), "private-canary") || strings.Contains(err.Error(), "test-secret")) {
			t.Fatal("config leaked")
		}
	}
	cfg = HTTPAPIConfig{Enabled: true, Principals: []HTTPAPIPrincipal{{
		Name: "client", Token: strings.Repeat("t", 32), TenantID: "tenant-a",
		BindingKeys: []string{"key"}, UserIDs: []string{"user"},
	}}}
	cfg.Principals = append(cfg.Principals, cfg.Principals[0])
	if cfg.Validate() == nil {
		t.Fatal("duplicate credential accepted")
	}
	t.Setenv("TRPC_AGENT_HTTP_API_PRINCIPALS_JSON", "")
	t.Setenv("TRPC_AGENT_HTTP_API_TOKEN", strings.Repeat("x", 48))
	cfg, err = LoadHTTPAPIConfigFromEnv()
	if err != nil || len(cfg.Principals) != 1 || cfg.Principals[0].UserIDs[0] != "alice" || cfg.Principals[0].TenantID != "tutorial-tenant" {
		t.Fatalf("tutorial shorthand: %v", err)
	}
	t.Setenv("TRPC_AGENT_HTTP_API_PRINCIPALS_JSON", valid)
	if _, err := LoadHTTPAPIConfigFromEnv(); err == nil {
		t.Fatal("ambiguous auth config")
	}
}

func TestSecretGrantConfigAndRoleFiltering(t *testing.T) {
	t.Setenv("TRPC_AGENT_SECRET_GRANTS_JSON", `[{"tenant_id":"tenant-a","purpose":"model","reference":"env://MODEL_KEY"}]`)
	grants, err := LoadSecretGrantsFromEnv()
	if err != nil || len(grants) != 1 {
		t.Fatalf("grants: %v", err)
	}
	for _, purpose := range []string{secret.TelegramWebhook, secret.TelegramBot, secret.WeComCallback, secret.WeComAES, secret.WeComApp, secret.Session, secret.Memory, secret.Knowledge, secret.Artifact, secret.Embedding} {
		grants = append(grants, secret.Grant{TenantID: "tenant-a", Purpose: purpose, Reference: "env://SOME_KEY"})
	}
	for _, tc := range []struct {
		role  string
		count int
	}{
		{RoleAll, 11}, {RoleGateway, 3}, {RoleRelay, 0}, {RoleSender, 2}, {RoleWorker, 6}, {RoleJobs, 6}, {RoleAdmin, 5},
	} {
		roles, _ := ParseRole(tc.role)
		if got := len(roles.SecretGrants(grants)); got != tc.count {
			t.Fatalf("%s grants=%d", tc.role, got)
		}
	}
	t.Setenv("TRPC_AGENT_SECRET_GRANTS_JSON", `[{"purpose":"private-canary","unexpected":true}]`)
	if _, err := LoadSecretGrantsFromEnv(); err == nil || strings.Contains(err.Error(), "private-canary") {
		t.Fatalf("invalid: %v", err)
	}
}
