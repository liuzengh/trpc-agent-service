package config

import (
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"strings"
	"testing"
)

func TestWeComMCPReceiverDefaultsOffAndScopesTargets(t *testing.T) {
	t.Setenv("WECOM_MCP_URL", "credential-canary")
	t.Setenv("TRPC_AGENT_WECOM_MCP_TARGETS_JSON", "")
	if targets, err := LoadWeComMCPTargetsFromEnv(); err != nil || len(targets) != 0 {
		t.Fatal("credential alone enabled receiver")
	}
	valid := `[{"tenant_id":"tenant-a","binding_id":"binding-a"}]`
	for _, raw := range []string{valid, `[{"tenant_id":"*","binding_id":"binding-a"}]`, valid + "{}", `[{"tenant_id":"tenant-a","binding_id":"binding-a","credential":"credential-canary"}]`, `[{"tenant_id":"tenant-a","binding_id":"binding-a"},{"tenant_id":"tenant-a","binding_id":"binding-a"}]`} {
		t.Setenv("TRPC_AGENT_WECOM_MCP_TARGETS_JSON", raw)
		_, err := LoadWeComMCPTargetsFromEnv()
		if (raw == valid) != (err == nil) || (err != nil && strings.Contains(err.Error(), "credential-canary")) {
			t.Fatal("unsafe receiver config accepted or leaked")
		}
	}
}
func TestWeComMCPPurposesAreRoleSeparated(t *testing.T) {
	grants := []secret.Grant{{TenantID: "tenant", Purpose: secret.WeComMCPRead, Reference: "env://MCP"}, {TenantID: "tenant", Purpose: secret.WeComMCPSend, Reference: "env://MCP"}}
	for _, tc := range []struct {
		role, purpose string
		count         int
	}{{RoleGateway, secret.WeComMCPRead, 1}, {RoleSender, secret.WeComMCPSend, 1}, {RoleWorker, "", 0}, {RoleAdmin, "", 0}, {RoleRelay, "", 0}, {RoleAll, "", 2}} {
		roles, _ := ParseRole(tc.role)
		got := roles.SecretGrants(grants)
		if len(got) != tc.count || (tc.count == 1 && got[0].Purpose != tc.purpose) {
			t.Fatal("MCP grant crossed process roles")
		}
	}
}
