package config

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestValidateTenantToolIntegrations(t *testing.T) {
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	app := &file.Tenants[0].Apps[0]
	app.Tools.Allow = append(app.Tools.Allow, "mcp__crm__lookup_customer", "create_ticket")
	app.MCPServers = []tenant.MCPServer{{
		ID: "crm", Endpoint: "https://mcp.example.com/mcp", Enabled: true, Idempotent: true,
		Credential:   tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "CRM_MCP_TOKEN"},
		AllowedTools: []string{"lookup_customer"},
	}}
	app.BusinessTools = []tenant.HTTPBusinessTool{{
		Name: "create_ticket", Description: "Create a support ticket.", Endpoint: "https://tools.example.com/tickets", Enabled: true,
		Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "TICKET_API_TOKEN"},
	}}
	if err := file.Validate(); err != nil {
		t.Fatalf("valid integrations rejected: %v", err)
	}
}

func TestValidateRejectsUnsafeTenantToolIntegrations(t *testing.T) {
	base := func(t *testing.T) *File {
		t.Helper()
		file, err := Load(strings.NewReader(validYAML))
		if err != nil {
			t.Fatal(err)
		}
		return file
	}
	tests := []struct {
		name string
		edit func(*tenant.AgentApp)
		want string
	}{
		{"insecure MCP endpoint", func(app *tenant.AgentApp) {
			app.Tools.Allow = append(app.Tools.Allow, "mcp__crm__lookup")
			app.MCPServers = []tenant.MCPServer{{ID: "crm", Endpoint: "http://mcp.example.com", Enabled: true, Idempotent: true, AllowedTools: []string{"lookup"}}}
		}, "HTTPS URL"},
		{"MCP tool absent from allowlist", func(app *tenant.AgentApp) {
			app.MCPServers = []tenant.MCPServer{{ID: "crm", Endpoint: "https://mcp.example.com", Enabled: true, Idempotent: true, AllowedTools: []string{"lookup"}}}
		}, `must include "mcp__crm__lookup"`},
		{"invalid MCP auth header", func(app *tenant.AgentApp) {
			app.Tools.Allow = append(app.Tools.Allow, "mcp__crm__lookup")
			app.MCPServers = []tenant.MCPServer{{ID: "crm", Endpoint: "https://mcp.example.com", Enabled: true, Idempotent: true, AllowedTools: []string{"lookup"}, CredentialHeader: "Cookie", Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "MCP_TOKEN"}}}
		}, "credential_header"},
		{"non-idempotent MCP server", func(app *tenant.AgentApp) {
			app.Tools.Allow = append(app.Tools.Allow, "mcp__crm__lookup")
			app.MCPServers = []tenant.MCPServer{{ID: "crm", Endpoint: "https://mcp.example.com", Enabled: true, AllowedTools: []string{"lookup"}}}
		}, "idempotent must be true"},
		{"business tool has no secret reference", func(app *tenant.AgentApp) {
			app.Tools.Allow = append(app.Tools.Allow, "tickets")
			app.BusinessTools = []tenant.HTTPBusinessTool{{Name: "tickets", Description: "Lookup tickets", Endpoint: "https://tools.example.com", Enabled: true}}
		}, "credential is required"},
		{"business tool conflicts with builtin", func(app *tenant.AgentApp) {
			app.BusinessTools = []tenant.HTTPBusinessTool{{Name: "calculator", Enabled: false}}
		}, "duplicate exposed tool"},
		{"disabled MCP remains allowed", func(app *tenant.AgentApp) {
			app.Tools.Allow = append(app.Tools.Allow, "mcp__crm__lookup")
			app.MCPServers = []tenant.MCPServer{{ID: "crm", AllowedTools: []string{"lookup"}, Enabled: false}}
		}, "must not include disabled tool"},
		{"disabled business tool remains allowed", func(app *tenant.AgentApp) {
			app.Tools.Allow = append(app.Tools.Allow, "tickets")
			app.BusinessTools = []tenant.HTTPBusinessTool{{Name: "tickets", Enabled: false}}
		}, "must not include disabled tool"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file := base(t)
			test.edit(&file.Tenants[0].Apps[0])
			err := file.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}
