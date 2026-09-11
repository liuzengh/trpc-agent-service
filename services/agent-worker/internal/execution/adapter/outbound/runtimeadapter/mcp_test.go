package runtimeadapter

import (
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"testing"
)

func TestMCPRequiredUsesExactSelection(t *testing.T) {
	_, p := runtimeFixture()
	p.MaxToolCalls = 4
	tool := domain.ToolPlan{Resource: "search", ServerURL: "https://mcp.example.test/mcp", ToolsetName: "selected", ToolName: "lookup", AuthKind: "bearer", Capability: "calculator.add"}
	tool.Credential = domain.CredentialUse{CredentialID: "crd_mcp", Purpose: "bearer_token", AudienceDigest: protocol.CredentialAudienceDigest("mcp_streamable_http", tool.ServerURL, tool.AuthKind)}
	p.Tools = []domain.ToolPlan{tool}
	uses, err := requiredUses(p)
	if err != nil || len(uses) != 3 || uses[2] != tool.Credential {
		t.Fatalf("uses=%v err=%v", uses, err)
	}
	second := tool
	second.Resource = "second"
	second.ToolName = "other"
	p.Tools = append(p.Tools, second)
	uses, err = requiredUses(p)
	if err != nil || len(uses) != 3 {
		t.Fatalf("same complete use must deduplicate: %v %v", uses, err)
	}
	for name, change := range map[string]func(*domain.ToolPlan){
		"wrong audience":  func(x *domain.ToolPlan) { x.ServerURL = "https://other.example.test/mcp" },
		"wrong purpose":   func(x *domain.ToolPlan) { x.Credential.Purpose = "api_key" },
		"none credential": func(x *domain.ToolPlan) { x.AuthKind = "none" },
		"missing bearer":  func(x *domain.ToolPlan) { x.Credential = domain.CredentialUse{} },
		"name":            func(x *domain.ToolPlan) { x.ToolName = "BAD NAME" },
		"resource":        func(x *domain.ToolPlan) { x.Resource = "a/b" },
		"capability":      func(x *domain.ToolPlan) { x.Capability = "Bad Capability" },
		"query":           func(x *domain.ToolPlan) { x.ServerURL += "?token=hidden" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := p
			x := tool
			change(&x)
			bad.Tools = []domain.ToolPlan{x}
			if _, err := requiredUses(bad); err == nil {
				t.Fatal("invalid plan accepted")
			}
		})
	}
	tool.AuthKind = "none"
	tool.Credential = domain.CredentialUse{}
	p.Tools = []domain.ToolPlan{tool}
	uses, err = requiredUses(p)
	if err != nil || len(uses) != 2 {
		t.Fatalf("none auth must not resolve credential: %v %v", uses, err)
	}
}
