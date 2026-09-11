package domain

import (
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

func TestWorkerMCPCompilationSelectedClosure(t *testing.T) {
	for _, capability := range []string{"web.search", "calculator.add", "mcp.search"} {
		t.Run(capability, func(t *testing.T) {
			in := workerSummaryInput()
			n := in.Agent.Spec.Nodes["assistant"]
			n.ToolSlots = []string{"search"}
			in.Agent.Spec.Nodes["assistant"] = n
			in.Agent.Spec.Requirements.Tools["search"] = agentdomain.CapabilityRequirement{Capability: capability}
			in.Profile.Spec.Tools["search"] = profiledomain.ToolResource{Kind: profiledomain.ToolKindMCPStreamableHTTP, ServerURL: "https://tools.example.test/mcp", ToolsetName: "selected", ToolName: "lookup", Auth: profiledomain.ToolAuth{Kind: profiledomain.AuthKindNone}, Capability: capability}
			in.Profile.Spec.Tools["unselected"] = profiledomain.ToolResource{Kind: profiledomain.ToolKindMCPStreamableHTTP, ServerURL: "https://unused.example.test/mcp", ToolsetName: "other", ToolName: "secret", Auth: profiledomain.ToolAuth{Kind: profiledomain.AuthKindNone}, Capability: capability}
			compiled, report := Compile(in)
			if !report.Valid {
				t.Fatal(report.Diagnostics)
			}
			if len(compiled.Content.Resources.Tools) != 1 || compiled.Content.Resources.Tools["search"].Capability != capability {
				t.Fatal("selection closure changed")
			}
			node := compiled.Content.AgentPlan.Nodes["assistant"]
			if len(node.CallableEntries) != 1 || node.CallableEntries[0] != "tools/search" {
				t.Fatal(node.CallableEntries)
			}
			if len(compiled.CredentialUses) != 3 {
				t.Fatal("none auth added credentials", compiled.CredentialUses)
			}
			if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
				t.Fatal(err)
			}
			wire, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent)
			if err != nil {
				t.Fatal(err)
			}
			if err = deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest); err != nil {
				t.Fatal(err)
			}
			in.Agent.Spec.Requirements.Tools["search"] = agentdomain.CapabilityRequirement{Capability: "different.operation"}
			_, report = Compile(in)
			if report.Valid {
				t.Fatal("capability mismatch accepted")
			}
			assertDiagnostic(t, report, DiagnosticCapabilityMismatch, DiagnosticSourceAgent, "/requirements/tools/search", "search", "")
		})
	}
}
