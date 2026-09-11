package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestPublicationPreservesResourceNamesAndRemoteToolIdentity(t *testing.T) {
	var spec domain.Spec
	if err := json.Unmarshal(readFixture(t, fixturePath(t, "valid", "model-tool.json")), &spec); err != nil {
		t.Fatal(err)
	}
	tool, ok := spec.Tools["search"]
	if !ok || tool.ToolName != "search_web" {
		t.Fatalf("fixture must separate logical tools.search from remote search_web: %#v", spec.Tools)
	}
	// Names are scoped to their resource category. Extra resources with the same
	// capability are valid; Profile publication does not select resources for an Agent.
	spec.Models["search"] = spec.Models["primary"]
	spec.Tools["backup"] = tool
	before := publishBoundarySpec(t, spec)
	var stored domain.Spec
	if err := json.Unmarshal(before.Document, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Models) != 2 || len(stored.Tools) != 2 ||
		stored.Models["search"].Kind != domain.ModelKindOpenAICompatible ||
		stored.Tools["search"].ToolName != "search_web" ||
		stored.Tools["backup"].Capability != domain.CapabilityWebSearch {
		t.Fatalf("publication changed resource identities or selected by capability: %#v", stored)
	}

	tool.ToolName = "search_web_v2"
	spec.Tools["search"] = tool
	after := publishBoundarySpec(t, spec)
	if before.Digest == after.Digest {
		t.Fatal("changing a remote tool name must change the Profile digest")
	}
	if err := json.Unmarshal(after.Document, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Tools) != 2 || stored.Tools["search"].ToolName != "search_web_v2" {
		t.Fatalf("remote tool change must preserve the logical resource name: %#v", stored.Tools)
	}
}

func TestPublicationAcceptsSeparateCredentialIDsWithoutEnvironment(t *testing.T) {
	digests := make(map[string]bool)
	for _, ref := range []string{"crd_0123456789abcdef0123456789abcdef", "crd_abcdef0123456789abcdef0123456789"} {
		t.Run(ref, func(t *testing.T) {
			canonical, report := domain.ValidateForPublication(
				modelDocument("https://model.example.test/v1", ref, []string{"chat"}), 1,
			)
			if !report.Valid {
				t.Fatalf("static publication should accept the reference without resolving it: %#v", report)
			}
			var stored domain.Spec
			if err := json.Unmarshal(canonical.Document, &stored); err != nil {
				t.Fatal(err)
			}
			if stored.Models["primary"].APIKeyCredentialID != ref {
				t.Fatalf("reference name changed: %#v", stored.Models["primary"])
			}
			if digests[canonical.Digest] {
				t.Fatal("different reference names must contribute to different Spec digests")
			}
			digests[canonical.Digest] = true
		})
	}
}

func TestPublicationKeepsDeploymentFieldsAndFutureToolProtocolsOutsideV1(t *testing.T) {
	schema := compilePublicSchema(t)
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"environment parameter", func(root map[string]any) { root["environment_id"] = "default" }},
		{"resource mapping table", func(root map[string]any) { root["bindings"] = map[string]any{} }},
		{"agent version parameter", func(root map[string]any) { root["agent_version_id"] = "version_1" }},
		{"unregistered tool kind", func(root map[string]any) {
			root["tools"].(map[string]any)["search"].(map[string]any)["kind"] = "future_tool"
		}},
		{"malformed MCP capability", func(root map[string]any) {
			root["tools"].(map[string]any)["search"].(map[string]any)["capability"] = "Custom Operation"
		}},
		{"arbitrary SDK options", func(root map[string]any) {
			root["tools"].(map[string]any)["search"].(map[string]any)["sdk_options"] = map[string]any{}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var root map[string]any
			if err := json.Unmarshal(readFixture(t, fixturePath(t, "valid", "model-tool.json")), &root); err != nil {
				t.Fatal(err)
			}
			test.mutate(root)
			document, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			validateWithPublicSchema(t, schema, document, false)
			if _, report := domain.ValidateForPublication(document, 1); report.Valid {
				t.Fatal("publication accepted a field or protocol outside Runtime Profile V1")
			}
		})
	}
}

func publishBoundarySpec(t *testing.T, spec domain.Spec) domain.CanonicalSpec {
	t.Helper()
	document, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	canonical, report := domain.ValidateForPublication(document, 1)
	if !report.Valid {
		t.Fatalf("publication report = %#v", report)
	}
	return canonical
}

func TestPublicationAcceptsExplicitMCPCapabilityGrammar(t *testing.T) {
	schema := compilePublicSchema(t)
	for _, capability := range []string{"web.search", "calculator.add", "mcp.search", "files.read"} {
		t.Run(capability, func(t *testing.T) {
			document := toolDocument(capability)
			validateWithPublicSchema(t, schema, document, true)
			canonical, report := domain.ValidateForPublication(document, 1)
			if !report.Valid {
				t.Fatal(report)
			}
			var stored domain.Spec
			if err := json.Unmarshal(canonical.Document, &stored); err != nil {
				t.Fatal(err)
			}
			if stored.Tools["search"].Capability != capability {
				t.Fatal("capability rewritten")
			}
		})
	}
}
