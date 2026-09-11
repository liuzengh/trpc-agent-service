package domain

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"

	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func workerSequenceInput(t *testing.T) CompileInput {
	t.Helper()
	in := knowledgeCredentialInput()
	first := in.Agent.Spec.Nodes["assistant"]
	second := first
	first.ToolSlots = []string{"search"}
	second.ToolSlots = []string{"lookup"}
	second.ModelSlot = "writer"
	second.KnowledgeSlots = []string{"reference"}
	second.AddSessionSummary = nil
	in.Agent.Spec.Root = "root"
	in.Agent.Spec.Nodes = map[string]agentdomain.Node{
		"root":   {Kind: agentdomain.NodeKindSequence, Children: []string{"second", "nested"}},
		"nested": {Kind: agentdomain.NodeKindSequence, Children: []string{"first"}},
		"first":  first, "second": second,
	}
	in.Agent.Spec.Requirements.Models["writer"] = agentdomain.ModelRequirement{Capabilities: []string{"chat", "tool_call"}}
	writer := in.Profile.Spec.Models["primary"]
	writer.Model = "writer-model"
	writer.BaseURL = "https://writer.example.test/v1"
	writer.APIKeyCredentialID = "crd_00000000000000000000000000000015"
	in.Profile.Spec.Models["writer"] = writer
	in.Agent.Spec.Requirements.Tools = map[string]agentdomain.CapabilityRequirement{"search": {Capability: "mcp.search"}, "lookup": {Capability: "mcp.lookup"}}
	in.Profile.Spec.Tools = map[string]profiledomain.ToolResource{
		"search":     {Kind: profiledomain.ToolKindMCPStreamableHTTP, ServerURL: "https://tools.example.test/mcp", ToolsetName: "service", ToolName: "search", Capability: "mcp.search", Auth: profiledomain.ToolAuth{Kind: profiledomain.AuthKindBearer, CredentialID: credentialSearch}},
		"lookup":     {Kind: profiledomain.ToolKindMCPStreamableHTTP, ServerURL: "https://lookup.example.test/mcp", ToolsetName: "service", ToolName: "lookup", Capability: "mcp.lookup", Auth: profiledomain.ToolAuth{Kind: profiledomain.AuthKindBearer, CredentialID: credentialDebug}},
		"unselected": {Kind: profiledomain.ToolKindMCPStreamableHTTP, ServerURL: "https://unselected.example.test/mcp", ToolsetName: "service", ToolName: "private", Capability: "mcp.private", Auth: profiledomain.ToolAuth{Kind: profiledomain.AuthKindNone}},
	}
	b := in.ManagedBackends["knowledge/docs"]
	q := *b.Qdrant
	q.Endpoint = "https://reference.example.test"
	q.Collection = "reference"
	b.Qdrant = &q
	b.BackendID = "reference"
	digest, err := b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	in.ManagedBackends["knowledge/reference"] = b
	ref := in.Profile.Spec.Knowledge["docs"]
	ref.BackendID = b.BackendID
	ref.CredentialAudienceDigest = digest
	ref.QdrantAPIKeyCredentialID = "crd_00000000000000000000000000000013"
	ref.Embedding.APIKeyCredentialID = "crd_00000000000000000000000000000014"
	ref.Embedding.BaseURL = "https://reference-embed.example.test/v1"
	in.Profile.Spec.Knowledge["reference"] = ref
	in.Agent.Spec.Requirements.Knowledge["reference"] = agentdomain.CapabilityRequirement{Capability: "knowledge.search"}
	in.Platform.Execution.AllowedEndpointHosts = append(in.Platform.Execution.AllowedEndpointHosts, "writer.example.test", "lookup.example.test", "reference.example.test", "reference-embed.example.test")
	redigestPlatform(t, &in.Platform)
	return in
}

func TestWorkerSequenceCompileOrderedMultiResourceClosure(t *testing.T) {
	in := workerSequenceInput(t)
	before, _ := json.Marshal(in.Agent.Spec)
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	after, _ := json.Marshal(in.Agent.Spec)
	if !bytes.Equal(before, after) {
		t.Fatal("compiler mutated ordered input")
	}
	content := compiled.Content
	if !slices.Equal(content.AgentPlan.Nodes["root"].Children, []string{"second", "nested"}) || !slices.Equal(content.AgentPlan.Nodes["nested"].Children, []string{"first"}) {
		t.Fatal("sequence source order changed")
	}
	if len(content.Resources.Models) != 3 || len(content.Resources.Tools) != 2 || len(content.Resources.Knowledge) != 2 || len(content.Resources.Storage) != 1 || len(compiled.CredentialUses) != 10 {
		t.Fatalf("whole-tree closure: models=%d tools=%d knowledge=%d storage=%d credentials=%d", len(content.Resources.Models), len(content.Resources.Tools), len(content.Resources.Knowledge), len(content.Resources.Storage), len(compiled.CredentialUses))
	}
	if content.Runtime.Summary.ModelResource != "summarizer" || content.ResolvedRequirements.Models["summarizer"] != "summarizer" {
		t.Fatal("summary model omitted")
	}
	if _, exists := content.Resources.Tools["unselected"]; exists {
		t.Fatal("unused Profile MCP authority leaked")
	}
	if slices.Contains(content.Execution.AllowedEndpointHosts, "unselected.example.test") {
		t.Fatal("unused endpoint leaked")
	}
	for id, expected := range map[string][]string{"first": {"knowledge/docs", "tools/search"}, "second": {"knowledge/reference", "tools/lookup"}} {
		if !slices.Equal(content.AgentPlan.Nodes[id].CallableEntries, expected) {
			t.Fatalf("%s inherited sibling tools: %v", id, content.AgentPlan.Nodes[id].CallableEntries)
		}
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
		t.Fatal("immutable read", err)
	}
	wire, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent)
	if err != nil {
		t.Fatal(err)
	}
	if err = deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest); err != nil {
		t.Fatal(err)
	}
	// Children are a sequence, not a set; same node map with reversed execution
	// order is a different immutable Manifest.
	root := in.Agent.Spec.Nodes["root"]
	root.Children = []string{"nested", "second"}
	in.Agent.Spec.Nodes["root"] = root
	reordered, nextReport := Compile(in)
	if !nextReport.Valid {
		t.Fatal(nextReport.Diagnostics)
	}
	if reordered.ContentDigest == compiled.ContentDigest || bytes.Equal(reordered.CanonicalContent, compiled.CanonicalContent) {
		t.Fatal("execution order did not affect Manifest identity")
	}
}

func TestWorkerSequenceCompileRejectsInvalidSecondLeafAndTopology(t *testing.T) {
	for name, change := range map[string]func(*CompileInput){
		"terminal parallel": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["nested"]
			n.Kind = agentdomain.NodeKindParallel
			in.Agent.Spec.Nodes["nested"] = n
		},
		"loop without explicit bound": func(in *CompileInput) {
			in.Agent.Spec.Nodes["nested"] = agentdomain.Node{Kind: agentdomain.NodeKindLoop, Body: "first", MaxIterations: 0}
		},
		"cycle": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["nested"]
			n.Children = []string{"root"}
			in.Agent.Spec.Nodes["nested"] = n
		},
		"duplicate": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["nested"]
			n.Children = []string{"first", "first"}
			in.Agent.Spec.Nodes["nested"] = n
		},
		"two parents": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["nested"]
			n.Children = []string{"first", "second"}
			in.Agent.Spec.Nodes["nested"] = n
		},
		"unreachable": func(in *CompileInput) { in.Agent.Spec.Nodes["orphan"] = in.Agent.Spec.Nodes["first"] },
		"missing child": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["nested"]
			n.Children = []string{"missing"}
			in.Agent.Spec.Nodes["nested"] = n
		},
		"empty children": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["nested"]
			n.Children = []string{}
			in.Agent.Spec.Nodes["nested"] = n
		},
		"missing second model": func(in *CompileInput) { delete(in.Profile.Spec.Models, "writer") },
		"second model lacks tool_call": func(in *CompileInput) {
			m := in.Profile.Spec.Models["writer"]
			m.Capabilities = []string{"chat"}
			in.Profile.Spec.Models["writer"] = m
		},
		"second MCP capability": func(in *CompileInput) {
			m := in.Profile.Spec.Tools["lookup"]
			m.Capability = "wrong.capability"
			in.Profile.Spec.Tools["lookup"] = m
		},
		"second knowledge unbound": func(in *CompileInput) { delete(in.ManagedBackends, "knowledge/reference") },
		"second knowledge per node": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["second"]
			n.KnowledgeSlots = []string{"reference", "docs"}
			in.Agent.Spec.Nodes["second"] = n
		},
		"second knowledge wrong tenant": func(in *CompileInput) {
			b := in.ManagedBackends["knowledge/reference"]
			b.TenantID = "other"
			in.ManagedBackends["knowledge/reference"] = b
		},
		"second generation": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["second"]
			v := int64(4097)
			n.Generation = &agentdomain.Generation{MaxOutputTokens: &v}
			in.Agent.Spec.Nodes["second"] = n
		},
		"summary model missing": func(in *CompileInput) { delete(in.Profile.Spec.Models, "summarizer") },
	} {
		t.Run(name, func(t *testing.T) {
			in := workerSequenceInput(t)
			change(&in)
			compiled, report := Compile(in)
			if report.Valid || len(compiled.CanonicalContent) != 0 || len(report.Diagnostics) == 0 {
				t.Fatal("invalid publication produced output", report)
			}
		})
	}
}

func TestWorkerSequenceSingleLeafRequiresNewReleasePin(t *testing.T) {
	in := workerSummaryInput()
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	wire, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent)
	if err != nil {
		t.Fatal(err)
	}
	if deploymentv1.WorkerV1PlanContract != "llm-sequence-parallel-loop-tree-v1" {
		t.Fatal("unexpected plan contract")
	}
	if err = deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest); err != nil {
		t.Fatal(err)
	}
	previous := "sha256:0b44e8d6f9fa6de5494713180b56445b2b4597ff9130b05b212ab0baab04156b"
	if wire.PlatformContract.Digest == previous || deploymentv1.ValidateWorkerV1(wire, previous) == nil {
		t.Fatal("new single-leaf publication accepted by old release pin")
	}
	wire.PlatformContract.Digest = previous
	if deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest) == nil {
		t.Fatal("new release dynamically accepted old pin")
	}
}
