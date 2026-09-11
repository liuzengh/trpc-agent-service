package domain

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func workerParallelInput(t *testing.T) CompileInput {
	t.Helper()
	in := workerSequenceInput(t)
	final := in.Agent.Spec.Nodes["second"]
	final.ToolSlots, final.KnowledgeSlots = []string{}, []string{}
	in.Agent.Spec.Nodes["root"] = agentdomain.Node{Kind: agentdomain.NodeKindSequence, Children: []string{"fanout", "final"}}
	in.Agent.Spec.Nodes["fanout"] = agentdomain.Node{Kind: agentdomain.NodeKindParallel, Children: []string{"nested", "first"}}
	in.Agent.Spec.Nodes["nested"] = agentdomain.Node{Kind: agentdomain.NodeKindParallel, Children: []string{"second"}}
	in.Agent.Spec.Nodes["final"] = final
	return in
}

func TestWorkerParallelCompilePreservesExplicitTreeAndResourceClosure(t *testing.T) {
	in := workerParallelInput(t)
	before, _ := json.Marshal(in.Agent.Spec)
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	after, _ := json.Marshal(in.Agent.Spec)
	if !bytes.Equal(before, after) {
		t.Fatal("compiler silently rewrote parallel input or added summary leaf")
	}
	content := compiled.Content
	if len(content.AgentPlan.Nodes) != len(in.Agent.Spec.Nodes) || !slices.Equal(content.AgentPlan.Nodes["fanout"].Children, []string{"nested", "first"}) || !slices.Equal(content.AgentPlan.Nodes["root"].Children, []string{"fanout", "final"}) || content.AgentPlan.Nodes["fanout"].Kind != "parallel" {
		t.Fatal("parallel kind or declared order changed")
	}
	if len(content.Resources.Models) != 3 || len(content.Resources.Tools) != 2 || len(content.Resources.Knowledge) != 2 || len(content.Resources.Storage) != 1 || len(compiled.CredentialUses) != 10 {
		t.Fatal("whole-tree resource closure changed")
	}
	if content.Runtime.Summary.ModelResource != "summarizer" || content.ResolvedRequirements.Models["summarizer"] != "summarizer" {
		t.Fatal("Summary model missing from global closure")
	}
	for id, expected := range map[string][]string{"first": {"knowledge/docs", "tools/search"}, "second": {"knowledge/reference", "tools/lookup"}, "final": {}} {
		if !slices.Equal(content.AgentPlan.Nodes[id].CallableEntries, expected) {
			t.Fatalf("%s has sibling authority: %v", id, content.AgentPlan.Nodes[id].CallableEntries)
		}
	}
	if _, exists := content.Resources.Tools["unselected"]; exists || slices.Contains(content.Execution.AllowedEndpointHosts, "unselected.example.test") {
		t.Fatal("unselected Profile authority leaked")
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
		t.Fatal(err)
	}
	wire, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent)
	if err != nil || deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest) != nil {
		t.Fatal("immutable Worker read rejected actual compiled parallel", err)
	}
	p := in.Agent.Spec.Nodes["fanout"]
	p.Children = []string{"first", "nested"}
	in.Agent.Spec.Nodes["fanout"] = p
	reordered, nextReport := Compile(in)
	if !nextReport.Valid || reordered.ContentDigest == compiled.ContentDigest || bytes.Equal(reordered.CanonicalContent, compiled.CanonicalContent) {
		t.Fatal("declared parallel order lost from immutable identity", nextReport)
	}
}

func TestWorkerParallelCompileExplainsTerminalSuccessor(t *testing.T) {
	for name, change := range map[string]func(*CompileInput){
		"root": func(in *CompileInput) {
			in.Agent.Spec.Root = "fanout"
			delete(in.Agent.Spec.Nodes, "root")
			delete(in.Agent.Spec.Nodes, "final")
		},
		"sequence tail": func(in *CompileInput) {
			in.Agent.Spec.Nodes["root"] = agentdomain.Node{Kind: agentdomain.NodeKindSequence, Children: []string{"final", "fanout"}}
		},
		"nested sequence tail": func(in *CompileInput) {
			in.Agent.Spec.Nodes["root"] = agentdomain.Node{Kind: agentdomain.NodeKindSequence, Children: []string{"final", "tail"}}
			in.Agent.Spec.Nodes["tail"] = agentdomain.Node{Kind: agentdomain.NodeKindSequence, Children: []string{"fanout"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			in := workerParallelInput(t)
			change(&in)
			before, _ := json.Marshal(in.Agent.Spec)
			compiled, report := Compile(in)
			if report.Valid || len(compiled.CanonicalContent) != 0 {
				t.Fatal("terminal parallel published or silently repaired")
			}
			found := false
			for _, d := range report.Diagnostics {
				if d.Code == DiagnosticEntrypointUnsupported && d.Severity == SeverityError && d.Source == DiagnosticSourcePlatform && d.Path == "/agent_plan" && strings.Contains(d.Message, `terminal parallel node "fanout" requires an explicit successor llm in sequence`) {
					found = true
				}
			}
			if !found {
				t.Fatal("actual public validation report lacks actionable reason", report.Diagnostics)
			}
			after, _ := json.Marshal(in.Agent.Spec)
			if !bytes.Equal(before, after) {
				t.Fatal("invalid input was modified")
			}
		})
	}
}

func TestWorkerParallelCompileChecksEveryBranch(t *testing.T) {
	for name, change := range map[string]func(*CompileInput){
		"branch tool capability": func(in *CompileInput) {
			p := in.Profile.Spec.Tools["lookup"]
			p.Capability = "wrong"
			in.Profile.Spec.Tools["lookup"] = p
		},
		"branch tool_call": func(in *CompileInput) {
			p := in.Profile.Spec.Models["writer"]
			p.Capabilities = []string{"chat"}
			in.Profile.Spec.Models["writer"] = p
		},
		"branch knowledge absent": func(in *CompileInput) { delete(in.ManagedBackends, "knowledge/reference") },
		"branch multiple knowledge": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["second"]
			n.KnowledgeSlots = []string{"reference", "docs"}
			in.Agent.Spec.Nodes["second"] = n
		},
		"branch generation": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["second"]
			max := int64(4097)
			n.Generation = &agentdomain.Generation{MaxOutputTokens: &max}
			in.Agent.Spec.Nodes["second"] = n
		},
		"shared child": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["nested"]
			n.Children = []string{"second", "first"}
			in.Agent.Spec.Nodes["nested"] = n
		},
		"loop without explicit bound": func(in *CompileInput) {
			in.Agent.Spec.Nodes["nested"] = agentdomain.Node{Kind: agentdomain.NodeKindLoop, Body: "second", MaxIterations: 0}
		},
		"Summary missing": func(in *CompileInput) { delete(in.Profile.Spec.Models, "summarizer") },
	} {
		t.Run(name, func(t *testing.T) {
			in := workerParallelInput(t)
			change(&in)
			compiled, report := Compile(in)
			if report.Valid || len(compiled.CanonicalContent) != 0 || len(report.Diagnostics) == 0 {
				t.Fatal("invalid parallel branch published", report)
			}
		})
	}
}

func TestWorkerParallelReleasePinRejectsSequenceRelease(t *testing.T) {
	if deploymentv1.WorkerV1PlanContract != "llm-sequence-parallel-loop-tree-v1" {
		t.Fatal("wrong pinned execution tree contract")
	}
	// Even a newly published single LLM has the new release identity. Neither
	// consumer pin matching nor publication dynamically falls back to Sequence.
	for name, in := range map[string]CompileInput{"single llm": workerSummaryInput(), "parallel": workerParallelInput(t)} {
		t.Run(name, func(t *testing.T) {
			compiled, report := Compile(in)
			if !report.Valid {
				t.Fatal(report.Diagnostics)
			}
			wire, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent)
			if err != nil || deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest) != nil {
				t.Fatal("new release publication rejected", err)
			}
			previous := "sha256:371030c61a5e4a4557ff67d0e14eb43eb53e0c08d3ccca90979c2b84e8722d69"
			if wire.PlatformContract.Digest == previous || deploymentv1.ValidateWorkerV1(wire, previous) == nil {
				t.Fatal("previous Sequence pin accepted new release")
			}
			wire.PlatformContract.Digest = previous
			if deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest) == nil {
				t.Fatal("new release accepted stale publication pin")
			}
		})
	}
}
