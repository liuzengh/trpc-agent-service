package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func workerLoopInput(t *testing.T) CompileInput {
	t.Helper()
	in := workerParallelInput(t)
	in.Agent.Spec.Nodes["cycle"] = agentdomain.Node{Kind: agentdomain.NodeKindLoop, Body: in.Agent.Spec.Root, MaxIterations: 2}
	in.Agent.Spec.Root = "cycle"
	return in
}

func TestWorkerLoopCompilePreservesExplicitBodyBoundAndClosure(t *testing.T) {
	for _, max := range []int64{1, 2, 32} {
		t.Run(fmt.Sprint(max), func(t *testing.T) {
			in := workerLoopInput(t)
			n := in.Agent.Spec.Nodes["cycle"]
			n.MaxIterations = max
			in.Agent.Spec.Nodes["cycle"] = n
			before, _ := json.Marshal(in.Agent.Spec)
			compiled, report := Compile(in)
			if !report.Valid {
				t.Fatal(report.Diagnostics)
			}
			after, _ := json.Marshal(in.Agent.Spec)
			if !bytes.Equal(before, after) {
				t.Fatal("compiler silently rewrote loop or bound")
			}
			content := compiled.Content
			if got := content.AgentPlan.Nodes["cycle"]; content.AgentPlan.Root != "cycle" || got.Kind != "loop" || got.Body != "root" || got.MaxIterations != max || len(content.AgentPlan.Nodes) != 7 {
				t.Fatal("body expanded or explicit fields changed", got)
			}
			if len(content.Resources.Models) != 3 || len(content.Resources.Tools) != 2 || len(content.Resources.Knowledge) != 2 || len(content.Resources.Storage) != 1 || len(compiled.CredentialUses) != 10 {
				t.Fatal("loop multiplied or dropped whole-tree authority")
			}
			if content.Runtime.Summary.ModelResource != "summarizer" || content.ResolvedRequirements.Models["summarizer"] != "summarizer" {
				t.Fatal("Summary model omitted")
			}
			if len(content.AgentPlan.Nodes["final"].CallableEntries) != 0 || len(content.AgentPlan.Nodes["first"].KnowledgeResources) != 1 || len(content.AgentPlan.Nodes["second"].KnowledgeResources) != 1 {
				t.Fatal("per-leaf authority changed")
			}
			if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
				t.Fatal(err)
			}
			wire, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent)
			if err != nil || deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest) != nil {
				t.Fatal("actual immutable loop rejected", err)
			}
			// Iterations are a declared source field and therefore change identity.
			n.MaxIterations = 3
			in.Agent.Spec.Nodes["cycle"] = n
			changed, nextReport := Compile(in)
			if !nextReport.Valid || changed.ContentDigest == compiled.ContentDigest {
				t.Fatal("iteration bound missing from immutable identity", nextReport)
			}
		})
	}
	// The minimal GUI/runtime shape: a root loop directly owns one LLM body.
	in := workerSummaryInput()
	in.Agent.Spec.Nodes["cycle"] = agentdomain.Node{Kind: agentdomain.NodeKindLoop, Body: in.Agent.Spec.Root, MaxIterations: 2}
	in.Agent.Spec.Root = "cycle"
	if _, report := Compile(in); !report.Valid {
		t.Fatal(report.Diagnostics)
	}
}

func TestWorkerLoopCompileRejectsInvalidBoundBodyAndAuthority(t *testing.T) {
	for name, change := range map[string]func(*CompileInput){
		"zero bound": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["cycle"]
			n.MaxIterations = 0
			in.Agent.Spec.Nodes["cycle"] = n
		},
		"over bound": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["cycle"]
			n.MaxIterations = 33
			in.Agent.Spec.Nodes["cycle"] = n
		},
		"missing body": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["cycle"]
			n.Body = "missing"
			in.Agent.Spec.Nodes["cycle"] = n
		},
		"self cycle": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["cycle"]
			n.Body = "cycle"
			in.Agent.Spec.Nodes["cycle"] = n
		},
		"shared body": func(in *CompileInput) {
			in.Agent.Spec.Nodes["nested"] = agentdomain.Node{Kind: agentdomain.NodeKindLoop, Body: "first", MaxIterations: 2}
		},
		"body tool capability": func(in *CompileInput) {
			p := in.Profile.Spec.Tools["lookup"]
			p.Capability = "wrong"
			in.Profile.Spec.Tools["lookup"] = p
		},
		"body tool_call": func(in *CompileInput) {
			p := in.Profile.Spec.Models["writer"]
			p.Capabilities = []string{"chat"}
			in.Profile.Spec.Models["writer"] = p
		},
		"body knowledge absent": func(in *CompileInput) { delete(in.ManagedBackends, "knowledge/reference") },
		"body multiple knowledge": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["second"]
			n.KnowledgeSlots = []string{"reference", "docs"}
			in.Agent.Spec.Nodes["second"] = n
		},
		"body generation": func(in *CompileInput) {
			n := in.Agent.Spec.Nodes["second"]
			max := int64(4097)
			n.Generation = &agentdomain.Generation{MaxOutputTokens: &max}
			in.Agent.Spec.Nodes["second"] = n
		},
		"Summary missing": func(in *CompileInput) { delete(in.Profile.Spec.Models, "summarizer") },
	} {
		t.Run(name, func(t *testing.T) {
			in := workerLoopInput(t)
			change(&in)
			compiled, report := Compile(in)
			if report.Valid || len(compiled.CanonicalContent) != 0 || len(report.Diagnostics) == 0 {
				t.Fatal("invalid loop published", report)
			}
		})
	}
}

// Control compiles already-published typed Agent snapshots. Test inactive union
// fields at the real public JSON boundary; marshaling an impossible typed Node
// would omit them before compilation and would not exercise public input.
func TestWorkerLoopSourceSchemaRejectsInactiveFields(t *testing.T) {
	in := workerLoopInput(t)
	valid, _ := json.Marshal(in.Agent.Spec)
	if _, report := agentdomain.ValidateForPublication(valid, 1); !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	for field, value := range map[string]any{"children": []string{}, "model_slot": "primary", "instruction": "hidden", "tool_slots": []string{}, "knowledge_slots": []string{}, "memory": map[string]any{"tools": []string{"memory_add"}}, "artifact": map[string]any{"enabled": true}, "add_session_summary": true} {
		t.Run(field, func(t *testing.T) {
			var raw map[string]any
			if err := json.Unmarshal(valid, &raw); err != nil {
				t.Fatal(err)
			}
			raw["nodes"].(map[string]any)["cycle"].(map[string]any)[field] = value
			body, _ := json.Marshal(raw)
			if _, report := agentdomain.ValidateForPublication(body, 1); report.Valid {
				t.Fatal("public Agent input accepted inactive loop field", field)
			}
		})
	}
}

func TestWorkerLoopCompileExplainsTerminalParallelBody(t *testing.T) {
	in := workerLoopInput(t)
	in.Agent.Spec.Nodes["cycle"] = agentdomain.Node{Kind: agentdomain.NodeKindLoop, Body: "fanout", MaxIterations: 2}
	delete(in.Agent.Spec.Nodes, "root")
	delete(in.Agent.Spec.Nodes, "final")
	before, _ := json.Marshal(in.Agent.Spec)
	compiled, report := Compile(in)
	if report.Valid || len(compiled.CanonicalContent) != 0 {
		t.Fatal("terminal parallel loop body published")
	}
	found := false
	for _, d := range report.Diagnostics {
		if d.Code == DiagnosticEntrypointUnsupported && d.Path == "/agent_plan" && strings.Contains(d.Message, `terminal parallel node "fanout" requires an explicit successor llm in sequence`) {
			found = true
		}
	}
	if !found {
		t.Fatal("missing explicit post-loop LLM diagnostic", report.Diagnostics)
	}
	after, _ := json.Marshal(in.Agent.Spec)
	if !bytes.Equal(before, after) {
		t.Fatal("compiler silently repaired loop body")
	}
}

func TestWorkerLoopReleasePinRejectsPriorParallelRelease(t *testing.T) {
	if deploymentv1.WorkerV1PlanContract != "llm-sequence-parallel-loop-tree-v1" {
		t.Fatal("unexpected plan contract")
	}
	for name, in := range map[string]CompileInput{"single llm": workerSummaryInput(), "loop": workerLoopInput(t)} {
		t.Run(name, func(t *testing.T) {
			compiled, report := Compile(in)
			if !report.Valid {
				t.Fatal(report.Diagnostics)
			}
			wire, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent)
			if err != nil || deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest) != nil {
				t.Fatal("new publication rejected", err)
			}
			previous := "sha256:2468ca3b94ce00885b4d28564ace2cb59336f9243c9b906bba6682348a651b07"
			if wire.PlatformContract.Digest == previous || deploymentv1.ValidateWorkerV1(wire, previous) == nil {
				t.Fatal("prior Parallel release accepted new publication")
			}
			wire.PlatformContract.Digest = previous
			if deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest) == nil {
				t.Fatal("new Loop release accepted stale publication")
			}
		})
	}
}
