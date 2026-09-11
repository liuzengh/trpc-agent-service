package deploymentv1

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestWorkerV1RejectsDataCapabilityPresence(t *testing.T) {
	for _, name := range []string{"runtime-empty", "memory-empty", "memory-tools", "memory-preload", "artifact-empty", "artifact", "summary-false", "summary-true"} {
		t.Run(name, func(t *testing.T) {
			c := workerFixture(t)
			if err := ValidateWorkerV1(c, c.PlatformContract.Digest); err != nil {
				t.Fatal(err)
			}
			n := c.AgentPlan.Nodes[c.AgentPlan.Root]
			flag := name == "summary-true"
			preload := int64(-1)
			switch name {
			case "runtime-empty":
				c.Runtime = &ManifestRuntime{}
			case "summary":
				c.Runtime = &ManifestRuntime{Summary: &ManifestSummary{Enabled: true, ModelResource: n.ModelResource, EventThreshold: 3}}
			case "memory-empty":
				n.Memory = &ManifestMemory{}
			case "memory-tools":
				n.Memory = &ManifestMemory{Resource: "memory", Tools: []string{"memory_load"}}
			case "memory-preload":
				n.Memory = &ManifestMemory{Resource: "memory", Tools: []string{}, PreloadLimit: &preload}
			case "artifact-empty":
				n.Artifact = &ManifestArtifact{}
			case "artifact":
				n.Artifact = &ManifestArtifact{Enabled: true, Resource: "artifact"}
			case "summary-false", "summary-true":
				n.AddSessionSummary = &flag
			}
			c.AgentPlan.Nodes[c.AgentPlan.Root] = n
			if err := ValidateWorkerV1(c, c.PlatformContract.Digest); !errors.Is(err, ErrUnsupportedWorkerManifest) {
				t.Fatalf("new field silently accepted: %v", err)
			}
		})
	}
}

func TestWorkerV1DataWireCannotBypassGate(t *testing.T) {
	for _, field := range []string{"runtime", "memory", "artifact", "add_session_summary"} {
		for _, value := range []any{nil, false, map[string]any{}, true} {
			t.Run(field+"/"+string(mustGateJSON(t, value)), func(t *testing.T) {
				c := workerFixture(t)
				var wire map[string]any
				if err := json.Unmarshal(mustGateJSON(t, c), &wire); err != nil {
					t.Fatal(err)
				}
				if field == "runtime" {
					wire[field] = value
				} else {
					wire["agent_plan"].(map[string]any)["nodes"].(map[string]any)[c.AgentPlan.Root].(map[string]any)[field] = value
				}
				decoded, err := DecodeManifestContent(mustGateJSON(t, wire))
				if err != nil {
					return
				}
				if err = ValidateWorkerV1(decoded, c.PlatformContract.Digest); !errors.Is(err, ErrUnsupportedWorkerManifest) {
					t.Fatalf("wire field bypassed decoder and gate: %v", err)
				}
			})
		}
	}
}

func TestWorkerV1LegacyWireRemainsExecutable(t *testing.T) {
	c := workerFixture(t)
	raw := mustGateJSON(t, c)
	decoded, err := DecodeManifestContent(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateWorkerV1(decoded, c.PlatformContract.Digest); err != nil {
		t.Fatal(err)
	}
	if string(mustGateJSON(t, decoded)) != string(raw) {
		t.Fatal("legacy wire changed")
	}
}

func mustGateJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
