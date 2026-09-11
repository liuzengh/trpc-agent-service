package domain

import (
	"encoding/json"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"testing"
)

func TestDataAggregateCloneOwnsPointers(t *testing.T) {
	limit := int64(-1)
	enabled := true
	c := ManifestContent{Runtime: &ManifestRuntime{Summary: &ManifestSummary{Enabled: true, ModelResource: "primary", EventThreshold: 3}}, AgentPlan: AgentPlan{Root: "root", Nodes: map[string]ManifestNode{"root": {Kind: agentdomain.NodeKindLLM, Memory: &ManifestMemory{Resource: "memory", Tools: []string{"memory_load"}, PreloadLimit: &limit}, Artifact: &ManifestArtifact{Enabled: true, Resource: "artifact"}, AddSessionSummary: &enabled}}}}
	clone := normalizeManifestContent(c)
	c.Runtime.Summary.EventThreshold = 8
	n := c.AgentPlan.Nodes["root"]
	n.Memory.Tools[0] = "memory_clear"
	*n.Memory.PreloadLimit = 7
	n.Artifact.Resource = "changed"
	*n.AddSessionSummary = false
	out := clone.AgentPlan.Nodes["root"]
	if clone.Runtime.Summary.EventThreshold != 3 || out.Memory.Tools[0] != "memory_load" || *out.Memory.PreloadLimit != -1 || out.Artifact.Resource != "artifact" || !*out.AddSessionSummary {
		t.Fatal("pointer alias")
	}
	// The intermediate aggregate wire must not bypass semantic publication checks.
	if validateManifestCredentialShape(clone) == nil {
		t.Fatal("pending aggregate accepted for publication")
	}
}

func TestDataAggregatePresenceRejectsNullAndFalse(t *testing.T) {
	for _, raw := range []string{`{"runtime":null}`, `{"runtime":{}}`, `{"agent_plan":{"nodes":{"root":{"kind":"llm","memory":null}}}}`, `{"agent_plan":{"nodes":{"root":{"kind":"llm","add_session_summary":false}}}}`} {
		if _, err := DecodeManifestContent(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
