package deploymentv1

import (
	"encoding/json"
	"os"
	"testing"
)

func aggregateFixture(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("examples/valid/runtime-manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err = json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	return env["content"].(map[string]any)
}
func TestDataAggregateRoundTrip(t *testing.T) {
	c := aggregateFixture(t)
	nodes := c["agent_plan"].(map[string]any)["nodes"].(map[string]any)
	var key string
	for id, v := range nodes {
		if v.(map[string]any)["kind"] == "llm" {
			key = id
			break
		}
	}
	n := nodes[key].(map[string]any)
	n["memory"] = map[string]any{"resource": "memory", "tools": []string{"memory_load"}, "preload_limit": -1}
	n["artifact"] = map[string]any{"resource": "artifact", "enabled": true}
	n["add_session_summary"] = true
	c["runtime"] = map[string]any{"summary": map[string]any{"enabled": true, "model_resource": "primary", "event_threshold": 3}}
	raw, _ := json.Marshal(c)
	out, err := DecodeManifestContent(raw)
	if err != nil {
		t.Fatal(err)
	}
	if out.Runtime == nil || out.Runtime.Summary.EventThreshold != 3 || out.AgentPlan.Nodes[key].Memory == nil || out.AgentPlan.Nodes[key].Artifact == nil || out.AgentPlan.Nodes[key].AddSessionSummary == nil {
		t.Fatal("component lost")
	}
	raw, err = json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	again, err := DecodeManifestContent(raw)
	if err != nil || again.AgentPlan.Nodes[key].Memory == nil {
		t.Fatal("roundtrip", err)
	}
	for _, field := range []string{"memory", "artifact", "add_session_summary"} {
		saved := n[field]
		for _, invalid := range []any{nil, false, map[string]any{}} {
			n[field] = invalid
			raw, _ = json.Marshal(c)
			if _, err = DecodeManifestContent(raw); err == nil {
				t.Fatalf("accepted %s=%v", field, invalid)
			}
		}
		n[field] = saved
	}
	for _, invalid := range []any{nil, map[string]any{}, map[string]any{"summary": nil}} {
		c["runtime"] = invalid
		raw, _ = json.Marshal(c)
		if _, err = DecodeManifestContent(raw); err == nil {
			t.Fatalf("accepted runtime %v", invalid)
		}
	}
}
