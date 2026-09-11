package manifestadapter

import (
	"context"
	"encoding/json"
	"testing"
)

func TestReaderSequenceOrderedNodeAuthority(t *testing.T) {
	reader, route, _, _ := readerDataInput(t, func(c map[string]any) {
		plan := c["agent_plan"].(map[string]any)
		nodes := plan["nodes"].(map[string]any)
		old := nodes["assistant"].(map[string]any)
		raw, _ := json.Marshal(old)
		var first map[string]any
		_ = json.Unmarshal(raw, &first)
		first["instruction"] = "research first"
		first["model_resource"] = "research"
		first["tool_resources"] = []string{"search"}
		first["callable_entries"] = []string{"tools/search"}
		nodes["researcher"] = first
		nodes["prepare"] = map[string]any{"kind": "sequence", "children": []string{"researcher"}}
		nodes["workflow"] = map[string]any{"kind": "sequence", "children": []string{"prepare", "assistant"}}
		plan["root"] = "workflow"
		resources := c["resources"].(map[string]any)
		models := resources["models"].(map[string]any)
		raw, _ = json.Marshal(models["primary"])
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		m["model"] = "research-model"
		m["capabilities"] = []string{"chat", "tool_call"}
		m["credential"].(map[string]any)["credential_id"] = "crd_22222222222222222222222222222222"
		models["research"] = m
		resolved := c["resolved_requirements"].(map[string]any)
		resolved["models"].(map[string]any)["research"] = "research"
		resolved["tools"] = map[string]any{"search": "search"}
		resources["tools"] = map[string]any{"search": map[string]any{"kind": "mcp_streamable_http", "adapter_version": "mcp-web-search-v1", "server_url": "https://mcp.example.test/mcp", "toolset_name": "chosen", "tool_name": "lookup", "auth": map[string]any{"kind": "none"}, "capability": "calculator.add"}}
		e := c["execution"].(map[string]any)
		e["allowed_endpoint_hosts"] = append(e["allowed_endpoint_hosts"].([]any), "mcp.example.test")
	})
	p, err := reader.Resolve(context.Background(), route)
	if err != nil {
		t.Fatal(err)
	}
	if p.NodeID != "workflow" || len(p.Nodes) != 4 || p.Nodes["workflow"].Children[0] != "prepare" || p.Nodes["workflow"].Children[1] != "assistant" {
		t.Fatal("declared order lost", p.Nodes)
	}
	if p.Nodes["researcher"].ModelName != "research-model" || p.Nodes["assistant"].ModelName != "chat-model" || len(p.Nodes["researcher"].ToolResources) != 1 || len(p.Nodes["assistant"].ToolResources) != 0 {
		t.Fatal("leaf authority mixed")
	}
	if len(p.Tools) != 1 || len(p.Uses()) != 3 {
		t.Fatal("global closure", p.Uses())
	}
	p.Nodes["workflow"].Children[0] = "changed"
	p.Nodes["researcher"].ToolResources[0] = "changed"
	next, err := reader.Resolve(context.Background(), route)
	if err != nil || next.Nodes["workflow"].Children[0] != "prepare" || next.Nodes["researcher"].ToolResources[0] != "search" {
		t.Fatal("projection alias", err)
	}
}
