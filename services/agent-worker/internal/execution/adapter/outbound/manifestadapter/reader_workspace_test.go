package manifestadapter

import (
	"context"
	"testing"
)

func TestReaderWorkspaceExplicitSingleLeaf(t *testing.T) {
	reader, route, _, _ := readerDataInput(t, func(c map[string]any) {
		readerDataNode(c)["workspace"] = map[string]any{"executor_resource": "shell", "tools": []string{"workspace_exec"}}
		resources := c["resources"].(map[string]any)
		resources["executors"] = map[string]any{"shell": map[string]any{"kind": "sdk_sandbox", "adapter_version": "sdk-sandbox-v1"}}
		resources["models"].(map[string]any)["primary"].(map[string]any)["capabilities"] = []string{"chat", "tool_call"}
		c["resolved_requirements"].(map[string]any)["executors"] = map[string]any{"shell": "shell"}
	})
	p, err := reader.Resolve(context.Background(), route)
	if err != nil {
		t.Fatal(err)
	}
	n := p.Nodes["assistant"]
	if len(p.Executors) != 1 || n.Workspace == nil || n.Workspace.ExecutorResource != "shell" || len(n.Workspace.Tools) != 1 || n.Workspace.Tools[0] != "workspace_exec" || n.Artifact || p.Artifact != nil {
		t.Fatal("workspace node authority lost or artifact inferred")
	}
	n.Workspace.Tools[0] = "workspace_save_artifact"
	next, err := reader.Resolve(context.Background(), route)
	if err != nil || next.Nodes["assistant"].Workspace.Tools[0] != "workspace_exec" {
		t.Fatal("aliased immutable workspace selection", err)
	}
}
