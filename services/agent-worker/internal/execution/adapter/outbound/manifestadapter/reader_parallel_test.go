package manifestadapter

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"testing"
)

func TestReaderParallelPreservesBranchesAndExplicitSuccessor(t *testing.T) {
	for _, terminalParallel := range []bool{false, true} {
		reader, route, _, _ := readerDataInput(t, func(c map[string]any) {
			plan := c["agent_plan"].(map[string]any)
			nodes := plan["nodes"].(map[string]any)
			for _, id := range []string{"branch_a", "branch_b"} {
				raw, _ := json.Marshal(nodes["assistant"])
				var leaf map[string]any
				_ = json.Unmarshal(raw, &leaf)
				leaf["instruction"] = id
				nodes[id] = leaf
			}
			nodes["branches"] = map[string]any{"kind": "parallel", "children": []string{"branch_b", "branch_a"}}
			children := []string{"branches", "assistant"}
			if terminalParallel {
				children = []string{"assistant", "branches"}
			}
			nodes["workflow"] = map[string]any{"kind": "sequence", "children": children}
			plan["root"] = "workflow"
		})
		p, err := reader.Resolve(context.Background(), route)
		if terminalParallel {
			if !errors.Is(err, application.ErrManifestUnsupported) {
				t.Fatal("terminal parallel not rejected", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if p.Nodes["branches"].Kind != "parallel" || p.Nodes["branches"].Children[0] != "branch_b" || p.Nodes["workflow"].Children[1] != "assistant" {
			t.Fatal("parallel order or successor changed")
		}
		if len(p.Uses()) != 2 || len(p.Nodes) != 5 {
			t.Fatal("shared model closure expanded")
		}
	}
}
