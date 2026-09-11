package manifestadapter

import (
	"context"
	"testing"
)

func TestReaderLoopKeepsPublishedBoundWithoutExpandingDependencies(t *testing.T) {
	for _, limit := range []int64{1, 2, 32} {
		reader, route, _, _ := readerDataInput(t, func(c map[string]any) {
			plan := c["agent_plan"].(map[string]any)
			nodes := plan["nodes"].(map[string]any)
			nodes["workflow"] = map[string]any{"kind": "loop", "body": "assistant", "max_iterations": limit}
			plan["root"] = "workflow"
		})
		p, err := reader.Resolve(context.Background(), route)
		if err != nil {
			t.Fatal(err)
		}
		n := p.Nodes["workflow"]
		if p.NodeID != "workflow" || n.Kind != "loop" || n.Body != "assistant" || n.MaxIterations != limit || len(n.Children) != 0 {
			t.Fatalf("loop projection changed: %+v", n)
		}
		if len(p.Nodes) != 2 || len(p.Uses()) != 2 {
			t.Fatal("loop expanded dependencies per iteration")
		}
	}
}
