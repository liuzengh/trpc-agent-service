package runtimeadapter

import (
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"testing"
)

func TestParallelPlanRequiresExplicitTerminalLLM(t *testing.T) {
	_, p := sequenceRuntimeFixture()
	second := p.Nodes["first"]
	second.ModelName = "other-branch"
	p.Nodes["other"] = second
	p.Nodes["branches"] = domain.NodePlan{Kind: "parallel", Children: []string{"first", "other"}}
	root := p.Nodes[p.NodeID]
	root.Children = []string{"branches", "last"}
	p.Nodes[p.NodeID] = root
	if _, err := requiredUses(p); err != nil {
		t.Fatal(err)
	}
	root.Children = []string{"last", "branches"}
	p.Nodes[p.NodeID] = root
	if _, err := requiredUses(p); err == nil {
		t.Fatal("terminal parallel accepted")
	}
	delete(p.Nodes, "last")
	delete(p.Nodes, p.NodeID)
	p.NodeID = "branches"
	if _, err := requiredUses(p); err == nil {
		t.Fatal("parallel root accepted")
	}
}
