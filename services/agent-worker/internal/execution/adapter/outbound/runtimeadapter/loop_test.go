package runtimeadapter

import (
	"context"
	"encoding/json"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"net/http"
	"testing"
)

func TestLoopPlanKeepsPublishedBodyAndIterations(t *testing.T) {
	for _, limit := range []int64{1, 2, 32} {
		_, p := sequenceRuntimeFixture()
		oldRoot := p.NodeID
		p.NodeID = "cycle"
		p.Nodes[p.NodeID] = domain.NodePlan{Kind: "loop", Body: oldRoot, MaxIterations: limit}
		if _, err := requiredUses(p); err != nil {
			t.Fatalf("limit %d: %v", limit, err)
		}
		for _, invalid := range []domain.NodePlan{
			{Kind: "loop", Body: oldRoot},
			{Kind: "loop", Body: oldRoot, MaxIterations: 33},
			{Kind: "loop", Body: "missing", MaxIterations: 1},
			{Kind: "loop", Body: "cycle", MaxIterations: 1},
			{Kind: "loop", Body: oldRoot, MaxIterations: 1, Children: []string{oldRoot}},
			{Kind: "loop", Body: oldRoot, MaxIterations: 1, Instruction: "implicit instruction"},
		} {
			copy := cloneSequencePlan(p)
			copy.Nodes[p.NodeID] = invalid
			if _, err := requiredUses(copy); err == nil {
				t.Fatalf("invalid loop accepted: %+v", invalid)
			}
		}
	}
}

func TestLoopPlanRejectsTerminalParallelBody(t *testing.T) {
	_, p := sequenceRuntimeFixture()
	p.Nodes[p.NodeID] = domain.NodePlan{Kind: "parallel", Children: []string{"first", "last"}}
	body := p.NodeID
	p.NodeID = "cycle"
	p.Nodes[p.NodeID] = domain.NodePlan{Kind: "loop", Body: body, MaxIterations: 2}
	if _, err := requiredUses(p); err == nil {
		t.Fatal("terminal parallel loop body accepted")
	}
}

func TestLoopFactoryPassesFixedIterationConfigAndResolvesOnce(t *testing.T) {
	g, p := sequenceRuntimeFixture()
	delete(p.Nodes, "last")
	p.Nodes[p.NodeID] = domain.NodePlan{Kind: "loop", Body: "first", MaxIterations: 32}
	calls := 0
	f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		var request resolveRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if len(request.Uses) != 2 {
			t.Error("loop expanded model closure", len(request.Uses))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseFixture(g, p))
	})
	store := &fakeStore{}
	f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) { return store, nil }
	rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	a := rt.(*attempt)
	p.Nodes[p.NodeID] = domain.NodePlan{Kind: "loop", Body: "mutated", MaxIterations: 1}
	req := trpcagent.Request{}
	if err := a.populateSequence(&req); err != nil {
		t.Fatal(err)
	}
	n := req.Nodes[a.plan.NodeID]
	if calls != 1 || n.Kind != "loop" || n.Body != "first" || n.MaxIterations != 32 || len(n.Children) != 0 {
		t.Fatalf("fixed loop altered: calls=%d node=%+v", calls, n)
	}
	if req.Nodes["first"].Model.APIKey != "fixture-api-key" || len(req.Nodes) != 2 {
		t.Fatal("leaf binding not preserved")
	}
}
