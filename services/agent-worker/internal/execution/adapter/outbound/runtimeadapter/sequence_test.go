package runtimeadapter

import (
	"context"
	"encoding/json"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"net/http"
	"testing"
)

func sequenceRuntimeFixture() (domain.Grant, domain.Plan) {
	g, p := runtimeFixture()
	p.ModelCredential.AudienceDigest = protocol.CredentialAudienceDigest("openai_compatible", p.ModelEndpoint)
	first := domain.NodePlan{Kind: "llm", ModelName: p.ModelName, ModelEndpoint: p.ModelEndpoint, ModelCredential: p.ModelCredential, Instruction: "first"}
	second := first
	second.ModelName = "writer"
	second.Instruction = "last"
	second.ModelCredential.CredentialID = "crd_writer"
	p.Nodes = map[string]domain.NodePlan{p.NodeID: {Kind: "sequence", Children: []string{"first", "last"}}, "first": first, "last": second}
	return g, p
}
func TestSequenceFactoryExactModelBatchAndDetachedNodes(t *testing.T) {
	g, p := sequenceRuntimeFixture()
	f, _ := newFixtureFactory(t, func(w http.ResponseWriter, r *http.Request) {
		var request resolveRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if len(request.Uses) != 3 {
			t.Error("model closure missing", request.Uses)
		}
		response := responseFixture(g, p)
		u := p.Nodes["last"].ModelCredential
		response.Credentials = append(response.Credentials, credentialWire{CredentialID: u.CredentialID, Purpose: u.Purpose, AudienceDigest: u.AudienceDigest, CredentialRevision: 2, Value: "writer-explicit-key"})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
	store := &fakeStore{}
	f.openStore = func(context.Context, string, sessionstore.Target, int) (candidateStore, error) { return store, nil }
	rt, err := f.Prepare(context.Background(), g, p, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	a := rt.(*attempt)
	p.Nodes[p.NodeID].Children[0] = "mutated"
	first := p.Nodes["first"]
	first.ModelName = "mutated"
	p.Nodes["first"] = first
	if a.plan.Nodes[a.plan.NodeID].Children[0] != "first" || a.plan.Nodes["first"].ModelName == "mutated" {
		t.Fatal("plan aliased")
	}
	req := trpcagent.Request{}
	if err = a.populateSequence(&req); err != nil {
		t.Fatal(err)
	}
	if req.Nodes["first"].Model.APIKey != "fixture-api-key" || req.Nodes["last"].Model.APIKey != "writer-explicit-key" {
		t.Fatal("node credentials swapped")
	}
	if len(req.Nodes["last"].Tools) > 0 || req.Nodes["last"].Memory != nil || req.Nodes["last"].Artifact {
		t.Fatal("node inherited capabilities")
	}
	rt.Close()
	if !store.closed || a.nodeModelKeys != nil {
		t.Fatal("attempt dependencies retained")
	}
}
func TestSequenceRuntimeRejectsInvalidNodeClosure(t *testing.T) {
	for name, mutate := range map[string]func(*domain.Plan){
		"orphan": func(p *domain.Plan) { p.Nodes["orphan"] = p.Nodes["last"] },
		"reuse": func(p *domain.Plan) {
			n := p.Nodes[p.NodeID]
			n.Children = []string{"first", "first"}
			p.Nodes[p.NodeID] = n
		},
		"cycle":       func(p *domain.Plan) { n := p.Nodes[p.NodeID]; n.Children = []string{p.NodeID}; p.Nodes[p.NodeID] = n },
		"unsupported": func(p *domain.Plan) { n := p.Nodes[p.NodeID]; n.Kind = "unknown"; p.Nodes[p.NodeID] = n },
		"model audience": func(p *domain.Plan) {
			n := p.Nodes["last"]
			n.ModelEndpoint = "https://changed.invalid"
			p.Nodes["last"] = n
		},
		"unbound tool":   func(p *domain.Plan) { n := p.Nodes["last"]; n.ToolResources = []string{"unbound"}; p.Nodes["last"] = n },
		"nonleaf option": func(p *domain.Plan) { n := p.Nodes[p.NodeID]; n.AddSessionSummary = true; p.Nodes[p.NodeID] = n },
		"memory missing": func(p *domain.Plan) {
			n := p.Nodes["last"]
			n.Memory = &domain.NodeMemoryPlan{Tools: []string{"memory_load"}}
			p.Nodes["last"] = n
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, p := sequenceRuntimeFixture()
			mutate(&p)
			if _, err := requiredUses(p); err == nil {
				t.Fatal("invalid graph accepted")
			}
		})
	}
}
