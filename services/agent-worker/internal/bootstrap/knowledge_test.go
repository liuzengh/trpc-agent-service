package bootstrap

import (
	"context"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"strings"
	"testing"
)

type knowledgePlanFixture struct{ plan domain.Plan }

func (f knowledgePlanFixture) Resolve(context.Context, domain.Route) (domain.Plan, error) {
	return f.plan, nil
}

func TestKnowledgeImportSelectsPublishedGraphResource(t *testing.T) {
	r := proof.KnowledgeRequest{TenantID: "tenant-a", ManifestRef: "manifest-a", ManifestDigest: "sha256:" + strings.Repeat("a", 64), DeploymentRevisionID: "revision-a", Resource: "docs_a", Operation: "import", Name: "entry.txt", Text: "larger-than-selected-cap"}
	small := domain.KnowledgePlan{Resource: "docs_a", Backend: datav1.Snapshot{Limits: datav1.Limits{MaxBytes: 1}}}
	other := domain.KnowledgePlan{Resource: "docs_b", Backend: datav1.Snapshot{Limits: datav1.Limits{MaxBytes: 2}}}
	base := domain.Plan{TenantID: r.TenantID, ManifestID: r.ManifestRef, ManifestDigest: r.ManifestDigest, DeploymentRevisionID: r.DeploymentRevisionID}
	for _, kind := range []string{"sequence", "parallel", "loop"} {
		p := base
		p.Nodes = map[string]domain.NodePlan{"root": {Kind: kind}}
		p.Knowledges = map[string]domain.KnowledgePlan{"docs_a": small, "docs_b": other}
		_, err := (knowledgeQueries{manifests: knowledgePlanFixture{p}}).ImportKnowledge(context.Background(), r)
		// The selected resource capacity must be checked before opening any network
		// dependency. The real import path is covered by the Qdrant joint gate.
		if !errors.Is(err, httpadapter.ErrArtifactCapacity) {
			t.Fatalf("%s selected import: %v", kind, err)
		}
		secondRequest := r
		secondRequest.Resource = "docs_b"
		_, err = (knowledgeQueries{manifests: knowledgePlanFixture{p}}).ImportKnowledge(context.Background(), secondRequest)
		if !errors.Is(err, httpadapter.ErrArtifactCapacity) {
			t.Fatalf("%s second selected resource: %v", kind, err)
		}
		for name, mutate := range map[string]func(*domain.Plan){
			"missing":           func(p *domain.Plan) { delete(p.Knowledges, "docs_a") },
			"resource mismatch": func(p *domain.Plan) { p.Knowledges["docs_a"] = other },
			"tenant":            func(p *domain.Plan) { p.TenantID = "tenant-b" },
			"manifest":          func(p *domain.Plan) { p.ManifestID = "other" },
			"digest":            func(p *domain.Plan) { p.ManifestDigest = "sha256:" + strings.Repeat("b", 64) },
			"revision":          func(p *domain.Plan) { p.DeploymentRevisionID = "other" },
		} {
			copy := p
			copy.Knowledges = map[string]domain.KnowledgePlan{"docs_a": small, "docs_b": other}
			copy.Knowledge = &small // A graph must never fall back to a legacy pointer.
			mutate(&copy)
			_, err := (knowledgeQueries{manifests: knowledgePlanFixture{copy}}).ImportKnowledge(context.Background(), r)
			if !errors.Is(err, httpadapter.ErrAttemptDenied) {
				t.Fatalf("%s/%s: %v", kind, name, err)
			}
		}
	}
	base.Knowledge = &small
	_, err := (knowledgeQueries{manifests: knowledgePlanFixture{base}}).ImportKnowledge(context.Background(), r)
	if !errors.Is(err, httpadapter.ErrArtifactCapacity) {
		t.Fatalf("legacy single LLM: %v", err)
	}
}
