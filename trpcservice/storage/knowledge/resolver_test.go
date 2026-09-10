package knowledge

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	upstream "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

type scoreFunc func(context.Context, *upstream.SearchRequest) (*upstream.SearchResult, error)

func (f scoreFunc) Search(ctx context.Context, req *upstream.SearchRequest) (*upstream.SearchResult, error) {
	return f(ctx, req)
}

func resultWith(documentID string, score float64) *upstream.Result {
	return &upstream.Result{Document: &document.Document{ID: documentID, Content: documentID}, Score: score}
}

func TestResolverResolveKnowledgeByRefCount(t *testing.T) {
	backend := scoreFunc(func(context.Context, *upstream.SearchRequest) (*upstream.SearchResult, error) {
		return &upstream.SearchResult{}, nil
	})
	resolver := Resolver{Backend: backend}
	ctx := context.Background()

	none, err := resolver.ResolveKnowledge(ctx, "t", "app", nil, 1)
	if err != nil || none != nil {
		t.Fatalf("empty refs = (%v, %v), want (nil, nil)", none, err)
	}
	single, err := resolver.ResolveKnowledge(ctx, "t", "app", []profile.VersionedRef{{ID: "kb1", Version: 1}}, 1)
	if err != nil || single == nil {
		t.Fatalf("single ref = (%v, %v), want non-nil", single, err)
	}
	if _, ok := single.(TenantKnowledge); !ok {
		t.Fatalf("single ref type = %T, want TenantKnowledge", single)
	}
	multi, err := resolver.ResolveKnowledge(ctx, "t", "app", []profile.VersionedRef{{ID: "kb1", Version: 1}, {ID: "kb2", Version: 2}}, 1)
	if err != nil || multi == nil {
		t.Fatalf("multi ref = (%v, %v), want non-nil", multi, err)
	}
	composite, ok := multi.(Composite)
	if !ok || len(composite) != 2 {
		t.Fatalf("multi ref type = %T len=%d, want Composite(2)", multi, len(composite))
	}
}

func TestResolverResolveKnowledgeRequiresBackendAndContext(t *testing.T) {
	if _, err := (Resolver{}).ResolveKnowledge(context.Background(), "t", "app", []profile.VersionedRef{{ID: "kb", Version: 1}}, 1); err != runtime.ErrCapabilityUnsupported {
		t.Fatalf("nil backend = %v, want ErrCapabilityUnsupported", err)
	}
	resolver := Resolver{Backend: scoreFunc(nil)}
	if _, err := resolver.ResolveKnowledge(context.Background(), "", "app", []profile.VersionedRef{{ID: "kb", Version: 1}}, 1); err != runtime.ErrInvariantViolation {
		t.Fatalf("empty tenant = %v, want ErrInvariantViolation", err)
	}
}

func TestCompositeMergesByDescendingScore(t *testing.T) {
	composite := Composite{
		scoreFunc(func(context.Context, *upstream.SearchRequest) (*upstream.SearchResult, error) {
			return &upstream.SearchResult{Document: resultWith("low", 0.3).Document, Score: 0.3, Documents: []*upstream.Result{resultWith("low", 0.3)}}, nil
		}),
		scoreFunc(func(context.Context, *upstream.SearchRequest) (*upstream.SearchResult, error) {
			return &upstream.SearchResult{Document: resultWith("high", 0.9).Document, Score: 0.9, Documents: []*upstream.Result{resultWith("high", 0.9), resultWith("mid", 0.6)}}, nil
		}),
	}
	result, err := composite.Search(context.Background(), &upstream.SearchRequest{MaxResults: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.Document.ID != "high" || result.Score != 0.9 {
		t.Fatalf("best = (%q, %v), want (high, 0.9)", result.Document.ID, result.Score)
	}
	if len(result.Documents) != 2 || result.Documents[0].Document.ID != "high" || result.Documents[1].Document.ID != "mid" {
		t.Fatalf("documents order = %v", result.Documents)
	}
}
