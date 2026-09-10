package knowledge

import (
	"context"
	"strconv"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/searchfilter"
)

func TestScopedKnowledgeForcesScopeAndSQLAuthorization(t *testing.T) {
	inner := &recordingKnowledge{result: &frameworkknowledge.SearchResult{
		Documents: []*frameworkknowledge.Result{
			{Document: chunkDocument("tenant-a", "app-a", "base-a", "doc-a", "1", "chunk-a", "g1"), Score: 0.9},
			{Document: chunkDocument("tenant-a", "app-a", "base-a", "doc-a", "1", "chunk-denied", "g1"), Score: 0.8},
			{Document: chunkDocument("tenant-b", "app-a", "base-a", "doc-a", "1", "chunk-forged", "g1"), Score: 1},
			{Document: chunkDocument("tenant-a", "app-a", "base-b", "doc-b", "2", "chunk-b", "g1"), Score: 0.7},
		},
	}}
	catalog := catalogFunc(func(_ context.Context, ref ChunkRef) (bool, error) {
		return ref.ChunkID != "chunk-denied", nil
	})
	view, err := NewScopedKnowledge(inner, catalog, tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}, "v1", []string{"base-b", "base-a"})
	if err != nil {
		t.Fatalf("new scoped knowledge: %v", err)
	}
	result, err := view.Search(context.Background(), &frameworkknowledge.SearchRequest{
		Query:      "question",
		MaxResults: 1,
		SearchFilter: &frameworkknowledge.SearchFilter{Metadata: map[string]any{
			MetadataTenantID: "forged",
			"category":       "guide",
		}},
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(result.Documents) != 1 || result.Documents[0].Document.ID != "chunk-a" {
		t.Fatalf("authorized documents = %#v", result.Documents)
	}
	if result.Text != "content chunk-a" {
		t.Fatalf("result text = %q", result.Text)
	}
	if len(inner.requests) != 1 {
		t.Fatalf("search calls = %d, want 1", len(inner.requests))
	}
	request := inner.requests[0]
	if got := request.SearchFilter.Metadata[MetadataTenantID]; got != "tenant-a" {
		t.Fatalf("tenant filter = %v", got)
	}
	if got := request.SearchFilter.Metadata[MetadataAppID]; got != "app-a" {
		t.Fatalf("app filter = %v", got)
	}
	if _, ok := request.SearchFilter.Metadata[MetadataKnowledgeBaseID]; ok {
		t.Fatal("base scope remained in caller metadata")
	}
	if got := request.SearchFilter.Metadata["category"]; got != "guide" {
		t.Fatalf("caller filter = %v", got)
	}
	condition := request.SearchFilter.FilterCondition
	if condition == nil || condition.Operator != searchfilter.OperatorIn || condition.Field != "metadata."+MetadataKnowledgeBaseID {
		t.Fatalf("base condition = %#v", condition)
	}
}

func TestScopedKnowledgeOverfetchesAfterAuthorizationRejectsTopCandidates(t *testing.T) {
	documents := make([]*frameworkknowledge.Result, 0, 10)
	for index := 0; index < 8; index++ {
		documents = append(documents, &frameworkknowledge.Result{
			Document: chunkDocument("tenant-a", "app-a", "base-a", "doc-a", "1", "denied-"+strconv.Itoa(index), "g1"),
			Score:    1 - float64(index)/100,
		})
	}
	documents = append(documents,
		&frameworkknowledge.Result{
			Document: chunkDocument("tenant-a", "app-a", "base-a", "doc-a", "1", "allowed-a", "g1"),
			Score:    0.1,
		},
		&frameworkknowledge.Result{
			Document: chunkDocument("tenant-a", "app-a", "base-a", "doc-a", "1", "allowed-b", "g1"),
			Score:    0.09,
		},
	)
	inner := &limitedKnowledge{documents: documents}
	view, err := NewScopedKnowledge(
		inner,
		catalogFunc(func(_ context.Context, ref ChunkRef) (bool, error) {
			return len(ref.ChunkID) < 8 || ref.ChunkID[:7] != "denied-", nil
		}),
		tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		"v1",
		[]string{"base-a"},
	)
	if err != nil {
		t.Fatalf("new scoped knowledge: %v", err)
	}
	result, err := view.Search(context.Background(), &frameworkknowledge.SearchRequest{Query: "question", MaxResults: 2})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(result.Documents) != 2 || result.Documents[0].Document.ID != "allowed-a" ||
		result.Documents[1].Document.ID != "allowed-b" {
		t.Fatalf("authorized documents = %#v", result.Documents)
	}
	if len(inner.requests) != 2 || inner.requests[0].MaxResults != 8 || inner.requests[1].MaxResults != 16 {
		t.Fatalf("search limits = %#v", inner.requests)
	}
}

func TestScopedKnowledgeKeepsHitWhenAnotherBaseIsEmpty(t *testing.T) {
	t.Parallel()
	inner := &limitedKnowledge{documents: []*frameworkknowledge.Result{{
		Document: chunkDocument("tenant-a", "app-a", "base-b", "doc-b", "1", "chunk-b", "g1"),
		Score:    0.8,
	}}}
	view, err := NewScopedKnowledge(
		inner,
		catalogFunc(func(context.Context, ChunkRef) (bool, error) { return true, nil }),
		tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		"v1",
		[]string{"base-a", "base-b"},
	)
	if err != nil {
		t.Fatalf("new scoped knowledge: %v", err)
	}
	result, err := view.Search(context.Background(), &frameworkknowledge.SearchRequest{Query: "question", MaxResults: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if result == nil || len(result.Documents) != 1 || result.Documents[0].Document.ID != "chunk-b" {
		t.Fatalf("search result = %#v", result)
	}
}

func TestScopedKnowledgeReturnsEmptyWhenAllBasesAreEmpty(t *testing.T) {
	t.Parallel()
	inner := &limitedKnowledge{}
	view, err := NewScopedKnowledge(
		inner,
		catalogFunc(func(context.Context, ChunkRef) (bool, error) { return true, nil }),
		tenant.Scope{TenantID: "tenant-a", AppID: "app-a"},
		"v1",
		[]string{"base-a", "base-b"},
	)
	if err != nil {
		t.Fatalf("new scoped knowledge: %v", err)
	}
	result, err := view.Search(context.Background(), &frameworkknowledge.SearchRequest{Query: "question", MaxResults: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if result == nil || len(result.Documents) != 0 {
		t.Fatalf("search result = %#v", result)
	}
}

type recordingKnowledge struct {
	result   *frameworkknowledge.SearchResult
	requests []*frameworkknowledge.SearchRequest
}

type limitedKnowledge struct {
	documents []*frameworkknowledge.Result
	requests  []*frameworkknowledge.SearchRequest
}

func (k *limitedKnowledge) Search(_ context.Context, req *frameworkknowledge.SearchRequest) (*frameworkknowledge.SearchResult, error) {
	k.requests = append(k.requests, req)
	limit := req.MaxResults
	if limit <= 0 || limit > len(k.documents) {
		limit = len(k.documents)
	}
	return &frameworkknowledge.SearchResult{Documents: k.documents[:limit]}, nil
}

func (k *recordingKnowledge) Search(_ context.Context, req *frameworkknowledge.SearchRequest) (*frameworkknowledge.SearchResult, error) {
	k.requests = append(k.requests, req)
	return k.result, nil
}

type catalogFunc func(context.Context, ChunkRef) (bool, error)

func (f catalogFunc) AvailableKnowledgeChunk(ctx context.Context, ref ChunkRef) (bool, error) {
	return f(ctx, ref)
}

func chunkDocument(tenantID, appID, baseID, documentID, version, chunkID, generation string) *document.Document {
	return &document.Document{
		ID:      chunkID,
		Content: "content " + chunkID,
		Metadata: map[string]any{
			MetadataTenantID:        tenantID,
			MetadataAppID:           appID,
			MetadataKnowledgeBaseID: baseID,
			MetadataDocumentID:      documentID,
			MetadataDocumentVersion: version,
			MetadataChunkID:         chunkID,
			MetadataIndexGeneration: generation,
		},
	}
}
