package knowledge

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	upstream "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

type backendFunc func(context.Context, *upstream.SearchRequest) (*upstream.SearchResult, error)

func (f backendFunc) Search(ctx context.Context, request *upstream.SearchRequest) (*upstream.SearchResult, error) {
	return f(ctx, request)
}

func TestTenantKnowledgeForcesScopeAndSanitizes(t *testing.T) {
	backend := backendFunc(func(_ context.Context, request *upstream.SearchRequest) (*upstream.SearchResult, error) {
		if request.SearchFilter.Metadata[metadataTenantID] != "tenant-a" ||
			request.SearchFilter.Metadata[metadataKnowledgeID] != "kb" ||
			request.SearchFilter.Metadata[metadataKnowledgeVersion] != int64(2) {
			t.Fatal("required filter was not injected")
		}
		doc := &document.Document{Content: "answer", EmbeddingText: "private", Metadata: map[string]any{
			metadataTenantID: "tenant-a", metadataKnowledgeID: "kb", metadataKnowledgeVersion: int64(2),
			"title": "safe", "acl": "private",
		}}
		return &upstream.SearchResult{Documents: []*upstream.Result{{Document: doc}}}, nil
	})
	service := TenantKnowledge{
		TenantID: "tenant-a", AgentAppID: "app", KnowledgeRef: profile.VersionedRef{ID: "kb", Version: 2}, ConfigVersion: 4,
		Backend: backend, Limits: RetrievalLimits{MaxResults: 5, AllowedMetadata: map[string]bool{"title": true}},
	}
	request := &upstream.SearchRequest{Query: "hello", SearchFilter: &upstream.SearchFilter{Metadata: map[string]any{"category": "docs"}}}
	result, err := service.Search(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := request.SearchFilter.Metadata[metadataTenantID]; exists {
		t.Fatal("caller request was mutated")
	}
	doc := result.Documents[0].Document
	if doc.EmbeddingText != "" || doc.Metadata["acl"] != nil || doc.Metadata["title"] != "safe" {
		t.Fatalf("result was not sanitized: %#v", doc)
	}
}

func TestTenantKnowledgeRejectsConflictingFilterAndCrossTenantResult(t *testing.T) {
	service := TenantKnowledge{
		TenantID: "tenant-a", AgentAppID: "app", KnowledgeRef: profile.VersionedRef{ID: "kb", Version: 2}, ConfigVersion: 4,
		Backend: backendFunc(func(context.Context, *upstream.SearchRequest) (*upstream.SearchResult, error) {
			doc := &document.Document{Content: "leak", Metadata: map[string]any{
				metadataTenantID: "tenant-b", metadataKnowledgeID: "kb", metadataKnowledgeVersion: int64(2),
			}}
			return &upstream.SearchResult{Document: doc}, nil
		}),
	}
	_, err := service.Search(context.Background(), &upstream.SearchRequest{Query: "x", SearchFilter: &upstream.SearchFilter{Metadata: map[string]any{metadataTenantID: "tenant-b"}}})
	if !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("conflicting filter: got %v", err)
	}
	_, err = service.Search(context.Background(), &upstream.SearchRequest{Query: "x"})
	if !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-tenant result: got %v", err)
	}
}

func TestTenantKnowledgeCountsPrimaryDocumentAgainstByteLimit(t *testing.T) {
	service := TenantKnowledge{
		TenantID: "tenant-a", AgentAppID: "app", KnowledgeRef: profile.VersionedRef{ID: "kb", Version: 2}, ConfigVersion: 4,
		Limits: RetrievalLimits{MaxResultBytes: 3},
		Backend: backendFunc(func(context.Context, *upstream.SearchRequest) (*upstream.SearchResult, error) {
			return &upstream.SearchResult{Document: &document.Document{Content: "four", Metadata: map[string]any{
				metadataTenantID: "tenant-a", metadataKnowledgeID: "kb", metadataKnowledgeVersion: int64(2),
			}}}, nil
		}),
	}
	_, err := service.Search(context.Background(), &upstream.SearchRequest{Query: "x"})
	if !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("primary document byte limit: got %v", err)
	}
}
