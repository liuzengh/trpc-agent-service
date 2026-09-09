package knowledgeingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"
)

type testEmbedder struct{ dimensions, calls int }

func (e *testEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	e.calls++
	return []float64{float64(len(text)), 1, 0}, nil
}
func (e *testEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	value, err := e.GetEmbedding(ctx, text)
	return value, nil, err
}
func (e *testEmbedder) GetDimensions() int { return e.dimensions }

var _ embedder.Embedder = (*testEmbedder)(nil)

func TestImportIsIdempotentAndUsesTenantScopedProvider(t *testing.T) {
	store := inmemory.New()
	emb := &testEmbedder{dimensions: 3}
	var gotTenant, gotApp string
	open := func(_ context.Context, tenantID, appID string) (vectorstore.VectorStore, func() error, error) {
		gotTenant, gotApp = tenantID, appID
		return store, func() error { return nil }, nil
	}
	req := Request{TenantID: "tenant-a", AgentAppID: "support", SourceID: "faq-1", Name: "Refund", Content: "Refunds settle in three days.", Metadata: map[string]any{"kind": "faq"}}
	first, err := Import(context.Background(), req, Services{OpenStore: open, Embedder: emb})
	if err != nil {
		t.Fatalf("first import: %v", err)
	}
	if first.Added != 1 || first.Skipped != 0 || first.ImportID == "" {
		t.Fatalf("first result = %+v", first)
	}
	second, err := Import(context.Background(), req, Services{OpenStore: open, Embedder: emb})
	if err != nil {
		t.Fatalf("retry import: %v", err)
	}
	if second.ImportID != first.ImportID || second.Skipped != 1 || second.Added != 0 || second.Updated != 0 {
		t.Fatalf("retry result = %+v", second)
	}
	if emb.calls != 1 {
		t.Fatalf("embedding calls = %d, want one on idempotent retry", emb.calls)
	}
	if gotTenant != "tenant-a" || gotApp != "support" {
		t.Fatalf("provider scope = %q/%q", gotTenant, gotApp)
	}
	count, err := store.Count(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("stored count = %d, err=%v", count, err)
	}
}

func TestImportReplacesStaleChunksForChangedSource(t *testing.T) {
	store := inmemory.New()
	emb := &testEmbedder{dimensions: 3}
	services := Services{OpenStore: func(context.Context, string, string) (vectorstore.VectorStore, func() error, error) {
		return store, nil, nil
	}, Embedder: emb}
	large := strings.Repeat("a", maxChunkBytes+20)
	first, err := Import(context.Background(), Request{TenantID: "tenant-a", AgentAppID: "support", SourceID: "article-1", Content: large}, services)
	if err != nil {
		t.Fatalf("large import: %v", err)
	}
	if first.ChunkCount != 2 || first.Added != 2 {
		t.Fatalf("large result = %+v", first)
	}
	second, err := Import(context.Background(), Request{TenantID: "tenant-a", AgentAppID: "support", SourceID: "article-1", Content: "replacement"}, services)
	if err != nil {
		t.Fatalf("replacement import: %v", err)
	}
	if second.ChunkCount != 1 || second.Updated != 1 || second.Deleted != 1 {
		t.Fatalf("replacement result = %+v", second)
	}
	count, err := store.Count(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("stored count after replacement = %d, err=%v", count, err)
	}
}

func TestImportRejectsUnboundedOrReservedInput(t *testing.T) {
	services := Services{OpenStore: func(context.Context, string, string) (vectorstore.VectorStore, func() error, error) {
		return inmemory.New(), nil, nil
	}, Embedder: &testEmbedder{dimensions: 3}}
	_, err := Import(context.Background(), Request{TenantID: "tenant-a", AgentAppID: "support", SourceID: "x", Content: strings.Repeat("x", MaxContentBytes+1)}, services)
	if !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("oversized error = %v", err)
	}
	_, err = Import(context.Background(), Request{TenantID: "tenant-a", AgentAppID: "support", SourceID: "x", Content: "ok", Metadata: map[string]any{"_tsa_tenant_id": "other"}}, services)
	if !errors.Is(err, ErrInvalidMetadata) {
		t.Fatalf("reserved metadata error = %v", err)
	}
}

func TestImportHonorsCanceledContextBeforeOpeningStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opened := false
	_, err := Import(ctx, Request{TenantID: "tenant-a", AgentAppID: "support", SourceID: "x", Content: "ok"}, Services{
		OpenStore: func(context.Context, string, string) (vectorstore.VectorStore, func() error, error) {
			opened = true
			return nil, nil, nil
		},
		Embedder: &testEmbedder{dimensions: 3},
	})
	if !errors.Is(err, context.Canceled) || opened {
		t.Fatalf("error=%v opened=%v", err, opened)
	}
}

func TestImportRejectsUnboundedExistingSource(t *testing.T) {
	store := inmemory.New()
	emb := &testEmbedder{dimensions: 3}
	services := Services{OpenStore: func(context.Context, string, string) (vectorstore.VectorStore, func() error, error) {
		return store, nil, nil
	}, Embedder: emb}
	for i := 0; i < MaxChunks+1; i++ {
		if err := store.Add(context.Background(), &document.Document{
			ID:      fmt.Sprintf("legacy-%d", i),
			Content: "legacy",
			Metadata: map[string]any{
				"source_id": "legacy-source",
			},
		}, []float64{1, 0, 0}); err != nil {
			t.Fatalf("seed legacy chunk %d: %v", i, err)
		}
	}
	_, err := Import(context.Background(), Request{
		TenantID: "tenant-a", AgentAppID: "support", SourceID: "legacy-source", Content: "replacement",
	}, services)
	if !errors.Is(err, ErrTooManyChunks) {
		t.Fatalf("unbounded source error = %v", err)
	}
}

func TestChunkNameIncludesStablePosition(t *testing.T) {
	if got := chunkName("FAQ", 1, 3); got != "FAQ [2/3]" {
		t.Fatalf("chunk name = %q", got)
	}
	if got := chunkName("", 0, 1); got != "knowledge-import" {
		t.Fatalf("default chunk name = %q", got)
	}
}
