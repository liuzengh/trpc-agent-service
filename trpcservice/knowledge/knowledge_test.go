package knowledge

import (
	"context"
	"errors"
	"hash/fnv"
	"strings"
	"sync"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

// hashEmbedder embeds text as a hashed bag of words: overlapping vocabulary
// yields similar vectors, which is all the unit tests need to assert ranking.
type hashEmbedder struct{ dim int }

func (e *hashEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	return e.vector(text), nil
}

func (e *hashEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := e.GetEmbedding(ctx, text)
	return v, nil, err
}

func (e *hashEmbedder) GetDimensions() int { return e.dim }

func (e *hashEmbedder) vector(text string) []float64 {
	v := make([]float64, e.dim)
	for _, tok := range strings.Fields(strings.ToLower(text)) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		v[h.Sum32()%uint32(e.dim)]++
	}
	return v
}

func testEmbedderFactory(dim int) EmbedderFactory {
	return func(_ context.Context, _ *KnowledgeBase) (embedder.Embedder, error) {
		return &hashEmbedder{dim: dim}, nil
	}
}

// failingEmbedderFactory simulates an unreachable embedding endpoint.
func failingEmbedderFactory() EmbedderFactory {
	return func(_ context.Context, _ *KnowledgeBase) (embedder.Embedder, error) {
		return nil, errors.New("embedding endpoint unreachable")
	}
}

func newTestManager(t *testing.T, embf EmbedderFactory) *Manager {
	t.Helper()
	return NewManager(InMemoryVectorStoreFactory(), embf)
}

func TestKBCRUD(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, testEmbedderFactory(64))

	kb := &KnowledgeBase{ID: "kb-1", TenantID: "t-1", Name: "docs", EmbeddingEndpointID: "e-1"}
	if err := m.Create(ctx, kb); err != nil {
		t.Fatalf("create: %v", err)
	}
	if kb.CollectionName != "t_1_kb_1" {
		t.Errorf("collection name = %q, want t_1_kb_1", kb.CollectionName)
	}

	// UUID-style ids (hyphens) normalize to Milvus-safe underscores.
	uuidKB := &KnowledgeBase{
		ID:       "6f1c9e0e-1234-4a5b-9c8d-abcdef012345",
		TenantID: "7d2e8f1f-4321-8b7c-6d5e-fedcba543210",
		Name:     "uuid kb", EmbeddingEndpointID: "e-1",
	}
	if err := m.Create(ctx, uuidKB); err != nil {
		t.Fatalf("create uuid kb: %v", err)
	}
	if strings.ContainsAny(uuidKB.CollectionName, "-") {
		t.Errorf("collection name %q must not contain hyphens", uuidKB.CollectionName)
	}
	if kb.Dimension != DefaultDimension {
		t.Errorf("dimension = %d, want default %d", kb.Dimension, DefaultDimension)
	}
	if err := m.Create(ctx, kb); err == nil {
		t.Error("duplicate create should fail")
	}

	got, err := m.Get(ctx, "kb-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != "docs" || got.TenantID != "t-1" {
		t.Errorf("round-trip mismatch: %+v", got)
	}

	list, err := m.List(ctx, "t-1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list t-1 = %v, %v; want 1", list, err)
	}
	other, _ := m.List(ctx, "t-2")
	if len(other) != 0 {
		t.Errorf("t-2 should see no KBs, got %d", len(other))
	}

	if err := m.Delete(ctx, "kb-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := m.Get(ctx, "kb-1"); !errors.Is(err, ErrKBNotFound) {
		t.Errorf("get after delete = %v, want ErrKBNotFound", err)
	}
}

func TestIngestTextAndSearch(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, testEmbedderFactory(64))

	kb := &KnowledgeBase{ID: "kb-1", TenantID: "t-1", Name: "docs", EmbeddingEndpointID: "e-1"}
	if err := m.Create(ctx, kb); err != nil {
		t.Fatalf("create: %v", err)
	}

	doc := &Document{ID: "d-1", KBID: "kb-1", Title: "fruit", Text: "apple banana cherry fruit salad"}
	if err := m.AddDocument(ctx, doc); err != nil {
		t.Fatalf("add document: %v", err)
	}
	got, err := m.GetDocument(ctx, "d-1")
	if err != nil {
		t.Fatalf("get document: %v", err)
	}
	if got.Status != StatusReady {
		t.Errorf("status = %q, want ready (error: %q)", got.Status, got.Error)
	}
	if got.ChunkCount < 1 {
		t.Errorf("chunk count = %d, want >= 1", got.ChunkCount)
	}

	hits, err := m.Search(ctx, "kb-1", "apple banana", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("search should find the ingested document")
	}
	if !strings.Contains(strings.ToLower(hits[0].Content), "apple") {
		t.Errorf("hit content = %q, want fruit text", hits[0].Content)
	}

	// documents listing
	docs, err := m.ListDocuments(ctx, "kb-1")
	if err != nil || len(docs) != 1 || docs[0].ID != "d-1" {
		t.Fatalf("list documents = %+v, %v", docs, err)
	}
}

func TestKBIsolation(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, testEmbedderFactory(64))

	for _, id := range []string{"kb-a", "kb-b"} {
		if err := m.Create(ctx, &KnowledgeBase{ID: id, TenantID: "t-1", Name: id, EmbeddingEndpointID: "e-1"}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	if err := m.AddDocument(ctx, &Document{ID: "d-a", KBID: "kb-a", Title: "a", Text: "alpha beta"}); err != nil {
		t.Fatalf("add to kb-a: %v", err)
	}

	hits, err := m.Search(ctx, "kb-b", "alpha beta", 5)
	if err != nil {
		t.Fatalf("search kb-b: %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("kb-b must not see kb-a documents, got %d hits", len(hits))
	}
}

func TestIngestFailureMarksDocFailed(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, failingEmbedderFactory())

	if err := m.Create(ctx, &KnowledgeBase{ID: "kb-1", TenantID: "t-1", Name: "docs", EmbeddingEndpointID: "e-missing"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	err := m.AddDocument(ctx, &Document{ID: "d-1", KBID: "kb-1", Title: "x", Text: "content"})
	if err == nil {
		t.Fatal("add document should fail with unreachable embedder")
	}
	got, _ := m.GetDocument(ctx, "d-1")
	if got == nil || got.Status != StatusFailed {
		t.Fatalf("document status = %+v, want failed", got)
	}
	if got.Error == "" {
		t.Error("failure reason should be recorded")
	}
}

func TestSearchEmptyKB(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, testEmbedderFactory(64))

	if err := m.Create(ctx, &KnowledgeBase{ID: "kb-1", TenantID: "t-1", Name: "docs", EmbeddingEndpointID: "e-1"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	hits, err := m.Search(ctx, "kb-1", "anything", 5)
	if err != nil {
		t.Fatalf("search on empty KB should not error, got %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("hits = %d, want 0", len(hits))
	}
}

func TestInMemoryFactorySharesStorePerCollection(t *testing.T) {
	f := InMemoryVectorStoreFactory()
	kb := &KnowledgeBase{CollectionName: "t_kb", Dimension: 64}
	v1, err := f(context.Background(), kb)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	v2, _ := f(context.Background(), kb)
	if v1 != v2 {
		t.Error("same collection must share one store instance")
	}
	// must satisfy the framework interface
	var _ vectorstore.VectorStore = v1
}

func TestSearchToolDistinctNames(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, testEmbedderFactory(64))

	for _, id := range []string{"kb-1", "kb-2"} {
		if err := m.Create(ctx, &KnowledgeBase{ID: id, TenantID: "t-1", Name: "docs " + id, EmbeddingEndpointID: "e-1"}); err != nil {
			t.Fatalf("create: %v", err)
		}
	}

	t1, err := m.SearchTool(ctx, "kb-1", "knowledge_search")
	if err != nil {
		t.Fatalf("search tool: %v", err)
	}
	t2, err := m.SearchTool(ctx, "kb-2", "knowledge_search_2")
	if err != nil {
		t.Fatalf("search tool: %v", err)
	}
	if t1 == nil || t2 == nil {
		t.Fatal("tools must not be nil")
	}
}

// concurrency smoke: parallel searches reuse the cached instance safely.
func TestSearchConcurrent(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, testEmbedderFactory(64))
	_ = m.Create(ctx, &KnowledgeBase{ID: "kb-1", TenantID: "t-1", Name: "docs", EmbeddingEndpointID: "e-1"})
	_ = m.AddDocument(ctx, &Document{ID: "d-1", KBID: "kb-1", Title: "x", Text: "alpha beta gamma"})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Search(ctx, "kb-1", "alpha", 3); err != nil {
				t.Errorf("concurrent search: %v", err)
			}
		}()
	}
	wg.Wait()
}
