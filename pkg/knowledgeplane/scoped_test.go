package knowledgeplane

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

type memoryVectorStore struct {
	docs         map[string]*document.Document
	embeddings   map[string][]float64
	lastDocument *document.Document
	lastQuery    *vectorstore.SearchQuery
}

func newMemoryVectorStore() *memoryVectorStore {
	return &memoryVectorStore{docs: map[string]*document.Document{}, embeddings: map[string][]float64{}}
}
func (m *memoryVectorStore) Add(_ context.Context, d *document.Document, e []float64) error {
	m.lastDocument = d.Clone()
	m.docs[d.ID] = d.Clone()
	m.embeddings[d.ID] = append([]float64(nil), e...)
	return nil
}
func (m *memoryVectorStore) Get(_ context.Context, id string) (*document.Document, []float64, error) {
	d := m.docs[id]
	if d == nil {
		return nil, nil, errors.New("not found")
	}
	return d.Clone(), append([]float64(nil), m.embeddings[id]...), nil
}
func (m *memoryVectorStore) Update(ctx context.Context, d *document.Document, e []float64) error {
	return m.Add(ctx, d, e)
}
func (m *memoryVectorStore) Delete(_ context.Context, id string) error {
	delete(m.docs, id)
	delete(m.embeddings, id)
	return nil
}
func (m *memoryVectorStore) Search(_ context.Context, q *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	clone := *q
	m.lastQuery = &clone
	out := &vectorstore.SearchResult{}
	for _, d := range m.docs {
		out.Results = append(out.Results, &vectorstore.ScoredDocument{Document: d.Clone(), Score: 1})
		break
	}
	return out, nil
}
func (m *memoryVectorStore) DeleteByFilter(context.Context, ...vectorstore.DeleteOption) error {
	return nil
}
func (m *memoryVectorStore) UpdateByFilter(context.Context, ...vectorstore.UpdateByFilterOption) (int64, error) {
	return 0, nil
}
func (m *memoryVectorStore) Count(context.Context, ...vectorstore.CountOption) (int, error) {
	return len(m.docs), nil
}
func (m *memoryVectorStore) GetMetadata(context.Context, ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	out := map[string]vectorstore.DocumentMetadata{}
	for id, d := range m.docs {
		out[id] = vectorstore.DocumentMetadata{Metadata: cloneMetadata(d.Metadata)}
	}
	return out, nil
}
func (m *memoryVectorStore) Close() error { return nil }

func TestScopedStoreHidesPhysicalIDsAndForcesTenantFilter(t *testing.T) {
	backend := newMemoryVectorStore()
	store, err := NewScopedStore("tenant-a", "support", backend)
	if err != nil {
		t.Fatalf("new scoped store: %v", err)
	}
	doc := &document.Document{ID: "faq-1", Name: "Refund", Content: "refunds take three days", Metadata: map[string]any{"kind": "faq"}}
	if err := store.Add(context.Background(), doc, []float64{1, 0, 0}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if backend.lastDocument.ID == "faq-1" {
		t.Fatal("logical document ID leaked into the shared Qdrant point ID")
	}
	if backend.lastDocument.Metadata[metadataTenantID] != "tenant-a" || backend.lastDocument.Metadata[metadataAgentAppID] != "support" {
		t.Fatalf("physical metadata = %#v", backend.lastDocument.Metadata)
	}

	result, err := store.Search(context.Background(), &vectorstore.SearchQuery{Vector: []float64{1, 0, 0}, Limit: 1})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if backend.lastQuery.Filter == nil || backend.lastQuery.Filter.Metadata[metadataTenantID] != "tenant-a" || backend.lastQuery.Filter.Metadata[metadataAgentAppID] != "support" {
		t.Fatalf("search filter = %#v", backend.lastQuery.Filter)
	}
	if len(result.Results) != 1 || result.Results[0].Document.ID != "faq-1" {
		t.Fatalf("logical search result = %#v", result)
	}
}

func TestQdrantConfigFailsClosedWithoutTLSOutsideExplicitLocalMode(t *testing.T) {
	_, err := NewQdrantScopedStore(context.Background(), "tenant-a", "support", QdrantConfig{
		Host: "qdrant.internal", Port: 6334, Collection: "agent_knowledge", Dimension: 3,
	})
	if !errors.Is(err, ErrInsecureQdrant) {
		t.Fatalf("error = %v, want ErrInsecureQdrant", err)
	}
}
