package dataprojection

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestKnowledgeProjectorMakesMigratedDocumentRetrievable(t *testing.T) {
	backend := newProjectionVectorStore()
	store, err := knowledgeplane.NewScopedStore("tenant-a", "support", backend)
	if err != nil {
		t.Fatal(err)
	}
	doc := &document.Document{
		ID: "faq-1", Name: "Refund", Content: "Refunds take three days.",
		Metadata: map[string]any{"kind": "faq"},
	}
	record, err := NewKnowledgeRecord("support", doc, []float64{1, 0, 0}, 17)
	if err != nil {
		t.Fatal(err)
	}
	projector, err := NewKnowledgeProjector(func(_ context.Context, tenantID, appID string) (*knowledgeplane.ScopedStore, error) {
		if tenantID != "tenant-a" || appID != "support" {
			t.Fatalf("resolver scope = %q/%q", tenantID, appID)
		}
		return store, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := projector.Apply(context.Background(), "tenant-a", record); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got, embedding, err := store.Get(context.Background(), "faq-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != "faq-1" || got.Content != "Refunds take three days." || got.Metadata["kind"] != "faq" {
		t.Fatalf("document = %#v", got)
	}
	if len(embedding) != 3 || embedding[0] != 1 {
		t.Fatalf("embedding = %#v", embedding)
	}
}

func TestKnowledgeProjectorReplaysTombstoneWithoutPayload(t *testing.T) {
	backend := newProjectionVectorStore()
	store, err := knowledgeplane.NewScopedStore("tenant-a", "support", backend)
	if err != nil {
		t.Fatal(err)
	}
	doc := &document.Document{ID: "faq-1", Content: "obsolete"}
	if err := store.Add(context.Background(), doc, []float64{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	record, err := NewKnowledgeTombstone("support", "faq-1", 18)
	if err != nil {
		t.Fatal(err)
	}
	if !record.Deleted || len(record.Payload) != 0 {
		t.Fatalf("tombstone = %#v", record)
	}
	projector, err := NewKnowledgeProjector(func(context.Context, string, string) (*knowledgeplane.ScopedStore, error) {
		return store, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := projector.Apply(context.Background(), "tenant-a", record); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Get(context.Background(), "faq-1"); err == nil {
		t.Fatal("deleted knowledge document is still retrievable")
	}
}

type projectionVectorStore struct {
	docs       map[string]*document.Document
	embeddings map[string][]float64
}

func newProjectionVectorStore() *projectionVectorStore {
	return &projectionVectorStore{docs: map[string]*document.Document{}, embeddings: map[string][]float64{}}
}

func (s *projectionVectorStore) Add(_ context.Context, doc *document.Document, embedding []float64) error {
	s.docs[doc.ID] = doc.Clone()
	s.embeddings[doc.ID] = append([]float64(nil), embedding...)
	return nil
}
func (s *projectionVectorStore) Get(_ context.Context, id string) (*document.Document, []float64, error) {
	doc := s.docs[id]
	if doc == nil {
		return nil, nil, errors.New("not found")
	}
	return doc.Clone(), append([]float64(nil), s.embeddings[id]...), nil
}
func (s *projectionVectorStore) Update(ctx context.Context, doc *document.Document, embedding []float64) error {
	return s.Add(ctx, doc, embedding)
}
func (s *projectionVectorStore) Delete(_ context.Context, id string) error {
	delete(s.docs, id)
	delete(s.embeddings, id)
	return nil
}
func (s *projectionVectorStore) Search(context.Context, *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	return &vectorstore.SearchResult{}, nil
}
func (s *projectionVectorStore) DeleteByFilter(context.Context, ...vectorstore.DeleteOption) error {
	return nil
}
func (s *projectionVectorStore) UpdateByFilter(context.Context, ...vectorstore.UpdateByFilterOption) (int64, error) {
	return 0, nil
}
func (s *projectionVectorStore) Count(_ context.Context, _ ...vectorstore.CountOption) (int, error) {
	return len(s.docs), nil
}
func (s *projectionVectorStore) GetMetadata(context.Context, ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	return map[string]vectorstore.DocumentMetadata{}, nil
}
func (s *projectionVectorStore) Close() error { return nil }
