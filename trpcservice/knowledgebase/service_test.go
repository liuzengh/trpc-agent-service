package knowledgebase

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

type fakeEmbedder struct{ err error }

func (embedder fakeEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	if embedder.err != nil {
		return nil, embedder.err
	}
	return []float64{1, 0}, nil
}
func (embedder fakeEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	value, err := embedder.GetEmbedding(ctx, text)
	return value, nil, err
}
func (fakeEmbedder) GetDimensions() int { return 2 }

type fakeStore struct {
	docs     []*document.Document
	query    *vectorstore.SearchQuery
	metadata map[string]vectorstore.DocumentMetadata
	deleted  []string
}

func (store *fakeStore) Add(_ context.Context, doc *document.Document, _ []float64) error {
	store.docs = append(store.docs, doc.Clone())
	return nil
}
func (*fakeStore) Get(context.Context, string) (*document.Document, []float64, error) {
	return nil, nil, nil
}
func (*fakeStore) Update(context.Context, *document.Document, []float64) error { return nil }
func (store *fakeStore) Delete(_ context.Context, id string) error {
	store.deleted = append(store.deleted, id)
	return nil
}
func (store *fakeStore) Search(_ context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	store.query = query
	return &vectorstore.SearchResult{Results: []*vectorstore.ScoredDocument{{Document: &document.Document{ID: "result", Content: "answer"}, Score: .9}}}, nil
}
func (*fakeStore) DeleteByFilter(context.Context, ...vectorstore.DeleteOption) error { return nil }
func (*fakeStore) UpdateByFilter(context.Context, ...vectorstore.UpdateByFilterOption) (int64, error) {
	return 0, nil
}
func (*fakeStore) Count(context.Context, ...vectorstore.CountOption) (int, error) { return 0, nil }
func (store *fakeStore) GetMetadata(context.Context, ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	return store.metadata, nil
}
func (*fakeStore) Close() error { return nil }

func TestIngestOverridesIsolationMetadataAndChunksUTF8(t *testing.T) {
	store := &fakeStore{}
	service, err := New("tenant-a", "app-a", store, fakeEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	content := "知识" + string(make([]rune, chunkRunes))
	ids, err := service.Ingest(context.Background(), IngestRequest{DocumentID: "doc", Content: content, Metadata: map[string]any{"tenant_id": "tenant-b", "app_id": "app-b", "kind": "guide"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) < 2 || len(store.docs) != len(ids) {
		t.Fatalf("chunks=%d docs=%d", len(ids), len(store.docs))
	}
	for _, doc := range store.docs {
		if doc.Metadata["tenant_id"] != "tenant-a" || doc.Metadata["app_id"] != "app-a" || doc.Metadata["kind"] != "guide" {
			t.Fatalf("untrusted isolation metadata survived: %#v", doc.Metadata)
		}
	}
}

func TestSearchAlwaysAddsTrustedIsolationFilter(t *testing.T) {
	store := &fakeStore{}
	service, _ := New("tenant-a", "app-a", store, fakeEmbedder{})
	result, err := service.Search(context.Background(), &knowledge.SearchRequest{Query: "query", SearchFilter: &knowledge.SearchFilter{Metadata: map[string]any{"tenant_id": "tenant-b", "topic": "ops"}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "answer" || store.query.Filter.Metadata["tenant_id"] != "tenant-a" || store.query.Filter.Metadata["app_id"] != "app-a" || store.query.Filter.Metadata["topic"] != "ops" {
		t.Fatalf("result=%#v filter=%#v", result, store.query.Filter.Metadata)
	}
}

func TestIngestDeletesOnlyStaleChunksAfterUpsert(t *testing.T) {
	store := &fakeStore{}
	service, _ := New("tenant-a", "app-a", store, fakeEmbedder{})
	ids, err := service.Ingest(context.Background(), IngestRequest{DocumentID: "doc", Content: "new"})
	if err != nil {
		t.Fatal(err)
	}
	store.metadata = map[string]vectorstore.DocumentMetadata{"stale": {}, ids[0]: {}}
	if _, err := service.Ingest(context.Background(), IngestRequest{DocumentID: "doc", Content: "new"}); err != nil {
		t.Fatal(err)
	}
	if len(store.deleted) != 1 || store.deleted[0] != "stale" {
		t.Fatalf("deleted=%v", store.deleted)
	}
}

func TestEmbeddingErrorsDoNotLeakProviderDetails(t *testing.T) {
	service, _ := New("tenant-a", "app-a", &fakeStore{}, fakeEmbedder{err: errors.New("provider leaked-secret")})
	_, err := service.Search(context.Background(), &knowledge.SearchRequest{Query: "query"})
	if err == nil || err.Error() != "knowledge: query embedding failed" {
		t.Fatalf("err=%v", err)
	}
}

func TestIngestSynchronouslyMirrorsKnowledgeIndex(t *testing.T) {
	primary, shadow := &fakeStore{}, &fakeStore{}
	service, _ := New("tenant-a", "app-a", primary, fakeEmbedder{})
	target, _ := New("tenant-a", "app-a", shadow, fakeEmbedder{})
	service.WithMigration(nil, target)
	if _, err := service.Ingest(context.Background(), IngestRequest{DocumentID: "doc", Content: "knowledge"}); err != nil {
		t.Fatal(err)
	}
	if len(primary.docs) != 1 || len(shadow.docs) != 1 || primary.docs[0].ID != shadow.docs[0].ID {
		t.Fatalf("primary=%d shadow=%d", len(primary.docs), len(shadow.docs))
	}
}
