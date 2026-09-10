package qdrant

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestSDKReadOnlyVectorStoreDelegatesSearchAndRechecksEnvelope(t *testing.T) {
	backend := newFakeQdrant()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a",
		VectorGeneration: "generation", RuntimeEngine: "sdk", GRPCPort: 6334, AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, _ := knowledgedriver.ImageDigest(image)
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	sdkDocument := sdkMirrorDocument(image, digest, "snapshot-a")
	var gotConfig sdkStoreConfig
	var gotQuery *vectorstore.SearchQuery
	store, err := newSDKReadOnlyVectorStore(adapter, RuntimeScope{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID,
		KnowledgeVersion: image.Key.KnowledgeVersion, EmbedderProfile: image.EmbeddingProfileID, EmbedderVersion: image.EmbeddingVersion,
		VectorGeneration: image.VectorGeneration}, func(_ context.Context, config sdkStoreConfig) (vectorstore.VectorStore, error) {
		gotConfig = config
		return &fakeSDKVectorStore{search: func(query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
			gotQuery = query
			return &vectorstore.SearchResult{Results: []*vectorstore.ScoredDocument{{Document: &document.Document{ID: pointID(image.Key)}, Score: 0.9}}}, nil
		}, get: func(string) (*document.Document, []float64, error) { return sdkDocument, []float64{0.25, 0.75}, nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Search(context.Background(), &vectorstore.SearchQuery{Query: "query", Vector: []float64{0.25, 0.75}, Limit: 2,
		SearchMode: vectorstore.SearchModeVector, Filter: &vectorstore.SearchFilter{Metadata: map[string]any{
			"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion, "title": "document",
		}}})
	if err != nil || len(result.Results) != 1 || result.Results[0].Document.ID != image.Key.ChunkID {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if gotConfig.host != "127.0.0.1" || gotConfig.port != 6334 || gotConfig.tls || gotConfig.collection != "knowledge" || gotConfig.dimension != 2 {
		t.Fatalf("sdk config=%+v", gotConfig)
	}
	if gotQuery == nil || gotQuery.Filter.Metadata[sdkScopeMetadataKey+".tenant_id"] != image.Key.TenantID || gotQuery.Filter.Metadata["title"] != "document" {
		t.Fatalf("sdk query=%+v", gotQuery)
	}
}

func TestSDKReadOnlyVectorStoreFailsClosedForForeignSDKCandidate(t *testing.T) {
	backend := newFakeQdrant()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a",
		VectorGeneration: "generation", RuntimeEngine: "sdk", GRPCPort: 6334, AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, _ := knowledgedriver.ImageDigest(image)
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	store, err := newSDKReadOnlyVectorStore(adapter, RuntimeScope{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID,
		KnowledgeVersion: image.Key.KnowledgeVersion, EmbedderProfile: image.EmbeddingProfileID, EmbedderVersion: image.EmbeddingVersion,
		VectorGeneration: image.VectorGeneration}, func(context.Context, sdkStoreConfig) (vectorstore.VectorStore, error) {
		return &fakeSDKVectorStore{search: func(*vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
			return &vectorstore.SearchResult{Results: []*vectorstore.ScoredDocument{{Document: &document.Document{ID: "foreign-point"}, Score: 0.9}}}, nil
		}, get: func(string) (*document.Document, []float64, error) {
			return &document.Document{ID: "foreign-point", Metadata: map[string]any{}}, []float64{0.25, 0.75}, nil
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Search(context.Background(), &vectorstore.SearchQuery{Query: "query", Vector: []float64{0.25, 0.75}, SearchMode: vectorstore.SearchModeVector,
		Filter: &vectorstore.SearchFilter{Metadata: map[string]any{"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion}}})
	if !errors.Is(err, runtime.ErrInvariantViolation) || result != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func sdkMirrorDocument(image knowledgedriver.ChunkImage, digest, watermark string) *document.Document {
	payload := encodeImage(image, digest, watermark)
	metadata := payload["metadata"].(map[string]any)
	return &document.Document{ID: payload["original_id"].(string), Content: payload["content"].(string), Metadata: metadata}
}

func TestSDKReadOnlyVectorStoreReusesStoreUntilTokenRotation(t *testing.T) {
	image := fixtureImage()
	digest, err := knowledgedriver.ImageDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	tokens := &rotatingToken{value: "token-one"}
	observer := &recordingSDKSearchObserver{}
	adapter, err := New(Config{Endpoint: "http://127.0.0.1:6333", Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a",
		VectorGeneration: "generation", RuntimeEngine: "sdk", GRPCPort: 6334, AllowInsecureHTTP: true, TokenSource: tokens, SDKSearchObserver: observer}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	created, closed := 0, 0
	store, err := newSDKReadOnlyVectorStore(adapter, RuntimeScope{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID,
		KnowledgeVersion: image.Key.KnowledgeVersion, EmbedderProfile: image.EmbeddingProfileID, EmbedderVersion: image.EmbeddingVersion,
		VectorGeneration: image.VectorGeneration}, func(_ context.Context, config sdkStoreConfig) (vectorstore.VectorStore, error) {
		mu.Lock()
		created++
		mu.Unlock()
		if config.apiKey == "" {
			t.Fatal("SDK store received empty token")
		}
		return &fakeSDKVectorStore{search: func(*vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
			return &vectorstore.SearchResult{Results: []*vectorstore.ScoredDocument{{Document: &document.Document{ID: pointID(image.Key)}, Score: 1}}}, nil
		}, get: func(string) (*document.Document, []float64, error) {
			return sdkMirrorDocument(image, digest, "snapshot-a"), []float64{0.25, 0.75}, nil
		}, close: func() error {
			mu.Lock()
			closed++
			mu.Unlock()
			return nil
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	query := sdkTestQuery(image)
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := store.Search(context.Background(), query); err != nil {
			t.Fatalf("search %d: %v", attempt, err)
		}
	}
	mu.Lock()
	if created != 1 || closed != 0 {
		t.Fatalf("before rotation created=%d closed=%d", created, closed)
	}
	mu.Unlock()
	tokens.Set("token-two")
	if _, err := store.Search(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if created != 2 || closed != 1 {
		t.Fatalf("after rotation created=%d closed=%d", created, closed)
	}
	mu.Unlock()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if closed != 2 {
		t.Fatalf("close count=%d", closed)
	}
	observations := observer.Observations()
	if len(observations) != 3 || observations[0].CandidateCount != 1 || observations[0].GetCalls != 1 || observations[0].Failed {
		t.Fatalf("observations=%+v", observations)
	}
}

func TestSDKReadOnlyVectorStoreRetiresRotatedStoreAfterInFlightSearch(t *testing.T) {
	image := fixtureImage()
	digest, err := knowledgedriver.ImageDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	tokens := &rotatingToken{value: "token-one"}
	adapter, err := New(Config{Endpoint: "http://127.0.0.1:6333", Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a",
		VectorGeneration: "generation", RuntimeEngine: "sdk", GRPCPort: 6334, AllowInsecureHTTP: true, TokenSource: tokens}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	firstGetStarted, releaseFirst := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	closed := map[string]int{}
	store, err := newSDKReadOnlyVectorStore(adapter, RuntimeScope{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID,
		KnowledgeVersion: image.Key.KnowledgeVersion, EmbedderProfile: image.EmbeddingProfileID, EmbedderVersion: image.EmbeddingVersion,
		VectorGeneration: image.VectorGeneration}, func(_ context.Context, config sdkStoreConfig) (vectorstore.VectorStore, error) {
		token := config.apiKey
		return &fakeSDKVectorStore{search: func(*vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
			return &vectorstore.SearchResult{Results: []*vectorstore.ScoredDocument{{Document: &document.Document{ID: pointID(image.Key)}, Score: 1}}}, nil
		}, get: func(string) (*document.Document, []float64, error) {
			if token == "token-one" {
				close(firstGetStarted)
				<-releaseFirst
			}
			return sdkMirrorDocument(image, digest, "snapshot-a"), []float64{0.25, 0.75}, nil
		}, close: func() error {
			mu.Lock()
			closed[token]++
			mu.Unlock()
			return nil
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, searchErr := store.Search(context.Background(), sdkTestQuery(image))
		firstDone <- searchErr
	}()
	select {
	case <-firstGetStarted:
	case <-time.After(time.Second):
		t.Fatal("first search did not reach Get")
	}
	tokens.Set("token-two")
	if _, err := store.Search(context.Background(), sdkTestQuery(image)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	oldClosed := closed["token-one"]
	mu.Unlock()
	if oldClosed != 0 {
		t.Fatalf("rotated store closed while search remained in flight: %d", oldClosed)
	}
	close(releaseFirst)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first search did not finish")
	}
	mu.Lock()
	oldClosed = closed["token-one"]
	mu.Unlock()
	if oldClosed != 1 {
		t.Fatalf("retired store close count=%d", oldClosed)
	}
}

func BenchmarkSDKReadOnlyVectorStoreSearch64Candidates(b *testing.B) {
	base := fixtureImage()
	documents := make(map[string]*document.Document, 64)
	results := make([]*vectorstore.ScoredDocument, 0, 64)
	for index := 0; index < 64; index++ {
		image := base
		image.Key.ChunkID = fmt.Sprintf("chunk-%d", index)
		digest, err := knowledgedriver.ImageDigest(image)
		if err != nil {
			b.Fatal(err)
		}
		documents[pointID(image.Key)] = sdkMirrorDocument(image, digest, "snapshot-a")
		results = append(results, &vectorstore.ScoredDocument{Document: &document.Document{ID: pointID(image.Key)}, Score: 1})
	}
	adapter, err := New(Config{Endpoint: "http://127.0.0.1:6333", Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a",
		VectorGeneration: "generation", RuntimeEngine: "sdk", GRPCPort: 6334, AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		b.Fatal(err)
	}
	store, err := newSDKReadOnlyVectorStore(adapter, RuntimeScope{TenantID: base.Key.TenantID, KnowledgeID: base.Key.KnowledgeID,
		KnowledgeVersion: base.Key.KnowledgeVersion, EmbedderProfile: base.EmbeddingProfileID, EmbedderVersion: base.EmbeddingVersion,
		VectorGeneration: base.VectorGeneration}, func(context.Context, sdkStoreConfig) (vectorstore.VectorStore, error) {
		return &fakeSDKVectorStore{search: func(*vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
			return &vectorstore.SearchResult{Results: results}, nil
		}, get: func(id string) (*document.Document, []float64, error) {
			return documents[id], []float64{0.25, 0.75}, nil
		}}, nil
	})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	query := sdkTestQuery(base)
	query.Limit = 64
	if _, err := store.Search(context.Background(), query); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := store.Search(context.Background(), query); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(1, "sdk_search_rpc/op")
	b.ReportMetric(64, "sdk_get_rpc/op")
	b.ReportMetric(65, "sdk_total_rpc/op")
}

func sdkTestQuery(image knowledgedriver.ChunkImage) *vectorstore.SearchQuery {
	return &vectorstore.SearchQuery{Query: "query", Vector: []float64{0.25, 0.75}, Limit: 1, SearchMode: vectorstore.SearchModeVector,
		Filter: &vectorstore.SearchFilter{Metadata: map[string]any{"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion}}}
}

type rotatingToken struct {
	mu    sync.Mutex
	value string
}

func (s *rotatingToken) Token(context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, nil
}

func (s *rotatingToken) Set(value string) {
	s.mu.Lock()
	s.value = value
	s.mu.Unlock()
}

type recordingSDKSearchObserver struct {
	mu     sync.Mutex
	values []SDKSearchObservation
}

func (s *recordingSDKSearchObserver) ObserveSDKSearch(_ context.Context, value SDKSearchObservation) {
	s.mu.Lock()
	s.values = append(s.values, value)
	s.mu.Unlock()
}

func (s *recordingSDKSearchObserver) Observations() []SDKSearchObservation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SDKSearchObservation(nil), s.values...)
}

type fakeSDKVectorStore struct {
	search func(*vectorstore.SearchQuery) (*vectorstore.SearchResult, error)
	get    func(string) (*document.Document, []float64, error)
	close  func() error
}

func (s *fakeSDKVectorStore) Add(context.Context, *document.Document, []float64) error {
	return errors.New("unexpected add")
}
func (s *fakeSDKVectorStore) Update(context.Context, *document.Document, []float64) error {
	return errors.New("unexpected update")
}
func (s *fakeSDKVectorStore) Delete(context.Context, string) error {
	return errors.New("unexpected delete")
}
func (s *fakeSDKVectorStore) DeleteByFilter(context.Context, ...vectorstore.DeleteOption) error {
	return errors.New("unexpected delete by filter")
}
func (s *fakeSDKVectorStore) UpdateByFilter(context.Context, ...vectorstore.UpdateByFilterOption) (int64, error) {
	return 0, errors.New("unexpected update by filter")
}
func (s *fakeSDKVectorStore) Count(context.Context, ...vectorstore.CountOption) (int, error) {
	return 0, errors.New("unexpected count")
}
func (s *fakeSDKVectorStore) GetMetadata(context.Context, ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	return nil, errors.New("unexpected metadata")
}
func (s *fakeSDKVectorStore) Get(_ context.Context, id string) (*document.Document, []float64, error) {
	if s.get == nil {
		return nil, nil, errors.New("unexpected get")
	}
	return s.get(id)
}
func (s *fakeSDKVectorStore) Search(_ context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	return s.search(query)
}
func (s *fakeSDKVectorStore) Close() error {
	if s.close != nil {
		return s.close()
	}
	return nil
}

var _ vectorstore.VectorStore = (*fakeSDKVectorStore)(nil)
