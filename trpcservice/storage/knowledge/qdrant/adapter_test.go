package qdrant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	upstream "trpc.group/trpc-go/trpc-agent-go/knowledge"
)

func TestAdapterAppliesReadsBackfillsAndScopesSearch(t *testing.T) {
	backend := newFakeQdrant()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a", AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, err := knowledgedriver.ImageDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	loaded, err := adapter.LoadChunk(context.Background(), image.Key)
	if err != nil || loaded.Key != image.Key || loaded.Content != image.Content {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	page, err := adapter.PageChunks(context.Background(), knowledgedriver.PageRequest{TenantID: image.Key.TenantID, SnapshotWatermark: "snapshot-a", Limit: 10})
	if err != nil || len(page.Chunks) != 1 || !page.Complete {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	fingerprint, err := adapter.Fingerprint(context.Background(), image.Key.TenantID, "snapshot-a")
	if err != nil || fingerprint.Count != 1 || fingerprint.Watermark != "snapshot-a" {
		t.Fatalf("fingerprint=%+v err=%v", fingerprint, err)
	}
	hits, err := adapter.Search(context.Background(), knowledgedriver.SearchRequest{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID, KnowledgeVersion: image.Key.KnowledgeVersion, Query: "query"})
	if err != nil || len(hits) != 1 || hits[0] != image.Key {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
	backend.injectForeign = true
	if _, err := adapter.Search(context.Background(), knowledgedriver.SearchRequest{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID, KnowledgeVersion: image.Key.KnowledgeVersion, Query: "query"}); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("foreign search result: %v", err)
	}
}

func TestAdapterRejectsMutationCollision(t *testing.T) {
	backend := newFakeQdrant()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a", AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, _ := knowledgedriver.ImageDigest(image)
	request := knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 1, Image: image, ImageDigest: digest}
	if _, err := adapter.ApplyChunk(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	changed := image
	changed.Content = "changed"
	changed.ContentDigest = hashText(changed.Content)
	changedDigest, _ := knowledgedriver.ImageDigest(changed)
	request.Image, request.ImageDigest = changed, changedDigest
	if _, err := adapter.ApplyChunk(context.Background(), request); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision=%v", err)
	}
}

func TestAdapterRejectsPointOutsideConfiguredSnapshot(t *testing.T) {
	backend := newFakeQdrant()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a", AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, _ := knowledgedriver.ImageDigest(image)
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	backend.setPayload(pointID(image.Key), "snapshot_watermark", "other-snapshot")
	if _, err := adapter.Fingerprint(context.Background(), image.Key.TenantID, "snapshot-a"); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("snapshot fence: %v", err)
	}
}

func TestFrameworkKnowledgeReturnsOnlyItsFixedVersion(t *testing.T) {
	backend := newFakeQdrant()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a", VectorGeneration: "generation", AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, _ := knowledgedriver.ImageDigest(image)
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	store, err := NewReadOnlyVectorStore(adapter, RuntimeScope{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID,
		KnowledgeVersion: image.Key.KnowledgeVersion, EmbedderProfile: image.EmbeddingProfileID,
		EmbedderVersion: image.EmbeddingVersion, VectorGeneration: image.VectorGeneration})
	if err != nil {
		t.Fatal(err)
	}
	knowledge := upstream.New(upstream.WithVectorStore(store), upstream.WithEmbedder(frameworkEmbedder{}))
	result, err := knowledge.Search(context.Background(), &upstream.SearchRequest{Query: "query", MaxResults: 3, SearchFilter: &upstream.SearchFilter{Metadata: map[string]any{
		"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion,
	}}})
	if err != nil || result.Document == nil || result.Document.ID != image.Key.ChunkID || result.Document.Content != image.Content {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	narrowed, err := knowledge.Search(context.Background(), &upstream.SearchRequest{Query: "query", MaxResults: 3, SearchFilter: &upstream.SearchFilter{Metadata: map[string]any{
		"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion, "title": "document",
	}}})
	if err != nil || narrowed.Document == nil || narrowed.Document.ID != image.Key.ChunkID {
		t.Fatalf("narrowed result=%+v err=%v", narrowed, err)
	}
	if _, err := knowledge.Search(context.Background(), &upstream.SearchRequest{Query: "query", SearchFilter: &upstream.SearchFilter{Metadata: map[string]any{"tenant_id": "foreign"}}}); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("conflicting tenant filter: %v", err)
	}
}

type fixedEmbedder struct{}

func (fixedEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{0.25, 0.75}, nil
}

type frameworkEmbedder struct{}

func (frameworkEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	return []float64{0.25, 0.75}, nil
}
func (frameworkEmbedder) GetEmbeddingWithUsage(context.Context, string) ([]float64, map[string]any, error) {
	return []float64{0.25, 0.75}, nil, nil
}
func (frameworkEmbedder) GetDimensions() int { return 2 }

type fakeQdrant struct {
	mu            sync.Mutex
	points        map[string]map[string]any
	injectForeign bool
}

func newFakeQdrant() *fakeQdrant { return &fakeQdrant{points: make(map[string]map[string]any)} }

func newTestServer(t *testing.T, backend *fakeQdrant) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	return server
}

func (f *fakeQdrant) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/points") {
		var body struct {
			Points []struct {
				ID      string         `json:"id"`
				Vector  []float32      `json:"vector"`
				Payload map[string]any `json:"payload"`
			} `json:"points"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		for _, point := range body.Points {
			f.points[point.ID] = map[string]any{"id": point.ID, "vector": point.Vector, "payload": point.Payload}
		}
		_, _ = w.Write([]byte(`{"result":{"status":"acknowledged"}}`))
		return
	}
	if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/points/") {
		id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		point, ok := f.points[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"result": point})
		return
	}
	if r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, "/scroll") || strings.HasSuffix(r.URL.Path, "/search")) {
		points := make([]any, 0, len(f.points))
		for _, point := range f.points {
			cloned := cloneMap(point)
			if f.injectForeign {
				payload := cloneMap(cloned["payload"].(map[string]any))
				payload["tenant_id"] = "foreign"
				cloned["payload"] = payload
			}
			points = append(points, cloned)
		}
		if strings.HasSuffix(r.URL.Path, "/scroll") {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"points": points, "next_page_offset": nil}})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"result": points})
		}
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (f *fakeQdrant) setPayload(id, key string, value any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	point, ok := f.points[id]
	if !ok {
		return
	}
	point["payload"].(map[string]any)[key] = value
}
func cloneMap(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}
func fixtureImage() knowledgedriver.ChunkImage {
	metadata := map[string]string{"title": "document"}
	return knowledgedriver.ChunkImage{Key: knowledgedriver.ChunkKey{TenantID: "tenant-a", KnowledgeID: "kb-a", KnowledgeVersion: 1, ChunkID: "chunk-a"}, Revision: 1, Operation: knowledgedriver.OperationUpsert, SourceDigest: strings.Repeat("a", 64), Content: "content", ContentDigest: hashText("content"), Metadata: metadata, MetadataDigest: hashJSON(metadata), EmbeddingProfileID: "embed", EmbeddingVersion: 1, VectorGeneration: "generation", Vector: []float32{0.25, 0.75}}
}
func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func hashJSON(value any) string { raw, _ := json.Marshal(value); return hashText(string(raw)) }
