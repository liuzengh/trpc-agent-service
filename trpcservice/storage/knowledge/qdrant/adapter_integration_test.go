package qdrant

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestAdapterQdrantIntegration(t *testing.T) {
	endpoint := os.Getenv("TRPC_QDRANT_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("TRPC_QDRANT_TEST_ENDPOINT is required")
	}
	collection := "trpc_knowledge_adapter_contract"
	// Qdrant's Cosine collection normalizes stored vectors. The migration image
	// digest intentionally commits the original embedding bytes, so the
	// collection must use a non-normalizing distance such as Dot.
	request, err := http.NewRequest(http.MethodPut, endpoint+"/collections/"+collection, bytes.NewBufferString(`{"vectors":{"size":2,"distance":"Dot"}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("create collection status=%d", response.StatusCode)
	}
	t.Cleanup(func() {
		request, _ := http.NewRequest(http.MethodDelete, endpoint+"/collections/"+collection, nil)
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			response.Body.Close()
		}
	})
	adapter, err := New(Config{Endpoint: endpoint, Collection: collection, VectorSize: 2, SnapshotWatermark: "integration-snapshot", AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, err := knowledgedriver.ImageDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "integration", MutationID: "integration-mutation", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	loaded, err := adapter.LoadChunk(context.Background(), image.Key)
	if err != nil || loaded.Key != image.Key {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	page, err := adapter.PageChunks(context.Background(), knowledgedriver.PageRequest{TenantID: image.Key.TenantID, SnapshotWatermark: "integration-snapshot", Limit: 10})
	if err != nil || len(page.Chunks) != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}

func TestSDKReadOnlyVectorStoreQdrantIntegration(t *testing.T) {
	endpoint := os.Getenv("TRPC_QDRANT_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("TRPC_QDRANT_TEST_ENDPOINT is required")
	}
	grpcPort := 6334
	if raw := os.Getenv("TRPC_QDRANT_TEST_GRPC_PORT"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 65535 {
			t.Fatalf("TRPC_QDRANT_TEST_GRPC_PORT=%q", raw)
		}
		grpcPort = value
	}
	collection := "trpc_knowledge_sdk_contract"
	request, err := http.NewRequest(http.MethodPut, endpoint+"/collections/"+collection, bytes.NewBufferString(`{"vectors":{"size":2,"distance":"Dot"}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("create collection status=%d", response.StatusCode)
	}
	t.Cleanup(func() {
		request, _ := http.NewRequest(http.MethodDelete, endpoint+"/collections/"+collection, nil)
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			response.Body.Close()
		}
	})
	adapter, err := New(Config{Endpoint: endpoint, Collection: collection, VectorSize: 2, SnapshotWatermark: "integration-snapshot",
		VectorGeneration: "generation", RuntimeEngine: "sdk", GRPCPort: grpcPort, AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, err := knowledgedriver.ImageDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "integration", MutationID: "sdk-integration-mutation", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	store, err := NewReadOnlyVectorStore(adapter, RuntimeScope{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID,
		KnowledgeVersion: image.Key.KnowledgeVersion, EmbedderProfile: image.EmbeddingProfileID, EmbedderVersion: image.EmbeddingVersion,
		VectorGeneration: image.VectorGeneration})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Search(context.Background(), &vectorstore.SearchQuery{Query: "query", Vector: []float64{0.25, 0.75}, SearchMode: vectorstore.SearchModeVector,
		Filter: &vectorstore.SearchFilter{Metadata: map[string]any{"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion}}})
	if err != nil || len(result.Results) != 1 || result.Results[0].Document.ID != image.Key.ChunkID {
		t.Fatalf("sdk result=%+v err=%v", result, err)
	}
}
