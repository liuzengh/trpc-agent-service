//go:build integration

// Round-trip test of the Milvus vectorstore factory against a real Milvus
// standalone container: KB creation auto-creates the collection, ingestion
// writes chunks, search reads them back.
package knowledgestore_test

import (
	"context"
	"hash/fnv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/knowledgestore"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
)

// hashEmbedder embeds text as a hashed bag of words so overlapping vocabulary
// yields similar vectors — all the round-trip needs to assert retrieval.
type hashEmbedder struct{ dim int }

func (e *hashEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	v := make([]float64, e.dim)
	for _, tok := range strings.Fields(strings.ToLower(text)) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		v[h.Sum32()%uint32(e.dim)]++
	}
	return v, nil
}

func (e *hashEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := e.GetEmbedding(ctx, text)
	return v, nil, err
}

func (e *hashEmbedder) GetDimensions() int { return e.dim }

func hashEmbedderFactory(dim int) knowledge.EmbedderFactory {
	return func(_ context.Context, _ *knowledge.KnowledgeBase) (embedder.Embedder, error) {
		return &hashEmbedder{dim: dim}, nil
	}
}

func TestMilvusVectorStoreRoundTrip(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		// v2.5+ is required: the framework vectorstore schema uses the BM25
		// function (full-text search), which 2.4 rejects.
		Image: "milvusdb/milvus:v2.5.6",
		Cmd:   []string{"milvus", "run", "standalone"},
		Env: map[string]string{
			"ETCD_USE_EMBED":     "true",
			"ETCD_DATA_DIR":      "/var/lib/milvus/etcd",
			"COMMON_STORAGETYPE": "local",
		},
		ExposedPorts: []string{"19530/tcp"},
		// The port opens before the internal services finish booting; the
		// proxy readiness line is the real signal (the gRPC client gets
		// "Milvus Proxy is not ready yet" until then).
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("19530/tcp"),
			wait.ForLog("Proxy successfully started").WithStartupTimeout(5*time.Minute),
		),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("milvus container unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	addr, err := c.PortEndpoint(ctx, "19530", "")
	if err != nil {
		t.Fatalf("milvus endpoint: %v", err)
	}

	m := knowledge.NewManager(knowledgestore.MilvusVectorStoreFactory(addr, "", ""), hashEmbedderFactory(64))
	kb := &knowledge.KnowledgeBase{ID: "kb-mil", TenantID: "t", Name: "docs", EmbeddingEndpointID: "e", Dimension: 64}
	if err := m.Create(ctx, kb); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := m.AddDocument(ctx, &knowledge.Document{
		ID: "d-mil", KBID: "kb-mil", Title: "fruit", Text: "apple banana cherry",
	}); err != nil {
		t.Fatalf("add document: %v", err)
	}
	doc, _ := m.GetDocument(ctx, "d-mil")
	if doc.Status != knowledge.StatusReady {
		t.Fatalf("doc status = %q (err: %q)", doc.Status, doc.Error)
	}

	// The store searches with Bounded consistency, so a just-inserted
	// document becomes visible within a short window: poll instead of
	// asserting on the first attempt.
	var hits []*knowledge.Hit
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		h, err := m.Search(ctx, "kb-mil", "apple banana", 5)
		if err == nil && len(h) > 0 {
			hits = h
			break
		}
		time.Sleep(time.Second)
	}
	if len(hits) == 0 {
		t.Fatal("search over milvus should find the ingested document")
	}
	if hits[0].Score <= 0 {
		t.Errorf("hit score = %f, want > 0", hits[0].Score)
	}
}
