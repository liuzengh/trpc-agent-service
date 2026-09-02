//go:build integration

// Round-trip test of the Milvus vectorstore factory against a real Milvus
// standalone container: KB creation auto-creates the collection, ingestion
// writes chunks, search reads them back.
package knowledge

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

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

	m := NewManager(MilvusVectorStoreFactory(addr, "", ""), testEmbedderFactory(64))
	kb := &KnowledgeBase{ID: "kb-mil", TenantID: "t", Name: "docs", EmbeddingEndpointID: "e", Dimension: 64}
	if err := m.Create(ctx, kb); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := m.AddDocument(ctx, &Document{
		ID: "d-mil", KBID: "kb-mil", Title: "fruit", Text: "apple banana cherry",
	}); err != nil {
		t.Fatalf("add document: %v", err)
	}
	doc, _ := m.GetDocument(ctx, "d-mil")
	if doc.Status != StatusReady {
		t.Fatalf("doc status = %q (err: %q)", doc.Status, doc.Error)
	}

	// The store searches with Bounded consistency, so a just-inserted
	// document becomes visible within a short window: poll instead of
	// asserting on the first attempt.
	var hits []*Hit
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
