package web

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
)

// webHashEmbedder mirrors the knowledge package test fake: bag-of-words
// hashing keeps overlapping vocabulary similar.
type webHashEmbedder struct{ dim int }

func (e *webHashEmbedder) GetEmbedding(_ context.Context, text string) ([]float64, error) {
	return e.vector(text), nil
}

func (e *webHashEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	v, err := e.GetEmbedding(ctx, text)
	return v, nil, err
}

func (e *webHashEmbedder) GetDimensions() int { return e.dim }

func (e *webHashEmbedder) vector(text string) []float64 {
	v := make([]float64, e.dim)
	for _, tok := range strings.Fields(strings.ToLower(text)) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		v[h.Sum32()%uint32(e.dim)]++
	}
	return v
}

func newKBServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mgr := knowledge.NewManager(knowledge.InMemoryVectorStoreFactory(),
		func(_ context.Context, _ *knowledge.KnowledgeBase) (embedder.Embedder, error) {
			return &webHashEmbedder{dim: 64}, nil
		})
	NewKnowledgeAPI(mgr).Register(mux)
	return httptest.NewServer(mux)
}

func TestKnowledgeAPICRUDAndSearch(t *testing.T) {
	srv := newKBServer(t)
	defer srv.Close()

	// create KB
	resp, err := http.Post(srv.URL+"/kbs", "application/json",
		bytes.NewBufferString(`{"id":"kb-1","tenant_id":"t-1","name":"docs","embedding_endpoint_id":"e-1"}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", resp.StatusCode)
	}
	var kb knowledge.KnowledgeBase
	if err := json.NewDecoder(resp.Body).Decode(&kb); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if kb.CollectionName != "t_1_kb_1" {
		t.Errorf("collection name = %q, want t_1_kb_1", kb.CollectionName)
	}

	// add document (inline text)
	resp, err = http.Post(srv.URL+"/kbs/kb-1/documents", "application/json",
		bytes.NewBufferString(`{"id":"d-1","title":"fruit","text":"apple banana cherry"}`))
	if err != nil {
		t.Fatalf("add document: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("add document status = %d", resp.StatusCode)
	}
	var doc knowledge.Document
	_ = json.NewDecoder(resp.Body).Decode(&doc)
	resp.Body.Close()
	if doc.Status != knowledge.StatusReady {
		t.Errorf("doc status = %q, want ready (err: %q)", doc.Status, doc.Error)
	}

	// search
	resp, err = http.Post(srv.URL+"/kbs/kb-1/search", "application/json",
		bytes.NewBufferString(`{"query":"apple banana"}`))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("search status = %d", resp.StatusCode)
	}
	var hits []*knowledge.Hit
	if err := json.NewDecoder(resp.Body).Decode(&hits); err != nil {
		t.Fatalf("decode hits: %v", err)
	}
	resp.Body.Close()
	if len(hits) == 0 {
		t.Error("search should find the ingested document")
	}

	// list documents
	resp, err = http.Get(srv.URL + "/kbs/kb-1/documents")
	if err != nil {
		t.Fatalf("list documents: %v", err)
	}
	var docs []*knowledge.Document
	_ = json.NewDecoder(resp.Body).Decode(&docs)
	resp.Body.Close()
	if len(docs) != 1 {
		t.Errorf("documents = %d, want 1", len(docs))
	}

	// missing KB -> 404
	resp, err = http.Get(srv.URL + "/kbs/nope")
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing kb status = %d, want 404", resp.StatusCode)
	}
}
