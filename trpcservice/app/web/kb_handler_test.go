package web

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
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
	return httptest.NewServer(asClaims(mux))
}

func TestKnowledgeAPICRUDAndSearch(t *testing.T) {
	srv := newKBServer(t)
	defer srv.Close()

	// A KB is a tenant asset managed by that tenant's admin.
	t1 := clientAs(adminClaims("t-1"))

	// create KB
	resp := postAs(t, t1, srv.URL+"/kbs",
		`{"id":"kb-1","tenant_id":"t-1","name":"docs","embedding_endpoint_id":"e-1"}`)
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
	resp = postAs(t, t1, srv.URL+"/kbs/kb-1/documents", `{"id":"d-1","title":"fruit","text":"apple banana cherry"}`)
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
	resp = postAs(t, t1, srv.URL+"/kbs/kb-1/search", `{"query":"apple banana"}`)
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
	resp = getAs(t, t1, srv.URL+"/kbs/kb-1/documents")
	var docs []*knowledge.Document
	_ = json.NewDecoder(resp.Body).Decode(&docs)
	resp.Body.Close()
	if len(docs) != 1 {
		t.Errorf("documents = %d, want 1", len(docs))
	}

	// missing KB -> 404
	resp = getAs(t, t1, srv.URL+"/kbs/nope")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing kb status = %d, want 404", resp.StatusCode)
	}
}

// TestKnowledgeAPIIsTenantScoped covers the KB boundary: a foreign tenant can
// neither read a KB nor reach its documents/search endpoints by id, while the
// create is pinned to the caller's own tenant.
func TestKnowledgeAPIIsTenantScoped(t *testing.T) {
	srv := newKBServer(t)
	defer srv.Close()

	t1 := clientAs(adminClaims("t-1"))
	t2 := clientAs(adminClaims("t-2"))

	// t-1 asks for t-2; the tenant is pinned back to t-1.
	resp := postAs(t, t1, srv.URL+"/kbs",
		`{"id":"kb-1","tenant_id":"t-2","name":"docs","embedding_endpoint_id":"e-1"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", resp.StatusCode)
	}
	var kb knowledge.KnowledgeBase
	_ = json.NewDecoder(resp.Body).Decode(&kb)
	resp.Body.Close()
	if kb.TenantID != "t-1" {
		t.Fatalf("kb tenant = %q, want t-1 (pinned to the caller)", kb.TenantID)
	}

	resp = postAs(t, t2, srv.URL+"/kbs", `{"id":"kb-2","name":"d","embedding_endpoint_id":"e-2"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("t-2 create status = %d", resp.StatusCode)
	}

	// t-2 sees only its own KB (whatever filter it asks for).
	resp = getAs(t, t2, srv.URL+"/kbs?tenant_id=t-1")
	var list []*knowledge.KnowledgeBase
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	resp.Body.Close()
	if len(list) != 1 || list[0].ID != "kb-2" {
		t.Errorf("t-2 list = %+v, want only kb-2", list)
	}

	// A foreign KB is missing on every route that names it.
	for _, tc := range []struct {
		method, suffix, body string
	}{
		{http.MethodGet, "", ""},
		{http.MethodDelete, "", ""},
		{http.MethodPost, "/documents", `{"title":"x","text":"y"}`},
		{http.MethodGet, "/documents", ""},
		{http.MethodPost, "/search", `{"query":"x"}`},
	} {
		resp = doAs(t, t2, tc.method, srv.URL+"/kbs/kb-1"+tc.suffix, tc.body)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s /kbs/kb-1%s = %d, want 404", tc.method, tc.suffix, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// The foreign KB is still usable by its own tenant.
	resp = getAs(t, t1, srv.URL+"/kbs/kb-1")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("t-1 get own kb = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
}
