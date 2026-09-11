package knowledgestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	knowledgetool "trpc.group/trpc-go/trpc-agent-go/knowledge/tool"
	sdktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestKnowledgeQdrantSDKFixture(t *testing.T) {
	endpoint := os.Getenv("KNOWLEDGE_TEST_QDRANT_ENDPOINT")
	if endpoint == "" {
		t.Skip("isolated Qdrant required")
	}
	var calls atomic.Int32
	var fail atomic.Bool
	var failAt atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/embeddings" || r.Header.Get("Authorization") != "Bearer fixture-key" {
			t.Error("embedding binding")
			w.WriteHeader(400)
			return
		}
		var req struct {
			Model      string
			Dimensions int
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil || req.Model != "fixture-embedding" || req.Dimensions != 3 {
			t.Error("fixed embedding contract")
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if fail.Load() || (failAt.Load() > 0 && calls.Load() >= failAt.Load()) {
			fmt.Fprint(w, `{"data":[{"embedding":[],"index":0}],"usage":{"prompt_tokens":1,"total_tokens":1}}`)
			return
		}
		fmt.Fprint(w, `{"object":"list","data":[{"object":"embedding","embedding":[1,0,0],"index":0}],"model":"fixture-embedding","usage":{"prompt_tokens":7,"total_tokens":7}}`)
	}))
	defer server.Close()
	ctx := context.Background()
	backend := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant", BackendID: "knowledge", BackendRevision: 1, Kind: datav1.Qdrant, Adapter: "managed-qdrant-v1", Isolation: "tenant-profile-resource-v1", Limits: datav1.Limits{TimeoutMS: 10000, MaxConcurrency: 4, MaxBytes: 65536}, Qdrant: &datav1.QdrantTarget{Endpoint: endpoint, Collection: "knowledge_fixture", VectorName: "selected_vector", Dimensions: 3, Distance: "cosine"}}
	key := "fixture-qdrant-key"
	admin := &restVectors{client: http.DefaultClient, endpoint: endpoint, collection: backend.Qdrant.Collection, key: key}
	if err := admin.request(ctx, "PUT", "", map[string]any{"vectors": map[string]any{"selected_vector": map[string]any{"size": 3, "distance": "Cosine"}}}, nil); err != nil {
		t.Fatal(err)
	}
	e := EmbedderConfig{Model: "fixture-embedding", BaseURL: server.URL + "/v1", APIKey: "fixture-key", Dimensions: 3}
	scope := Scope{TenantID: "tenant", ProfileID: "profile", ResourceID: "docs"}
	s, err := Open(ctx, backend, e, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	result, err := s.ImportText(ctx, "private filename", "The orchid is violet. It is the knowledge canary.")
	if err != nil || result.Documents != 1 {
		t.Fatal(result, err)
	}
	found, err := s.Search(ctx, &knowledge.SearchRequest{Query: "orchid"})
	if err != nil || found == nil || !strings.Contains(found.Text, "orchid") {
		t.Fatalf("SDK search %+v %v", found, err)
	}
	// Call the real SDK tool, whose default SearchRequest contains a non-nil
	// empty SearchFilter. This catches the public WithKnowledge assembly seam.
	searchTool := knowledgetool.NewKnowledgeSearchTool(s)
	callable, ok := searchTool.(sdktool.CallableTool)
	if !ok {
		t.Fatal("SDK tool is not callable")
	}
	toolResult, err := callable.Call(ctx, []byte(`{"query":"orchid"}`))
	if err != nil {
		t.Fatal("actual SDK search tool", err)
	}
	encoded, err := json.Marshal(toolResult)
	if err != nil || !strings.Contains(string(encoded), "orchid") {
		t.Fatal("SDK tool result", string(encoded), err)
	}
	empty := &knowledge.SearchFilter{DocumentIDs: []string{}, Metadata: map[string]any{}}
	if _, err = s.Search(ctx, &knowledge.SearchRequest{Query: "orchid", SearchFilter: empty}); err != nil {
		t.Fatal("semantic empty filter", err)
	}
	if empty.Metadata == nil || empty.DocumentIDs == nil {
		t.Fatal("caller filter mutated")
	}
	before := calls.Load()
	if _, err = s.Search(ctx, &knowledge.SearchRequest{Query: "orchid", SearchFilter: &knowledge.SearchFilter{Metadata: map[string]any{"worker_scope": "other"}}}); !errors.Is(err, ErrUnsupported) || calls.Load() != before {
		t.Fatal("scope filter override", err)
	}
	for _, change := range []func(*datav1.Snapshot, *EmbedderConfig, *Scope){
		func(_ *datav1.Snapshot, _ *EmbedderConfig, s *Scope) { s.ProfileID = "other" },
		func(_ *datav1.Snapshot, _ *EmbedderConfig, s *Scope) { s.ResourceID = "other" },
		func(b *datav1.Snapshot, _ *EmbedderConfig, s *Scope) { b.TenantID = "other"; s.TenantID = "other" },
		func(b *datav1.Snapshot, _ *EmbedderConfig, _ *Scope) { b.BackendRevision = 2 },
	} {
		b, c, sc := backend, e, scope
		change(&b, &c, &sc)
		other, err := Open(ctx, b, c, key, sc)
		if err != nil {
			t.Fatal(err)
		}
		out, err := other.Search(ctx, &knowledge.SearchRequest{Query: "orchid", MaxResults: 3})
		other.Close()
		if !errors.Is(err, ErrNoResults) {
			t.Fatal(err)
		}
		if out != nil && (out.Text != "" || len(out.Documents) > 0) {
			t.Fatalf("isolation %+v", out)
		}
	}
	// Fingerprints include model and embedding endpoint, exclude key rotation.
	same, err := Open(ctx, backend, e, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	changed := e
	changed.Model = "other-model"
	other, err := Open(ctx, backend, changed, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	if same.vectors.scope == other.vectors.scope {
		t.Fatal("embedding model fingerprint absent")
	}
	same.Close()
	other.Close()
	changed = e
	changed.APIKey = "rotated"
	rotated, err := Open(ctx, backend, changed, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.vectors.scope != s.vectors.scope {
		t.Fatal("credential rotation changed scope")
	}
	rotated.Close()
	bad := backend
	target := *backend.Qdrant
	bad.Qdrant = &target
	bad.Qdrant.VectorName = "wrong"
	if _, err = Open(ctx, bad, e, key, scope); !errors.Is(err, ErrIdentity) {
		t.Fatal("named vector", err)
	}
	bad.Qdrant.VectorName = "selected_vector"
	bad.Qdrant.Distance = "dot"
	if _, err = Open(ctx, bad, e, key, scope); !errors.Is(err, ErrIdentity) {
		t.Fatal("metric", err)
	}
	bad.Qdrant.Distance = "cosine"
	bad.Qdrant.Dimensions = 4
	badE := e
	badE.Dimensions = 4
	if _, err = Open(ctx, bad, badE, key, scope); !errors.Is(err, ErrIdentity) {
		t.Fatal("dimension", err)
	}
	if _, err = s.ImportText(ctx, "empty", " "); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.ImportText(cancelCtx, "cancel", "body"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	fail.Store(true)
	before = calls.Load()
	if _, err = s.ImportText(ctx, "emptyembedding", "must not accept empty vectors"); err == nil {
		t.Fatal("empty embedding accepted")
	}
	if calls.Load() != before+1 {
		t.Fatal("unexpected retries")
	}
	fail.Store(false)
	// Repeat import uses deterministic scoped IDs, not duplicate anonymous points.
	if _, err = s.ImportText(ctx, "private filename", "The orchid is violet. It is the knowledge canary."); err != nil {
		t.Fatal(err)
	}
	var count struct{ Result struct{ Count int } }
	if err = admin.request(ctx, "POST", "/points/count", map[string]any{"exact": true}, &count); err != nil || count.Result.Count != 1 {
		t.Fatal(count, err)
	}
	// A multi-chunk import can fail after an earlier point has committed.
	// The operation must report failure, not a successful whole-document import.
	failAt.Store(calls.Load() + 2)
	if _, err = s.ImportText(ctx, "partial", strings.Repeat("long knowledge chunk content. ", 500)); err == nil {
		t.Fatal("partial import reported success")
	}
	if err = admin.request(ctx, "POST", "/points/count", map[string]any{"exact": true}, &count); err != nil || count.Result.Count <= 1 {
		t.Fatal("partial fixture did not reach storage", count, err)
	}
	failAt.Store(0)
	s.Close()
	if _, err = s.Search(ctx, &knowledge.SearchRequest{Query: "closed"}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	t.Logf("KNOWLEDGE_QDRANT_SDK_FIXTURE=PASS sdk_reader_chunking_import=true sdk_embedding_http_calls=%d sdk_retrieval=true fixed_named_vector=true scoped=true empty_vector_rejected=true real_external_embedding=false", calls.Load())
}
