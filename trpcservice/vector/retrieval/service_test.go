package retrieval

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

func testConfig() Config {
	return Config{Model: "test-model", ModelVersion: "v1", SchemaVersion: "schema-v1", Dimension: 8}
}

func testTenant(id string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: id, AgentAppID: "agent-a", BindingID: "binding-a", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
}

type fakeEmbedder struct {
	mu        sync.Mutex
	embedding vector.Embedding
	err       error
	calls     int
	queries   []string
}

func (e *fakeEmbedder) Embed(ctx context.Context, text string) (vector.Embedding, error) {
	e.mu.Lock()
	e.calls++
	e.queries = append(e.queries, text)
	embedding, err := e.embedding, e.err
	e.mu.Unlock()
	if err != nil {
		return vector.Embedding{}, err
	}
	if cErr := ctx.Err(); cErr != nil {
		return vector.Embedding{}, cErr
	}
	return embedding, nil
}

type fakeStore struct {
	mu      sync.Mutex
	hits    []vector.SearchResult
	err     error
	calls   []int
	tenants []string
	block   bool
	fill    bool
}

func (s *fakeStore) Search(ctx context.Context, request vector.SearchRequest) ([]vector.SearchResult, error) {
	s.mu.Lock()
	s.calls = append(s.calls, request.TopK)
	if tc, ok := tenant.FromContext(ctx); ok {
		s.tenants = append(s.tenants, tc.TenantID)
	} else {
		s.tenants = append(s.tenants, "")
	}
	hits, err, block := s.hits, s.err, s.block
	s.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	if s.fill && len(hits) > 0 && len(hits) < request.TopK {
		filled := make([]vector.SearchResult, 0, request.TopK)
		for len(filled) < request.TopK {
			filled = append(filled, hits...)
		}
		hits = filled[:request.TopK]
	}
	if request.TopK < len(hits) {
		hits = hits[:request.TopK]
	}
	return hits, nil
}

func (s *fakeStore) Ready(context.Context) error                        { return nil }
func (s *fakeStore) Upsert(context.Context, vector.UpsertRequest) error { return nil }
func (s *fakeStore) Delete(context.Context, vector.DeleteRequest) error { return nil }
func (s *fakeStore) Close(context.Context) error                        { return nil }

type fakeHydrator struct {
	mu    sync.Mutex
	facts map[string]Fact
	err   error
	calls [][]string
}

func (h *fakeHydrator) Hydrate(_ context.Context, _ tenant.TenantContext, sourceIDs []string) (map[string]Fact, error) {
	h.mu.Lock()
	h.calls = append(h.calls, append([]string(nil), sourceIDs...))
	facts, err := h.facts, h.err
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	result := make(map[string]Fact, len(sourceIDs))
	for _, id := range sourceIDs {
		if fact, ok := facts[id]; ok {
			result[id] = fact
		}
	}
	return result, nil
}

func mustRef(t *testing.T, tc tenant.TenantContext, cfg Config, sourceID, content string, version int64, sequence int64) vector.VectorDocumentRef {
	t.Helper()
	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: sourceID, ProjectionScope: "memory:user", SourceVersion: version, SourceSequence: sequence, Content: content, Model: cfg.Model, ModelVersion: cfg.ModelVersion, Dimension: cfg.Dimension, SchemaVersion: cfg.SchemaVersion}
	ref, err := vector.BuildDocumentRef(tenant.WithContext(context.Background(), tc), source)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func hitFor(t *testing.T, tc tenant.TenantContext, cfg Config, sourceID, content string, score float64) vector.SearchResult {
	t.Helper()
	ref := mustRef(t, tc, cfg, sourceID, content, 1, 0)
	metadata, err := ref.SafeMetadata()
	if err != nil {
		t.Fatal(err)
	}
	return vector.SearchResult{Ref: ref, Score: score, Metadata: metadata}
}

func factFor(sourceID, content string) Fact {
	return Fact{MemoryID: sourceID, Scope: "user", Content: content, SourceVersion: 1, SourceSequence: 0}
}

func newTestService(t *testing.T, cfg Config, store vector.VectorStore, embedder vector.EmbeddingProvider, hydrator Hydrator) *Service {
	t.Helper()
	service, err := NewService(cfg, Dependencies{Store: store, Embedder: embedder, Hydrator: hydrator})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestNewServiceFailsClosed(t *testing.T) {
	if _, err := NewService(Config{}, Dependencies{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero config accepted: %v", err)
	}
	cfg, err := testConfig().WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewService(cfg, Dependencies{Store: &fakeStore{}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing deps accepted: %v", err)
	}
	broken := testConfig()
	broken.Model = " "
	if _, err := NewService(broken, Dependencies{Store: &fakeStore{}, Embedder: &fakeEmbedder{}, Hydrator: HydratorFunc(nil)}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid model accepted: %v", err)
	}
	broken = testConfig()
	broken.Dimension = 0
	if _, err := NewService(broken, Dependencies{Store: &fakeStore{}, Embedder: &fakeEmbedder{}, Hydrator: HydratorFunc(nil)}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid dimension accepted: %v", err)
	}
}

func TestRequestValidationRejectsCallerOverrides(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	service := newTestService(t, cfg, &fakeStore{}, &fakeEmbedder{embedding: vector.Embedding{Model: cfg.Model, ModelVersion: cfg.ModelVersion, Dimension: cfg.Dimension, Values: make([]float64, 8)}}, &fakeHydrator{})
	tc := testTenant("tenant-a")
	ctx := tenant.WithContext(context.Background(), tc)
	cases := []struct {
		name string
		req  Request
	}{
		{"empty query", Request{Query: "", TopK: 1}},
		{"blank query", Request{Query: "   ", TopK: 1}},
		{"oversized query", Request{Query: strings.Repeat("a", cfg.MaxQueryBytes+1), TopK: 1}},
		{"zero topK", Request{Query: "hello", TopK: 0}},
		{"topK above ceiling", Request{Query: "hello", TopK: cfg.MaxTopK + 1}},
		{"nan minScore", Request{Query: "hello", TopK: 1, MinScore: math.NaN()}},
		{"inf minScore", Request{Query: "hello", TopK: 1, MinScore: math.Inf(1)}},
		{"negative minScore", Request{Query: "hello", TopK: 1, MinScore: -0.1}},
		{"minScore above one", Request{Query: "hello", TopK: 1, MinScore: 1.1}},
		{"unknown source type", Request{Query: "hello", TopK: 1, SourceTypes: []string{"knowledge"}}},
		{"disallowed source type", Request{Query: "hello", TopK: 1, SourceTypes: []string{"prompt-history"}}},
	}
	for _, testCase := range cases {
		if _, err := service.Retrieve(ctx, tc, testCase.req); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%s accepted: %v", testCase.name, err)
		}
	}
}

func TestRetrieveWithoutTenantContextFailsClosed(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	service := newTestService(t, cfg, &fakeStore{}, &fakeEmbedder{}, &fakeHydrator{})
	if _, err := service.Retrieve(context.Background(), tenant.TenantContext{}, Request{Query: "hello", TopK: 1}); !errors.Is(err, vector.ErrInvalidTenant) {
		t.Fatalf("missing tenant accepted: %v", err)
	}
}

func queryEmbedding(cfg Config) vector.Embedding {
	return vector.Embedding{Model: cfg.Model, ModelVersion: cfg.ModelVersion, Dimension: cfg.Dimension, Values: make([]float64, cfg.Dimension)}
}

func TestEmbeddingMismatchAndErrorsFailClosed(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	ctx := tenant.WithContext(context.Background(), tc)
	req := Request{Query: "hello world", TopK: 1}

	wrongDimension := &fakeEmbedder{embedding: vector.Embedding{Model: cfg.Model, ModelVersion: cfg.ModelVersion, Dimension: 4, Values: make([]float64, 4)}}
	service := newTestService(t, cfg, &fakeStore{}, wrongDimension, &fakeHydrator{})
	if _, err := service.Retrieve(ctx, tc, req); !errors.Is(err, vector.ErrInvalidDimension) {
		t.Fatalf("dimension mismatch accepted: %v", err)
	}

	wrongModel := &fakeEmbedder{embedding: vector.Embedding{Model: "other", ModelVersion: cfg.ModelVersion, Dimension: 8, Values: make([]float64, 8)}}
	service = newTestService(t, cfg, &fakeStore{}, wrongModel, &fakeHydrator{})
	if _, err := service.Retrieve(ctx, tc, req); !errors.Is(err, vector.ErrInvalidModel) {
		t.Fatalf("model mismatch accepted: %v", err)
	}

	embedderErr := &fakeEmbedder{err: vector.ErrUnavailable}
	service = newTestService(t, cfg, &fakeStore{}, embedderErr, &fakeHydrator{})
	if _, err := service.Retrieve(ctx, tc, req); !errors.Is(err, vector.ErrUnavailable) {
		t.Fatalf("embedder unavailable not classified: %v", err)
	}

	unknownErr := &fakeEmbedder{err: errors.New("raw provider socket timeout 10.0.0.1")}
	service = newTestService(t, cfg, &fakeStore{}, unknownErr, &fakeHydrator{})
	_, err := service.Retrieve(ctx, tc, req)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unknown embedder error not mapped: %v", err)
	}
	if strings.Contains(err.Error(), "raw provider") || strings.Contains(err.Error(), "10.0.0.1") {
		t.Fatalf("raw embedder error leaked: %v", err)
	}
}

func TestEmbedderCancellationAndDeadlineKeepContextIdentity(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	req := Request{Query: "hello world", TopK: 1}

	embedderErr := &fakeEmbedder{err: context.Canceled}
	service := newTestService(t, cfg, &fakeStore{}, embedderErr, &fakeHydrator{})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Retrieve(cancelled, tc, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}

	deadlineErr := &fakeEmbedder{err: context.DeadlineExceeded}
	service = newTestService(t, cfg, &fakeStore{}, deadlineErr, &fakeHydrator{})
	if _, err := service.Retrieve(context.Background(), tc, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost: %v", err)
	}
}

func TestQueryTextAndVectorsNeverLeakIntoErrors(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	secretQuery := "classified-project-xenon-query"
	store := &fakeStore{err: errors.New("postgres://user:secret@10.1.2.3:5432/db raw failure")}
	embedder := &fakeEmbedder{err: vector.ErrUnavailable}
	service := newTestService(t, cfg, store, embedder, &fakeHydrator{})
	_, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: secretQuery, TopK: 1})
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), secretQuery) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("query or credential text leaked: %v", err)
	}
}

func TestSearchUsesTrustedTenantContextFromServer(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	store := &fakeStore{}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	hydrator := &fakeHydrator{facts: map[string]Fact{}}
	service := newTestService(t, cfg, store, embedder, hydrator)
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("unexpected results: %+v", results)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.calls) != 1 || store.calls[0] != 3*cfg.CandidateMultiplier {
		t.Fatalf("unexpected search calls: %v", store.calls)
	}
	for _, tenantID := range store.tenants {
		if tenantID != "tenant-a" {
			t.Fatalf("search context tenant not server-owned: %q", tenantID)
		}
	}
}

func TestCandidateValidationFiltersInvalidHits(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	other := testTenant("tenant-b")
	valid := hitFor(t, tc, cfg, "memory-1", "content one", 0.9)
	wrongTenant := valid
	wrongTenant.Ref = mustRef(t, other, cfg, "memory-2", "content two", 1, 0)
	malformed := valid
	malformed.Ref.DocumentID = "bogus-document"
	wrongModel := valid
	wrongModel.Ref.Model = "other-model"
	wrongDimension := valid
	wrongDimension.Ref.Dimension = 4
	nonFinite := valid
	nonFinite.Score = math.NaN()
	belowMin := valid
	belowMin.Score = 0.1
	deletedOp := valid
	deletedOp.Ref.Operation = vector.OperationDelete
	deletedOp.Ref.Deleted = true

	store := &fakeStore{hits: []vector.SearchResult{valid, wrongTenant, malformed, wrongModel, wrongDimension, nonFinite, belowMin, deletedOp}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	facts := map[string]Fact{"memory-1": factFor("memory-1", "content one")}
	hydrator := &fakeHydrator{facts: facts}
	service := newTestService(t, cfg, store, embedder, hydrator)
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].MemoryID != "memory-1" || results[0].DocumentID != valid.Ref.DocumentID {
		t.Fatalf("unexpected results: %+v", results)
	}
}

func TestCandidateOrderingIsDeterministic(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	first := hitFor(t, tc, cfg, "memory-1", "content one", 0.5)
	second := hitFor(t, tc, cfg, "memory-2", "content two", 0.9)
	third := hitFor(t, tc, cfg, "memory-3", "content three", 0.5)
	store := &fakeStore{hits: []vector.SearchResult{first, second, third}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	facts := map[string]Fact{"memory-1": factFor("memory-1", "content one"), "memory-2": factFor("memory-2", "content two"), "memory-3": factFor("memory-3", "content three")}
	service := newTestService(t, cfg, store, embedder, &fakeHydrator{facts: facts})
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("unexpected result count: %d", len(results))
	}
	if results[0].MemoryID != "memory-2" {
		t.Fatalf("highest score not first: %+v", results)
	}
	if results[1].DocumentID >= results[2].DocumentID {
		t.Fatalf("tie-break not by document identity: %+v", results)
	}
}

func TestDuplicateDocumentIDKeepsHighestScore(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	low := hitFor(t, tc, cfg, "memory-1", "content one", 0.4)
	high := hitFor(t, tc, cfg, "memory-1", "content one", 0.8)
	store := &fakeStore{hits: []vector.SearchResult{low, high}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	service := newTestService(t, cfg, store, embedder, &fakeHydrator{facts: map[string]Fact{"memory-1": factFor("memory-1", "content one")}})
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Score != 0.8 {
		t.Fatalf("duplicate not deduped to highest: %+v", results)
	}
}

func TestCandidateExpansionIsBounded(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRounds = 2
	cfg.MaxTotalCandidates = 16
	expanded, err := cfg.WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	tc := testTenant("tenant-a")
	mismatched := hitFor(t, tc, expanded, "memory-x", "content", 0.9)
	mismatched.Ref.Model = "other-model"
	store := &fakeStore{hits: []vector.SearchResult{mismatched}, fill: true}
	embedder := &fakeEmbedder{embedding: queryEmbedding(expanded)}
	service := newTestService(t, expanded, store, embedder, &fakeHydrator{facts: map[string]Fact{}})
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("unexpected results: %+v", results)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.calls) != 2 {
		t.Fatalf("expected two bounded rounds, got %v", store.calls)
	}
	if store.calls[0] != 2*expanded.CandidateMultiplier || store.calls[1] != 2*expanded.CandidateMultiplier*2 {
		t.Fatalf("unexpected round limits: %v", store.calls)
	}
}

func TestExpansionStopsWhenBackendExhausted(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRounds = 3
	expanded, err := cfg.WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	tc := testTenant("tenant-a")
	store := &fakeStore{}
	embedder := &fakeEmbedder{embedding: queryEmbedding(expanded)}
	service := newTestService(t, expanded, store, embedder, &fakeHydrator{facts: map[string]Fact{}})
	if _, err = service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 2}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.calls) != 1 {
		t.Fatalf("backend exhaustion must stop expansion: %v", store.calls)
	}
}

func TestBackendUnavailableAndUnknownFailSafely(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	ctx := tenant.WithContext(context.Background(), tc)
	req := Request{Query: "hello", TopK: 1}

	store := &fakeStore{err: vector.ErrUnavailable}
	service := newTestService(t, cfg, store, &fakeEmbedder{embedding: queryEmbedding(cfg)}, &fakeHydrator{})
	if _, err := service.Retrieve(ctx, tc, req); !errors.Is(err, vector.ErrUnavailable) {
		t.Fatalf("unavailable not classified: %v", err)
	}

	store = &fakeStore{err: errors.New("raw milvus grpc 10.2.3.4:19530 broken")}
	service = newTestService(t, cfg, store, &fakeEmbedder{embedding: queryEmbedding(cfg)}, &fakeHydrator{})
	_, err := service.Retrieve(ctx, tc, req)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unknown backend error not mapped: %v", err)
	}
	if strings.Contains(err.Error(), "raw milvus") || strings.Contains(err.Error(), "10.2.3.4") {
		t.Fatalf("raw backend error leaked: %v", err)
	}
}

func TestSearchCancellationAndDeadline(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	req := Request{Query: "hello", TopK: 1}

	store := &fakeStore{block: true}
	service := newTestService(t, cfg, store, &fakeEmbedder{embedding: queryEmbedding(cfg)}, &fakeHydrator{})
	cancelled, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	if _, err := service.Retrieve(cancelled, tc, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost during search: %v", err)
	}

	store = &fakeStore{block: true}
	service = newTestService(t, cfg, store, &fakeEmbedder{embedding: queryEmbedding(cfg)}, &fakeHydrator{})
	deadline, deadlineCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer deadlineCancel()
	if _, err := service.Retrieve(deadline, tc, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost during search: %v", err)
	}
}

func TestHydrationFiltersNonAuthoritativeCandidates(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	active := hitFor(t, tc, cfg, "memory-active", "active content", 0.9)
	missing := hitFor(t, tc, cfg, "memory-missing", "missing content", 0.8)
	deleted := hitFor(t, tc, cfg, "memory-deleted", "deleted content", 0.7)
	stale := hitFor(t, tc, cfg, "memory-stale", "stale content", 0.6)
	rewritten := hitFor(t, tc, cfg, "memory-rewritten", "rewritten content", 0.5)
	scopeMismatch := hitFor(t, tc, cfg, "memory-scope", "scoped content", 0.4)

	facts := map[string]Fact{
		"memory-active":    factFor("memory-active", "active content"),
		"memory-deleted":   {MemoryID: "memory-deleted", Scope: "user", Content: "deleted content", SourceVersion: 1, SourceSequence: 0, Deleted: true},
		"memory-stale":     {MemoryID: "memory-stale", Scope: "user", Content: "stale content", SourceVersion: 2, SourceSequence: 0},
		"memory-rewritten": factFor("memory-rewritten", "content changed in PostgreSQL"),
		"memory-scope":     {MemoryID: "memory-scope", Scope: "session", Content: "scoped content", SourceVersion: 1, SourceSequence: 0},
	}
	store := &fakeStore{hits: []vector.SearchResult{active, missing, deleted, stale, rewritten, scopeMismatch}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	hydrator := &fakeHydrator{facts: facts}
	service := newTestService(t, cfg, store, embedder, hydrator)
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].MemoryID != "memory-active" {
		t.Fatalf("unexpected hydrated results: %+v", results)
	}
	hydrator.mu.Lock()
	defer hydrator.mu.Unlock()
	if len(hydrator.calls) != 1 || len(hydrator.calls[0]) != 6 {
		t.Fatalf("hydration was not a single bounded batch: %+v", hydrator.calls)
	}
}

func TestHydrationUnavailableNeverReturnsRawHits(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	hit := hitFor(t, tc, cfg, "memory-1", "content one", 0.9)
	store := &fakeStore{hits: []vector.SearchResult{hit}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	service := newTestService(t, cfg, store, embedder, &fakeHydrator{err: ErrUnavailable})
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 3})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("hydration unavailable not classified: %v", err)
	}
	if results != nil {
		t.Fatalf("vector hits leaked despite hydration failure: %+v", results)
	}
}

func TestHydrationChunkingRespectsBatchLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxTopK = 4
	cfg.MaxTotalCandidates = 4
	cfg.HydrationBatchLimit = 2
	batched, err := cfg.WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	tc := testTenant("tenant-a")
	var hits []vector.SearchResult
	facts := map[string]Fact{}
	for i := 0; i < 6; i++ {
		sourceID := fmt.Sprintf("memory-%d", i)
		hits = append(hits, hitFor(t, tc, batched, sourceID, "content", 0.95-float64(i)*0.01))
		facts[sourceID] = factFor(sourceID, "content")
	}
	store := &fakeStore{hits: hits}
	embedder := &fakeEmbedder{embedding: queryEmbedding(batched)}
	hydrator := &fakeHydrator{facts: facts}
	service := newTestService(t, batched, store, embedder, hydrator)
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Fatalf("unexpected result count: %d", len(results))
	}
	hydrator.mu.Lock()
	defer hydrator.mu.Unlock()
	if len(hydrator.calls) != 2 {
		t.Fatalf("expected 2 bounded chunks, got %d", len(hydrator.calls))
	}
	for _, call := range hydrator.calls {
		if len(call) > 2 {
			t.Fatalf("batch limit exceeded: %v", call)
		}
	}
	_ = hits
}

func TestHydrationCancellationStopsPipeline(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	hit := hitFor(t, tc, cfg, "memory-1", "content one", 0.9)
	store := &fakeStore{hits: []vector.SearchResult{hit}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	hydrator := HydratorFunc(func(ctx context.Context, _ tenant.TenantContext, _ []string) (map[string]Fact, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	service := newTestService(t, cfg, store, embedder, hydrator)
	deadline, deadlineCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer deadlineCancel()
	results, err := service.Retrieve(tenant.WithContext(deadline, tc), tc, Request{Query: "hello", TopK: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hydration cancellation not surfaced: %v", err)
	}
	if results != nil {
		t.Fatalf("fake success after cancellation: %+v", results)
	}
}

func TestResultsCarryNoBackendOrLeaseData(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	hit := hitFor(t, tc, cfg, "memory-1", "content one", 0.9)
	hit.Metadata["injected"] = "raw-milvus-value"
	store := &fakeStore{hits: []vector.SearchResult{hit}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	service := newTestService(t, cfg, store, embedder, &fakeHydrator{facts: map[string]Fact{"memory-1": factFor("memory-1", "content one")}})
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("unexpected results: %+v", results)
	}
	result := results[0]
	if strings.Contains(result.DocumentID+"|"+result.MemoryID, "raw-milvus-value") {
		t.Fatal("raw metadata leaked into result identity")
	}
	if result.Content != "content one" || result.Scope != "user" || result.SourceVersion != 1 {
		t.Fatalf("hydrated fact not from PostgreSQL: %+v", result)
	}
}

func TestResultCountRespectsTopK(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	var hits []vector.SearchResult
	facts := map[string]Fact{}
	for i := 0; i < 6; i++ {
		sourceID := fmt.Sprintf("memory-%d", i)
		hits = append(hits, hitFor(t, tc, cfg, sourceID, "content", 0.95-float64(i)*0.01))
		facts[sourceID] = factFor(sourceID, "content")
	}
	store := &fakeStore{hits: hits}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	service := newTestService(t, cfg, store, embedder, &fakeHydrator{facts: facts})
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("topK not respected: %d", len(results))
	}
}

func TestTenantNarrowingFiltersOtherSourceHits(t *testing.T) {
	cfg, _ := testConfig().WithDefaults()
	tc := testTenant("tenant-a")
	memoryHit := hitFor(t, tc, cfg, "memory-1", "memory content", 0.9)
	store := &fakeStore{hits: []vector.SearchResult{memoryHit}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(cfg)}
	service := newTestService(t, cfg, store, embedder, &fakeHydrator{facts: map[string]Fact{"memory-1": factFor("memory-1", "memory content")}})
	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 3, SourceTypes: []string{vector.SourceTypeMemory}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("allowlisted narrowing broke active source: %+v", results)
	}
	otherSource := memoryHit
	otherSource.Ref.SourceType = "knowledge"
	store = &fakeStore{hits: []vector.SearchResult{otherSource}}
	service = newTestService(t, cfg, store, embedder, &fakeHydrator{})
	results, err = service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "hello", TopK: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("non-allowlisted source hydrated: %+v", results)
	}
}
