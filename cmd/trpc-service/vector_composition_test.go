package main

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/retrieval"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

type fakeVectorStore struct {
	mu      sync.Mutex
	upserts int
	deletes int
	err     error
	hits    []vector.SearchResult
}

func (s *fakeVectorStore) Ready(context.Context) error { return nil }
func (s *fakeVectorStore) Upsert(context.Context, vector.UpsertRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts++
	return s.err
}
func (s *fakeVectorStore) Delete(context.Context, vector.DeleteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	return s.err
}
func (s *fakeVectorStore) Search(context.Context, vector.SearchRequest) ([]vector.SearchResult, error) {
	return s.hits, s.err
}
func (s *fakeVectorStore) Close(context.Context) error { return nil }

type fakeVectorLeases struct{}

func (fakeVectorLeases) Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (storage.Lease, error) {
	return storage.Lease{OwnerID: "test-owner", FenceToken: 1, Epoch: 1, ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (fakeVectorLeases) Renew(_ context.Context, _ tenant.TenantContext, l storage.Lease, ttl time.Duration) (storage.Lease, error) {
	l.ExpiresAt = time.Now().Add(ttl)
	return l, nil
}
func (fakeVectorLeases) Release(context.Context, tenant.TenantContext, storage.Lease) error {
	return nil
}
func (fakeVectorLeases) Validate(context.Context, tenant.TenantContext, storage.Lease) error {
	return nil
}

func lazyTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	// pgxpool establishes connections lazily; construction never dials.
	pool, err := pgxpool.New(context.Background(), "postgres://lazy:lazy@127.0.0.1:1/lazy?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func vectorEnabledEnv(t *testing.T) {
	t.Helper()
	t.Setenv("VECTOR_BACKEND", "milvus")
	t.Setenv("MILVUS_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("MILVUS_CREDENTIAL_REF", "env://MILVUS_PASSWORD")
	t.Setenv("MILVUS_PASSWORD", "synthetic-password")
	t.Setenv("MILVUS_COLLECTION", "synthetic_collection")
	t.Setenv("VECTOR_MODEL", "test-model")
	t.Setenv("VECTOR_MODEL_VERSION", "v1")
	t.Setenv("VECTOR_SCHEMA_VERSION", "schema-v1")
	t.Setenv("VECTOR_DIMENSION", "8")
}

func TestVectorCompositionDefaultsDisabled(t *testing.T) {
	if _, err := parseVectorCompositionConfig(); err != nil {
		t.Fatal(err)
	}
	composition, err := assembleVectorComposition(context.Background(), vectorCompositionInput{})
	if err != nil {
		t.Fatal(err)
	}
	if composition != nil {
		t.Fatal("default composition must be nil")
	}
}

func TestVectorCompositionEnabledFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T)
	}{
		{"backend none with worker", func(t *testing.T) { t.Setenv("VECTOR_BACKEND", "none"); t.Setenv("VECTOR_WORKER_ENABLED", "true") }},
		{"unknown backend", func(t *testing.T) { t.Setenv("VECTOR_BACKEND", "qdrant") }},
		{"missing endpoint", func(t *testing.T) { t.Setenv("MILVUS_ENDPOINT", "") }},
		{"missing collection", func(t *testing.T) { t.Setenv("MILVUS_COLLECTION", "") }},
		{"missing model", func(t *testing.T) { t.Setenv("VECTOR_MODEL", "") }},
		{"missing dimension", func(t *testing.T) { t.Setenv("VECTOR_DIMENSION", "") }},
		{"invalid dimension", func(t *testing.T) { t.Setenv("VECTOR_DIMENSION", "99999") }},
		{"invalid max attempts", func(t *testing.T) { t.Setenv("VECTOR_TASK_MAX_ATTEMPTS", "999") }},
		{"worker without redis", func(t *testing.T) { t.Setenv("VECTOR_WORKER_ENABLED", "true"); t.Setenv("VECTOR_REDIS_URL", "") }},
		{"invalid enabled flag", func(t *testing.T) { t.Setenv("VECTOR_WORKER_ENABLED", "sometimes") }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			vectorEnabledEnv(t)
			testCase.mutate(t)
			embedder, embedderErr := vector.NewDeterministicEmbedder("test-model", "v1", 8, 1024)
			if embedderErr != nil {
				t.Fatal(embedderErr)
			}
			if _, err := assembleVectorComposition(context.Background(), vectorCompositionInput{pool: lazyTestPool(t), dependencies: &productionAssemblyDependencies{VectorEmbedder: embedder}}); err == nil {
				t.Fatal("incomplete enabled configuration accepted")
			}
		})
	}
}

func TestVectorCompositionEnabledWithoutEmbedderFailsClosed(t *testing.T) {
	vectorEnabledEnv(t)
	t.Setenv("VECTOR_WORKER_ENABLED", "true")
	t.Setenv("VECTOR_REDIS_URL", "redis://127.0.0.1:1/15")
	if _, err := assembleVectorComposition(context.Background(), vectorCompositionInput{pool: lazyTestPool(t), dependencies: &productionAssemblyDependencies{}}); err == nil {
		t.Fatal("enabled worker without production embedder must fail closed")
	}
}

func TestVectorCompositionEnabledWithSeams(t *testing.T) {
	vectorEnabledEnv(t)
	t.Setenv("VECTOR_WORKER_ENABLED", "true")
	t.Setenv("RETRIEVAL_ENABLED", "true")
	t.Setenv("VECTOR_REDIS_URL", "redis://127.0.0.1:1/15")
	embedder, err := vector.NewDeterministicEmbedder("test-model", "v1", 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	composition, err := assembleVectorComposition(context.Background(), vectorCompositionInput{
		pool: lazyTestPool(t),
		dependencies: &productionAssemblyDependencies{
			VectorEmbedder: embedder,
			VectorStore:    &fakeVectorStore{},
			VectorLeases:   fakeVectorLeases{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if composition == nil || composition.worker == nil || composition.retrieval == nil || composition.memory == nil || composition.store == nil {
		t.Fatalf("incomplete composition: %+v", composition)
	}
	if composition.redis != nil {
		t.Fatal("redis backend constructed despite injected lease seam")
	}
}

func TestVectorCompositionDisabledByDefaultInRuntime(t *testing.T) {
	f := newProductionTestFixture(t)
	factory := &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}
	hooks := newProductionRuntimeHooks()
	runtimeValue, _ := assembleProductionTestRuntime(t, f, factory, &recordingProductionSender{channel: lark.Channel, calls: make(chan storage.OutboxMessage, 1)}, &recordingProductionSender{channel: telegram.Channel, calls: make(chan storage.OutboxMessage, 1)}, hooks)
	if runtimeValue.vector != nil {
		t.Fatal("vector composition constructed while disabled")
	}
	if runtimeValue.readiness.vectorProbe != nil {
		t.Fatal("vector probe registered while disabled")
	}
}

func vectorFixtureTenant(id string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: id, AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
}

func waitVectorTaskSucceeded(t *testing.T, pool *pgxpool.Pool, schema, tenantID, memoryID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var status string
		err := pool.QueryRow(context.Background(), "SELECT status FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2 ORDER BY source_version DESC LIMIT 1", tenantID, memoryID).Scan(&status)
		if err == nil && status == "succeeded" {
			return
		}
		if time.Now().After(deadline) {
			var category string
			_ = pool.QueryRow(context.Background(), "SELECT COALESCE(last_error_category,'') FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2 ORDER BY source_version DESC LIMIT 1", tenantID, memoryID).Scan(&category)
			t.Fatalf("vector task did not succeed: last=%s category=%s err=%v", status, category, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestProductionRuntimeVectorEnabledLifecycle(t *testing.T) {
	f := newProductionTestFixture(t)
	vectorEnabledEnv(t)
	t.Setenv("VECTOR_WORKER_ENABLED", "true")
	t.Setenv("RETRIEVAL_ENABLED", "true")
	t.Setenv("VECTOR_REDIS_URL", "redis://127.0.0.1:1/15")
	embedder, err := vector.NewDeterministicEmbedder("test-model", "v1", 8, 1024)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeVectorStore{}
	scopedPool, err := pgxpool.NewWithConfig(context.Background(), func() *pgxpool.Config {
		config, configErr := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
		if configErr != nil {
			t.Fatal(configErr)
		}
		config.MaxConns = 4
		config.ConnConfig.RuntimeParams["search_path"] = f.schema
		return config
	}())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scopedPool.Close)
	runtimeValue, err := assembleProductionWithDependencies(context.Background(), productionTestResponder{factory: &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)}}, productionAssemblyDependencies{
		agentFactory:   &deterministicAgentFactory{result: "unused", started: make(chan agent.AgentInput, 1)},
		VectorEmbedder: embedder,
		VectorStore:    store,
		VectorLeases:   fakeVectorLeases{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtimeValue.vector == nil || runtimeValue.vector.worker == nil || runtimeValue.vector.retrieval == nil || runtimeValue.vector.memory == nil {
		t.Fatal("enabled runtime missing vector components")
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := runtimeValue.Stop(stopCtx); err != nil {
			t.Logf("runtime stop failed: %v", err)
		}
	})
	seedProductionRows(t, f, runtimeValue.pool)
	if err := runtimeValue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	tc := vectorFixtureTenant(f.tenantID)
	ctx := tenant.WithContext(context.Background(), tc)
	if err = runtimeValue.vector.memory.Put(ctx, tc, memory.Memory{TenantID: f.tenantID, ID: "mem-lifecycle", Scope: memory.ScopeUser, ScopeID: "scope-user", Kind: "fact", Content: "lifecycle content"}); err != nil {
		t.Fatal(err)
	}
	waitVectorTaskSucceeded(t, scopedPool, f.schema, f.tenantID, "mem-lifecycle", 30*time.Second)
	if store.upserts != 1 {
		t.Fatalf("fake store upserts=%d", store.upserts)
	}
	value, err := runtimeValue.vector.memory.Get(ctx, tc, "mem-lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: value.ID, ProjectionScope: "memory:user", SourceVersion: value.Version, SourceSequence: value.SourceSeq, Content: value.Content, Model: "test-model", ModelVersion: "v1", Dimension: 8, SchemaVersion: "schema-v1"}
	ref, err := vector.BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := ref.SafeMetadata()
	if err != nil {
		t.Fatal(err)
	}
	store.hits = []vector.SearchResult{{Ref: ref, Score: 0.99, Metadata: metadata}}
	results, err := runtimeValue.vector.retrieval.Retrieve(ctx, tc, retrieval.Request{Query: "lifecycle content", TopK: 3, MinScore: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].MemoryID != "mem-lifecycle" || results[0].Content != "lifecycle content" {
		t.Fatalf("unexpected retrieval results: %+v", results)
	}
}
