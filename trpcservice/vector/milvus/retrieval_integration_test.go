//go:build integration

package milvus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/retrieval"
)

func retrievalStackFixture(t *testing.T) (*pgxpool.Pool, *retrieval.PostgresHydrator, tenant.TenantContext, vector.BackendConfig) {
	t.Helper()
	pgURL := os.Getenv("TEST_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if pgURL == "" {
		lab := testinfra.NewDockerLab(t)
		lab.Start(ctx)
		lab.WaitHealthy(ctx)
		pgURL = lab.PostgresURL(ctx)
	}
	base, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: pgURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	schema := "p106d_milvus_" + strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		t.Fatal(err)
	}
	cfg := pgstore.PostgresConfig{URL: pgURL, SearchPath: schema, MaxConns: 4, MinConns: 1, AllowDestructiveDown: true}
	pool, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	sourceFS := os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations"))
	const migrationCount = 8
	migrator, err := pgstore.NewMigratorWithPool(pool, cfg, sourceFS)
	if err != nil {
		t.Fatal(err)
	}
	if err = migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ('tenant-a', 'retrieval a', 'active', 1), ('tenant-b', 'retrieval b', 'active', 1)`); err != nil {
		t.Fatal(err)
	}
	hydrator, err := retrieval.NewPostgresHydrator(pool, retrieval.HydratorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	tc := tenant.TenantContext{TenantID: "tenant-a", AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := migrator.Down(cleanupCtx, migrationCount); err != nil {
			t.Logf("migration cleanup failed: %v", err)
		}
		pool.Close()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
	})
	return pool, hydrator, tc, integrationConfig()
}

type retrievalMemoryFixture struct {
	TenantID string
	MemoryID string
	Content  string
	Version  int64
	Deleted  bool
}

func (f retrievalMemoryFixture) insert(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO memory (tenant_id, memory_id, scope, scope_id, kind, content, version, source_seq, deleted)
        VALUES ($1,$2,'user','scope-user','fact',$3,$4,0,$5) ON CONFLICT (tenant_id, memory_id) DO UPDATE SET content=EXCLUDED.content, version=EXCLUDED.version, deleted=EXCLUDED.deleted, updated_at=now()`,
		f.TenantID, f.MemoryID, f.Content, f.Version, f.Deleted); err != nil {
		t.Fatal(err)
	}
}

func (f retrievalMemoryFixture) upsertVector(t *testing.T, store vector.VectorStore, cfg vector.BackendConfig) {
	t.Helper()
	tc := tenant.TenantContext{TenantID: f.TenantID, AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: f.MemoryID, ProjectionScope: "memory:user", SourceVersion: f.Version, SourceSequence: 0, Content: f.Content, Model: cfg.Model, ModelVersion: cfg.ModelVersion, Dimension: cfg.Dimension, SchemaVersion: cfg.SchemaVersion}
	embedder, err := vector.NewDeterministicEmbedder(cfg.Model, cfg.ModelVersion, cfg.Dimension, 1024)
	if err != nil {
		t.Fatal(err)
	}
	embedding, err := embedder.Embed(context.Background(), f.Content)
	if err != nil {
		t.Fatal(err)
	}
	request, err := vector.BuildUpsertRequest(tenant.WithContext(context.Background(), tc), source, embedding)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Upsert(tenant.WithContext(context.Background(), tc), request); err != nil {
		t.Fatal(err)
	}
}

func (f retrievalMemoryFixture) queryVector(t *testing.T, cfg vector.BackendConfig) []float64 {
	t.Helper()
	embedder, err := vector.NewDeterministicEmbedder(cfg.Model, cfg.ModelVersion, cfg.Dimension, 1024)
	if err != nil {
		t.Fatal(err)
	}
	embedding, err := embedder.Embed(context.Background(), f.Content)
	if err != nil {
		t.Fatal(err)
	}
	return embedding.Values
}

func syntheticRetrievalService(t *testing.T, store vector.VectorStore, hydrator *retrieval.PostgresHydrator, backendCfg vector.BackendConfig) *retrieval.Service {
	t.Helper()
	embedder, err := vector.NewDeterministicEmbedder(backendCfg.Model, backendCfg.ModelVersion, backendCfg.Dimension, 1024)
	if err != nil {
		t.Fatal(err)
	}
	service, err := retrieval.NewService(retrieval.Config{Model: backendCfg.Model, ModelVersion: backendCfg.ModelVersion, SchemaVersion: backendCfg.SchemaVersion, Dimension: backendCfg.Dimension}, retrieval.Dependencies{Store: store, Embedder: embedder, Hydrator: hydrator})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestLocalMilvusRetrievalHydrationEndToEnd(t *testing.T) {
	address, credentials := integrationEndpointAndCredentials(t)
	_, store := integrationStore(t, integrationConfig(), address, credentials)
	pool, hydrator, tc, backendCfg := retrievalStackFixture(t)

	live := retrievalMemoryFixture{TenantID: "tenant-a", MemoryID: "mem-live", Content: "live retrieval memory", Version: 1}
	tombstoned := retrievalMemoryFixture{TenantID: "tenant-a", MemoryID: "mem-dead", Content: "tombstoned retrieval memory", Version: 1, Deleted: true}
	staleRow := retrievalMemoryFixture{TenantID: "tenant-a", MemoryID: "mem-stale", Content: "stale hit memory", Version: 3}
	staleHit := retrievalMemoryFixture{TenantID: "tenant-a", MemoryID: "mem-stale", Content: "stale hit memory", Version: 2}
	other := retrievalMemoryFixture{TenantID: "tenant-b", MemoryID: "mem-other", Content: "tenant b retrieval memory", Version: 1}

	live.insert(t, pool)
	tombstoned.insert(t, pool)
	staleRow.insert(t, pool)
	other.insert(t, pool)
	live.upsertVector(t, store, backendCfg)
	tombstoned.upsertVector(t, store, backendCfg)
	staleHit.upsertVector(t, store, backendCfg)
	other.upsertVector(t, store, backendCfg)

	service := syntheticRetrievalService(t, store, hydrator, backendCfg)
	tcA := tc
	ctxA := tenant.WithContext(context.Background(), tcA)

	results, err := service.Retrieve(ctxA, tcA, retrieval.Request{Query: live.Content, TopK: 5, MinScore: 0.95})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].MemoryID != live.MemoryID || results[0].Content != live.Content || results[0].SourceVersion != 1 {
		t.Fatalf("unexpected live results: %+v", results)
	}

	results, err = service.Retrieve(ctxA, tcA, retrieval.Request{Query: tombstoned.Content, TopK: 5, MinScore: 0.95})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("tombstoned source hydrated: %+v", results)
	}

	results, err = service.Retrieve(ctxA, tcA, retrieval.Request{Query: staleHit.Content, TopK: 5, MinScore: 0.95})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("stale hit hydrated: %+v", results)
	}

	results, err = service.Retrieve(ctxA, tcA, retrieval.Request{Query: other.Content, TopK: 5, MinScore: 0.95})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("tenant-b content surfaced to tenant-a: %+v", results)
	}
}

func TestLocalMilvusRetrievalClosedStoreFailsSafely(t *testing.T) {
	address, credentials := integrationEndpointAndCredentials(t)
	_, store := integrationStore(t, integrationConfig(), address, credentials)
	_, hydrator, tc, backendCfg := retrievalStackFixture(t)
	service := syntheticRetrievalService(t, store, hydrator, backendCfg)
	ctx := tenant.WithContext(context.Background(), tc)
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Retrieve(ctx, tc, retrieval.Request{Query: "anything", TopK: 1}); err == nil {
		t.Fatal("closed backend retrieval returned success")
	}
}
