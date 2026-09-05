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
	redisstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/task"
)

func workerStackFixture(t *testing.T) (*pgxpool.Pool, *task.PostgresRepository, *redisstore.Store, tenant.TenantContext) {
	t.Helper()
	pgURL := os.Getenv("TEST_DATABASE_URL")
	redisURL := os.Getenv("TEST_REDIS_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if pgURL == "" || redisURL == "" {
		lab := testinfra.NewDockerLab(t)
		lab.Start(ctx)
		lab.WaitHealthy(ctx)
		if pgURL == "" {
			pgURL = lab.PostgresURL(ctx)
		}
		if redisURL == "" {
			redisURL = lab.RedisURL(ctx)
		}
	}
	base, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: pgURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	schema := "p106c_milvus_" + strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))
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
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ('tenant-a', 'milvus worker integration', 'active', 1)`); err != nil {
		t.Fatal(err)
	}
	repo, err := task.NewPostgresRepository(pool, task.RepositoryConfig{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	rb, err := redisstore.NewBackend(redisstore.Config{URL: redisURL, KeyPrefix: "p106c-milvus-" + fmt.Sprintf("%x", time.Now().UnixNano()), SessionLeaseTTL: 10 * time.Second, RenewInterval: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	leases := redisstore.NewStore(rb)
	tc := tenant.TenantContext{TenantID: "tenant-a", AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
	t.Cleanup(func() {
		_ = rb.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := migrator.Down(cleanupCtx, migrationCount); err != nil {
			t.Logf("migration cleanup failed: %v", err)
		}
		pool.Close()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
	})
	return pool, repo, leases, tc
}

func waitForTaskTerminal(t *testing.T, repo *task.PostgresRepository, tc tenant.TenantContext, taskID string, want task.State) task.Task {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		current, err := repo.Get(context.Background(), tc, taskID)
		if err == nil {
			if current.State == want {
				return current
			}
			if current.State.Terminal() {
				t.Fatalf("task reached terminal state %s (%s) instead of %s", current.State, current.LastError, want)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("worker did not reach terminal state in time")
	return task.Task{}
}

func TestLocalMilvusWorkerUpsertAndDeleteEndToEnd(t *testing.T) {
	address, credentials := integrationEndpointAndCredentials(t)
	cfg := integrationConfig()
	_, store := integrationStore(t, cfg, address, credentials)
	_, repo, leases, tc := workerStackFixture(t)
	ctx := tenant.WithContext(context.Background(), tc)

	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: "milvus-e2e-1", ProjectionScope: "memory:user", SourceVersion: 1, SourceSequence: 1, Content: "milvus worker integration content", Model: cfg.Model, ModelVersion: cfg.ModelVersion, Dimension: cfg.Dimension, SchemaVersion: cfg.SchemaVersion}
	upsertRef, err := vector.BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := repo.Enqueue(ctx, tc, upsertRef, time.Now().UTC())
	if err != nil || !enqueued.Created {
		t.Fatalf("enqueue upsert: created=%v err=%v", enqueued.Created, err)
	}
	embedder, err := vector.NewDeterministicEmbedder(cfg.Model, cfg.ModelVersion, cfg.Dimension, 1024)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := task.NewWorker(task.Config{WorkerID: "milvus-worker", Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseTTL: 10 * time.Second, LeaseRenewInterval: 3 * time.Second, TaskTimeout: 30 * time.Second, ShutdownTimeout: 5 * time.Second}, task.Dependencies{Repository: repo, Leases: leases, Store: store, Embedder: embedder, Source: task.SourceProjectorFunc(func(_ context.Context, _ tenant.TenantContext, t task.Task) (vector.SourceDocument, error) {
		if t.Operation == vector.OperationDelete {
			deleted := source
			deleted.Deleted = true
			deleted.Content = ""
			return deleted, nil
		}
		return source, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.Start(); err != nil {
		t.Fatal(err)
	}
	completed := waitForTaskTerminal(t, repo, tc, enqueued.Task.TaskID, task.StateSucceeded)
	if completed.Attempt != 1 {
		t.Fatalf("unexpected attempt count: %d", completed.Attempt)
	}
	embedding, err := embedder.Embed(ctx, source.Content)
	if err != nil {
		t.Fatal(err)
	}
	results, err := store.Search(ctx, vector.SearchRequest{Query: embedding.Values, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, result := range results {
		if result.Ref.DocumentID == upsertRef.DocumentID {
			found = true
		}
	}
	if !found {
		t.Fatalf("upserted document missing from search results: %d results", len(results))
	}

	deleted := source
	deleted.Deleted = true
	deleted.Content = ""
	deleteRef, err := vector.BuildDocumentRef(ctx, deleted)
	if err != nil {
		t.Fatal(err)
	}
	tombstone, err := repo.Enqueue(ctx, tc, deleteRef, time.Now().UTC())
	if err != nil || !tombstone.Created {
		t.Fatalf("enqueue delete: created=%v err=%v", tombstone.Created, err)
	}
	_ = waitForTaskTerminal(t, repo, tc, tombstone.Task.TaskID, task.StateSucceeded)
	results, err = store.Search(ctx, vector.SearchRequest{Query: embedding.Values, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Ref.DocumentID == upsertRef.DocumentID {
			t.Fatal("tombstoned document still returned by search")
		}
	}
	if err = worker.Stop(); err != nil {
		t.Fatal(err)
	}
	counts, err := repo.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[task.StateSucceeded] != 2 {
		t.Fatalf("unexpected counts: %+v", counts)
	}
}
