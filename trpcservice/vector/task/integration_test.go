//go:build integration

package task

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	redisstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/redis/go-redis/v9"
)

func integrationTaskFixture(t *testing.T) (*pgxpool.Pool, *PostgresRepository, storage.LeaseStore, tenant.TenantContext, vector.SourceDocument, func()) {
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
	schema := "p106c_task_" + strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))
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
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ('tenant-a', 'task integration', 'active', 1)`); err != nil {
		t.Fatal(err)
	}
	repo, err := NewPostgresRepository(pool, RepositoryConfig{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	prefix := "p106c-task-" + fmt.Sprintf("%x", time.Now().UnixNano())
	rb, err := redisstore.NewBackend(redisstore.Config{URL: redisURL, KeyPrefix: prefix, SessionLeaseTTL: 3 * time.Second, RenewInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	leases := redisstore.NewStore(rb)
	tc := tenant.TenantContext{TenantID: "tenant-a", AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: "memory-a", ProjectionScope: "memory:user", SourceVersion: 1, SourceSequence: 1, Content: "integration content", Model: "test-model", ModelVersion: "v1", Dimension: 8, SchemaVersion: "schema-v1"}
	cleanup := func() {
		_ = rb.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := migrator.Down(cleanupCtx, migrationCount); err != nil {
			t.Logf("migration cleanup failed: %v", err)
		}
		pool.Close()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		client, parseErr := redis.ParseURL(redisURL)
		if parseErr == nil {
			cleanClient := redis.NewClient(client)
			defer cleanClient.Close()
			var cursor uint64
			for {
				keys, next, scanErr := cleanClient.Scan(cleanupCtx, cursor, prefix+":*", 100).Result()
				if scanErr != nil {
					break
				}
				if len(keys) > 0 {
					_, _ = cleanClient.Del(cleanupCtx, keys...).Result()
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
		}
	}
	t.Cleanup(cleanup)
	return pool, repo, leases, tc, source, cleanup
}

func TestPostgresRedisVectorTaskLifecycle(t *testing.T) {
	_, repo, leases, tc, source, _ := integrationTaskFixture(t)
	ctx := tenant.WithContext(context.Background(), tc)
	ref, err := vector.BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	enqueued, err := repo.Enqueue(ctx, tc, ref, now)
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued.Created {
		t.Fatal("first enqueue was not created")
	}
	duplicate, err := repo.Enqueue(ctx, tc, ref, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Created || duplicate.Task.TaskID != enqueued.Task.TaskID {
		t.Fatalf("deduplication failed: %+v", duplicate)
	}
	candidate, err := repo.Candidate(ctx, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := leases.Acquire(ctx, tc, candidate.TaskID, "worker-a", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	guard := LeaseRef{Owner: lease.OwnerID, Epoch: lease.Epoch, Fence: lease.FenceToken, ExpiresAt: lease.ExpiresAt}
	claimed, err := repo.Claim(ctx, candidate, guard, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if claimed.State != StateRunning || claimed.Attempt != 1 {
		t.Fatalf("unexpected claim: %+v", claimed)
	}
	if _, err = repo.Complete(ctx, candidate, guard, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	head, ok, err := repo.Head(ctx, tc.TenantID, ref.DocumentID)
	if err != nil || !ok {
		t.Fatalf("head=%+v ok=%v err=%v", head, ok, err)
	}
	if head.Version != ref.SourceVersion || head.Operation != vector.OperationUpsert {
		t.Fatalf("unexpected head: %+v", head)
	}
	if err = leases.Release(ctx, tc, lease); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRedisVectorTaskFencesExpiredOwner(t *testing.T) {
	_, repo, leases, tc, source, _ := integrationTaskFixture(t)
	ctx := tenant.WithContext(context.Background(), tc)
	ref, err := vector.BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Enqueue(ctx, tc, ref, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	candidate, err := repo.Candidate(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := leases.Acquire(ctx, tc, candidate.TaskID, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	oldGuard := LeaseRef{Owner: oldLease.OwnerID, Epoch: oldLease.Epoch, Fence: oldLease.FenceToken, ExpiresAt: oldLease.ExpiresAt}
	if _, err = repo.Claim(ctx, candidate, oldGuard, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	newLease, err := leases.Acquire(ctx, tc, candidate.TaskID, "worker-b", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	newGuard := LeaseRef{Owner: newLease.OwnerID, Epoch: newLease.Epoch, Fence: newLease.FenceToken, ExpiresAt: newLease.ExpiresAt}
	if _, err = repo.Claim(ctx, candidate, newGuard, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Complete(ctx, candidate, oldGuard, time.Now().UTC()); err == nil {
		t.Fatal("expired owner completed task")
	}
	if _, err = repo.Fail(ctx, candidate, newGuard, Failure{Kind: FailureRetry, Category: CategoryUnavailable, NextAttempt: time.Now().Add(time.Second)}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	_ = leases.Release(ctx, tc, newLease)
}

func TestPostgresRedisVectorTaskExhaustedRetryDeadLetters(t *testing.T) {
	pool, repo, leases, tc, source, _ := integrationTaskFixture(t)
	strict, err := NewPostgresRepository(pool, RepositoryConfig{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx := tenant.WithContext(context.Background(), tc)
	ref, err := vector.BuildDocumentRef(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := strict.Enqueue(ctx, tc, ref, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !enqueued.Created {
		t.Fatal("expected created task")
	}
	candidate, err := strict.Candidate(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := leases.Acquire(ctx, tc, candidate.TaskID, "worker-a", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	guard := LeaseRef{Owner: lease.OwnerID, Epoch: lease.Epoch, Fence: lease.FenceToken, ExpiresAt: lease.ExpiresAt}
	if _, err = strict.Claim(ctx, candidate, guard, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	failed, err := strict.Fail(ctx, candidate, guard, Failure{Kind: FailureRetry, Category: CategoryUnavailable, NextAttempt: time.Now().Add(time.Second)}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != StateDeadLetter || failed.DeadLetteredAt.IsZero() {
		t.Fatalf("expected dead letter, got state=%s deadLetteredAt=%v", failed.State, failed.DeadLetteredAt)
	}
	if _, err = strict.Candidate(ctx, time.Now().Add(2*time.Second)); !errIsNotFound(err) {
		t.Fatalf("dead-letter task remained claimable: %v", err)
	}
	counts, err := repo.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts[StateDeadLetter] != 1 {
		t.Fatalf("unexpected counts: %+v", counts)
	}
	_ = leases.Release(ctx, tc, lease)
}

func errIsNotFound(err error) bool { return errors.Is(err, ErrNotFound) }
