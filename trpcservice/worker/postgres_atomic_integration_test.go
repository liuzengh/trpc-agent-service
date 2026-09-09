package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	postgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type postgresAtomicWorkerFixture struct {
	pool        *pgxpool.Pool
	base        *pgxpool.Pool
	queue       *queue.PostgresQueue
	store       *postgres.CoordinationStore
	repository  *postgres.ExecutionResultRepository
	coordinator *postgres.AtomicCompletionCoordinator
	ctx         context.Context
	cancel      context.CancelFunc
	tenant      tenant.TenantContext
	schema      string
}

func newPostgresAtomicWorkerFixture(t *testing.T) *postgresAtomicWorkerFixture {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; PostgreSQL atomic Worker not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	cfg := postgres.PostgresConfig{URL: url, MaxConns: 8, MinConns: 1}
	base, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p009c_worker_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	migrator, err := postgres.NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../migrations")))
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	tc := tenant.TenantContext{
		TenantID: "tenant-worker", AgentAppID: "agent-worker", BindingID: "binding-worker", Channel: "web",
		ExternalUser: "worker-user", ExternalChat: "worker-chat",
		RequestID: "request-worker", MessageID: "message-worker", TraceID: "trace-worker", ConfigVersion: 1,
		BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
	}
	if _, err := pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name) VALUES ($1, $1)`, tc.TenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ($1, $2, 'worker test')`, tc.TenantID, tc.AgentAppID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ($1, $2, $3, 'worker-test')`, tc.TenantID, tc.Channel, tc.BindingID); err != nil {
		t.Fatal(err)
	}
	sessionID := "session-worker"
	if _, err := pool.Exec(ctx, `INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user) VALUES ($1,$2,$3,1,$4,$5,'worker-chat','worker-user')`, tc.TenantID, sessionID, tc.AgentAppID, tc.Channel, tc.BindingID); err != nil {
		t.Fatal(err)
	}
	tc.SessionID = sessionID
	q, err := queue.NewPostgresQueue(pool, queue.PostgresQueueConfig{
		MaxJobAge: 2 * time.Hour, MaxVisibilityExtension: 2 * time.Hour, PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := postgres.NewCoordinationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := postgres.NewExecutionResultRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := postgres.NewAtomicCompletionCoordinator(pool)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &postgresAtomicWorkerFixture{
		pool: pool, base: base, queue: q, store: store, repository: repository, coordinator: coordinator,
		ctx: ctx, cancel: cancel, tenant: tc, schema: schema,
	}
	t.Cleanup(func() {
		_ = q.Close()
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	})
	return fixture
}

func waitForWorkerQueueState(t *testing.T, f *postgresAtomicWorkerFixture, jobID, expected string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var status string
		err := f.pool.QueryRow(ctx, `SELECT status FROM job_queue WHERE tenant_id=$1 AND job_id=$2`, f.tenant.TenantID, jobID).Scan(&status)
		if err != nil {
			t.Fatal(err)
		}
		if status == expected {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("queue status=%s, expected=%s", status, expected)
		case <-ticker.C:
		}
	}
}

func atomicWorkerConfig(job queue.AgentJob, owner string, runtime testAgents) Config {
	return Config{
		WorkerID: owner, Concurrency: 1, VisibilityTimeout: 2 * time.Second, LeaseTTL: 2 * time.Second,
		CleanupTimeout: 100 * time.Millisecond, RetryDelay: 5 * time.Second,
		ResolveAgent: AgentResolverFunc(func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error) {
			return agent.AgentSpec{TenantID: job.Tenant.TenantID, AgentAppID: job.Agent.AgentAppID, Version: job.Agent.Version, Name: "assistant", ModelProvider: "fake"}, nil
		}),
	}
}

func stopPostgresAtomicWorker(t *testing.T, w *Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.Stop(ctx); err != nil && !errors.Is(err, ErrDrainTimeout) {
		t.Fatal(err)
	}
}

func TestPostgresWorkerUsesAtomicCompletionWithoutIndependentCommitOrAck(t *testing.T) {
	f := newPostgresAtomicWorkerFixture(t)
	job := workerJob(t)
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	sink := &testSink{}
	executor, err := execution.NewExecutor(f.store, testAgents{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWithAtomicCompletion(f.queue, executor, atomicWorkerConfig(job, "worker-pg-atomic", testAgents{}), f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForWorkerQueueState(t, f, job.JobID, "acked")
	if sink.Count() != 0 {
		t.Fatalf("legacy execution sink commits=%d", sink.Count())
	}
	result, err := f.repository.GetExecutionResult(f.ctx, tenant.TenantContext{TenantID: f.tenant.TenantID}, job.JobID, job.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.OwnerID != "worker-pg-atomic" || result.TenantID != f.tenant.TenantID || result.SessionID != f.tenant.SessionID {
		t.Fatalf("worker result=%+v", result)
	}
	var status string
	var attempt int
	var lockedBy *string
	var aggregateID, dedupKey string
	var payload []byte
	if err := f.pool.QueryRow(f.ctx, `
SELECT status, attempt, locked_by, aggregate_id, dedup_key, payload
FROM outbox_message WHERE tenant_id=$1 AND outbox_id=$2`, f.tenant.TenantID, "reply-"+job.ExecutionID).Scan(&status, &attempt, &lockedBy, &aggregateID, &dedupKey, &payload); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempt != 1 || lockedBy != nil || aggregateID != job.ExecutionID || dedupKey != f.tenant.TenantID+"|"+job.ExecutionID+"|agent.reply" || len(payload) == 0 {
		t.Fatalf("worker reply outbox status=%s attempt=%d locked_by=%v aggregate=%s dedup=%s payload=%s", status, attempt, lockedBy, aggregateID, dedupKey, payload)
	}
	outboxRepository, err := postgres.NewOutboxRepository(f.pool, postgres.OutboxRepositoryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := outboxRepository.ClaimBatch(f.ctx, f.tenant, "dispatcher-worker", 1)
	if err != nil || len(claimed) != 1 || claimed[0].ID != "reply-"+job.ExecutionID {
		t.Fatalf("dispatcher could not claim committed reply: %v %+v", err, claimed)
	}
	if err := outboxRepository.MarkCompleted(f.ctx, f.tenant, "dispatcher-worker", claimed[0].ID); err != nil {
		t.Fatal(err)
	}
	stopPostgresAtomicWorker(t, w)
}

func TestPostgresWorkerAtomicCompletionFailureDoesNotAckOrCommit(t *testing.T) {
	f := newPostgresAtomicWorkerFixture(t)
	job := workerJob(t)
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	sink := &testSink{}
	executor, err := execution.NewExecutor(f.store, testAgents{err: agent.ErrProviderFailure}, sink)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWithAtomicCompletion(f.queue, executor, atomicWorkerConfig(job, "worker-pg-failure", testAgents{err: agent.ErrProviderFailure}), f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForWorkerQueueState(t, f, job.JobID, "queued")
	if sink.Count() != 0 {
		t.Fatalf("failure path legacy commits=%d", sink.Count())
	}
	if count := completionResultCountForWorker(t, f, job); count != 0 {
		t.Fatalf("failure path result rows=%d", count)
	}
	if count := completionOutboxCountForWorker(t, f, job); count != 0 {
		t.Fatalf("failure path outbox rows=%d", count)
	}
	stopPostgresAtomicWorker(t, w)
}

func completionResultCountForWorker(t *testing.T, f *postgresAtomicWorkerFixture, job queue.AgentJob) int {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM execution_result WHERE tenant_id=$1 AND job_id=$2`, job.Tenant.TenantID, job.JobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func completionOutboxCountForWorker(t *testing.T, f *postgresAtomicWorkerFixture, job queue.AgentJob) int {
	t.Helper()
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox_message WHERE tenant_id=$1 AND aggregate_id=$2`, job.Tenant.TenantID, job.ExecutionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPostgresWorkerFenceLossPreventsAtomicCompletion(t *testing.T) {
	f := newPostgresAtomicWorkerFixture(t)
	job := workerJob(t)
	if _, err := f.queue.Enqueue(f.ctx, job); err != nil {
		t.Fatal(err)
	}
	entered := make(chan agent.AgentInput, 1)
	releaseRuntime := make(chan struct{})
	sink := &testSink{}
	agents := testAgents{inputCh: entered, waitCh: releaseRuntime}
	executor, err := execution.NewExecutor(f.store, agents, sink)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWithAtomicCompletion(f.queue, executor, atomicWorkerConfig(job, "worker-pg-fence", agents), f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not start")
	}
	if _, err := f.store.BumpEpoch(f.ctx, f.tenant.TenantID, f.tenant.SessionID); err != nil {
		t.Fatal(err)
	}
	close(releaseRuntime)
	waitForWorkerQueueState(t, f, job.JobID, "queued")
	if sink.Count() != 0 || completionResultCountForWorker(t, f, job) != 0 || completionOutboxCountForWorker(t, f, job) != 0 {
		t.Fatalf("fence loss committed result=%d outbox=%d legacy=%d", completionResultCountForWorker(t, f, job), completionOutboxCountForWorker(t, f, job), sink.Count())
	}
	stopPostgresAtomicWorker(t, w)
}
