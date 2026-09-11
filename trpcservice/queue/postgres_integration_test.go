package queue

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/store"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const queueWorkerPostgresEnv = "TEST_QUEUE_WORKER_POSTGRES_DSN"

type failFirstPostgresInboxCompletion struct {
	*store.Postgres
	failures atomic.Int32
}

func (s *failFirstPostgresInboxCompletion) CompleteInboxBatch(
	ctx context.Context,
	inboxID, owner string,
	outboxes []store.OutboxRecord,
	now time.Time,
) error {
	if s.failures.Add(-1) >= 0 {
		return errors.New("injected PostgreSQL Inbox completion failure")
	}
	return s.Postgres.CompleteInboxBatch(ctx, inboxID, owner, outboxes, now)
}

func queueWorkerIntegrationConfig(tenantID string) (config.TenantConfig, config.ChannelConfig) {
	tenantConfig := config.TenantConfig{
		TenantID: tenantID, Version: "v1", Enabled: true,
		App:   config.AppConfig{Name: "assistant", AgentName: "mock-agent"},
		Model: config.ModelConfig{Provider: "mock", Name: "mock"},
		Tools: config.ToolPolicy{Allow: []string{"calculator", "current_time"}},
		Data: config.DataConfig{
			Session: config.BackendConfig{Type: "sql", DSNEnv: queueWorkerPostgresEnv},
			Memory:  config.BackendConfig{Type: "inmemory"}, Summary: config.BackendConfig{Type: "disabled"},
			Artifact: config.BackendConfig{Type: "inmemory"}, Knowledge: config.BackendConfig{Type: "disabled"},
			AuditLog: config.BackendConfig{Type: "stdout"},
		},
		Budget: config.BudgetPolicy{RequestsPerMinute: 1000, MaxInputChars: 10000},
	}
	binding := config.ChannelConfig{
		Type: "telegram", BindingID: "atomic-bot", Enabled: true, MaxMessageLength: 4096,
	}
	return tenantConfig, binding
}

func newQueueWorkerIntegrationService(
	runtimes *agentruntime.Manager,
	coordinator coordination.Coordinator,
) *worker.Service {
	return worker.NewService(
		runtimes, coordinator, channels.NewRegistry(channels.NewTelegram(nil)),
		governance.NewFilter(), nil, metrics.NewMetrics(),
		worker.Options{LockTTL: time.Minute, DedupTTL: time.Hour, RunTimeout: 5 * time.Second},
	)
}

func isolatedQueueWorkerPostgres(t *testing.T) (string, *pgxpool.Pool) {
	t.Helper()
	baseDSN := os.Getenv("TEST_POSTGRES_DSN")
	if baseDSN == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	schema := "queue_worker_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create integration schema: %v", err)
	}
	poolConfig, err := pgxpool.ParseConfig(baseDSN)
	if err != nil {
		admin.Close()
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	verifyPool, err := pgxpool.NewWithConfig(ctx, poolConfig.Copy())
	if err != nil {
		admin.Close()
		t.Fatalf("open verification pool: %v", err)
	}
	t.Cleanup(func() {
		verifyPool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop integration schema: %v", err)
		}
		admin.Close()
	})
	parsedDSN, err := url.Parse(baseDSN)
	if err != nil || (parsedDSN.Scheme != "postgres" && parsedDSN.Scheme != "postgresql") {
		t.Fatalf("TEST_POSTGRES_DSN must be a PostgreSQL URI for isolated queue tests")
	}
	query := parsedDSN.Query()
	query.Set("search_path", schema)
	parsedDSN.RawQuery = query.Encode()
	return parsedDSN.String(), verifyPool
}

// This is the release-gate path that lower-level transaction tests cannot
// prove: Queue and Session use independent pgx pools, the real Worker/Manager
// executes a mock Agent turn, and the Session transaction calls back into the
// Queue store to finish Inbox and Outbox in one commit.
func TestPostgresIntegrationDurableWorkerStrictSessionAtomicCommit(t *testing.T) {
	dsn, verifyPool := isolatedQueueWorkerPostgres(t)
	t.Setenv(queueWorkerPostgresEnv, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueStore, err := store.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := queueStore.EnsureSchema(ctx); err != nil {
		_ = queueStore.Close()
		t.Fatal(err)
	}
	runtimes := agentruntime.NewManager()
	coordinator := coordination.NewInMemory()
	workerService := newQueueWorkerIntegrationService(runtimes, coordinator)
	tenantConfig, binding := queueWorkerIntegrationConfig("queue-worker-atomic")
	durable := NewDurable(
		queueStore, workerService,
		fakeResolver{binding: tenant.Binding{Tenant: tenantConfig, Channel: binding}},
		workerService, metrics.NewMetrics(),
		DurableOptions{PollInterval: time.Millisecond, LeaseTTL: time.Minute, BatchSize: 1, WorkerCount: 1},
	)
	t.Cleanup(func() {
		_ = durable.Close()
		_ = runtimes.Close()
		_ = coordinator.Close()
	})
	task := worker.Task{
		Tenant: tenantConfig, Binding: binding,
		Message: domain.InboundMessage{
			ExternalMessageID: "atomic-" + uuid.NewString(), ExternalUserID: "user-1",
			ConversationID: "conversation-1", ReplyTarget: "chat-1",
			Scope: domain.ScopeDirect, Text: "hello", ReceivedAt: time.Now().UTC(),
		},
	}
	if err := durable.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	if !durable.relayOnce(ctx) {
		t.Fatal("durable relay did not lease the submitted Inbox")
	}

	principalID, sessionID := domain.Identity(domain.InboundMessage{
		TenantID: tenantConfig.TenantID, BindingID: binding.BindingID, Channel: binding.Type,
		ExternalMessageID: task.Message.ExternalMessageID, ExternalUserID: task.Message.ExternalUserID,
		ConversationID: task.Message.ConversationID, ReplyTarget: task.Message.ReplyTarget,
		Scope: task.Message.Scope, Text: task.Message.Text,
	}, tenantConfig.App.Name)
	appNamespace := domain.AppNamespace(tenantConfig.TenantID, tenantConfig.App.Name)
	var (
		inboxStatus, atomicMode, databaseIdentity string
		pipelineVersion, sessionVersion           int
		outboxCount, eventCount                   int
	)
	if err := verifyPool.QueryRow(ctx, `
		SELECT status, pipeline_schema_version, atomic_commit_mode, database_identity
		FROM runtime_inbox WHERE external_message_id = $1`, task.Message.ExternalMessageID).
		Scan(&inboxStatus, &pipelineVersion, &atomicMode, &databaseIdentity); err != nil {
		t.Fatal(err)
	}
	if err := verifyPool.QueryRow(ctx, `
		SELECT version,
			(SELECT count(*) FROM session_turn_events
			 WHERE app_name=$1 AND user_id=$2 AND session_id=$3),
			(SELECT count(*) FROM runtime_outbox)
		FROM session_turn_sessions
		WHERE app_name=$1 AND user_id=$2 AND session_id=$3`,
		appNamespace, principalID, sessionID).Scan(&sessionVersion, &eventCount, &outboxCount); err != nil {
		t.Fatal(err)
	}
	if inboxStatus != store.InboxProcessed || pipelineVersion != worker.DurablePipelineVersion ||
		atomicMode != string(worker.AtomicCommitRequired) || databaseIdentity == "" ||
		sessionVersion != 1 || eventCount == 0 || outboxCount != 1 {
		t.Fatalf("atomic pipeline snapshot: inbox=%s pipeline=%d mode=%s identity=%t session=%d events=%d outbox=%d",
			inboxStatus, pipelineVersion, atomicMode, databaseIdentity != "", sessionVersion, eventCount, outboxCount)
	}
}

// A raw pre-v2 SQL row cannot be upgraded in place to the combined commit:
// its Session transaction may commit before the legacy Inbox transaction. A
// process restart must therefore recover the canonical reply from PostgreSQL,
// not depend on the first process's coordinator cache or rerun the Agent.
func TestPostgresIntegrationRawLegacyStrictSessionReplayAfterInboxFailure(t *testing.T) {
	dsn, verifyPool := isolatedQueueWorkerPostgres(t)
	t.Setenv(queueWorkerPostgresEnv, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queueStore, err := store.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := queueStore.EnsureSchema(ctx); err != nil {
		_ = queueStore.Close()
		t.Fatal(err)
	}
	failingStore := &failFirstPostgresInboxCompletion{Postgres: queueStore}
	failingStore.failures.Store(1)
	tenantConfig, binding := queueWorkerIntegrationConfig("queue-worker-legacy-replay")

	firstRuntimes := agentruntime.NewManager()
	firstCoordinator := coordination.NewInMemory()
	firstWorker := newQueueWorkerIntegrationService(firstRuntimes, firstCoordinator)
	durable := NewDurable(
		failingStore, firstWorker,
		fakeResolver{binding: tenant.Binding{Tenant: tenantConfig, Channel: binding}},
		firstWorker, metrics.NewMetrics(),
		DurableOptions{
			PollInterval: time.Millisecond, LeaseTTL: time.Minute, BatchSize: 1,
			WorkerCount: 1, RetryBase: time.Millisecond, RetryMax: time.Millisecond,
			InboxMaxAttempts: 3,
		},
	)
	t.Cleanup(func() {
		_ = durable.Close()
		_ = firstRuntimes.Close()
		_ = firstCoordinator.Close()
	})
	task := worker.Task{
		Tenant: tenantConfig, Binding: binding,
		Message: domain.InboundMessage{
			ExternalMessageID: "legacy-" + uuid.NewString(), ExternalUserID: "user-legacy",
			ConversationID: "conversation-legacy", ReplyTarget: "chat-legacy",
			Scope: domain.ScopeDirect, Text: "hello legacy", ReceivedAt: time.Now().UTC(),
		},
	}
	insertRawLegacyTask(t, failingStore, task)
	if !durable.relayOnce(ctx) {
		t.Fatal("first legacy relay did not lease the Inbox")
	}

	var firstStatus string
	var firstAttempts, firstSessionVersion, firstOutboxes int
	principalID, sessionID := domain.Identity(domain.InboundMessage{
		TenantID: tenantConfig.TenantID, BindingID: binding.BindingID, Channel: binding.Type,
		ExternalMessageID: task.Message.ExternalMessageID, ExternalUserID: task.Message.ExternalUserID,
		ConversationID: task.Message.ConversationID, ReplyTarget: task.Message.ReplyTarget,
		Scope: task.Message.Scope, Text: task.Message.Text,
	}, tenantConfig.App.Name)
	appNamespace := domain.AppNamespace(tenantConfig.TenantID, tenantConfig.App.Name)
	if err := verifyPool.QueryRow(ctx, `
		SELECT status, attempt_count,
			(SELECT version FROM session_turn_sessions
			 WHERE app_name=$2 AND user_id=$3 AND session_id=$4),
			(SELECT count(*) FROM runtime_outbox)
		FROM runtime_inbox WHERE external_message_id=$1`,
		task.Message.ExternalMessageID, appNamespace, principalID, sessionID).
		Scan(&firstStatus, &firstAttempts, &firstSessionVersion, &firstOutboxes); err != nil {
		t.Fatal(err)
	}
	if firstStatus != store.InboxRetry || firstAttempts != 1 || firstSessionVersion != 1 || firstOutboxes != 0 {
		t.Fatalf("first legacy failure snapshot: status=%s attempts=%d session=%d outboxes=%d",
			firstStatus, firstAttempts, firstSessionVersion, firstOutboxes)
	}

	// Discard both process-local caches before retrying. The replacement Worker
	// must reopen the SQL runtime and obtain its result from session_turns.
	if err := firstRuntimes.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstCoordinator.Close(); err != nil {
		t.Fatal(err)
	}
	secondRuntimes := agentruntime.NewManager()
	secondCoordinator := coordination.NewInMemory()
	secondWorker := newQueueWorkerIntegrationService(secondRuntimes, secondCoordinator)
	t.Cleanup(func() {
		_ = secondRuntimes.Close()
		_ = secondCoordinator.Close()
	})
	durable.processor = secondWorker
	durable.deliverer = secondWorker
	time.Sleep(3 * time.Millisecond)
	if !durable.relayOnce(ctx) {
		t.Fatal("replacement Worker did not lease the retryable legacy Inbox")
	}

	var (
		status, atomicMode, databaseIdentity string
		pipelineVersion, attempts            int
		sessionVersion, turnCount            int
		outboxCount, eventCount              int
	)
	if err := verifyPool.QueryRow(ctx, `
		SELECT status, attempt_count, pipeline_schema_version, atomic_commit_mode, database_identity
		FROM runtime_inbox WHERE external_message_id=$1`, task.Message.ExternalMessageID).
		Scan(&status, &attempts, &pipelineVersion, &atomicMode, &databaseIdentity); err != nil {
		t.Fatal(err)
	}
	if err := verifyPool.QueryRow(ctx, `
		SELECT version,
			(SELECT count(*) FROM session_turns
			 WHERE app_name=$1 AND user_id=$2 AND session_id=$3),
			(SELECT count(*) FROM session_turn_events
			 WHERE app_name=$1 AND user_id=$2 AND session_id=$3),
			(SELECT count(*) FROM runtime_outbox)
		FROM session_turn_sessions
		WHERE app_name=$1 AND user_id=$2 AND session_id=$3`,
		appNamespace, principalID, sessionID).
		Scan(&sessionVersion, &turnCount, &eventCount, &outboxCount); err != nil {
		t.Fatal(err)
	}
	if status != store.InboxProcessed || attempts != 2 || pipelineVersion != 0 || atomicMode != "" ||
		databaseIdentity != "" || sessionVersion != 1 || turnCount != 1 || eventCount == 0 || outboxCount != 1 {
		t.Fatalf("legacy replay snapshot: status=%s attempts=%d pipeline=%d mode=%q identity=%q session=%d turns=%d events=%d outboxes=%d",
			status, attempts, pipelineVersion, atomicMode, databaseIdentity,
			sessionVersion, turnCount, eventCount, outboxCount)
	}
}
