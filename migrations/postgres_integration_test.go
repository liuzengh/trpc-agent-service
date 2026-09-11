package migrations_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

func TestPostgresMigrationsConcurrentApplyIsSerialized(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	var databaseName string
	if err := database.QueryRowContext(context.Background(), "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(databaseName), "test") {
		t.Skipf("TEST_POSTGRES_DSN database %q must contain 'test' before schema reset", databaseName)
	}
	if _, err := database.ExecContext(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
		t.Fatalf("reset public schema: %v", err)
	}

	const replicas = 8
	start := make(chan struct{})
	errorsByReplica := make(chan error, replicas)
	var group sync.WaitGroup
	for replica := 0; replica < replicas; replica++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			errorsByReplica <- migrations.Apply(context.Background(), database)
		}()
	}
	close(start)
	group.Wait()
	close(errorsByReplica)
	for err := range errorsByReplica {
		if err != nil {
			t.Fatalf("concurrent migrations.Apply() error = %v", err)
		}
	}
	var count int
	if err := database.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM schema_migrations WHERE version=1").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("baseline rows = %d, want 1", count)
	}
}

func TestPostgresMigrationsAndStateTransaction(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.PingContext(context.Background()); err != nil {
		t.Fatalf("PingContext() error = %v", err)
	}
	var databaseName string
	if err := database.QueryRowContext(context.Background(), "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("resolve test database name: %v", err)
	}
	if !strings.Contains(strings.ToLower(databaseName), "test") {
		t.Skipf("TEST_POSTGRES_DSN database %q must contain 'test' before schema reset", databaseName)
	}
	if _, err := database.ExecContext(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
		t.Fatalf("reset public schema error = %v", err)
	}

	if err := migrations.Apply(context.Background(), database); err != nil {
		t.Fatalf("first migrations.Apply() error = %v", err)
	}
	if err := migrations.Apply(context.Background(), database); err != nil {
		t.Fatalf("second migrations.Apply() error = %v", err)
	}
	assertTenantRoleAccess(t, database)
	assertFrameworkSessionRLS(t, database)

	var migrationCount int
	if err := database.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM schema_migrations").Scan(&migrationCount); err != nil {
		t.Fatalf("query migration count error = %v", err)
	}
	if migrationCount != 1 {
		t.Fatalf("applied migrations = %d, want 1", migrationCount)
	}
	var baselineChecksum string
	if err := database.QueryRowContext(context.Background(), "SELECT checksum FROM schema_migrations WHERE version=1").Scan(&baselineChecksum); err != nil {
		t.Fatalf("query migration checksum error = %v", err)
	}
	if len(strings.TrimSpace(baselineChecksum)) != 64 {
		t.Fatalf("baseline checksum length = %d, want 64", len(strings.TrimSpace(baselineChecksum)))
	}
	if _, err := database.ExecContext(context.Background(), "UPDATE schema_migrations SET checksum=repeat('0', 64) WHERE version=1"); err != nil {
		t.Fatalf("corrupt migration checksum error = %v", err)
	}
	if err := migrations.Apply(context.Background(), database); err == nil || !strings.Contains(err.Error(), "baseline 000001_init.sql has changed") {
		t.Fatalf("migrations.Apply() after checksum drift = %v, want baseline drift rejection", err)
	}
	if _, err := database.ExecContext(context.Background(), "UPDATE schema_migrations SET checksum=$1 WHERE version=1", baselineChecksum); err != nil {
		t.Fatalf("restore migration checksum error = %v", err)
	}
	if _, err := database.ExecContext(context.Background(), "INSERT INTO schema_migrations (version, checksum) VALUES (2, repeat('0', 64))"); err != nil {
		t.Fatalf("insert unsupported migration version error = %v", err)
	}
	if err := migrations.Apply(context.Background(), database); err == nil || !strings.Contains(err.Error(), "unsupported migration version 2") {
		t.Fatalf("migrations.Apply() with second migration version = %v, want single-baseline rejection", err)
	}
	if _, err := database.ExecContext(context.Background(), "DELETE FROM schema_migrations WHERE version=2"); err != nil {
		t.Fatalf("remove unsupported migration version error = %v", err)
	}

	seedControlPlane(t, database)
	store, err := storage.NewPostgresStateStore(database, []byte("integration-test-audit-key-32bytes"))
	if err != nil {
		t.Fatalf("NewPostgresStateStore() error = %v", err)
	}
	dedup, err := storage.NewPostgresExecutionDedupStore(database)
	if err != nil {
		t.Fatalf("NewPostgresExecutionDedupStore() error = %v", err)
	}
	beginState, err := dedup.Begin(context.Background(), "tenant-a", "support", "telegram", "telegram-bot-a", "update-42", "trace-42", time.Minute)
	if err != nil || beginState != storage.ExecutionFresh {
		t.Fatalf("Begin() state = %v, error = %v, want fresh", beginState, err)
	}
	recordedOutbox, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID:      "tenant-a",
		AppCode:       "support",
		SessionKey:    "tenant-a/support/telegram/chat-1",
		MessageID:     "update-42",
		Channel:       "telegram",
		BindingID:     "telegram-bot-a",
		TraceID:       "trace-42",
		Action:        "agent.reply",
		Result:        "success",
		AuditDetail:   "token=top-secret",
		OutboxType:    "agent.reply.completed",
		OutboxPayload: []byte(`{"message_id":"update-42"}`),
		ModelUsage: &storage.ModelUsage{
			ProviderID: "mixed", ModelName: "mixed", Known: true,
			PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, CostMicros: 9,
			Breakdown: []storage.ModelUsageSegment{
				{ProviderID: "openai-primary", ModelName: "gpt-4o-mini", ReportedModel: "gpt-4o-mini-2026", PromptTokens: 6, CompletionTokens: 3, TotalTokens: 9, CostMicros: 5},
				{ProviderID: "fallback", ModelName: "fallback-model", PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6, CostMicros: 4},
			},
		},
		ExecutionTrace: &storage.AgentExecutionTrace{
			Status: "completed", RootAgentName: "assistant", RootInvocationID: "inv-42",
			Steps: []storage.ExecutionTraceStep{{StepID: "step-42", NodeID: "assistant#model", NodeType: "llm"}},
		},
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	var messageStatus string
	if err := database.QueryRowContext(context.Background(), "SELECT status FROM messages WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND message_id=$4", "tenant-a", "telegram", "telegram-bot-a", "update-42").Scan(&messageStatus); err != nil {
		t.Fatalf("query completed message error = %v", err)
	}
	if messageStatus != "completed" {
		t.Fatalf("message status after completion = %q, want completed", messageStatus)
	}
	var usageProvider, usageModel, usageBreakdown string
	if err := database.QueryRowContext(context.Background(), `
SELECT provider_id, model_name, usage_breakdown::text
FROM model_usage_ledger
WHERE tenant_id=$1 AND channel_type=$2 AND binding_id=$3 AND message_id=$4`,
		"tenant-a", "telegram", "telegram-bot-a", "update-42",
	).Scan(&usageProvider, &usageModel, &usageBreakdown); err != nil {
		t.Fatalf("query model usage breakdown: %v", err)
	}
	if usageProvider != "mixed" || usageModel != "mixed" || !strings.Contains(usageBreakdown, "openai-primary") || !strings.Contains(usageBreakdown, "fallback-model") {
		t.Fatalf("model usage identity = %q/%q breakdown=%s", usageProvider, usageModel, usageBreakdown)
	}
	storedTrace, err := store.GetExecutionTrace(context.Background(), "tenant-a", "telegram", "telegram-bot-a", "update-42")
	if err != nil || storedTrace.Trace.RootInvocationID != "inv-42" || len(storedTrace.Trace.Steps) != 1 {
		t.Fatalf("GetExecutionTrace() = %#v, %v", storedTrace, err)
	}
	var auditDetail string
	if err := database.QueryRowContext(context.Background(), "SELECT redacted_detail FROM audit_events WHERE tenant_id=$1 AND trace_id=$2", "tenant-a", "trace-42").Scan(&auditDetail); err != nil {
		t.Fatalf("query audit detail error = %v", err)
	}
	if !strings.HasPrefix(auditDetail, "hmac-sha256:") || strings.Contains(auditDetail, "top-secret") {
		t.Fatalf("audit detail = %q, want opaque HMAC digest", auditDetail)
	}
	if err := store.MarkOutboxDelivered(context.Background(), "tenant-a", recordedOutbox.ID); err != nil {
		t.Fatalf("MarkOutboxDelivered() error = %v", err)
	}
	if _, err := database.ExecContext(context.Background(), `
UPDATE outbox_events SET delivered_at=NOW()-INTERVAL '8 days'
WHERE tenant_id='tenant-a' AND id=$1`, recordedOutbox.ID); err != nil {
		t.Fatalf("age delivered outbox error = %v", err)
	}
	deletedOutbox, err := store.PurgeDeliveredOutboxBefore(context.Background(), "tenant-a", time.Now().Add(-7*24*time.Hour), 1000)
	if err != nil || deletedOutbox != 1 {
		t.Fatalf("PurgeDeliveredOutboxBefore() = %d, %v; want 1, nil", deletedOutbox, err)
	}
	var retainedOutbox int
	if err := database.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM outbox_events WHERE tenant_id='tenant-a' AND id=$1", recordedOutbox.ID).Scan(&retainedOutbox); err != nil {
		t.Fatalf("read retained outbox count: %v", err)
	}
	if retainedOutbox != 0 {
		t.Fatalf("delivered outbox retained after purge: %d", retainedOutbox)
	}

	// Cross-node takeover: a stale processing claim must be claimable by a
	// second node, and the takeover write must carry the new owner's trace ID.
	beginState, err = dedup.Begin(context.Background(), "tenant-a", "support", "telegram", "telegram-bot-a", "update-takeover", "trace-old", time.Minute)
	if err != nil || beginState != storage.ExecutionFresh {
		t.Fatalf("Begin() takeover claim state = %v, error = %v, want fresh", beginState, err)
	}
	if _, err := database.ExecContext(context.Background(), "UPDATE messages SET updated_at = NOW() - make_interval(secs => 120) WHERE tenant_id = 'tenant-a' AND channel_type = 'telegram' AND binding_id = 'telegram-bot-a' AND message_id = 'update-takeover'"); err != nil {
		t.Fatalf("age takeover claim error = %v", err)
	}
	beginState, err = dedup.Begin(context.Background(), "tenant-a", "support", "telegram", "telegram-bot-a", "update-takeover", "trace-new", time.Minute)
	if err != nil || beginState != storage.ExecutionFresh {
		t.Fatalf("Begin() takeover state = %v, error = %v, want fresh", beginState, err)
	}
	var takeoverTrace string
	if err := database.QueryRowContext(context.Background(), "SELECT trace_id FROM messages WHERE tenant_id='tenant-a' AND channel_type='telegram' AND binding_id='telegram-bot-a' AND message_id='update-takeover'").Scan(&takeoverTrace); err != nil {
		t.Fatalf("query takeover trace error = %v", err)
	}
	if takeoverTrace != "trace-new" {
		t.Fatalf("takeover trace = %q, want trace-new", takeoverTrace)
	}

	beginState, err = dedup.Begin(context.Background(), "tenant-a", "support", "telegram", "telegram-bot-a", "update-rollback", "trace-rollback", time.Minute)
	if err != nil || beginState != storage.ExecutionFresh {
		t.Fatalf("Begin() rollback state = %v, error = %v, want fresh", beginState, err)
	}
	_, err = store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID:      "tenant-a",
		AppCode:       "support",
		SessionKey:    "tenant-a/support/telegram/rollback",
		MessageID:     "update-rollback",
		Channel:       "telegram",
		BindingID:     "telegram-bot-a",
		TraceID:       "trace-rollback",
		Action:        "agent.reply",
		Result:        "success",
		AuditDetail:   "token=must-not-persist",
		OutboxType:    "agent.reply.completed",
		OutboxPayload: []byte("not-json"),
		ExecutionTrace: &storage.AgentExecutionTrace{
			Status: "completed", RootInvocationID: "inv-rollback",
		},
	})
	if err == nil {
		t.Fatal("RecordExecution() with invalid JSON payload error = nil, want failure")
	}
	_, err = store.GetSession(context.Background(), "tenant-a", "tenant-a/support/telegram/rollback")
	if !errors.Is(err, storage.ErrSessionNotFound) {
		t.Fatalf("rolled-back session error = %v, want ErrSessionNotFound", err)
	}
	audits, err := store.ListAudit(context.Background(), "tenant-a", "trace-rollback")
	if err != nil {
		t.Fatalf("ListAudit() error = %v", err)
	}
	if len(audits) != 0 {
		t.Fatalf("rolled-back audit events = %d, want 0", len(audits))
	}
	if _, err := store.GetExecutionTrace(context.Background(), "tenant-a", "telegram", "telegram-bot-a", "update-rollback"); !errors.Is(err, storage.ErrExecutionTraceNotFound) {
		t.Fatalf("rolled-back execution trace error = %v, want ErrExecutionTraceNotFound", err)
	}
	failedTrace := storage.ExecutionTraceRecord{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", BindingID: "telegram-bot-a", MessageID: "update-failed", TraceID: "trace-failed",
		Trace: storage.AgentExecutionTrace{Status: "failed", RootInvocationID: "inv-failed"},
	}
	if err := store.RecordExecutionTrace(context.Background(), failedTrace); err != nil {
		t.Fatalf("RecordExecutionTrace(failed) error = %v", err)
	}
	gotFailedTrace, err := store.GetExecutionTrace(context.Background(), "tenant-a", "telegram", "telegram-bot-a", "update-failed")
	if err != nil || gotFailedTrace.Trace.Status != "failed" {
		t.Fatalf("failed trace = %#v, %v", gotFailedTrace, err)
	}

	var vectorTable, legacyChunkTable sql.NullString
	if err := database.QueryRowContext(context.Background(), "SELECT to_regclass('public.knowledge_vectors')::text").Scan(&vectorTable); err != nil {
		t.Fatalf("resolve framework knowledge vector table: %v", err)
	}
	if !vectorTable.Valid || vectorTable.String != "knowledge_vectors" {
		t.Fatalf("framework knowledge vector table = %#v, want knowledge_vectors", vectorTable)
	}
	if err := database.QueryRowContext(context.Background(), "SELECT to_regclass('public.knowledge_chunks')::text").Scan(&legacyChunkTable); err != nil {
		t.Fatalf("resolve legacy knowledge chunk table: %v", err)
	}
	if legacyChunkTable.Valid {
		t.Fatalf("legacy knowledge_chunks table still exists: %q", legacyChunkTable.String)
	}
	var legacyProjectionColumns int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema='public' AND table_name='knowledge_documents' AND column_name IN ('content','embedding')`).Scan(&legacyProjectionColumns); err != nil {
		t.Fatalf("inspect knowledge management projection: %v", err)
	}
	if legacyProjectionColumns != 0 {
		t.Fatalf("knowledge_documents still exposes %d legacy RAG content/embedding columns", legacyProjectionColumns)
	}
	var frameworkSessionTimestampColumns int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema='public'
  AND table_name IN ('session_states','session_events','session_track_events','session_summaries','app_states','user_states')
  AND column_name IN ('created_at','updated_at','expires_at','deleted_at')
  AND data_type='timestamp with time zone'`).Scan(&frameworkSessionTimestampColumns); err != nil {
		t.Fatalf("inspect framework Session timestamp columns: %v", err)
	}
	if frameworkSessionTimestampColumns != 23 {
		t.Fatalf("framework Session timestamptz columns = %d, want 23", frameworkSessionTimestampColumns)
	}
	var frameworkMemoryColumns, frameworkMemoryIndexes int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema='public' AND table_name='memories'
  AND column_name IN ('memory_id','app_name','user_id','memory_data','created_at','updated_at','deleted_at')`).Scan(&frameworkMemoryColumns); err != nil {
		t.Fatalf("inspect framework Memory columns: %v", err)
	}
	if frameworkMemoryColumns != 7 {
		t.Fatalf("framework Memory columns = %d, want 7", frameworkMemoryColumns)
	}
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM pg_indexes
WHERE schemaname='public' AND tablename='memories'
  AND indexname IN ('idx_memories_app_user','idx_memories_updated_at','idx_memories_deleted_at')`).Scan(&frameworkMemoryIndexes); err != nil {
		t.Fatalf("inspect framework Memory indexes: %v", err)
	}
	if frameworkMemoryIndexes != 3 {
		t.Fatalf("framework Memory indexes = %d, want 3", frameworkMemoryIndexes)
	}
	var frameworkMemoryTimestampColumns int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema='public' AND table_name='memories'
  AND column_name IN ('created_at','updated_at','deleted_at')
  AND data_type='timestamp without time zone'`).Scan(&frameworkMemoryTimestampColumns); err != nil {
		t.Fatalf("inspect framework Memory timestamp types: %v", err)
	}
	if frameworkMemoryTimestampColumns != 3 {
		t.Fatalf("framework Memory timestamp-without-time-zone columns = %d, want 3", frameworkMemoryTimestampColumns)
	}
	var frameworkRLSCount int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM pg_class
WHERE relnamespace='public'::regnamespace
  AND relname IN ('memories','session_states','session_events','session_track_events','session_summaries','app_states','user_states')
  AND relrowsecurity AND relforcerowsecurity`).Scan(&frameworkRLSCount); err != nil {
		t.Fatalf("inspect framework RLS: %v", err)
	}
	if frameworkRLSCount != 7 {
		t.Fatalf("framework RLS tables = %d, want 7", frameworkRLSCount)
	}
	var frameworkRLSPolicies int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM pg_policies
WHERE schemaname='public'
  AND tablename IN ('memories','session_states','session_events','session_track_events','session_summaries','app_states','user_states')
  AND policyname LIKE 'tenant_scope_%'`).Scan(&frameworkRLSPolicies); err != nil {
		t.Fatalf("inspect framework RLS policies: %v", err)
	}
	if frameworkRLSPolicies != 7 {
		t.Fatalf("framework RLS policies = %d, want 7", frameworkRLSPolicies)
	}
	var frameworkVectorEpochColumns int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema='public' AND table_name='knowledge_vectors'
  AND column_name IN ('created_at','updated_at') AND data_type='bigint'`).Scan(&frameworkVectorEpochColumns); err != nil {
		t.Fatalf("inspect framework vector timestamp types: %v", err)
	}
	if frameworkVectorEpochColumns != 2 {
		t.Fatalf("framework vector epoch columns = %d, want 2", frameworkVectorEpochColumns)
	}
	var platformLifecycleColumns int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema='public' AND (
    (table_name='tenants' AND column_name='updated_at') OR
    (table_name='platform_users' AND column_name='updated_at') OR
    (table_name='local_credentials' AND column_name='created_at') OR
    (table_name='tenant_members' AND column_name='updated_at') OR
    (table_name='channel_bindings' AND column_name='updated_at') OR
    (table_name='sessions' AND column_name='created_at') OR
    (table_name='execution_traces' AND column_name='created_at') OR
    (table_name='knowledge_documents' AND column_name='created_at') OR
    (table_name='knowledge_document_sources' AND column_name='created_at')
)`).Scan(&platformLifecycleColumns); err != nil {
		t.Fatalf("inspect platform lifecycle columns: %v", err)
	}
	if platformLifecycleColumns != 9 {
		t.Fatalf("platform lifecycle columns = %d, want 9", platformLifecycleColumns)
	}
	var platformCriticalIndexes int
	if err := database.QueryRowContext(context.Background(), `
SELECT COUNT(*) FROM pg_indexes
WHERE schemaname='public' AND indexname IN (
    'idx_tenant_members_user',
    'idx_messages_tenant_updated',
    'idx_audit_events_purge',
    'idx_channel_identities_lookup',
    'idx_tenant_backend_profiles_profile',
    'idx_outbox_events_delivered_retention'
)`).Scan(&platformCriticalIndexes); err != nil {
		t.Fatalf("inspect platform critical indexes: %v", err)
	}
	if platformCriticalIndexes != 6 {
		t.Fatalf("platform critical indexes = %d, want 6", platformCriticalIndexes)
	}
	var redundantArtifactIndex sql.NullString
	if err := database.QueryRowContext(context.Background(), "SELECT to_regclass('public.idx_artifacts_session')::text").Scan(&redundantArtifactIndex); err != nil {
		t.Fatalf("inspect redundant Artifact index: %v", err)
	}
	if redundantArtifactIndex.Valid {
		t.Fatalf("redundant Artifact index still exists: %q", redundantArtifactIndex.String)
	}
	var sessionMigrationDriverCheck, ingestDriverCheck string
	if err := database.QueryRowContext(context.Background(), `
SELECT pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid='session_backend_migrations'::regclass
  AND conname='session_backend_migrations_source_driver_check'`).Scan(&sessionMigrationDriverCheck); err != nil {
		t.Fatalf("inspect Session migration driver constraint: %v", err)
	}
	for _, driver := range []string{"postgres", "redis", "inmemory", "mysql", "sqlite", "mongodb", "clickhouse"} {
		if !strings.Contains(sessionMigrationDriverCheck, "'"+driver+"'") {
			t.Fatalf("Session migration driver constraint missing %q: %s", driver, sessionMigrationDriverCheck)
		}
	}
	if err := database.QueryRowContext(context.Background(), `
SELECT pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid='knowledge_ingest_jobs'::regclass
  AND conname='knowledge_ingest_jobs_backend_driver_check'`).Scan(&ingestDriverCheck); err != nil {
		t.Fatalf("inspect knowledge ingest driver constraint: %v", err)
	}
	for _, driver := range []string{"pgvector", "qdrant", "elasticsearch"} {
		if !strings.Contains(ingestDriverCheck, "'"+driver+"'") {
			t.Fatalf("knowledge ingest driver constraint missing %q: %s", driver, ingestDriverCheck)
		}
	}
	sessionKey, err := store.ResolveSession(context.Background(), storage.SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "web", BindingID: "web-console",
		ConversationID: "user-1", ExternalUserID: "user-1", SubjectID: "user-1", Scope: "direct",
	}, "tenant-a/support/session/user-1")
	if err != nil || sessionKey == "" {
		t.Fatalf("ResolveSession() = %q, error = %v", sessionKey, err)
	}
	if _, err := database.ExecContext(context.Background(), "UPDATE sessions SET updated_at=NOW()-INTERVAL '40 days' WHERE tenant_id=$1 AND session_key=$2", "tenant-a", sessionKey); err != nil {
		t.Fatalf("age session for idle archive error = %v", err)
	}
	archived, err := store.ArchiveIdleSessions(context.Background(), time.Now().Add(-30*24*time.Hour), 10)
	if err != nil || archived != 1 {
		t.Fatalf("ArchiveIdleSessions() = %d, %v; want 1, nil", archived, err)
	}
	archivedSession, err := store.GetSession(context.Background(), "tenant-a", sessionKey)
	if err != nil || archivedSession.Status != "archived" || archivedSession.ArchivedAt == nil {
		t.Fatalf("idle archived session = %+v, error = %v", archivedSession, err)
	}

	usageGovernor, err := governance.NewPostgresUsageGovernor(database)
	if err != nil {
		t.Fatalf("NewPostgresUsageGovernor() error = %v", err)
	}
	usageRequest := governance.UsageReservationRequest{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", BindingID: "telegram-bot-a",
		MessageID: "usage-1", TraceID: "trace-usage-1", MaxConcurrentRuns: 1,
		TokenBudget: 100, ReservedTokens: 60, LeaseTTL: time.Minute,
	}
	unknownReservation, err := usageGovernor.Reserve(context.Background(), usageRequest)
	if err != nil {
		t.Fatalf("Reserve(unknown) error = %v", err)
	}
	concurrent := usageRequest
	concurrent.MessageID, concurrent.TraceID = "usage-concurrent", "trace-usage-concurrent"
	if _, err := usageGovernor.Reserve(context.Background(), concurrent); !errors.Is(err, governance.ErrConcurrentRunLimit) {
		t.Fatalf("concurrent Reserve() error = %v, want ErrConcurrentRunLimit", err)
	}
	if err := usageGovernor.SettleUnknown(context.Background(), unknownReservation); err != nil {
		t.Fatalf("SettleUnknown() error = %v", err)
	}
	var unknownStatus string
	var unknownTotal sql.NullInt64
	var unknownReserved int64
	if err := database.QueryRowContext(context.Background(), `
SELECT status,total_tokens,reserved_tokens FROM model_usage_reservations
WHERE reservation_id=$1`, unknownReservation.ID).Scan(&unknownStatus, &unknownTotal, &unknownReserved); err != nil {
		t.Fatalf("read unknown usage settlement: %v", err)
	}
	if unknownStatus != "settled_unknown" || unknownTotal.Valid || unknownReserved != 60 {
		t.Fatalf("unknown usage settlement = status:%q total:%v reserved:%d", unknownStatus, unknownTotal, unknownReserved)
	}
	overBudget := usageRequest
	overBudget.MessageID, overBudget.TraceID = "usage-over-budget", "trace-usage-over-budget"
	overBudget.ReservedTokens = 50
	if _, err := usageGovernor.Reserve(context.Background(), overBudget); !errors.Is(err, governance.ErrTokenBudget) {
		t.Fatalf("Reserve() after unknown usage error = %v, want ErrTokenBudget", err)
	}
	knownRequest := usageRequest
	knownRequest.MessageID, knownRequest.TraceID = "usage-known", "trace-usage-known"
	knownRequest.ReservedTokens = 40
	knownReservation, err := usageGovernor.Reserve(context.Background(), knownRequest)
	if err != nil {
		t.Fatalf("Reserve(known) error = %v", err)
	}
	if err := usageGovernor.SettleKnown(context.Background(), knownReservation, governance.SettledUsage{
		PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30, CostMicros: 7,
	}); err != nil {
		t.Fatalf("SettleKnown() error = %v", err)
	}
	if err := usageGovernor.Renew(context.Background(), knownReservation, time.Minute); !errors.Is(err, governance.ErrUsageLeaseLost) {
		t.Fatalf("Renew() after settlement error = %v, want ErrUsageLeaseLost", err)
	}
	if err := usageGovernor.SettleUnknown(context.Background(), knownReservation); !errors.Is(err, governance.ErrUsageLeaseLost) {
		t.Fatalf("second settlement error = %v, want ErrUsageLeaseLost", err)
	}
	var knownStatus string
	var knownTotal int
	if err := database.QueryRowContext(context.Background(), `
SELECT status,total_tokens FROM model_usage_reservations WHERE reservation_id=$1`, knownReservation.ID).Scan(&knownStatus, &knownTotal); err != nil {
		t.Fatalf("read known usage settlement: %v", err)
	}
	if knownStatus != "settled_known" || knownTotal != 30 {
		t.Fatalf("known usage settlement = status:%q total:%d", knownStatus, knownTotal)
	}

	toolLedger, err := platformtool.NewPostgresExecutionLedger(database, []byte("integration-test-tool-ledger-key-material-32bytes"))
	if err != nil {
		t.Fatalf("NewPostgresExecutionLedger() error = %v", err)
	}
	toolRequest := platformtool.ExecutionRequest{
		TenantID: "tenant-a", RequestID: "tool-request-1", ToolCallID: "call-a",
		ToolName: "refund_order", Arguments: []byte(`{"order_id":"42"}`), TraceID: "trace-tool-1", LeaseTTL: time.Minute,
	}
	firstTool, err := toolLedger.Begin(context.Background(), toolRequest)
	if err != nil || !firstTool.Created || firstTool.Status != platformtool.ExecutionRunning {
		t.Fatalf("tool Begin() = %+v, %v", firstTool, err)
	}
	if err := toolLedger.Complete(context.Background(), "tenant-a", firstTool.IdempotencyKey, map[string]any{"status": "refunded"}); err != nil {
		t.Fatalf("tool Complete() error = %v", err)
	}
	replayRequest := toolRequest
	replayRequest.ToolCallID = "call-b"
	replay, err := toolLedger.Begin(context.Background(), replayRequest)
	if err != nil || replay.Created || replay.Status != platformtool.ExecutionCompleted {
		t.Fatalf("tool replay Begin() = %+v, %v", replay, err)
	}
	replayedResult, ok := replay.Result.(map[string]any)
	if !ok || replayedResult["status"] != "refunded" {
		t.Fatalf("tool replay result = %#v", replay.Result)
	}
	var encryptedResult []byte
	if err := database.QueryRowContext(context.Background(), `
SELECT result_ciphertext FROM tool_executions WHERE tenant_id=$1 AND idempotency_key=$2`,
		"tenant-a", firstTool.IdempotencyKey).Scan(&encryptedResult); err != nil {
		t.Fatalf("read encrypted tool result: %v", err)
	}
	if len(encryptedResult) == 0 || strings.Contains(string(encryptedResult), "refunded") {
		t.Fatal("tool execution ledger did not keep result encrypted")
	}
	unknownRequest := toolRequest
	unknownRequest.RequestID, unknownRequest.ToolCallID, unknownRequest.TraceID = "tool-request-unknown", "call-unknown", "trace-tool-unknown"
	unknownTool, err := toolLedger.Begin(context.Background(), unknownRequest)
	if err != nil || !unknownTool.Created {
		t.Fatalf("unknown tool Begin() = %+v, %v", unknownTool, err)
	}
	if _, err := database.ExecContext(context.Background(), `
UPDATE tool_executions SET lease_until=NOW()-INTERVAL '1 second'
WHERE tenant_id=$1 AND idempotency_key=$2`, "tenant-a", unknownTool.IdempotencyKey); err != nil {
		t.Fatalf("expire tool execution lease: %v", err)
	}
	unknownDecision, err := toolLedger.Begin(context.Background(), unknownRequest)
	if err != nil || unknownDecision.Created || unknownDecision.Status != platformtool.ExecutionOutcomeUnknown {
		t.Fatalf("expired tool Begin() = %+v, %v", unknownDecision, err)
	}
	if err := toolLedger.Complete(context.Background(), "tenant-a", unknownTool.IdempotencyKey, "late"); !errors.Is(err, platformtool.ErrToolOutcomeUnknown) {
		t.Fatalf("late tool Complete() error = %v, want ErrToolOutcomeUnknown", err)
	}

	artifactService, err := storage.NewPostgresArtifactService(database)
	if err != nil {
		t.Fatalf("NewPostgresArtifactService() error = %v", err)
	}
	artifactInfo := agentartifact.SessionInfo{AppName: "tenant-a/support", UserID: "user-1", SessionID: sessionKey}
	artifactVersion, err := artifactService.SaveArtifact(context.Background(), artifactInfo, "a-1.txt", &agentartifact.Artifact{Data: []byte("artifact"), MimeType: "text/plain"})
	if err != nil || artifactVersion != 0 {
		t.Fatalf("SaveArtifact() = %d, %v", artifactVersion, err)
	}
	gotArtifact, err := artifactService.LoadArtifact(context.Background(), artifactInfo, "a-1.txt", nil)
	if err != nil || gotArtifact == nil || string(gotArtifact.Data) != "artifact" {
		t.Fatalf("LoadArtifact() = %#v, %v", gotArtifact, err)
	}

	// Development keeps one current baseline file instead of an upgrade/down
	// chain. A disposable schema reset must install cleanly from that file.
	if _, err := database.ExecContext(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
		t.Fatalf("reset public schema before baseline reinstall: %v", err)
	}
	if err := migrations.Apply(context.Background(), database); err != nil {
		t.Fatalf("re-apply current baseline after schema reset: %v", err)
	}
	assertTenantRoleAccess(t, database)
}

func assertTenantRoleAccess(t *testing.T, database *sql.DB) {
	t.Helper()
	rows, err := database.QueryContext(context.Background(), `
SELECT class.relname,
       has_table_privilege('trpc_tenant', class.oid, 'SELECT'),
       has_table_privilege('trpc_tenant', class.oid, 'INSERT'),
       has_table_privilege('trpc_tenant', class.oid, 'UPDATE'),
       has_table_privilege('trpc_tenant', class.oid, 'DELETE')
FROM pg_class AS class
JOIN pg_namespace AS namespace ON namespace.oid=class.relnamespace
WHERE namespace.nspname='public'
  AND class.relkind IN ('r','p')
  AND class.relrowsecurity
  AND class.relforcerowsecurity
ORDER BY class.relname`)
	if err != nil {
		t.Fatalf("query tenant role privileges: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var table string
		var selectOK, insertOK, updateOK, deleteOK bool
		if err := rows.Scan(&table, &selectOK, &insertOK, &updateOK, &deleteOK); err != nil {
			t.Fatalf("scan tenant role privileges: %v", err)
		}
		count++
		if !selectOK || !insertOK || !updateOK || !deleteOK {
			t.Fatalf("trpc_tenant privileges on %s = select:%v insert:%v update:%v delete:%v", table, selectOK, insertOK, updateOK, deleteOK)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tenant role privileges: %v", err)
	}
	if count == 0 {
		t.Fatal("current baseline contains no FORCE RLS tenant tables")
	}
}

func assertFrameworkSessionRLS(t *testing.T, database *sql.DB) {
	t.Helper()
	ctx := context.Background()
	const (
		tenantID = "framework-rls-a"
		appName  = tenantID + "/support"
		otherApp = "framework-rls-b/support"
	)
	if _, err := database.ExecContext(ctx, `
INSERT INTO session_states (app_name,user_id,session_id,state)
VALUES ($1,'user-b','foreign-session','{}'::jsonb)`, otherApp); err != nil {
		t.Fatalf("seed foreign framework Session: %v", err)
	}

	tx, err := dbscope.BeginTenantTransaction(ctx, database, tenantID)
	if err != nil {
		t.Fatalf("begin framework RLS transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SELECT set_config('app.app_name', $1, true)", appName); err != nil {
		t.Fatalf("set framework app scope: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_states (app_name,user_id,session_id,state)
VALUES ($1,'user-a','own-session','{}'::jsonb)`, appName); err != nil {
		t.Fatalf("insert own framework Session through RLS: %v", err)
	}
	var visible int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM session_states").Scan(&visible); err != nil {
		t.Fatalf("query scoped framework Sessions: %v", err)
	}
	if visible != 1 {
		t.Fatalf("scoped framework Sessions = %d, want 1", visible)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit framework RLS transaction: %v", err)
	}

	forbidden, err := dbscope.BeginTenantTransaction(ctx, database, tenantID)
	if err != nil {
		t.Fatalf("begin cross-app framework RLS transaction: %v", err)
	}
	defer func() { _ = forbidden.Rollback() }()
	if _, err := forbidden.ExecContext(ctx, "SELECT set_config('app.app_name', $1, true)", appName); err != nil {
		t.Fatalf("set cross-app framework scope: %v", err)
	}
	if _, err := forbidden.ExecContext(ctx, `
INSERT INTO session_states (app_name,user_id,session_id,state)
VALUES ($1,'user-b','forbidden-session','{}'::jsonb)`, otherApp); err == nil {
		t.Fatal("framework Session RLS accepted cross-application insert")
	}
}

func seedControlPlane(t *testing.T, database *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := database.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1)", "tenant-a"); err != nil {
		t.Fatalf("insert tenant error = %v", err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO applications (tenant_id, app_code, status) VALUES ($1, $2, $3)", "tenant-a", "support", "active"); err != nil {
		t.Fatalf("insert application error = %v", err)
	}
}
