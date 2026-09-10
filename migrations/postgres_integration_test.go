package migrations_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

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
	grantTenantRoleAccess(t, database)

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
	beginState, err := dedup.Begin(context.Background(), "tenant-a", "telegram", "telegram-bot-a", "update-42", "trace-42", time.Minute)
	if err != nil || beginState != storage.ExecutionFresh {
		t.Fatalf("Begin() state = %v, error = %v, want fresh", beginState, err)
	}
	_, err = store.RecordExecution(context.Background(), storage.ExecutionRecord{
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
		ModelUsage:    &storage.ModelUsage{ProviderID: "openai-primary", ModelName: "gpt-4o-mini", PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, CostMicros: 9},
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

	// Cross-node takeover: a stale processing claim must be claimable by a
	// second node, and the takeover write must carry the new owner's trace ID.
	beginState, err = dedup.Begin(context.Background(), "tenant-a", "telegram", "telegram-bot-a", "update-takeover", "trace-old", time.Minute)
	if err != nil || beginState != storage.ExecutionFresh {
		t.Fatalf("Begin() takeover claim state = %v, error = %v, want fresh", beginState, err)
	}
	if _, err := database.ExecContext(context.Background(), "UPDATE messages SET updated_at = NOW() - make_interval(secs => 120) WHERE tenant_id = 'tenant-a' AND channel_type = 'telegram' AND binding_id = 'telegram-bot-a' AND message_id = 'update-takeover'"); err != nil {
		t.Fatalf("age takeover claim error = %v", err)
	}
	beginState, err = dedup.Begin(context.Background(), "tenant-a", "telegram", "telegram-bot-a", "update-takeover", "trace-new", time.Minute)
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

	beginState, err = dedup.Begin(context.Background(), "tenant-a", "telegram", "telegram-bot-a", "update-rollback", "trace-rollback", time.Minute)
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
	grantTenantRoleAccess(t, database)
}

func grantTenantRoleAccess(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.ExecContext(context.Background(), `
GRANT USAGE ON SCHEMA public TO trpc_tenant;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO trpc_tenant;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trpc_tenant`); err != nil {
		t.Fatalf("grant tenant role access to current baseline: %v", err)
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
