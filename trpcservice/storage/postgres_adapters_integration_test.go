package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func openStorageIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()
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
		t.Fatalf("PostgreSQL ping error = %v", err)
	}
	return database
}

func seedStorageIntegrationTenant(t *testing.T, database *sql.DB, tenantID string) {
	t.Helper()
	if _, err := database.ExecContext(context.Background(), `INSERT INTO tenants (id, display_name, status)
VALUES ($1, $2, 'active') ON CONFLICT (id) DO UPDATE SET status='active'`, tenantID, "客服测试租户"); err != nil {
		t.Fatalf("seed tenant %q: %v", tenantID, err)
	}
}

func TestPostgresAuditRetentionPurgesOnlySafeOperationalEvidence(t *testing.T) {
	database := openStorageIntegrationDB(t)
	ctx := context.Background()
	const tenantID = "storage-audit-retention-test"
	seedStorageIntegrationTenant(t, database, tenantID)
	if _, err := database.ExecContext(ctx, `INSERT INTO applications (tenant_id, app_code, status)
VALUES ($1, 'support', 'active') ON CONFLICT (tenant_id, app_code) DO NOTHING`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, table := range []string{"tool_approvals", "tool_executions", "model_usage_ledger", "execution_traces", "audit_events", "messages"} {
			_, _ = database.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE tenant_id=$1", tenantID)
		}
		_, _ = database.ExecContext(context.Background(), "DELETE FROM applications WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID)
	})

	now := time.Now().UTC()
	old := now.Add(-10 * 24 * time.Hour)
	cutoff := now.Add(-7 * 24 * time.Hour)
	if _, err := database.ExecContext(ctx, `
INSERT INTO messages (tenant_id, app_code, channel_type, binding_id, message_id, status, trace_id, created_at, updated_at)
VALUES ($1,'support','web','web-console','old-message','completed','old-trace',$2,$2)`, tenantID, old); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
INSERT INTO audit_events (id, tenant_id, trace_id, action, result, created_at)
VALUES ('old-audit',$1,'old-trace','agent.reply','completed',$2),
       ('recent-audit',$1,'recent-trace','agent.reply','completed',$3)`, tenantID, old, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
INSERT INTO execution_traces (tenant_id, app_code, channel_type, binding_id, message_id, trace_id, projection, created_at, updated_at)
VALUES ($1,'support','web','web-console','old-message','old-trace','{"status":"completed"}'::jsonb,$2,$2),
       ($1,'support','web','web-console','recent-message','recent-trace','{"status":"completed"}'::jsonb,$3,$3)`, tenantID, old, now); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
INSERT INTO tool_approvals (
    approval_token, tenant_id, app_code, config_version, channel_type, binding_id,
    conversation_id, external_user_id, tool_name, status, created_at, expires_at, resolved_at, updated_at
) VALUES ('old-approval',$1,'support',1,'web','web-console','conversation','user','request_refund','approved',$2,$3,$3,$3)`,
		tenantID, old.Add(-time.Hour), old); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
INSERT INTO tool_executions (
    tenant_id, request_id, tool_call_id, tool_name, arguments_hash, idempotency_key,
    status, result_hash, result_ciphertext, trace_id, started_at, completed_at, updated_at
) VALUES
($1,'old-message','completed-call','request_refund',repeat('a',64),repeat('b',64),'completed',repeat('c',64),decode('01','hex'),'old-trace',$2,$2,$2),
($1,'old-message','unknown-call','request_refund',repeat('d',64),repeat('e',64),'outcome_unknown',NULL,NULL,'old-trace',$2,$2,$2)`, tenantID, old); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
INSERT INTO model_usage_ledger (
    id, tenant_id, app_code, channel_type, binding_id, message_id, trace_id, provider_id, model_name,
    usage_known, prompt_tokens, cached_prompt_tokens, completion_tokens, total_tokens, cost_micros, created_at
) VALUES ('old-usage',$1,'support','web','web-console','old-message','old-trace','provider','model',true,10,0,5,15,1,$2)`, tenantID, old); err != nil {
		t.Fatal(err)
	}

	store, err := NewPostgresStateStore(database, []byte("storage-audit-retention-key-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	removed, err := store.PurgeAuditBefore(ctx, tenantID, cutoff)
	if err != nil || removed != 1 {
		t.Fatalf("PurgeAuditBefore() = %d, %v; want one old audit row", removed, err)
	}

	assertCount := func(table, predicate string, want int) {
		t.Helper()
		var got int
		query := "SELECT COUNT(*) FROM " + table + " WHERE tenant_id=$1"
		if predicate != "" {
			query += " AND " + predicate
		}
		if err := database.QueryRowContext(ctx, query, tenantID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s count = %d, want %d", table, got, want)
		}
	}
	assertCount("audit_events", "", 1)
	assertCount("execution_traces", "", 1)
	assertCount("tool_approvals", "", 0)
	assertCount("tool_executions", "status='completed'", 0)
	assertCount("tool_executions", "status='outcome_unknown'", 1)
	assertCount("messages", "message_id='old-message'", 1)
	assertCount("model_usage_ledger", "id='old-usage'", 1)
}

func TestPostgresBackendProfileStoreLifecycle(t *testing.T) {
	database := openStorageIntegrationDB(t)
	ctx := context.Background()
	const (
		tenantID  = "storage-profile-test"
		profileID = "support-cache-test"
	)
	seedStorageIntegrationTenant(t, database, tenantID)
	_, _ = database.ExecContext(ctx, "DELETE FROM tenant_backend_profiles WHERE tenant_id=$1", tenantID)
	_, _ = database.ExecContext(ctx, "DELETE FROM backend_profiles WHERE profile_id=$1", profileID)
	t.Cleanup(func() {
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenant_backend_profiles WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM backend_profiles WHERE profile_id=$1", profileID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID)
	})

	store, err := NewPostgresBackendProfileStore(database)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateBackendProfile(ctx, BackendProfile{
		ProfileID: profileID, DisplayName: "客服缓存", Driver: "redis", ConnectionRef: "env:SUPPORT_REDIS", Domains: []string{BackendDomainSession},
	})
	if err != nil || created.ProfileID != profileID || created.Status != BackendProfileActive {
		t.Fatalf("CreateBackendProfile() = %#v, %v", created, err)
	}
	got, err := store.GetBackendProfile(ctx, profileID)
	if err != nil || got.Driver != "redis" || len(got.Domains) != 1 || got.Domains[0] != BackendDomainSession {
		t.Fatalf("GetBackendProfile() = %#v, %v", got, err)
	}
	profiles, err := store.ListBackendProfiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, profile := range profiles {
		found = found || profile.ProfileID == profileID
	}
	if !found {
		t.Fatalf("ListBackendProfiles() did not contain %q", profileID)
	}
	if err := store.ReplaceTenantBackendProfiles(ctx, tenantID, []string{profileID}); err != nil {
		t.Fatalf("ReplaceTenantBackendProfiles() error = %v", err)
	}
	allowed, err := store.ListTenantBackendProfiles(ctx, tenantID)
	if err != nil || len(allowed) != 1 || allowed[0].ProfileID != profileID || !allowed[0].Available {
		t.Fatalf("ListTenantBackendProfiles() = %#v, %v", allowed, err)
	}
	backend, err := store.ResolveTenantBackend(ctx, tenantID, BackendDomainSession, profileID)
	if err != nil || backend != (config.BackendConfig{Driver: "redis", ConnectionRef: "env:SUPPORT_REDIS"}) {
		t.Fatalf("ResolveTenantBackend() = %#v, %v", backend, err)
	}
	if _, err := store.ResolveTenantBackend(ctx, tenantID, BackendDomainArtifact, profileID); err == nil {
		t.Fatal("ResolveTenantBackend() accepted unsupported domain")
	}
	if _, err := store.ResolveTenantBackend(ctx, "other-tenant", BackendDomainSession, profileID); !errors.Is(err, ErrBackendProfileUnauthorized) {
		t.Fatalf("unauthorized ResolveTenantBackend() error = %v", err)
	}
	if err := store.DeleteBackendProfile(ctx, profileID); err == nil {
		t.Fatal("DeleteBackendProfile() deleted an authorized profile")
	}

	disabled, err := store.UpdateBackendProfile(ctx, profileID, BackendProfileUpdate{DisplayName: "客服缓存", Status: BackendProfileDisabled})
	if err != nil || disabled.Status != BackendProfileDisabled {
		t.Fatalf("disable backend profile = %#v, %v", disabled, err)
	}
	if _, err := store.ResolveTenantBackend(ctx, tenantID, BackendDomainSession, profileID); !errors.Is(err, ErrBackendProfileUnavailable) {
		t.Fatalf("disabled ResolveTenantBackend() error = %v", err)
	}
	if err := store.ReplaceTenantBackendProfiles(ctx, tenantID, []string{profileID}); err == nil {
		t.Fatal("ReplaceTenantBackendProfiles() accepted disabled profile")
	}
	if err := store.ReplaceTenantBackendProfiles(ctx, tenantID, nil); err != nil {
		t.Fatalf("clear tenant backend profiles: %v", err)
	}
	if err := store.DeleteBackendProfile(ctx, profileID); err != nil {
		t.Fatalf("DeleteBackendProfile() error = %v", err)
	}
	if _, err := store.GetBackendProfile(ctx, profileID); !errors.Is(err, ErrBackendProfileNotFound) {
		t.Fatalf("GetBackendProfile(deleted) error = %v", err)
	}
}

func TestPostgresExecutionDedupStoreLifecycle(t *testing.T) {
	database := openStorageIntegrationDB(t)
	ctx := context.Background()
	const tenantID = "storage-dedup-test"
	seedStorageIntegrationTenant(t, database, tenantID)
	if _, err := database.ExecContext(ctx, `INSERT INTO applications (tenant_id, app_code, status)
VALUES ($1, 'support', 'active') ON CONFLICT (tenant_id, app_code) DO NOTHING`, tenantID); err != nil {
		t.Fatalf("seed dedup application: %v", err)
	}
	_, _ = database.ExecContext(ctx, "DELETE FROM messages WHERE tenant_id=$1", tenantID)
	t.Cleanup(func() {
		_, _ = database.ExecContext(context.Background(), "DELETE FROM messages WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM applications WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID)
	})

	store, err := NewPostgresExecutionDedupStore(database)
	if err != nil {
		t.Fatal(err)
	}
	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-1", "trace-a", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("first Begin() = %q, %v", state, err)
	}
	filtered, err := store.ListClaims(ctx, tenantID, "support", 10)
	if err != nil {
		t.Fatalf("ListClaims(app) error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].MessageID != "message-1" || filtered[0].AppCode != "support" {
		t.Fatalf("ListClaims(app) = %#v, want fresh processing claim visible before route/trace persistence", filtered)
	}
	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-1", "trace-b", time.Minute); err != nil || state != ExecutionInProgress {
		t.Fatalf("duplicate Begin() = %q, %v", state, err)
	}
	if err := store.Renew(ctx, tenantID, "telegram", "support-bot", "message-1", "trace-a"); err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if err := store.Renew(ctx, tenantID, "telegram", "support-bot", "message-1", "trace-b"); err == nil {
		t.Fatal("Renew() accepted foreign trace")
	}
	claims, err := store.ListClaims(ctx, tenantID, "", 10)
	if err != nil || len(claims) != 1 || claims[0].MessageID != "message-1" || claims[0].Status != "processing" {
		t.Fatalf("ListClaims() = %#v, %v", claims, err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE messages SET status='completed' WHERE tenant_id=$1 AND message_id='message-1'", tenantID); err != nil {
		t.Fatal(err)
	}
	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-1", "trace-c", time.Minute); err != nil || state != ExecutionCompleted {
		t.Fatalf("completed Begin() = %q, %v", state, err)
	}

	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-failed", "trace-failed", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("failed-message Begin() = %q, %v", state, err)
	}
	if err := store.Fail(ctx, tenantID, "telegram", "support-bot", "message-failed", "trace-failed"); err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	claims, err = store.ListClaims(ctx, tenantID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	var failed Claim
	for _, claim := range claims {
		if claim.MessageID == "message-failed" {
			failed = claim
		}
	}
	if failed.Status != "failed" {
		t.Fatalf("failed claim = %+v", failed)
	}
	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-failed", "trace-retry", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("retry failed message Begin() = %q, %v", state, err)
	}

	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-2", "trace-old", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("message-2 Begin() = %q, %v", state, err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE messages SET updated_at=NOW()-INTERVAL '2 minutes' WHERE tenant_id=$1 AND message_id='message-2'", tenantID); err != nil {
		t.Fatal(err)
	}
	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-2", "trace-new", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("stale takeover Begin() = %q, %v", state, err)
	}
	if err := store.Abort(ctx, tenantID, "telegram", "support-bot", "message-2", "trace-old"); err != nil {
		t.Fatalf("old owner Abort() error = %v", err)
	}
	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-2", "trace-third", time.Minute); err != nil || state != ExecutionInProgress {
		t.Fatalf("Begin() after old abort = %q, %v", state, err)
	}
	if err := store.Abort(ctx, tenantID, "telegram", "support-bot", "message-2", "trace-new"); err != nil {
		t.Fatalf("current owner Abort() error = %v", err)
	}
	if state, err := store.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-2", "trace-third", time.Minute); err != nil || state != ExecutionFresh {
		t.Fatalf("Begin() after abort = %q, %v", state, err)
	}
}

func TestPostgresExecutionTraceLifecycle(t *testing.T) {
	database := openStorageIntegrationDB(t)
	ctx := context.Background()
	const tenantID = "storage-trace-test"
	seedStorageIntegrationTenant(t, database, tenantID)
	if _, err := database.ExecContext(ctx, `INSERT INTO applications (tenant_id, app_code, status)
VALUES ($1, 'support', 'active') ON CONFLICT (tenant_id, app_code) DO NOTHING`, tenantID); err != nil {
		t.Fatalf("seed trace application: %v", err)
	}
	_, _ = database.ExecContext(ctx, "DELETE FROM execution_traces WHERE tenant_id=$1", tenantID)
	t.Cleanup(func() {
		_, _ = database.ExecContext(context.Background(), "DELETE FROM execution_traces WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM applications WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID)
	})
	store, err := NewPostgresStateStore(database, []byte("storage-integration-audit-key-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	record := ExecutionTraceRecord{
		TenantID: tenantID, AppCode: "support", Channel: "web", BindingID: "web-console", MessageID: "message-1", TraceID: "trace-1",
		Trace: AgentExecutionTrace{
			Status: "completed", RootAgentName: "support", RootInvocationID: "invocation-1",
			Usage: &ExecutionTraceUsage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
			Steps: []ExecutionTraceStep{{StepID: "step-1", NodeType: "llm"}},
		},
	}
	if err := store.RecordExecutionTrace(ctx, record); err != nil {
		t.Fatalf("RecordExecutionTrace() error = %v", err)
	}
	got, err := store.GetExecutionTrace(ctx, tenantID, "web", "web-console", "message-1")
	if err != nil || got.TraceID != "trace-1" || got.Trace.Usage == nil || got.Trace.Usage.TotalTokens != 15 {
		t.Fatalf("GetExecutionTrace() = %#v, %v", got, err)
	}
	listed, err := store.ListExecutionTraces(ctx, tenantID, []ExecutionTraceRef{
		{Channel: "web", BindingID: "web-console", MessageID: "message-1"},
		{Channel: "web", BindingID: "web-console", MessageID: "missing"},
	})
	if err != nil || len(listed) != 1 || listed[0].TraceID != "trace-1" {
		t.Fatalf("ListExecutionTraces() = %#v, %v", listed, err)
	}
	record.TraceID = "trace-2"
	record.Trace.Status = "failed"
	if err := store.RecordExecutionTrace(ctx, record); err != nil {
		t.Fatalf("RecordExecutionTrace(update) error = %v", err)
	}
	updated, err := store.GetExecutionTrace(ctx, tenantID, "web", "web-console", "message-1")
	if err != nil || updated.TraceID != "trace-2" || updated.Trace.Status != "failed" {
		t.Fatalf("updated trace = %#v, %v", updated, err)
	}
	if _, err := store.GetExecutionTrace(ctx, tenantID, "web", "web-console", "missing"); !errors.Is(err, ErrExecutionTraceNotFound) {
		t.Fatalf("missing trace error = %v", err)
	}
}

func TestPostgresKnowledgeIngestQueueLifecycle(t *testing.T) {
	database := openStorageIntegrationDB(t)
	ctx := context.Background()
	const tenantID = "storage-ingest-test"
	seedStorageIntegrationTenant(t, database, tenantID)
	if _, err := database.ExecContext(ctx, `INSERT INTO applications (tenant_id, app_code, status)
VALUES ($1, 'support', 'active') ON CONFLICT (tenant_id, app_code) DO NOTHING`, tenantID); err != nil {
		t.Fatalf("seed ingest application: %v", err)
	}
	// ClaimKnowledgeIngest is intentionally a global worker queue. The
	// infrastructure database is dedicated to this test process, so clear
	// stale jobs left by earlier integration cases before asserting ownership.
	if _, err := database.ExecContext(ctx, "DELETE FROM knowledge_ingest_jobs"); err != nil {
		t.Fatalf("clear global ingest queue: %v", err)
	}
	for _, table := range []string{"knowledge_ingest_jobs", "knowledge_document_sources", "knowledge_documents"} {
		if _, err := database.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", tenantID); err != nil {
			t.Fatalf("clear %s: %v", table, err)
		}
	}
	t.Cleanup(func() {
		for _, table := range []string{"knowledge_ingest_jobs", "knowledge_document_sources", "knowledge_documents"} {
			_, _ = database.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE tenant_id=$1", tenantID)
		}
		_, _ = database.ExecContext(context.Background(), "DELETE FROM applications WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID)
	})

	queue, err := NewPostgresKnowledgeIngestQueue(database)
	if err != nil {
		t.Fatal(err)
	}
	request := KnowledgeIngestRequest{
		TenantID: tenantID, AppCode: "support", DocumentID: "guide-1",
		Name: "客服手册", Filename: "support.md", ContentType: "text/markdown", Data: []byte("# 客服手册"),
		ChunkSize: 800, Overlap: 80, Metadata: map[string]string{"category": "support"},
		Backend: config.BackendConfig{Driver: "pgvector"},
	}
	jobID, err := queue.EnqueueKnowledgeIngest(ctx, request)
	if err != nil || jobID == "" {
		t.Fatalf("EnqueueKnowledgeIngest() = %q, %v", jobID, err)
	}
	if _, err := queue.EnqueueKnowledgeIngest(ctx, request); !errors.Is(err, ErrKnowledgeIngestActive) {
		t.Fatalf("duplicate enqueue error = %v", err)
	}
	sources, err := queue.ListKnowledgeDocumentSources(ctx, tenantID, "support")
	if err != nil || len(sources) != 1 || sources[0].DocumentID != "guide-1" || sources[0].Metadata["category"] != "support" {
		t.Fatalf("ListKnowledgeDocumentSources() = %#v, %v", sources, err)
	}
	if err := queue.SaveKnowledgeDocumentSnapshot(ctx, tenantID, "support", "guide-1", []byte(`[ {"id":"chunk-1"} ]`)); err != nil {
		t.Fatalf("SaveKnowledgeDocumentSnapshot() error = %v", err)
	}
	sources, err = queue.ListKnowledgeDocumentSources(ctx, tenantID, "support")
	if err != nil || len(sources) != 1 || len(sources[0].CanonicalDocuments) == 0 {
		t.Fatalf("source after snapshot = %#v, %v", sources, err)
	}

	job, ok, err := queue.ClaimKnowledgeIngest(ctx, "worker-a", time.Minute)
	if err != nil || !ok || job.ID != jobID || job.Attempts != 1 || string(job.Data) != "# 客服手册" {
		t.Fatalf("ClaimKnowledgeIngest() = %#v, %v, %v", job, ok, err)
	}
	if terminal, err := queue.FailKnowledgeIngest(ctx, job.ID, "worker-a", "temporary indexing error", 2); err != nil || terminal {
		t.Fatalf("FailKnowledgeIngest(retry) = %v, %v", terminal, err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE knowledge_ingest_jobs SET available_at=NOW() WHERE id=$1", job.ID); err != nil {
		t.Fatal(err)
	}
	job, ok, err = queue.ClaimKnowledgeIngest(ctx, "worker-b", time.Minute)
	if err != nil || !ok || job.Attempts != 2 {
		t.Fatalf("retry ClaimKnowledgeIngest() = %#v, %v, %v", job, ok, err)
	}
	if terminal, err := queue.FailKnowledgeIngest(ctx, job.ID, "worker-b", "terminal indexing error", 2); err != nil || !terminal {
		t.Fatalf("FailKnowledgeIngest(terminal) = %v, %v", terminal, err)
	}

	jobID, err = queue.EnqueueKnowledgeIngest(ctx, request)
	if err != nil {
		t.Fatalf("re-enqueue failed job: %v", err)
	}
	job, ok, err = queue.ClaimKnowledgeIngest(ctx, "worker-c", time.Minute)
	if err != nil || !ok || job.ID != jobID {
		t.Fatalf("ClaimKnowledgeIngest(requeued) = %#v, %v, %v", job, ok, err)
	}
	if err := queue.CompleteKnowledgeIngest(ctx, KnowledgeIngestCompletion{
		JobID: job.ID, Owner: "worker-c", TenantID: tenantID, AppCode: "support", DocumentID: "guide-1", TotalChunks: 3,
	}); err != nil {
		t.Fatalf("CompleteKnowledgeIngest() error = %v", err)
	}
	if _, ok, err := queue.ClaimKnowledgeIngest(ctx, "worker-d", time.Minute); err != nil || ok {
		t.Fatalf("ClaimKnowledgeIngest(empty) ok=%v err=%v", ok, err)
	}

	cancelRequest := request
	cancelRequest.DocumentID = "guide-cancel"
	cancelRequest.Name = "待取消手册"
	if _, err := queue.EnqueueKnowledgeIngest(ctx, cancelRequest); err != nil {
		t.Fatal(err)
	}
	if err := queue.CancelKnowledgeIngest(ctx, tenantID, "support", "guide-cancel"); err != nil {
		t.Fatalf("CancelKnowledgeIngest() error = %v", err)
	}
}

func TestPostgresKnowledgeMigrationStoreLifecycle(t *testing.T) {
	database := openStorageIntegrationDB(t)
	ctx := context.Background()
	const tenantID = "storage-migration-test"
	seedStorageIntegrationTenant(t, database, tenantID)
	if _, err := database.ExecContext(ctx, `INSERT INTO applications (tenant_id, app_code, status)
VALUES ($1, 'support', 'active') ON CONFLICT (tenant_id, app_code) DO NOTHING`, tenantID); err != nil {
		t.Fatalf("seed migration application: %v", err)
	}
	_, _ = database.ExecContext(ctx, "DELETE FROM knowledge_backend_migrations WHERE tenant_id=$1", tenantID)
	t.Cleanup(func() {
		_, _ = database.ExecContext(context.Background(), "DELETE FROM knowledge_backend_migrations WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM applications WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID)
	})

	store, err := NewPostgresKnowledgeMigrationStore(database)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateKnowledgeMigration(ctx, KnowledgeMigrationStatus{
		TenantID: tenantID, AppCode: "support", SourceProfileID: "platform-pgvector", TargetProfileID: "support-qdrant",
		Source: config.BackendConfig{Driver: "pgvector"}, Target: config.BackendConfig{Driver: "qdrant", ConnectionRef: "env:SUPPORT_QDRANT"},
	})
	if err != nil || created.ID == "" || created.Generation != 1 || created.Phase != KnowledgeMigrationPrepared {
		t.Fatalf("CreateKnowledgeMigration() = %#v, %v", created, err)
	}
	if _, err := store.CreateKnowledgeMigration(ctx, KnowledgeMigrationStatus{
		TenantID: tenantID, AppCode: "support", Source: config.BackendConfig{Driver: "pgvector"}, Target: config.BackendConfig{Driver: "qdrant"},
	}); !errors.Is(err, ErrKnowledgeMigrationConflict) {
		t.Fatalf("second active migration error = %v", err)
	}
	active, ok, err := store.ActiveKnowledgeMigration(ctx, tenantID, "support")
	if err != nil || !ok || active.ID != created.ID {
		t.Fatalf("ActiveKnowledgeMigration() = %#v, %v, %v", active, ok, err)
	}
	listed, err := store.ListKnowledgeMigrations(ctx, tenantID, "support", 0)
	if err != nil || len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("ListKnowledgeMigrations() = %#v, %v", listed, err)
	}

	created.Phase = KnowledgeMigrationReindexed
	created.ReindexedDocuments = 4
	updated, err := store.UpdateKnowledgeMigration(ctx, created, 1)
	if err != nil || updated.Generation != 2 || updated.Phase != KnowledgeMigrationReindexed || updated.ReindexedDocuments != 4 {
		t.Fatalf("UpdateKnowledgeMigration() = %#v, %v", updated, err)
	}
	if _, err := store.UpdateKnowledgeMigration(ctx, updated, 1); !errors.Is(err, ErrKnowledgeMigrationConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	locked := false
	if err := store.WithKnowledgeMigrationLock(ctx, tenantID, "support", func() error {
		locked = true
		return nil
	}); err != nil || !locked {
		t.Fatalf("WithKnowledgeMigrationLock() locked=%v err=%v", locked, err)
	}
	updated.Phase = KnowledgeMigrationDone
	done, err := store.UpdateKnowledgeMigration(ctx, updated, 2)
	if err != nil || done.Generation != 3 || done.Phase != KnowledgeMigrationDone {
		t.Fatalf("finish migration = %#v, %v", done, err)
	}
	if active, ok, err := store.ActiveKnowledgeMigration(ctx, tenantID, "support"); err != nil || ok || active.ID != "" {
		t.Fatalf("ActiveKnowledgeMigration(done) = %#v, %v, %v", active, ok, err)
	}
	got, err := store.GetKnowledgeMigration(ctx, tenantID, "support", done.ID)
	if err != nil || got.Generation != 3 || got.Phase != KnowledgeMigrationDone {
		t.Fatalf("GetKnowledgeMigration() = %#v, %v", got, err)
	}
}

func TestPostgresSessionCatalogAndRoutingLifecycle(t *testing.T) {
	database := openStorageIntegrationDB(t)
	ctx := context.Background()
	const tenantID = "storage-session-test"
	seedStorageIntegrationTenant(t, database, tenantID)
	if _, err := database.ExecContext(ctx, `INSERT INTO applications (tenant_id, app_code, status)
VALUES ($1, 'support', 'active') ON CONFLICT (tenant_id, app_code) DO NOTHING`, tenantID); err != nil {
		t.Fatalf("seed session application: %v", err)
	}
	for _, userID := range []string{"platform-user-1", "platform-user-2"} {
		if _, err := database.ExecContext(ctx, `INSERT INTO platform_users (platform_user_id, display_name, email, status)
VALUES ($1, '客服测试用户', '', 'active') ON CONFLICT (platform_user_id) DO NOTHING`, userID); err != nil {
			t.Fatalf("seed platform user %q: %v", userID, err)
		}
	}
	for _, table := range []string{
		"model_usage_ledger", "execution_traces", "audit_events", "outbox_events", "inbound_message_routes",
		"session_execution_leases", "channel_conversations", "sessions", "messages",
	} {
		_, _ = database.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", tenantID)
	}
	t.Cleanup(func() {
		for _, table := range []string{
			"model_usage_ledger", "execution_traces", "audit_events", "outbox_events", "inbound_message_routes",
			"session_execution_leases", "channel_conversations", "sessions", "messages",
		} {
			_, _ = database.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE tenant_id=$1", tenantID)
		}
		_, _ = database.ExecContext(context.Background(), "DELETE FROM applications WHERE tenant_id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM platform_users WHERE platform_user_id IN ('platform-user-1','platform-user-2')")
	})

	state, err := NewPostgresStateStore(database, []byte("storage-session-audit-key-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	dedup, err := NewPostgresExecutionDedupStore(database)
	if err != nil {
		t.Fatal(err)
	}
	route := SessionRoute{
		TenantID: tenantID, AppCode: "support", Channel: "telegram", BindingID: "support-bot",
		ConversationID: "chat-1", ExternalUserID: "external-user-1", SubjectID: "external:telegram:support-bot:external-user-1", Scope: "direct",
	}
	firstKey := tenantID + "/support/session/first"
	resolved, err := state.ResolveSession(ctx, route, firstKey)
	if err != nil || resolved != firstKey {
		t.Fatalf("ResolveSession(first) = %q, %v", resolved, err)
	}
	fixedRouteUpdatedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if _, err := database.ExecContext(ctx, `
UPDATE channel_conversations SET updated_at=$6
WHERE tenant_id=$1 AND app_code=$2 AND channel_type=$3 AND binding_id=$4 AND external_conversation_id=$5 AND ended_at IS NULL`,
		route.TenantID, route.AppCode, route.Channel, route.BindingID, route.ConversationID, fixedRouteUpdatedAt); err != nil {
		t.Fatalf("pin route updated_at: %v", err)
	}
	resolvedAgain, err := state.ResolveSession(ctx, route, tenantID+"/support/session/unexpected")
	if err != nil || resolvedAgain != firstKey {
		t.Fatalf("ResolveSession(existing) = %q, %v", resolvedAgain, err)
	}
	var routeUpdatedAt time.Time
	if err := database.QueryRowContext(ctx, `
SELECT updated_at FROM channel_conversations
WHERE tenant_id=$1 AND app_code=$2 AND channel_type=$3 AND binding_id=$4 AND external_conversation_id=$5 AND ended_at IS NULL`,
		route.TenantID, route.AppCode, route.Channel, route.BindingID, route.ConversationID).Scan(&routeUpdatedAt); err != nil {
		t.Fatalf("read route updated_at: %v", err)
	}
	if !routeUpdatedAt.Equal(fixedRouteUpdatedAt) {
		t.Fatalf("ResolveSession(existing) rewrote route updated_at: got %s want %s", routeUpdatedAt, fixedRouteUpdatedAt)
	}
	got, err := state.GetSession(ctx, tenantID, firstKey)
	if err != nil || got.SessionKey != firstKey || got.SubjectID != route.SubjectID || len(got.Conversations) != 1 {
		t.Fatalf("GetSession() = %#v, %v", got, err)
	}
	listed, err := state.ListSessions(ctx, tenantID, 10)
	if err != nil || len(listed) != 1 || listed[0].SessionKey != firstKey {
		t.Fatalf("ListSessions() = %#v, %v", listed, err)
	}
	applicationSessions, err := state.ListApplicationSessions(ctx, tenantID, "support")
	if err != nil || len(applicationSessions) != 1 || applicationSessions[0].SessionKey != firstKey {
		t.Fatalf("ListApplicationSessions() = %#v, %v", applicationSessions, err)
	}
	claimable, err := state.ListClaimableSessions(ctx, tenantID, "telegram", "support-bot", "external-user-1", 10)
	if err != nil || len(claimable) != 1 || claimable[0].SessionKey != firstKey {
		t.Fatalf("ListClaimableSessions() = %#v, %v", claimable, err)
	}
	if err := state.ClaimSession(ctx, tenantID, firstKey, "platform-user-1"); err != nil {
		t.Fatalf("ClaimSession() error = %v", err)
	}
	if err := state.ClaimSession(ctx, tenantID, firstKey, "platform-user-2"); !errors.Is(err, ErrSessionOwnedByAnotherUser) {
		t.Fatalf("foreign ClaimSession() error = %v", err)
	}
	if err := state.ClaimSession(ctx, tenantID, "missing-session", "platform-user-1"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing ClaimSession() error = %v", err)
	}

	if begin, err := dedup.Begin(ctx, tenantID, "support", "telegram", "support-bot", "message-1", "trace-1", time.Minute); err != nil || begin != ExecutionFresh {
		t.Fatalf("dedup Begin() = %q, %v", begin, err)
	}
	event, err := state.RecordExecution(ctx, ExecutionRecord{
		TenantID: tenantID, AppCode: "support", SessionKey: firstKey,
		MessageID: "message-1", Channel: "telegram", BindingID: "support-bot",
		ConversationID: "chat-1", ConversationScope: "direct", ExternalUserID: "external-user-1",
		ActorExternalUserID: "external-user-1", ActorPlatformUserID: "platform-user-1", TriggerType: "direct",
		TraceID: "trace-1", Action: "agent.reply", Result: "queued", SubjectID: route.SubjectID, OwnerPlatformUserID: "platform-user-1",
		OutboxType: "channel_reply.telegram", OutboxPayload: []byte(`{"channel":"telegram","text":"reply"}`), OutboxRequestID: "request-1",
	})
	if err != nil || event.ID == "" {
		t.Fatalf("RecordExecution() = %#v, %v", event, err)
	}
	backlog, err := state.OutboxBacklog(ctx, tenantID, "channel_reply.telegram")
	if err != nil || backlog.Pending != 1 || backlog.OldestCreatedAt.IsZero() {
		t.Fatalf("OutboxBacklog() = %#v, %v", backlog, err)
	}
	pendingOutbox, err := state.ListPendingOutbox(ctx, tenantID, 10)
	if err != nil || len(pendingOutbox) != 1 || pendingOutbox[0].ID != event.ID {
		t.Fatalf("ListPendingOutbox() = %#v, %v", pendingOutbox, err)
	}
	claimedOutbox, err := state.ClaimPendingOutboxByType(ctx, tenantID, "channel-worker-1", time.Minute, 10, "channel_reply.telegram")
	if err != nil || len(claimedOutbox) != 1 || claimedOutbox[0].ID != event.ID || claimedOutbox[0].DeliveryAttempts != 1 {
		t.Fatalf("ClaimPendingOutboxByType() = %#v, %v", claimedOutbox, err)
	}
	if err := state.RenewOutboxDelivery(ctx, tenantID, event.ID, "channel-worker-1", 2*time.Minute); err != nil {
		t.Fatalf("RenewOutboxDelivery() error = %v", err)
	}
	foundOutbox, err := state.FindOutboxByRequestID(ctx, tenantID, "request-1")
	if err != nil || foundOutbox.ID != event.ID || foundOutbox.DeliveryOwner != "channel-worker-1" || foundOutbox.LeaseExpiresAt == nil {
		t.Fatalf("FindOutboxByRequestID() = %#v, %v", foundOutbox, err)
	}
	batchOutbox, err := state.FindOutboxByRequestIDs(ctx, tenantID, []string{"request-1", "request-1", "missing"})
	if err != nil || len(batchOutbox) != 1 || batchOutbox[0].ID != event.ID {
		t.Fatalf("FindOutboxByRequestIDs() = %#v, %v", batchOutbox, err)
	}
	if err := state.CompleteOutboxDelivery(ctx, tenantID, event.ID, "channel-worker-1", "provider-message-1"); err != nil {
		t.Fatalf("CompleteOutboxDelivery() error = %v", err)
	}
	completedOutbox, err := state.FindOutboxByRequestID(ctx, tenantID, "request-1")
	if err != nil || completedOutbox.DeliveredAt == nil || completedOutbox.DeliveryReceipt != "provider-message-1" || completedOutbox.DeliveryOwner != "" {
		t.Fatalf("completed outbox = %#v, %v", completedOutbox, err)
	}
	if err := state.MarkOutboxDelivered(ctx, tenantID, event.ID); err != nil {
		t.Fatalf("MarkOutboxDelivered(idempotent) error = %v", err)
	}
	backlog, err = state.OutboxBacklog(ctx, tenantID, "channel_reply.telegram")
	if err != nil || backlog.Pending != 0 {
		t.Fatalf("OutboxBacklog(after delivery) = %#v, %v", backlog, err)
	}
	deletedOutbox, err := state.PurgeDeliveredOutboxBefore(ctx, tenantID, time.Now().Add(time.Hour), 10)
	if err != nil || deletedOutbox != 1 {
		t.Fatalf("PurgeDeliveredOutboxBefore() = %d, %v", deletedOutbox, err)
	}

	retryTracker, err := NewPostgresRetryTracker(database)
	if err != nil {
		t.Fatal(err)
	}
	if attempt, err := retryTracker.Increment(ctx, tenantID, firstKey, "message-1"); err != nil || attempt != 1 {
		t.Fatalf("retry Increment(first) = %d, %v", attempt, err)
	}
	if attempt, err := retryTracker.Increment(ctx, tenantID, firstKey, "message-1"); err != nil || attempt != 2 {
		t.Fatalf("retry Increment(second) = %d, %v", attempt, err)
	}
	if attempt, err := retryTracker.ListAttempts(ctx, tenantID, firstKey, "message-1"); err != nil || attempt != 2 {
		t.Fatalf("retry ListAttempts() = %d, %v", attempt, err)
	}
	if err := retryTracker.Clear(ctx, tenantID, firstKey, "message-1"); err != nil {
		t.Fatalf("retry Clear() error = %v", err)
	}
	if attempt, err := retryTracker.ListAttempts(ctx, tenantID, firstKey, "message-1"); err != nil || attempt != 0 {
		t.Fatalf("retry ListAttempts(after clear) = %d, %v", attempt, err)
	}

	lease, err := state.AcquireSessionExecutionLease(ctx, tenantID, firstKey, "execution-1", time.Minute)
	if err != nil || lease.FencingToken == 0 || lease.OwnerID != "execution-1" {
		t.Fatalf("AcquireSessionExecutionLease() = %#v, %v", lease, err)
	}
	renewedLease, err := state.RenewSessionExecutionLease(ctx, lease, 2*time.Minute)
	if err != nil || renewedLease.FencingToken != lease.FencingToken || !renewedLease.LeaseUntil.After(lease.LeaseUntil) {
		t.Fatalf("RenewSessionExecutionLease() = %#v, %v", renewedLease, err)
	}
	if err := state.ReleaseSessionExecutionLease(ctx, renewedLease); err != nil {
		t.Fatalf("ReleaseSessionExecutionLease() error = %v", err)
	}
	routes, err := state.ListInboundMessageRoutes(ctx, tenantID, firstKey)
	if err != nil || len(routes) != 1 || routes[0].MessageID != "message-1" || routes[0].ActorPlatformUserID != "platform-user-1" {
		t.Fatalf("ListInboundMessageRoutes() = %#v, %v", routes, err)
	}
	routes, err = state.ListInboundMessageRoutesByMessageIDs(ctx, tenantID, firstKey, []string{" message-1 ", "message-1", "", "missing"})
	if err != nil || len(routes) != 1 || routes[0].MessageID != "message-1" {
		t.Fatalf("ListInboundMessageRoutesByMessageIDs() = %#v, %v", routes, err)
	}
	emptyRoutes, err := state.ListInboundMessageRoutesByMessageIDs(ctx, tenantID, firstKey, nil)
	if err != nil || len(emptyRoutes) != 0 {
		t.Fatalf("ListInboundMessageRoutesByMessageIDs(empty) = %#v, %v", emptyRoutes, err)
	}

	if err := state.EndChannelIdentityRoutes(ctx, tenantID, "telegram", "support-bot", "external-user-1"); err != nil {
		t.Fatalf("EndChannelIdentityRoutes() error = %v", err)
	}
	secondKey := tenantID + "/support/session/second"
	resolved, err = state.ResolveSession(ctx, route, secondKey)
	if err != nil || resolved != secondKey {
		t.Fatalf("ResolveSession(after end) = %q, %v", resolved, err)
	}
	switchResult, err := state.SwitchSession(ctx, SessionSwitchRequest{
		Route: route, SessionKey: tenantID + "/support/session/switched", RequestID: "switch-request-1",
		OutboxType: "channel_reply.telegram", OutboxPayload: []byte(`{"channel":"telegram","text":"new session"}`),
	})
	if err != nil || switchResult.Replayed || switchResult.SessionKey == "" {
		t.Fatalf("SwitchSession() = %#v, %v", switchResult, err)
	}
	replayed, err := state.SwitchSession(ctx, SessionSwitchRequest{
		Route: route, SessionKey: tenantID + "/support/session/ignored", RequestID: "switch-request-1",
		OutboxType: "channel_reply.telegram", OutboxPayload: []byte(`{"channel":"telegram","text":"new session"}`),
	})
	if err != nil || !replayed.Replayed || replayed.SessionKey != switchResult.SessionKey {
		t.Fatalf("SwitchSession(replay) = %#v, %v", replayed, err)
	}
	if err := state.ArchiveSession(ctx, tenantID, switchResult.SessionKey); err != nil {
		t.Fatalf("ArchiveSession() error = %v", err)
	}
	archived, err := state.GetSession(ctx, tenantID, switchResult.SessionKey)
	if err != nil || archived.Status != "archived" || archived.ArchivedAt == nil {
		t.Fatalf("archived session = %#v, %v", archived, err)
	}
	if err := state.ArchiveSession(ctx, tenantID, "missing-session"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("ArchiveSession(missing) error = %v", err)
	}
}

func TestPostgresSessionCatalogRejectsIncompleteQueries(t *testing.T) {
	database := openStorageIntegrationDB(t)
	state, err := NewPostgresStateStore(database, []byte("storage-session-audit-key-at-least-32-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := state.GetSession(ctx, "", "session"); err == nil {
		t.Fatal("GetSession() accepted missing tenant")
	}
	if _, err := state.GetSession(ctx, "tenant", ""); err == nil {
		t.Fatal("GetSession() accepted missing key")
	}
	if _, err := state.ListSessions(ctx, "", 10); err == nil {
		t.Fatal("ListSessions() accepted missing tenant")
	}
	if _, err := state.ListSessions(ctx, "tenant", 0); err == nil {
		t.Fatal("ListSessions() accepted zero limit")
	}
	if _, err := state.ListApplicationSessions(ctx, "tenant", ""); err == nil {
		t.Fatal("ListApplicationSessions() accepted missing app")
	}
	if err := state.EndChannelIdentityRoutes(ctx, "", "telegram", "bot", "user"); err == nil {
		t.Fatal("EndChannelIdentityRoutes() accepted incomplete route")
	}
	if _, err := state.ListClaimableSessions(ctx, "tenant", "telegram", "bot", "user", 0); err == nil {
		t.Fatal("ListClaimableSessions() accepted zero limit")
	}
	if err := state.ClaimSession(ctx, "tenant", "", "user"); err == nil {
		t.Fatal("ClaimSession() accepted missing session")
	}
	if _, err := state.ListInboundMessageRoutes(ctx, "tenant", ""); err == nil {
		t.Fatal("ListInboundMessageRoutes() accepted missing session")
	}
	if _, err := state.ListInboundMessageRoutesByMessageIDs(ctx, "", "session", []string{"message"}); err == nil {
		t.Fatal("ListInboundMessageRoutesByMessageIDs() accepted missing tenant")
	}
	if err := state.ArchiveSession(ctx, "tenant", ""); err == nil {
		t.Fatal("ArchiveSession() accepted missing session")
	}
}
