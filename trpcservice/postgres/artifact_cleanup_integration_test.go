//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestArtifactCleanupClaimsExpiresRetriesAndProtectsActiveSession(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	cleanupArtifactFixtures(t, ctx, pool)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupArtifactFixtures(t, cleanupCtx, pool)
	})

	tenantID := fmt.Sprintf("artifact-cleanup-%d", time.Now().UnixNano())
	appID := "support"
	config := integrationAppConfig("v1", "cleanup-model")
	config.TenantID, config.AppID = tenantID, appID
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: tenantID, Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Support", ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create app: %v", err)
	}

	old := time.Now().UTC().Add(-2 * time.Hour)
	for _, sessionID := range []string{"deleted", "expired", "active", "reply"} {
		if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, "user-1", sessionID); err != nil {
			t.Fatalf("create session lane %s: %v", sessionID, err)
		}
	}
	insertCleanupArtifact(t, ctx, pool, tenantID, appID, "deleted", "deleted.txt", platformartifact.StatusDeleted, old)
	insertCleanupArtifact(t, ctx, pool, tenantID, appID, "expired", "expired.txt", platformartifact.StatusAvailable, old)
	insertCleanupArtifact(t, ctx, pool, tenantID, appID, "active", "active.txt", platformartifact.StatusAvailable, old)
	insertCleanupArtifact(t, ctx, pool, tenantID, appID, "reply", "reply.txt", platformartifact.StatusAvailable, old)
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.execution (
    tenant_id, app_id, request_id, session_principal_id, session_id, user_id,
    turn_seq, config_version, tenant_source, source_id, idempotency_key,
    payload_hash, command, status, trace_id
) VALUES ($1, $2, 'request-active', 'user-1', 'active', 'user-1', 1, 'v1',
          'authenticated_claims', 'source-active', 'idempotency-active',
          decode(repeat('ab', 32), 'hex'),
          '{"artifact_refs":["artifact://active.txt@0"]}'::jsonb,
          'RUNNING', 'trace-active')`, tenantID, appID); err != nil {
		t.Fatalf("create active execution: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.channel_binding (
    tenant_id, app_id, binding_id, channel, external_account, status
) VALUES ($1, $2, 'binding-cleanup', 'feishu', 'cleanup-account', 'ACTIVE')`, tenantID, appID); err != nil {
		t.Fatalf("create reply binding: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.execution (
    tenant_id, app_id, request_id, session_principal_id, session_id, user_id,
    turn_seq, config_version, tenant_source, source_id, idempotency_key,
    payload_hash, command, status, trace_id
) VALUES ($1, $2, 'request-reply', 'user-1', 'reply', 'user-1', 1, 'v1',
          'authenticated_claims', 'source-reply', 'idempotency-reply',
          decode(repeat('cd', 32), 'hex'), '{}'::jsonb, 'SUCCEEDED', 'trace-reply')`, tenantID, appID); err != nil {
		t.Fatalf("create reply execution: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.reply_outbox (
    reply_id, tenant_id, app_id, binding_id, channel, request_id,
    source_event_id, revision, target_ref, payload, status
) VALUES (concat('reply-cleanup-', $1::text), $1, $2, 'binding-cleanup', 'feishu', 'request-reply',
          'event-reply', 1, '{}'::jsonb,
          '{"text":"artifact://reply.txt@0"}'::jsonb, 'PENDING')`, tenantID, appID); err != nil {
		t.Fatalf("create pending reply: %v", err)
	}

	retentionBefore := time.Now().UTC().Add(-time.Hour)
	candidates, err := store.ClaimArtifactCleanup(
		ctx, "cleanup-worker-1", time.Now().UTC().Add(-time.Hour), &retentionBefore, time.Minute, 10,
	)
	if err != nil {
		t.Fatalf("claim cleanup candidates: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("cleanup candidates = %d, want 2: %#v", len(candidates), candidates)
	}
	byID := make(map[string]platformartifact.CleanupCandidate, len(candidates))
	for _, candidate := range candidates {
		byID[candidate.Record.Filename] = candidate
		if candidate.Record.ConfigVersion != "v1" || candidate.Record.TenantID != tenantID || candidate.Record.AppID != appID {
			t.Fatalf("candidate scope/config = %#v", candidate)
		}
	}
	if byID["expired.txt"].Record.Status != platformartifact.StatusDeleted {
		t.Fatalf("expired artifact status = %q, want DELETED", byID["expired.txt"].Record.Status)
	}
	if _, ok := byID["active.txt"]; ok {
		t.Fatal("active-session artifact was claimed")
	}
	if _, ok := byID["reply.txt"]; ok {
		t.Fatal("pending-reply artifact was claimed")
	}

	if err := store.CompleteArtifactCleanup(ctx, byID["deleted.txt"], "cleanup-worker-1"); err != nil {
		t.Fatalf("complete deleted artifact cleanup: %v", err)
	}
	if err := store.RetryArtifactCleanup(
		ctx,
		byID["expired.txt"],
		"cleanup-worker-1",
		time.Now().UTC().Add(time.Hour),
		errors.New("delete failed: password=artifact-secret"),
	); err != nil {
		t.Fatalf("retry expired artifact cleanup: %v", err)
	}

	var completedAt *time.Time
	var owner *string
	var lastError string
	if err := pool.QueryRow(ctx, `
SELECT cleanup_completed_at, cleanup_owner, cleanup_last_error
FROM platform.artifact
WHERE tenant_id = $1 AND app_id = $2 AND filename = 'deleted.txt'`, tenantID, appID).Scan(&completedAt, &owner, &lastError); err != nil {
		t.Fatalf("read completed cleanup: %v", err)
	}
	if completedAt == nil || owner != nil {
		t.Fatalf("completed cleanup state = completed_at %v owner %v", completedAt, owner)
	}
	if err := pool.QueryRow(ctx, `
SELECT cleanup_last_error
FROM platform.artifact
WHERE tenant_id = $1 AND app_id = $2 AND filename = 'expired.txt'`, tenantID, appID).Scan(&lastError); err != nil {
		t.Fatalf("read retry cleanup: %v", err)
	}
	if strings.Contains(lastError, "artifact-secret") {
		t.Fatalf("persisted cleanup error contains secret: %q", lastError)
	}
}

func TestArtifactAdmissionAndCleanupSerializeOnArtifactRow(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	ctx := p.ctx
	// The integration database is intentionally reusable between runs. Remove
	// stale rows from the artifact-focused tests so the global cleanup worker
	// cannot claim an unrelated candidate before this test's fixture.
	if _, err := p.pool.Exec(ctx, `
DELETE FROM platform.artifact
WHERE tenant_id LIKE 'artifact-reservation-%' OR tenant_id LIKE 'im05-%'`); err != nil {
		t.Fatalf("clean stale artifact fixtures: %v", err)
	}
	old := time.Now().UTC().Add(-2 * time.Hour)
	credentialSuffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	credentialDigest, err := auth.DigestAPIKey("tas_artifact_admission_key_" + credentialSuffix)
	if err != nil {
		t.Fatalf("digest credential: %v", err)
	}
	credential := auth.Credential{
		ID:        "credential-artifact-admission-" + credentialSuffix,
		TenantID:  p.scope.TenantID,
		AppID:     p.scope.AppID,
		KeyPrefix: "tas_art",
		Status:    auth.CredentialActive,
	}
	if err := p.store.CreateCredential(ctx, credentialDigest, credential); err != nil {
		t.Fatalf("create credential: %v", err)
	}

	insertArtifact := func(sessionID, filename, objectKey string, createdAt time.Time) {
		t.Helper()
		if _, err := p.pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, 'user-1', $3)
ON CONFLICT DO NOTHING`, p.scope.TenantID, p.scope.AppID, sessionID); err != nil {
			t.Fatalf("insert session lane %s: %v", sessionID, err)
		}
		if _, err := p.pool.Exec(ctx, `
INSERT INTO platform.artifact (
    artifact_id, tenant_id, app_id, session_principal_id, session_id,
    filename, version, object_key, mime_type, size_bytes, status, config_version,
    created_at, updated_at, cleanup_next_attempt_at
) VALUES (gen_random_uuid()::text, $1, $2, 'user-1', $3, $4, 0, $5,
          'text/plain', 1, 'AVAILABLE', 'v1', $6, $6, $6)`,
			p.scope.TenantID, p.scope.AppID, sessionID, filename, objectKey, createdAt); err != nil {
			t.Fatalf("insert artifact %s: %v", filename, err)
		}
	}
	insertArtifact("artifact-cleanup-first", "cleanup-first.txt", "objects/cleanup-first", old.Add(-time.Minute))
	insertArtifact("admission-first", "admission-first.txt", "objects/admission-first", old)

	cleanupFirst, err := p.store.ClaimArtifactCleanup(
		ctx,
		"artifact-cleanup-first-worker",
		time.Now().UTC().Add(-time.Hour),
		ptrTime(time.Now().UTC().Add(-time.Hour)),
		time.Minute,
		1,
	)
	if err != nil || len(cleanupFirst) != 1 || cleanupFirst[0].Record.Filename != "cleanup-first.txt" {
		t.Fatalf("cleanup-first candidates = %#v, err = %v", cleanupFirst, err)
	}
	if _, err := p.store.Admit(ctx, artifactAdmissionRequest(
		p,
		credential,
		credentialDigest,
		"request-cleanup-first",
		"artifact-cleanup-first",
		"cleanup-first.txt",
	)); err == nil {
		t.Fatal("admission succeeded for an artifact already claimed/deleted by cleanup")
	}

	admitted, err := p.store.Admit(ctx, artifactAdmissionRequest(
		p,
		credential,
		credentialDigest,
		"request-admission-first",
		"admission-first",
		"admission-first.txt",
	))
	if err != nil {
		t.Fatalf("admission-first request: %v", err)
	}
	if admitted.RequestID != "request-admission-first" {
		t.Fatalf("admitted request id = %q", admitted.RequestID)
	}
	cleanupAfterAdmission, err := p.store.ClaimArtifactCleanup(
		ctx,
		"admission-after-worker",
		time.Now().UTC().Add(-time.Hour),
		ptrTime(time.Now().UTC().Add(-time.Hour)),
		time.Minute,
		10,
	)
	if err != nil {
		t.Fatalf("cleanup after admission: %v", err)
	}
	for _, candidate := range cleanupAfterAdmission {
		if candidate.Record.Filename == "admission-first.txt" {
			t.Fatal("cleanup claimed an artifact referenced by an admitted execution")
		}
	}
}

func artifactAdmissionRequest(
	p im05Fixture,
	credential auth.Credential,
	digest [32]byte,
	requestID, sessionID, filename string,
) gateway.AdmissionRequest {
	return gateway.AdmissionRequest{
		RequestID:      requestID,
		IdempotencyKey: requestID,
		Identity: gateway.AdmissionIdentity{
			Tenant: tenant.RuntimeContext{
				TenantID:           p.scope.TenantID,
				AppID:              p.scope.AppID,
				ConfigVersion:      "v1",
				SessionPrincipalID: "user-1",
				SessionID:          sessionID,
				UserID:             "user-1",
				TraceID:            requestID + "-trace",
			},
			Source:           gateway.TenantSourceAuthenticatedClaims,
			SourceID:         credential.ID,
			CredentialDigest: gateway.CredentialDigest(digest),
		},
		Message: gateway.Message{
			Text:         "use artifact",
			ArtifactRefs: []string{"artifact://" + filename + "@0"},
		},
	}
}

func ptrTime(value time.Time) *time.Time { return &value }

func TestInboundArtifactCleanupClaimsRetriesAndCompletes(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tenantID := fmt.Sprintf("artifact-cleanup-inbound-%d", time.Now().UnixNano())
	appID := "support"
	config := integrationAppConfig("v1", "cleanup-inbound-model")
	config.TenantID, config.AppID = tenantID, appID
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: tenantID, Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Support", ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.channel_binding (
    tenant_id, app_id, binding_id, channel, external_account, status
) VALUES ($1, $2, 'binding-inbound-cleanup', 'feishu', 'inbound-cleanup-account', 'ACTIVE')`, tenantID, appID); err != nil {
		t.Fatalf("create binding: %v", err)
	}

	old := time.Now().UTC().Add(-2 * time.Hour)
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.inbound_artifact (
    tenant_id, app_id, binding_id, external_message_id, item_no,
    artifact_ref, config_version, filename, object_key, mime_type, size_bytes,
    status, created_at, updated_at, cleanup_next_attempt_at
) VALUES ($1, $2, 'binding-inbound-cleanup', 'message-inbound-cleanup', 0,
          'artifact://inbound/cleanup@0', 'v1', 'cleanup.bin', 'objects/inbound-cleanup',
          'application/octet-stream', 1, 'PENDING', $3, $3, $3)`, tenantID, appID, old); err != nil {
		t.Fatalf("insert inbound artifact: %v", err)
	}

	candidates, err := store.ClaimInboundArtifactCleanup(ctx, "inbound-cleanup-1", time.Now().UTC().Add(-time.Hour), time.Minute, 10)
	if err != nil {
		t.Fatalf("claim inbound cleanup: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Attempts != 1 || candidates[0].ObjectKey != "objects/inbound-cleanup" {
		t.Fatalf("inbound cleanup candidates = %#v", candidates)
	}
	candidate := candidates[0]
	if err := store.RetryInboundArtifactCleanup(ctx, candidate, "inbound-cleanup-1", time.Now().UTC().Add(time.Hour), errors.New("delete failed: api_key=cleanup-secret")); err != nil {
		t.Fatalf("retry inbound cleanup: %v", err)
	}
	var lastError string
	if err := pool.QueryRow(ctx, `
SELECT cleanup_last_error
FROM platform.inbound_artifact
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = 'binding-inbound-cleanup'`, tenantID, appID).Scan(&lastError); err != nil {
		t.Fatalf("read inbound cleanup retry: %v", err)
	}
	if strings.Contains(lastError, "cleanup-secret") {
		t.Fatalf("persisted inbound cleanup error contains secret: %q", lastError)
	}

	if _, err := pool.Exec(ctx, `
UPDATE platform.inbound_artifact
SET cleanup_next_attempt_at = clock_timestamp() - interval '1 second'
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = 'binding-inbound-cleanup'`, tenantID, appID); err != nil {
		t.Fatalf("make inbound cleanup retryable: %v", err)
	}
	candidates, err = store.ClaimInboundArtifactCleanup(ctx, "inbound-cleanup-2", time.Now().UTC().Add(-time.Hour), time.Minute, 10)
	if err != nil {
		t.Fatalf("reclaim inbound cleanup: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Attempts != 2 {
		t.Fatalf("reclaimed inbound cleanup candidates = %#v", candidates)
	}
	if err := store.CompleteInboundArtifactCleanup(ctx, candidates[0], "inbound-cleanup-2"); err != nil {
		t.Fatalf("complete inbound cleanup: %v", err)
	}

	var completedAt *time.Time
	var owner *string
	if err := pool.QueryRow(ctx, `
SELECT cleanup_completed_at, cleanup_owner
FROM platform.inbound_artifact
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = 'binding-inbound-cleanup'`, tenantID, appID).Scan(&completedAt, &owner); err != nil {
		t.Fatalf("read completed inbound cleanup: %v", err)
	}
	if completedAt == nil || owner != nil {
		t.Fatalf("completed inbound cleanup state = completed_at %v owner %v", completedAt, owner)
	}
}

func TestInboundArtifactUploadReservationSurvivesCrashWindow(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	tenantID := fmt.Sprintf("artifact-cleanup-upload-%d", time.Now().UnixNano())
	appID := "support"
	config := integrationAppConfig("v1", "cleanup-upload-model")
	config.TenantID, config.AppID = tenantID, appID
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: tenantID, Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Support", ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.channel_binding (
    tenant_id, app_id, binding_id, channel, external_account, status
) VALUES ($1, $2, 'binding-upload-crash', 'feishu', 'upload-crash-account', 'ACTIVE')`, tenantID, appID); err != nil {
		t.Fatalf("create binding: %v", err)
	}
	record := platformpostgres.StagedInboundArtifact{
		TenantID: tenantID, AppID: appID, BindingID: "binding-upload-crash",
		ExternalMessageID: "message-upload-crash", ItemNo: 0,
		ArtifactRef: "artifact://inbound/crash-1@0", ConfigVersion: "v1",
		Filename: "inbound/crash-1", ObjectKey: "objects/inbound-crash-1",
		MIMEType: "application/octet-stream", Size: 4,
	}
	reserved, err := store.ReserveInboundArtifact(ctx, record)
	if err != nil || !reserved.Created || reserved.Status != "UPLOADING" {
		t.Fatalf("reserve result = %+v, err = %v", reserved, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE platform.inbound_artifact
SET updated_at = clock_timestamp() - interval '2 hours',
    cleanup_next_attempt_at = clock_timestamp() - interval '2 hours'
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = 'binding-upload-crash'`, tenantID, appID); err != nil {
		t.Fatalf("age upload reservation: %v", err)
	}
	candidates, err := store.ClaimInboundArtifactCleanup(ctx, "upload-cleanup-1", time.Now().UTC().Add(-time.Minute), time.Minute, 10)
	if err != nil {
		t.Fatalf("claim active upload cleanup: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("active upload was claimed before lease expiry: %#v", candidates)
	}
	if _, err := pool.Exec(ctx, `
UPDATE platform.inbound_artifact
SET upload_lease_until = clock_timestamp() - interval '1 second'
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = 'binding-upload-crash'`, tenantID, appID); err != nil {
		t.Fatalf("expire upload reservation lease: %v", err)
	}
	candidates, err = store.ClaimInboundArtifactCleanup(ctx, "upload-cleanup-1", time.Now().UTC().Add(-time.Minute), time.Minute, 10)
	if err != nil || len(candidates) != 1 || candidates[0].ObjectKey != record.ObjectKey {
		t.Fatalf("upload cleanup candidates = %#v, err = %v", candidates, err)
	}
	if _, err := store.FinalizeInboundArtifactUpload(ctx, reserved); err == nil {
		t.Fatal("stale uploader finalized after cleanup reclaimed its lease")
	}
	if err := store.CompleteInboundArtifactCleanup(ctx, candidates[0], "upload-cleanup-1"); err != nil {
		t.Fatalf("complete upload cleanup: %v", err)
	}
	retryRecord := record
	retryRecord.ArtifactRef = "artifact://inbound/crash-2@0"
	retryRecord.ObjectKey = "objects/inbound-crash-2"
	retry, err := store.ReserveInboundArtifact(ctx, retryRecord)
	if err != nil || !retry.Created || retry.Status != "UPLOADING" {
		t.Fatalf("retry reservation = %+v, err = %v", retry, err)
	}
}

func cleanupArtifactFixtures(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, query := range []string{
		`DELETE FROM platform.reply_outbox WHERE tenant_id LIKE 'artifact-cleanup-%'`,
		`DELETE FROM platform.execution_event WHERE tenant_id LIKE 'artifact-cleanup-%'`,
		`DELETE FROM platform.execution WHERE tenant_id LIKE 'artifact-cleanup-%'`,
		`DELETE FROM platform.inbound_artifact WHERE tenant_id LIKE 'artifact-cleanup-%'`,
		`DELETE FROM platform.channel_binding WHERE tenant_id LIKE 'artifact-cleanup-%'`,
		`DELETE FROM platform.artifact
         WHERE tenant_id LIKE 'artifact-cleanup-%'
            OR tenant_id LIKE 'artifact-reservation-%'
            OR tenant_id LIKE 'im05-%'`,
		`DELETE FROM platform.session_lane WHERE tenant_id LIKE 'artifact-cleanup-%'`,
	} {
		if _, err := pool.Exec(ctx, query); err != nil {
			t.Fatalf("clean artifact fixtures: %v", err)
		}
	}
}

func insertCleanupArtifact(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	scopeTenant, appID, sessionID, filename string,
	status platformartifact.Status,
	createdAt time.Time,
) {
	t.Helper()
	// Kept as a separate helper so the test data makes the SQL authority and
	// the exact object key explicit.
	_, err := pool.Exec(ctx, `
INSERT INTO platform.artifact (
    artifact_id, tenant_id, app_id, session_principal_id, session_id,
    filename, version, object_key, mime_type, size_bytes, status, config_version,
    created_at, updated_at, cleanup_next_attempt_at
) VALUES ($1, $2, $3, 'user-1', $4, $5, 0, $6, 'text/plain', 1, $7, 'v1', $8, $8, $8)`,
		"artifact-"+scopeTenant+"-"+sessionID,
		scopeTenant,
		appID,
		sessionID,
		filename,
		"objects/"+sessionID,
		status,
		createdAt,
	)
	if err != nil {
		t.Fatalf("insert cleanup artifact %s: %v", filename, err)
	}
}
