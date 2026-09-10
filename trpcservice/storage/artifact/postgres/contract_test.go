package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
	objectmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/inmemory"
)

func TestObjectBackedArtifactStorePostgreSQL16(t *testing.T) {
	db := openContractDB(t)
	ctx := context.Background()
	const (
		tenantID  = "t_01ARZ3NDEKTSV4RRFFQ69G5FAY"
		appID     = "app_01ARZ3NDEKTSV4RRFFQ69G5FAY"
		requestID = "artifact-object-request"
	)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM artifact_object_upload WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM media_artifact WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM prepared_payload WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM inbox WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM agent_app WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM tenant WHERE tenant_id=$1`, tenantID)
	})
	if _, err := db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES($1,'artifact-contract','Artifact Contract')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name) VALUES($1,$2,'artifact-contract','Artifact Contract')`, tenantID, appID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,agent_app_id,session_id,state,payload_ref,payload_digest,key_version)
VALUES($1,'artifact','account','message',$2,$3,'session','dispatch_pending','inbound://artifact',repeat('a',64),1)`, tenantID, requestID, appID); err != nil {
		t.Fatal(err)
	}

	objects := objectmemory.New()
	store := NewWithObjectStore(db, objects)
	firstInput := contractArtifact(t, tenantID, requestID, 0, []byte("object-backed content"))
	if _, err := NewWithObjectStore(db, nil).PutArtifact(ctx, firstInput); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("nil object store err=%v", err)
	}
	if _, err := NewWithObjectStoreOptions(db, objects, ObjectStoreOptions{PutTimeout: time.Minute, UploadProtection: time.Second}).
		PutArtifact(ctx, firstInput); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("unsafe upload protection err=%v", err)
	}
	first, err := store.PutArtifact(ctx, firstInput)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := store.PutArtifact(ctx, firstInput)
	if err != nil || repeated.ArtifactRef != first.ArtifactRef {
		t.Fatalf("repeated=%#v err=%v", repeated, err)
	}
	var uploadCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM artifact_object_upload WHERE tenant_id=$1`, tenantID).Scan(&uploadCount); err != nil || uploadCount != 0 {
		t.Fatalf("committed upload intents=%d err=%v", uploadCount, err)
	}
	loaded, err := store.GetArtifact(ctx, tenantID, first.ArtifactID)
	if err != nil || string(loaded.Content) != "object-backed content" {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	var storageKind, objectKey string
	var contentIsNull bool
	var contentSize int64
	if err := db.QueryRowContext(ctx, `SELECT storage_kind,object_key,content IS NULL,content_size FROM media_artifact WHERE tenant_id=$1 AND artifact_id=$2`,
		tenantID, first.ArtifactID).Scan(&storageKind, &objectKey, &contentIsNull, &contentSize); err != nil {
		t.Fatal(err)
	}
	if storageKind != storageObject || !contentIsNull || contentSize != int64(len(firstInput.Content)) ||
		strings.Contains(objectKey, tenantID) || strings.Contains(objectKey, requestID) {
		t.Fatalf("kind=%q key=%q null=%t size=%d", storageKind, objectKey, contentIsNull, contentSize)
	}
	if _, err := New(db).GetArtifact(ctx, tenantID, first.ArtifactID); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("object row read without object store err=%v", err)
	}
	if _, err := NewWithObjectStore(db, objectmemory.New()).GetArtifact(ctx, tenantID, first.ArtifactID); !errors.Is(err, runtime.ErrBackendUnavailable) {
		t.Fatalf("missing object err=%v", err)
	}

	legacy := New(db)
	legacyInput := contractArtifact(t, tenantID, requestID, 1, []byte("postgres bytea content"))
	legacyRecord, err := legacy.PutArtifact(ctx, legacyInput)
	if err != nil {
		t.Fatal(err)
	}
	rollingRead, err := store.GetArtifact(ctx, tenantID, legacyRecord.ArtifactID)
	if err != nil || string(rollingRead.Content) != "postgres bytea content" {
		t.Fatalf("rolling read=%#v err=%v", rollingRead, err)
	}
	collision := firstInput
	collision.Content = []byte("changed content")
	digest := sha256.Sum256(collision.Content)
	collision.ContentDigest = hex.EncodeToString(digest[:])
	if _, err := store.PutArtifact(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("collision err=%v", err)
	}

	now := time.Now().UTC()
	orphanInput := contractArtifact(t, tenantID, requestID, 2, []byte("uncommitted object"))
	orphanKey, err := objectstore.StableKey(tenantID, orphanInput.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.protectObjectUpload(ctx, orphanInput, orphanKey); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.PutObject(ctx, objectstore.Object{TenantID: tenantID, ObjectKey: orphanKey,
		ContentDigest: orphanInput.ContentDigest, Content: orphanInput.Content}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE artifact_object_upload SET protect_until=$3 WHERE tenant_id=$1 AND object_key=$2`,
		tenantID, orphanKey, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	reconciler := artifact.ObjectLifecycleReconciler{Store: store, Objects: objects, Owner: "artifact-contract-cleaner",
		Now: func() time.Time { return now }, ClaimTTL: time.Minute, BatchSize: 10}
	if handled, err := reconciler.RunOnce(ctx); err != nil || handled != 1 {
		t.Fatalf("orphan cleanup handled=%d err=%v", handled, err)
	}
	if _, err := objects.GetObject(ctx, tenantID, orphanKey); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("orphan object remains err=%v", err)
	}

	firstKey, err := objectstore.StableKey(tenantID, first.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO artifact_object_upload(
tenant_id,object_key,artifact_id,request_id,content_digest,content_size,protect_until)
VALUES($1,$2,$3,$4,$5,$6,$7)`, tenantID, firstKey, first.ArtifactID, requestID, first.ContentDigest,
		len(first.Content), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if handled, err := reconciler.RunOnce(ctx); err != nil || handled != 1 {
		t.Fatalf("referenced cleanup handled=%d err=%v", handled, err)
	}
	if _, err := objects.GetObject(ctx, tenantID, firstKey); err != nil {
		t.Fatalf("referenced object removed: %v", err)
	}

	wrongDigest := strings.Repeat("0", 64)
	if wrongDigest == first.ContentDigest {
		wrongDigest = strings.Repeat("1", 64)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO artifact_object_upload(
tenant_id,object_key,artifact_id,request_id,content_digest,content_size,protect_until)
VALUES($1,$2,$3,$4,$5,$6,$7)`, tenantID, firstKey, first.ArtifactID, requestID, wrongDigest,
		len(first.Content), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if handled, err := reconciler.RunOnce(ctx); !errors.Is(err, runtime.ErrVersionMismatch) || handled != 1 {
		t.Fatalf("mismatched reference handled=%d err=%v", handled, err)
	}
	var state, errorClass string
	var attempt int
	var quarantinedAt time.Time
	if err := db.QueryRowContext(ctx, `SELECT state,cleanup_attempt,last_error_class,quarantined_at
FROM artifact_object_upload WHERE tenant_id=$1 AND object_key=$2`, tenantID, firstKey).
		Scan(&state, &attempt, &errorClass, &quarantinedAt); err != nil {
		t.Fatal(err)
	}
	if state != string(artifact.ObjectQuarantined) || attempt != 1 ||
		errorClass != artifact.CleanupErrorVersionMismatch || quarantinedAt.IsZero() {
		t.Fatalf("state=%q attempt=%d class=%q quarantined_at=%v", state, attempt, errorClass, quarantinedAt)
	}
	var auditKind, auditState, auditRef string
	if err := db.QueryRowContext(ctx, `SELECT kind,state,payload_ref FROM outbox
WHERE tenant_id=$1 AND aggregate_id=$2 AND idempotency_key LIKE 'artifact-quarantine-upload:%'`, tenantID, first.ArtifactID).
		Scan(&auditKind, &auditState, &auditRef); err != nil {
		t.Fatal(err)
	}
	if auditKind != "audit" || auditState != "pending" ||
		!strings.HasPrefix(auditRef, "artifact-quarantine://"+tenantID+"/upload/"+first.ArtifactID+"/") {
		t.Fatalf("quarantine audit kind=%q state=%q ref=%q", auditKind, auditState, auditRef)
	}
	if handled, err := reconciler.RunOnce(ctx); err != nil || handled != 0 {
		t.Fatalf("quarantined row reclaimed handled=%d err=%v", handled, err)
	}
	if _, err := objects.GetObject(ctx, tenantID, firstKey); err != nil {
		t.Fatalf("quarantined object removed: %v", err)
	}

	retainedInput := contractArtifact(t, tenantID, requestID, 3, []byte("referenced retention content"))
	retained, err := store.PutArtifact(ctx, retainedInput)
	if err != nil {
		t.Fatal(err)
	}
	preparedContent := []byte(`{"media":"retained"}`)
	preparedDigest := sha256.Sum256(preparedContent)
	payloadStore := messagingpostgres.NewWithPayloadKey(db, bytes.Repeat([]byte{0x42}, 32), 1)
	prepared := messaging.PreparedPayloadRecord{TenantID: tenantID, RequestID: requestID,
		PayloadRef: "prepared://" + tenantID + "/retention-contract", SourcePayloadRef: "inbound://" + tenantID + "/" + requestID,
		ContentDigest: hex.EncodeToString(preparedDigest[:]), Content: preparedContent, KeyVersion: 1,
		ArtifactRetention: time.Hour, ArtifactReferences: []messaging.PreparedArtifactReference{{ArtifactID: retained.ArtifactID}}}
	if err := payloadStore.PutPreparedPayload(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	if err := payloadStore.PutPreparedPayload(ctx, prepared); err != nil {
		t.Fatalf("prepared payload retry: %v", err)
	}
	var preparedCreatedAt, retainUntil time.Time
	var retentionSeconds int64
	if err := db.QueryRowContext(ctx, `SELECT p.created_at,p.artifact_retention_seconds,r.retain_until
FROM prepared_payload p JOIN artifact_reference r ON r.tenant_id=p.tenant_id AND r.reference_id=p.payload_ref
WHERE p.tenant_id=$1 AND p.request_id=$2 AND r.artifact_id=$3`, tenantID, requestID, retained.ArtifactID).
		Scan(&preparedCreatedAt, &retentionSeconds, &retainUntil); err != nil {
		t.Fatal(err)
	}
	if retentionSeconds != 3600 || !retainUntil.Equal(preparedCreatedAt.Add(time.Hour)) {
		t.Fatalf("created=%v seconds=%d retain_until=%v", preparedCreatedAt, retentionSeconds, retainUntil)
	}
	retentionReconciler := artifact.RetentionReconciler{Store: store, Objects: objects, Owner: "retention-contract-cleaner",
		OrphanGrace: 365 * 24 * time.Hour, Now: func() time.Time { return retainUntil.Add(-time.Second) }}
	if handled, err := retentionReconciler.RunOnce(ctx); err != nil || handled != 0 {
		t.Fatalf("live reference handled=%d err=%v", handled, err)
	}
	retentionReconciler.Now = func() time.Time { return retainUntil }
	if handled, err := retentionReconciler.RunOnce(ctx); err != nil || handled != 1 {
		t.Fatalf("expired reference handled=%d err=%v", handled, err)
	}
	if _, err := store.GetArtifact(ctx, tenantID, retained.ArtifactID); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("expired artifact metadata err=%v", err)
	}
}

func contractArtifact(t *testing.T, tenantID, requestID string, ordinal int, content []byte) artifact.Record {
	t.Helper()
	source := sha256.Sum256([]byte("source-" + strconv.Itoa(ordinal)))
	sourceDigest := hex.EncodeToString(source[:])
	artifactID, artifactRef, err := artifact.StableIdentity(tenantID, requestID, ordinal, sourceDigest)
	if err != nil {
		t.Fatal(err)
	}
	contentDigest := sha256.Sum256(content)
	return artifact.Record{TenantID: tenantID, RequestID: requestID, ArtifactID: artifactID, ArtifactRef: artifactRef,
		Ordinal: ordinal, SourceDigest: sourceDigest, ContentDigest: hex.EncodeToString(contentDigest[:]), MediaType: "text/plain",
		Kind: "file", Content: content, MalwareScanVersion: "av-contract-1", DLPVersion: "dlp-contract-1"}
}

func openContractDB(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var serverMajor int
	if err := db.QueryRowContext(context.Background(), `SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &serverMajor); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || serverMajor != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, serverMajor)
	}
	return db
}
