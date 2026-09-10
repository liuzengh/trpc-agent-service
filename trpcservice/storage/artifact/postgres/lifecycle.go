package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
)

const (
	defaultObjectPutTimeout = 2 * time.Minute
	defaultUploadProtection = 10 * time.Minute
	maximumLifecycleBatch   = 1000
)

func (s *Store) putTimeout() time.Duration {
	if s.objectPutTimeout > 0 {
		return s.objectPutTimeout
	}
	return defaultObjectPutTimeout
}

func (s *Store) protectionWindow() (time.Duration, error) {
	protection := s.uploadProtection
	if protection <= 0 {
		protection = defaultUploadProtection
	}
	if protection <= s.putTimeout() {
		return 0, runtime.ErrInvariantViolation
	}
	return protection, nil
}

func (s *Store) protectObjectUpload(ctx context.Context, in artifact.Record, objectKey string) (artifact.ObjectUpload, error) {
	protection, err := s.protectionWindow()
	if err != nil {
		return artifact.ObjectUpload{}, err
	}
	protectUntil := time.Now().UTC().Add(protection)
	upload, err := scanObjectUpload(s.db.QueryRowContext(ctx, `INSERT INTO artifact_object_upload(
tenant_id,object_key,artifact_id,request_id,content_digest,content_size,state,protect_until)
VALUES($1,$2,$3,$4,$5,$6,'uploading',$7)
ON CONFLICT (tenant_id,object_key) DO UPDATE SET
protect_until=GREATEST(artifact_object_upload.protect_until,EXCLUDED.protect_until),
updated_at=clock_timestamp(),version=artifact_object_upload.version+1
WHERE artifact_object_upload.artifact_id=EXCLUDED.artifact_id
  AND artifact_object_upload.request_id=EXCLUDED.request_id
  AND artifact_object_upload.content_digest=EXCLUDED.content_digest
  AND artifact_object_upload.content_size=EXCLUDED.content_size
  AND artifact_object_upload.state='uploading'
RETURNING tenant_id,object_key,artifact_id,request_id,content_digest,content_size,state,protect_until,
COALESCE(claim_owner,''),COALESCE(claim_until,'epoch'),cleanup_attempt,COALESCE(last_error_class,''),
COALESCE(quarantined_at,'epoch'),version`, in.TenantID, objectKey, in.ArtifactID, in.RequestID,
		in.ContentDigest, len(in.Content), protectUntil))
	if errors.Is(err, sql.ErrNoRows) {
		return artifact.ObjectUpload{}, runtime.ErrVersionConflict
	}
	if err != nil {
		return artifact.ObjectUpload{}, translate(err)
	}
	return upload, nil
}

func (s *Store) renewObjectUpload(ctx context.Context, upload artifact.ObjectUpload) (artifact.ObjectUpload, error) {
	protection, err := s.protectionWindow()
	if err != nil {
		return artifact.ObjectUpload{}, err
	}
	protectUntil := time.Now().UTC().Add(protection)
	renewed, err := scanObjectUpload(s.db.QueryRowContext(ctx, `UPDATE artifact_object_upload SET
protect_until=$7,updated_at=clock_timestamp(),version=version+1
WHERE tenant_id=$1 AND object_key=$2 AND artifact_id=$3 AND request_id=$4 AND content_digest=$5 AND content_size=$6
  AND state='uploading' AND version=$8
RETURNING tenant_id,object_key,artifact_id,request_id,content_digest,content_size,state,protect_until,
COALESCE(claim_owner,''),COALESCE(claim_until,'epoch'),cleanup_attempt,COALESCE(last_error_class,''),
COALESCE(quarantined_at,'epoch'),version`, upload.TenantID, upload.ObjectKey, upload.ArtifactID,
		upload.RequestID, upload.ContentDigest, upload.ContentSize, protectUntil, upload.Version))
	if errors.Is(err, sql.ErrNoRows) {
		return artifact.ObjectUpload{}, runtime.ErrVersionConflict
	}
	if err != nil {
		return artifact.ObjectUpload{}, translate(err)
	}
	return renewed, nil
}

func (s *Store) commitObjectMetadata(ctx context.Context, in artifact.Record, upload artifact.ObjectUpload) (storedMetadata, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storedMetadata{}, err
	}
	defer tx.Rollback()
	locked, err := scanObjectUpload(tx.QueryRowContext(ctx, `SELECT tenant_id,object_key,artifact_id,request_id,content_digest,
content_size,state,protect_until,COALESCE(claim_owner,''),COALESCE(claim_until,'epoch'),cleanup_attempt,
COALESCE(last_error_class,''),COALESCE(quarantined_at,'epoch'),version
FROM artifact_object_upload WHERE tenant_id=$1 AND object_key=$2 FOR UPDATE`, upload.TenantID, upload.ObjectKey))
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !sameObjectUpload(locked, upload)) {
		return storedMetadata{}, runtime.ErrVersionConflict
	}
	if err != nil {
		return storedMetadata{}, translate(err)
	}
	stored, err := insertMetadata(ctx, tx, in, storageObject, upload.ObjectKey, nil)
	if err != nil {
		return storedMetadata{}, err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM artifact_object_upload
WHERE tenant_id=$1 AND object_key=$2 AND state='uploading' AND version=$3`, upload.TenantID, upload.ObjectKey, upload.Version)
	if err != nil {
		return storedMetadata{}, translate(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return storedMetadata{}, runtime.ErrVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return storedMetadata{}, err
	}
	return stored, nil
}

func (s *Store) resolveConcurrentArtifact(ctx context.Context, in artifact.Record) (artifact.Record, bool, error) {
	stored, err := s.GetArtifact(ctx, in.TenantID, in.ArtifactID)
	if errors.Is(err, runtime.ErrNotFound) {
		return artifact.Record{}, false, nil
	}
	if err != nil {
		return artifact.Record{}, true, err
	}
	if !sameArtifact(stored, in) {
		return artifact.Record{}, true, runtime.ErrIdempotencyCollision
	}
	return stored, true, nil
}

func (s *Store) ClaimExpiredObjectUploads(ctx context.Context, now time.Time, owner string, ttl time.Duration, limit int) ([]artifact.ObjectUpload, error) {
	if s == nil || s.db == nil || now.IsZero() || owner == "" || ttl <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > maximumLifecycleBatch {
		limit = maximumLifecycleBatch
	}
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
  SELECT tenant_id,object_key FROM artifact_object_upload
  WHERE (state='uploading' AND protect_until <= $1)
     OR (state='cleanup_claimed' AND claim_until <= $1)
  ORDER BY created_at,tenant_id,object_key
  LIMIT $4
  FOR UPDATE SKIP LOCKED
)
UPDATE artifact_object_upload u SET state='cleanup_claimed',claim_owner=$2,claim_until=$3,
updated_at=clock_timestamp(),version=u.version+1
FROM candidates c WHERE u.tenant_id=c.tenant_id AND u.object_key=c.object_key
RETURNING u.tenant_id,u.object_key,u.artifact_id,u.request_id,u.content_digest,u.content_size,u.state,u.protect_until,
u.claim_owner,u.claim_until,u.cleanup_attempt,COALESCE(u.last_error_class,''),COALESCE(u.quarantined_at,'epoch'),u.version`,
		now, owner, now.Add(ttl), limit)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	uploads := make([]artifact.ObjectUpload, 0, limit)
	for rows.Next() {
		upload, err := scanObjectUpload(rows)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, upload)
	}
	return uploads, rows.Err()
}

func (s *Store) ObjectUploadReferenced(ctx context.Context, upload artifact.ObjectUpload) (bool, error) {
	if s == nil || s.db == nil || upload.State != artifact.ObjectCleanupClaimed {
		return false, runtime.ErrInvariantViolation
	}
	var artifactID, requestID, contentDigest, storageKind string
	var contentSize int64
	err := s.db.QueryRowContext(ctx, `SELECT artifact_id,request_id,content_digest,content_size,storage_kind
FROM media_artifact WHERE tenant_id=$1 AND object_key=$2`, upload.TenantID, upload.ObjectKey).
		Scan(&artifactID, &requestID, &contentDigest, &contentSize, &storageKind)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, translate(err)
	}
	if storageKind != storageObject || artifactID != upload.ArtifactID || requestID != upload.RequestID ||
		contentDigest != upload.ContentDigest || contentSize != upload.ContentSize {
		return false, runtime.ErrVersionMismatch
	}
	return true, nil
}

func (s *Store) FinishObjectUploadCleanup(ctx context.Context, upload artifact.ObjectUpload) error {
	if s == nil || s.db == nil || upload.State != artifact.ObjectCleanupClaimed || upload.ClaimOwner == "" || upload.Version < 1 {
		return runtime.ErrInvariantViolation
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM artifact_object_upload
WHERE tenant_id=$1 AND object_key=$2 AND state='cleanup_claimed' AND claim_owner=$3 AND version=$4`,
		upload.TenantID, upload.ObjectKey, upload.ClaimOwner, upload.Version)
	if err != nil {
		return translate(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}

func (s *Store) DeferObjectUploadCleanup(
	ctx context.Context, upload artifact.ObjectUpload, protectUntil time.Time, errorClass string,
) error {
	if s == nil || s.db == nil || upload.State != artifact.ObjectCleanupClaimed || upload.ClaimOwner == "" ||
		upload.Version < 1 || protectUntil.IsZero() || errorClass == "" {
		return runtime.ErrInvariantViolation
	}
	result, err := s.db.ExecContext(ctx, `UPDATE artifact_object_upload SET state='uploading',protect_until=$5,
claim_owner=NULL,claim_until=NULL,cleanup_attempt=cleanup_attempt+1,last_error_class=$6,
updated_at=clock_timestamp(),version=version+1
WHERE tenant_id=$1 AND object_key=$2 AND state='cleanup_claimed' AND claim_owner=$3 AND version=$4`,
		upload.TenantID, upload.ObjectKey, upload.ClaimOwner, upload.Version, protectUntil, errorClass)
	if err != nil {
		return translate(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}

func (s *Store) QuarantineObjectUpload(
	ctx context.Context, upload artifact.ObjectUpload, errorClass string, quarantinedAt time.Time,
) error {
	if s == nil || s.db == nil || upload.State != artifact.ObjectCleanupClaimed || upload.ClaimOwner == "" ||
		upload.Version < 1 || errorClass == "" || quarantinedAt.IsZero() {
		return runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return translate(err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE artifact_object_upload SET state='quarantined',
claim_owner=NULL,claim_until=NULL,cleanup_attempt=cleanup_attempt+1,last_error_class=$5,quarantined_at=$6,
updated_at=clock_timestamp(),version=version+1
WHERE tenant_id=$1 AND object_key=$2 AND state='cleanup_claimed' AND claim_owner=$3 AND version=$4`,
		upload.TenantID, upload.ObjectKey, upload.ClaimOwner, upload.Version, errorClass, quarantinedAt)
	if err != nil {
		return translate(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return runtime.ErrVersionConflict
	}
	nextVersion := upload.Version + 1
	outboxID := fmt.Sprintf("artifact-quarantine-upload:%s:%s:%d", upload.TenantID, upload.ArtifactID, nextVersion)
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
VALUES($1,$2,'audit',$3,$4,$2,$5)`,
		upload.TenantID, outboxID, upload.ArtifactID, nextVersion,
		fmt.Sprintf("artifact-quarantine://%s/upload/%s/%d", upload.TenantID, upload.ArtifactID, nextVersion))
	if err != nil {
		return translate(err)
	}
	return tx.Commit()
}

func scanObjectUpload(row rowScanner) (artifact.ObjectUpload, error) {
	var upload artifact.ObjectUpload
	err := row.Scan(&upload.TenantID, &upload.ObjectKey, &upload.ArtifactID, &upload.RequestID, &upload.ContentDigest,
		&upload.ContentSize, &upload.State, &upload.ProtectUntil, &upload.ClaimOwner, &upload.ClaimUntil,
		&upload.CleanupAttempt, &upload.LastErrorClass, &upload.QuarantinedAt, &upload.Version)
	return upload, err
}

func sameObjectUpload(left, right artifact.ObjectUpload) bool {
	return left.TenantID == right.TenantID && left.ObjectKey == right.ObjectKey && left.ArtifactID == right.ArtifactID &&
		left.RequestID == right.RequestID && left.ContentDigest == right.ContentDigest && left.ContentSize == right.ContentSize &&
		left.State == artifact.ObjectUploading && left.Version == right.Version
}

var _ artifact.ObjectLifecycleStore = (*Store)(nil)
