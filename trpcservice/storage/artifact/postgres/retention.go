package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
)

func (s *Store) ClaimExpiredArtifacts(
	ctx context.Context, now, orphanBefore time.Time, owner string, ttl time.Duration, limit int,
) ([]artifact.RetainedArtifact, error) {
	if s == nil || s.db == nil || now.IsZero() || orphanBefore.IsZero() || !orphanBefore.Before(now) || owner == "" || ttl <= 0 {
		return nil, runtime.ErrInvariantViolation
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > maximumLifecycleBatch {
		limit = maximumLifecycleBatch
	}
	rows, err := s.db.QueryContext(ctx, `WITH candidates AS (
  SELECT a.tenant_id,a.artifact_id
  FROM media_artifact a
  WHERE a.retention_managed AND ((
    a.lifecycle_state='active' AND (
      (a.created_at <= $2 AND NOT EXISTS (
        SELECT 1 FROM artifact_reference r
        WHERE r.tenant_id=a.tenant_id AND r.artifact_id=a.artifact_id
      ))
      OR
      (EXISTS (
        SELECT 1 FROM artifact_reference r
        WHERE r.tenant_id=a.tenant_id AND r.artifact_id=a.artifact_id
      ) AND NOT EXISTS (
        SELECT 1 FROM artifact_reference r
        WHERE r.tenant_id=a.tenant_id AND r.artifact_id=a.artifact_id AND r.retain_until > $1
      ))
    )
  ) OR (a.lifecycle_state='delete_claimed' AND a.claim_until <= $1))
  ORDER BY a.created_at,a.tenant_id,a.artifact_id
  LIMIT $5
  FOR UPDATE OF a SKIP LOCKED
)
UPDATE media_artifact a SET lifecycle_state='delete_claimed',claim_owner=$3,claim_until=$4,
lifecycle_version=a.lifecycle_version+1
FROM candidates c WHERE a.tenant_id=c.tenant_id AND a.artifact_id=c.artifact_id
RETURNING a.tenant_id,a.request_id,a.artifact_id,a.content_digest,a.storage_kind,COALESCE(a.object_key,''),
a.lifecycle_state,a.claim_owner,a.claim_until,a.delete_attempt,COALESCE(a.last_error_class,''),
COALESCE(a.quarantined_at,'epoch'),a.lifecycle_version`, now, orphanBefore, owner, now.Add(ttl), limit)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()
	values := make([]artifact.RetainedArtifact, 0, limit)
	for rows.Next() {
		value, err := scanRetainedArtifact(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (s *Store) FinishArtifactDeletion(ctx context.Context, value artifact.RetainedArtifact) error {
	if !validClaimedArtifact(value) {
		return runtime.ErrInvariantViolation
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM media_artifact
WHERE tenant_id=$1 AND artifact_id=$2 AND lifecycle_state='delete_claimed' AND claim_owner=$3 AND lifecycle_version=$4`,
		value.TenantID, value.ArtifactID, value.ClaimOwner, value.Version)
	if err != nil {
		return translate(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}

func (s *Store) DeferArtifactDeletion(
	ctx context.Context, value artifact.RetainedArtifact, claimUntil time.Time, errorClass string,
) error {
	if !validClaimedArtifact(value) || claimUntil.IsZero() || errorClass == "" {
		return runtime.ErrInvariantViolation
	}
	result, err := s.db.ExecContext(ctx, `UPDATE media_artifact SET claim_until=$5,delete_attempt=delete_attempt+1,
last_error_class=$6,lifecycle_version=lifecycle_version+1
WHERE tenant_id=$1 AND artifact_id=$2 AND lifecycle_state='delete_claimed' AND claim_owner=$3 AND lifecycle_version=$4`,
		value.TenantID, value.ArtifactID, value.ClaimOwner, value.Version, claimUntil, errorClass)
	if err != nil {
		return translate(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return runtime.ErrVersionConflict
	}
	return nil
}

func (s *Store) QuarantineArtifactDeletion(
	ctx context.Context, value artifact.RetainedArtifact, errorClass string, quarantinedAt time.Time,
) error {
	if !validClaimedArtifact(value) || errorClass == "" || quarantinedAt.IsZero() {
		return runtime.ErrInvariantViolation
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return translate(err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE media_artifact SET lifecycle_state='quarantined',claim_owner=NULL,claim_until=NULL,
delete_attempt=delete_attempt+1,last_error_class=$5,quarantined_at=$6,lifecycle_version=lifecycle_version+1
WHERE tenant_id=$1 AND artifact_id=$2 AND lifecycle_state='delete_claimed' AND claim_owner=$3 AND lifecycle_version=$4`,
		value.TenantID, value.ArtifactID, value.ClaimOwner, value.Version, errorClass, quarantinedAt)
	if err != nil {
		return translate(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return runtime.ErrVersionConflict
	}
	nextVersion := value.Version + 1
	outboxID := fmt.Sprintf("artifact-quarantine-retention:%s:%s:%d", value.TenantID, value.ArtifactID, nextVersion)
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox(tenant_id,outbox_id,kind,aggregate_id,event_seq,idempotency_key,payload_ref)
VALUES($1,$2,'audit',$3,$4,$2,$5)`,
		value.TenantID, outboxID, value.ArtifactID, nextVersion,
		fmt.Sprintf("artifact-quarantine://%s/retention/%s/%d", value.TenantID, value.ArtifactID, nextVersion))
	if err != nil {
		return translate(err)
	}
	return tx.Commit()
}

func scanRetainedArtifact(row rowScanner) (artifact.RetainedArtifact, error) {
	var value artifact.RetainedArtifact
	err := row.Scan(&value.TenantID, &value.RequestID, &value.ArtifactID, &value.ContentDigest, &value.Backend,
		&value.ObjectKey, &value.State, &value.ClaimOwner, &value.ClaimUntil, &value.DeleteAttempt,
		&value.LastErrorClass, &value.QuarantinedAt, &value.Version)
	return value, err
}

func validClaimedArtifact(value artifact.RetainedArtifact) bool {
	return value.TenantID != "" && value.ArtifactID != "" && value.State == artifact.RetentionDeleteClaimed &&
		value.ClaimOwner != "" && value.Version >= 1
}

var _ artifact.RetentionStore = (*Store)(nil)
