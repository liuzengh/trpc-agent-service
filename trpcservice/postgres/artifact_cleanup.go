package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

const maxArtifactCleanupBatchSize = 1000

// ClaimArtifactCleanup leases a bounded set of SQL-authorized object keys.
// Cleanup locks the artifact row before changing its status. Admission locks
// referenced artifact rows before inserting an execution, so the two paths
// serialize instead of relying on an inconsistent active-execution snapshot.
func (s *Store) ClaimArtifactCleanup(
	ctx context.Context,
	owner string,
	pendingBefore time.Time,
	retentionBefore *time.Time,
	leaseDuration time.Duration,
	limit int,
) ([]platformartifact.CleanupCandidate, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("artifact cleanup owner is required")
	}
	if pendingBefore.IsZero() {
		return nil, errors.New("artifact cleanup pending cutoff is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("artifact cleanup lease duration must be positive")
	}
	if limit <= 0 || limit > maxArtifactCleanupBatchSize {
		return nil, errors.New("artifact cleanup batch size is invalid")
	}
	if retentionBefore != nil && retentionBefore.IsZero() {
		return nil, errors.New("artifact cleanup retention cutoff is invalid")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var retention any
	if retentionBefore != nil {
		retention = retentionBefore.UTC()
	}
	if _, err := s.pool.Exec(ctx, `
UPDATE platform.artifact
SET cleanup_last_error = 'artifact cleanup quarantined: config version is missing',
    cleanup_next_attempt_at = 'infinity'::timestamptz,
    updated_at = clock_timestamp()
WHERE object_key <> '' AND config_version = ''
  AND cleanup_completed_at IS NULL AND cleanup_last_error = ''`); err != nil {
		return nil, fmt.Errorf("quarantine artifacts without config version: %w", err)
	}
	rows, err := s.pool.Query(ctx, `
WITH candidates AS (
    SELECT a.artifact_id
    FROM platform.artifact AS a
    WHERE a.object_key <> ''
      AND a.config_version <> ''
      AND a.cleanup_completed_at IS NULL
      AND a.cleanup_next_attempt_at <= clock_timestamp()
      AND (a.cleanup_owner IS NULL OR a.cleanup_lease_until <= clock_timestamp())
      AND (
          a.status = 'DELETED'
          OR (a.status = 'PENDING' AND a.created_at <= $3)
          OR (
              $4::timestamptz IS NOT NULL
              AND a.status = 'AVAILABLE'
              AND a.created_at <= $4
          )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM platform.execution AS active_execution
          WHERE active_execution.tenant_id = a.tenant_id
            AND active_execution.app_id = a.app_id
            AND active_execution.session_principal_id = a.session_principal_id
            AND active_execution.session_id = a.session_id
            AND active_execution.status IN ('PENDING', 'RUNNING', 'WAITING_APPROVAL', 'UNCERTAIN')
      )
      AND NOT EXISTS (
          SELECT 1
          FROM platform.execution AS event_execution
          JOIN platform.execution_event AS execution_event
            ON execution_event.tenant_id = event_execution.tenant_id
           AND execution_event.app_id = event_execution.app_id
           AND execution_event.request_id = event_execution.request_id
          WHERE event_execution.tenant_id = a.tenant_id
            AND event_execution.app_id = a.app_id
            AND event_execution.session_principal_id = a.session_principal_id
            AND event_execution.session_id = a.session_id
            AND event_execution.status IN ('PENDING', 'RUNNING', 'WAITING_APPROVAL', 'UNCERTAIN')
            AND strpos(
                    execution_event.payload::text,
                    to_jsonb(('artifact://' || a.filename || '@' || a.version::text)::text)::text
                ) > 0
      )
      AND NOT EXISTS (
          SELECT 1
          FROM platform.reply_outbox AS reply
          JOIN platform.execution AS reply_execution
            ON reply_execution.tenant_id = reply.tenant_id
           AND reply_execution.app_id = reply.app_id
           AND reply_execution.request_id = reply.request_id
          WHERE reply.tenant_id = a.tenant_id
            AND reply.app_id = a.app_id
            AND reply_execution.session_principal_id = a.session_principal_id
            AND reply_execution.session_id = a.session_id
            AND reply.status IN ('PENDING', 'SENDING', 'UNCERTAIN')
            AND (
                strpos(
                    reply.payload::text,
                    to_jsonb(('artifact://' || a.filename || '@' || a.version::text)::text)::text
                ) > 0
                OR strpos(
                    reply.target_ref::text,
                    to_jsonb(('artifact://' || a.filename || '@' || a.version::text)::text)::text
                ) > 0
            )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM platform.inbound_artifact AS inbound
          WHERE inbound.tenant_id = a.tenant_id
            AND inbound.app_id = a.app_id
            AND inbound.object_key = a.object_key
            AND inbound.status = 'PENDING'
      )
    ORDER BY a.cleanup_next_attempt_at, a.updated_at, a.artifact_id
    FOR UPDATE OF a SKIP LOCKED
    LIMIT $5
)
UPDATE platform.artifact AS a
SET status = CASE WHEN a.status = 'AVAILABLE' THEN 'DELETED' ELSE a.status END,
    cleanup_owner = $1,
    cleanup_lease_until = clock_timestamp() + $2::interval,
    cleanup_attempts = a.cleanup_attempts + 1,
    cleanup_last_error = '',
    updated_at = clock_timestamp()
FROM candidates AS c
WHERE a.artifact_id = c.artifact_id
RETURNING a.artifact_id, a.tenant_id, a.app_id, a.session_principal_id,
          a.session_id, a.filename, a.version, a.object_key, a.mime_type,
          a.size_bytes, a.status, a.config_version,
          a.created_at, a.updated_at, a.cleanup_attempts`,
		owner,
		intervalLiteral(leaseDuration),
		pendingBefore.UTC(),
		retention,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("claim artifact cleanup: %w", err)
	}
	defer rows.Close()
	candidates := make([]platformartifact.CleanupCandidate, 0)
	for rows.Next() {
		candidate, err := scanArtifactCleanupCandidate(rows)
		if err != nil {
			return nil, fmt.Errorf("scan artifact cleanup candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate artifact cleanup candidates: %w", err)
	}
	return candidates, nil
}

// ClaimInboundArtifactCleanup leases pre-admission objects that were deleted,
// left pending, or left uploading after the uploader lease expired. ATTACHED
// rows are excluded by SQL and cannot be reclaimed by this worker.
func (s *Store) ClaimInboundArtifactCleanup(
	ctx context.Context,
	owner string,
	pendingBefore time.Time,
	leaseDuration time.Duration,
	limit int,
) ([]platformartifact.InboundCleanupCandidate, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("inbound artifact cleanup owner is required")
	}
	if pendingBefore.IsZero() {
		return nil, errors.New("inbound artifact cleanup pending cutoff is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("inbound artifact cleanup lease duration must be positive")
	}
	if limit <= 0 || limit > maxArtifactCleanupBatchSize {
		return nil, errors.New("inbound artifact cleanup batch size is invalid")
	}
	rows, err := s.pool.Query(ctx, `
WITH candidates AS (
    SELECT tenant_id, app_id, binding_id, external_message_id, item_no
    FROM platform.inbound_artifact
    WHERE cleanup_completed_at IS NULL
      AND cleanup_next_attempt_at <= clock_timestamp()
      AND (cleanup_owner IS NULL OR cleanup_lease_until <= clock_timestamp())
      AND (
          status = 'DELETED'
          OR (status = 'PENDING' AND updated_at <= $3)
          OR (
              status = 'UPLOADING'
              AND upload_lease_until <= clock_timestamp()
              AND updated_at <= $3
          )
      )
    ORDER BY cleanup_next_attempt_at, updated_at, external_message_id, item_no
    FOR UPDATE SKIP LOCKED
    LIMIT $4
)
UPDATE platform.inbound_artifact AS a
SET status = 'DELETED',
    upload_token = NULL,
    upload_lease_until = NULL,
    cleanup_owner = $1,
    cleanup_lease_until = clock_timestamp() + $2::interval,
    cleanup_attempts = a.cleanup_attempts + 1,
    cleanup_last_error = '',
    updated_at = clock_timestamp()
FROM candidates AS c
WHERE a.tenant_id = c.tenant_id
  AND a.app_id = c.app_id
  AND a.binding_id = c.binding_id
  AND a.external_message_id = c.external_message_id
  AND a.item_no = c.item_no
RETURNING a.tenant_id, a.app_id, a.binding_id, a.external_message_id,
          a.item_no, a.artifact_ref, a.config_version, a.object_key,
          a.cleanup_attempts`,
		owner,
		intervalLiteral(leaseDuration),
		pendingBefore.UTC(),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("claim inbound artifact cleanup: %w", err)
	}
	defer rows.Close()
	candidates := make([]platformartifact.InboundCleanupCandidate, 0)
	for rows.Next() {
		var candidate platformartifact.InboundCleanupCandidate
		if err := rows.Scan(
			&candidate.TenantID,
			&candidate.AppID,
			&candidate.BindingID,
			&candidate.ExternalMessageID,
			&candidate.ItemNo,
			&candidate.ArtifactRef,
			&candidate.ConfigVersion,
			&candidate.ObjectKey,
			&candidate.Attempts,
		); err != nil {
			return nil, fmt.Errorf("scan inbound artifact cleanup candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inbound artifact cleanup candidates: %w", err)
	}
	return candidates, nil
}

// CompleteInboundArtifactCleanup acknowledges an exact object only while the
// cleanup worker still owns its live lease.
func (s *Store) CompleteInboundArtifactCleanup(
	ctx context.Context,
	candidate platformartifact.InboundCleanupCandidate,
	owner string,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if owner == "" {
		return errors.New("inbound artifact cleanup owner is required")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.inbound_artifact
SET cleanup_completed_at = clock_timestamp(),
    cleanup_owner = NULL,
    cleanup_lease_until = NULL,
    cleanup_last_error = '',
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5
  AND artifact_ref = $6 AND cleanup_owner = $7
  AND cleanup_completed_at IS NULL
  AND cleanup_lease_until > clock_timestamp()`,
		candidate.TenantID, candidate.AppID, candidate.BindingID,
		candidate.ExternalMessageID, candidate.ItemNo, candidate.ArtifactRef, owner,
	)
	if err != nil {
		return fmt.Errorf("complete inbound artifact cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("inbound artifact cleanup lease is lost")
	}
	return nil
}

// RetryInboundArtifactCleanup releases a failed cleanup claim into the
// durable retry queue while retaining only a redacted error summary.
func (s *Store) RetryInboundArtifactCleanup(
	ctx context.Context,
	candidate platformartifact.InboundCleanupCandidate,
	owner string,
	nextAttempt time.Time,
	cause error,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if owner == "" {
		return errors.New("inbound artifact cleanup owner is required")
	}
	if nextAttempt.IsZero() {
		return errors.New("inbound artifact cleanup next attempt is required")
	}
	if cause == nil {
		return errors.New("inbound artifact cleanup failure is required")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.inbound_artifact
SET cleanup_owner = NULL,
    cleanup_lease_until = NULL,
    cleanup_next_attempt_at = $7,
    cleanup_last_error = $8,
    updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5
  AND artifact_ref = $6 AND cleanup_owner = $9
  AND cleanup_completed_at IS NULL
  AND cleanup_lease_until > clock_timestamp()`,
		candidate.TenantID, candidate.AppID, candidate.BindingID,
		candidate.ExternalMessageID, candidate.ItemNo, candidate.ArtifactRef,
		nextAttempt.UTC(), platformlog.SafeError(cause), owner,
	)
	if err != nil {
		return fmt.Errorf("retry inbound artifact cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("inbound artifact cleanup lease is lost")
	}
	return nil
}

// CompleteArtifactCleanup marks the exact object as removed only while the
// worker still owns its live lease. A stale worker cannot acknowledge a new
// owner's claim after lease expiry.
func (s *Store) CompleteArtifactCleanup(
	ctx context.Context,
	candidate platformartifact.CleanupCandidate,
	owner string,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if owner == "" {
		return errors.New("artifact cleanup owner is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.artifact
SET cleanup_completed_at = clock_timestamp(),
    cleanup_owner = NULL,
    cleanup_lease_until = NULL,
    cleanup_last_error = '',
    updated_at = clock_timestamp()
WHERE artifact_id = $1
  AND cleanup_owner = $2
  AND cleanup_completed_at IS NULL
  AND cleanup_lease_until > clock_timestamp()`, candidate.Record.ID, owner)
	if err != nil {
		return fmt.Errorf("complete artifact cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("artifact cleanup lease is lost")
	}
	return nil
}

// RetryArtifactCleanup returns a failed object to the durable retry queue and
// stores only a redacted error summary.
func (s *Store) RetryArtifactCleanup(
	ctx context.Context,
	candidate platformartifact.CleanupCandidate,
	owner string,
	nextAttempt time.Time,
	cause error,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if owner == "" {
		return errors.New("artifact cleanup owner is required")
	}
	if nextAttempt.IsZero() {
		return errors.New("artifact cleanup next attempt is required")
	}
	if cause == nil {
		return errors.New("artifact cleanup failure is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lastError := platformlog.SafeError(cause)
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.artifact
SET cleanup_owner = NULL,
    cleanup_lease_until = NULL,
    cleanup_next_attempt_at = $3,
    cleanup_last_error = $4,
    updated_at = clock_timestamp()
WHERE artifact_id = $1
  AND cleanup_owner = $2
  AND cleanup_completed_at IS NULL
  AND cleanup_lease_until > clock_timestamp()`, candidate.Record.ID, owner, nextAttempt.UTC(), lastError)
	if err != nil {
		return fmt.Errorf("retry artifact cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("artifact cleanup lease is lost")
	}
	return nil
}

func scanArtifactCleanupCandidate(scanner interface{ Scan(...any) error }) (platformartifact.CleanupCandidate, error) {
	var candidate platformartifact.CleanupCandidate
	if err := scanner.Scan(
		&candidate.Record.ID,
		&candidate.Record.TenantID,
		&candidate.Record.AppID,
		&candidate.Record.SessionPrincipalID,
		&candidate.Record.SessionID,
		&candidate.Record.Filename,
		&candidate.Record.Version,
		&candidate.Record.ObjectKey,
		&candidate.Record.MIMEType,
		&candidate.Record.Size,
		&candidate.Record.Status,
		&candidate.Record.ConfigVersion,
		&candidate.Record.CreatedAt,
		&candidate.Record.UpdatedAt,
		&candidate.Attempts,
	); err != nil {
		return platformartifact.CleanupCandidate{}, err
	}
	return candidate, nil
}

var _ platformartifact.CleanupStore = (*Store)(nil)
var _ platformartifact.InboundCleanupStore = (*Store)(nil)
