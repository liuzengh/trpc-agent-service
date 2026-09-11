package datamigration

import (
	"context"
	"errors"
	"regexp"
)

type ObjectStatus string

const (
	ObjectPending    ObjectStatus = "pending"
	ObjectCopied     ObjectStatus = "copied"
	ObjectVerified   ObjectStatus = "verified"
	ObjectMismatch   ObjectStatus = "mismatch"
	ObjectTombstoned ObjectStatus = "tombstoned"
	ObjectFailed     ObjectStatus = "failed"
)

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ObjectRecord contains hashes and watermarks only; migration content remains
// in the source/target backends and never enters the control-plane ledger.
type ObjectRecord struct {
	MigrationID           string
	ObjectKey             string
	SourceVer             string
	SourceHash            string
	TargetHash            string
	Status                ObjectStatus
	Tombstone             bool
	SourceSessionEpoch    string
	SourceThroughSequence int64
	TargetVersion         string
	RollbackState         string
}

type ReconciliationRecord struct {
	MigrationID string
	ObjectKey   string
	SourceHash  string
	TargetHash  string
	Status      string
	Detail      string
}

type ReconciliationRecorder interface {
	PutReconciliation(context.Context, ReconciliationRecord) error
}

// PutObject records one idempotent copy/reconciliation outcome. A later
// catch-up batch may replace the source version/hash for the same stable key.
func (s *PostgresStore) PutObject(ctx context.Context, record ObjectRecord) error {
	if s == nil || s.pool == nil || record.MigrationID == "" || record.ObjectKey == "" ||
		!sha256Pattern.MatchString(record.SourceHash) ||
		(record.TargetHash != "" && !sha256Pattern.MatchString(record.TargetHash)) ||
		!validObjectStatus(record.Status) || !validRollbackState(normalizedRollbackState(record.RollbackState)) {
		return errors.New("data migration object record is invalid")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO data_migration_objects
			(migration_id, object_key, source_version, content_sha256, target_sha256,
			 status, tombstone, copied_at, verified_at, source_session_epoch,
			 source_through_sequence, target_version, rollback_state)
		VALUES ($1, $2, $3, $4, $5, $6, $7,
			CASE WHEN $6 IN ('copied', 'verified', 'mismatch', 'tombstoned') THEN clock_timestamp() END,
			CASE WHEN $6 IN ('verified', 'mismatch') THEN clock_timestamp() END,
			$8, $9, $10, $11)
		ON CONFLICT (migration_id, object_key) DO UPDATE
		SET source_version = EXCLUDED.source_version,
		    content_sha256 = EXCLUDED.content_sha256,
		    target_sha256 = EXCLUDED.target_sha256,
		    status = EXCLUDED.status,
		    tombstone = EXCLUDED.tombstone,
		    copied_at = CASE WHEN EXCLUDED.status IN ('copied', 'verified', 'mismatch', 'tombstoned')
		                     THEN clock_timestamp() ELSE data_migration_objects.copied_at END,
			verified_at = CASE WHEN EXCLUDED.status IN ('verified', 'mismatch')
		                       THEN clock_timestamp() ELSE data_migration_objects.verified_at END,
		    source_session_epoch = EXCLUDED.source_session_epoch,
		    source_through_sequence = EXCLUDED.source_through_sequence,
		    target_version = EXCLUDED.target_version,
		    rollback_state = EXCLUDED.rollback_state,
		    updated_at = clock_timestamp()`,
		record.MigrationID, record.ObjectKey, record.SourceVer, record.SourceHash,
		record.TargetHash, record.Status, record.Tombstone, record.SourceSessionEpoch,
		record.SourceThroughSequence, record.TargetVersion, normalizedRollbackState(record.RollbackState))
	if err != nil {
		return errors.New("record data migration object")
	}
	return nil
}

func (s *PostgresStore) PutReconciliation(ctx context.Context, record ReconciliationRecord) error {
	if s == nil || s.pool == nil || record.MigrationID == "" || record.ObjectKey == "" ||
		(record.SourceHash != "" && !sha256Pattern.MatchString(record.SourceHash)) ||
		(record.TargetHash != "" && !sha256Pattern.MatchString(record.TargetHash)) {
		return errors.New("data migration reconciliation record is invalid")
	}
	switch record.Status {
	case "matched", "mismatch", "stale_source", "rollback_deleted", "rollback_conflict":
	default:
		return errors.New("data migration reconciliation status is invalid")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO data_migration_reconciliations
			(migration_id, object_key, source_hash, target_hash, status, detail)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (migration_id, object_key) DO UPDATE
		SET source_hash = EXCLUDED.source_hash, target_hash = EXCLUDED.target_hash,
		    status = EXCLUDED.status, detail = EXCLUDED.detail, checked_at = clock_timestamp()`,
		record.MigrationID, record.ObjectKey, record.SourceHash, record.TargetHash, record.Status, record.Detail)
	if err != nil {
		return errors.New("record data migration reconciliation")
	}
	return nil
}

func normalizedRollbackState(value string) string {
	if value == "" {
		return "eligible"
	}
	return value
}

func validRollbackState(value string) bool {
	switch value {
	case "eligible", "deleted", "conflict", "retained":
		return true
	default:
		return false
	}
}

func validObjectStatus(status ObjectStatus) bool {
	switch status {
	case ObjectPending, ObjectCopied, ObjectVerified, ObjectMismatch, ObjectTombstoned, ObjectFailed:
		return true
	default:
		return false
	}
}
