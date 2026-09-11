package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

const (
	SessionRepairScopeSession     = "session"
	SessionRepairScopeUser        = "user"
	SessionRepairScopeApplication = "application"
)

type SessionMigrationRepair struct {
	MigrationID      string
	TenantID         string
	AppCode          string
	Scope            string
	ScopeKey         string
	SubjectID        string
	RouteGeneration  uint64
	PrimaryProfileID string
	ReplicaProfileID string
	Revision         uint64
	Attempts         int
	LeaseOwner       string
	LeaseUntil       time.Time
	NextAttempt      time.Time
	LastError        string
	UpdatedAt        time.Time
}

type SessionMigrationRepairStore interface {
	UpsertSessionMigrationRepair(context.Context, SessionMigrationRepair) (SessionMigrationRepair, error)
	CountPendingSessionMigrationRepairs(context.Context, string, string) (int, error)
	ClaimSessionMigrationRepairs(context.Context, string, string, string, int, time.Duration) ([]SessionMigrationRepair, error)
	CompleteSessionMigrationRepair(context.Context, SessionMigrationRepair, string) (bool, error)
	FailSessionMigrationRepair(context.Context, SessionMigrationRepair, string, time.Time, string) error
}

func (s *PostgresSessionMigrationStore) UpsertSessionMigrationRepair(ctx context.Context, repair SessionMigrationRepair) (SessionMigrationRepair, error) {
	repair, err := normalizeSessionMigrationRepair(repair)
	if err != nil {
		return SessionMigrationRepair{}, err
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, repair.TenantID)
	if err != nil {
		return SessionMigrationRepair{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var (
		currentGeneration                uint64
		currentPhase                     SessionMigrationPhase
		sourceProfileID, targetProfileID string
	)
	if err := tx.QueryRowContext(ctx, `
SELECT generation, phase, source_profile_id, target_profile_id FROM session_backend_migrations
WHERE migration_id=$1 AND tenant_id=$2 AND app_code=$3
FOR UPDATE`, repair.MigrationID, repair.TenantID, repair.AppCode).Scan(
		&currentGeneration, &currentPhase, &sourceProfileID, &targetProfileID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SessionMigrationRepair{}, ErrSessionMigrationNotFound
		}
		return SessionMigrationRepair{}, fmt.Errorf("lock Session migration for repair intent: %w", err)
	}
	if currentGeneration != repair.RouteGeneration {
		return SessionMigrationRepair{}, ErrSessionMigrationConflict
	}
	expectedPrimary, expectedReplica, ok := sessionMigrationRepairDirection(currentPhase, sourceProfileID, targetProfileID)
	if !ok || repair.PrimaryProfileID != expectedPrimary || repair.ReplicaProfileID != expectedReplica {
		return SessionMigrationRepair{}, ErrSessionMigrationConflict
	}
	err = tx.QueryRowContext(ctx, `
INSERT INTO session_migration_repairs (
    migration_id, tenant_id, app_code, scope, scope_key, subject_id,
    route_generation, primary_profile_id, replica_profile_id,
    revision, attempts, lease_owner, lease_until, next_attempt_at, last_error, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1,0,'',NULL,NOW(),'',NOW())
ON CONFLICT (migration_id, scope, scope_key, subject_id) DO UPDATE
SET revision=session_migration_repairs.revision+1,
    tenant_id=EXCLUDED.tenant_id,
    app_code=EXCLUDED.app_code,
    route_generation=EXCLUDED.route_generation,
    attempts=0,
    lease_owner='',
    lease_until=NULL,
    next_attempt_at=NOW(),
    last_error='',
    updated_at=NOW()
WHERE session_migration_repairs.primary_profile_id=EXCLUDED.primary_profile_id
  AND session_migration_repairs.replica_profile_id=EXCLUDED.replica_profile_id
RETURNING revision, attempts, lease_owner, COALESCE(lease_until, 'epoch'::timestamptz), next_attempt_at, last_error, updated_at`,
		repair.MigrationID, repair.TenantID, repair.AppCode, repair.Scope, repair.ScopeKey, repair.SubjectID,
		repair.RouteGeneration, repair.PrimaryProfileID, repair.ReplicaProfileID,
	).Scan(&repair.Revision, &repair.Attempts, &repair.LeaseOwner, &repair.LeaseUntil, &repair.NextAttempt, &repair.LastError, &repair.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionMigrationRepair{}, ErrSessionMigrationConflict
	}
	if err != nil {
		return SessionMigrationRepair{}, fmt.Errorf("upsert Session migration repair: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return SessionMigrationRepair{}, fmt.Errorf("commit Session migration repair: %w", err)
	}
	return repair, nil
}

func sessionMigrationRepairDirection(phase SessionMigrationPhase, sourceProfileID, targetProfileID string) (string, string, bool) {
	switch phase {
	case SessionMigrationDualWrite, SessionMigrationBackfill, SessionMigrationVerify:
		return sourceProfileID, targetProfileID, true
	case SessionMigrationCutRead:
		return targetProfileID, sourceProfileID, true
	default:
		return "", "", false
	}
}

func (s *PostgresSessionMigrationStore) CountPendingSessionMigrationRepairs(ctx context.Context, tenantID, migrationID string) (int, error) {
	tenantID = strings.TrimSpace(tenantID)
	migrationID = strings.TrimSpace(migrationID)
	if tenantID == "" || migrationID == "" {
		return 0, errors.New("Session migration repair count requires tenant and migration")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM session_migration_repairs WHERE tenant_id=$1 AND migration_id=$2`, tenantID, migrationID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count Session migration repairs: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PostgresSessionMigrationStore) ClaimSessionMigrationRepairs(ctx context.Context, tenantID, migrationID, owner string, limit int, leaseTTL time.Duration) ([]SessionMigrationRepair, error) {
	tenantID = strings.TrimSpace(tenantID)
	migrationID = strings.TrimSpace(migrationID)
	owner = strings.TrimSpace(owner)
	if tenantID == "" || migrationID == "" || owner == "" || leaseTTL <= 0 {
		return nil, errors.New("Session migration repair claim requires tenant, migration, owner, and lease TTL")
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
WITH claimable AS (
    SELECT migration_id, scope, scope_key, subject_id
    FROM session_migration_repairs
    WHERE tenant_id=$1 AND migration_id=$2
      AND next_attempt_at <= NOW()
      AND (lease_until IS NULL OR lease_until < NOW())
    ORDER BY next_attempt_at, updated_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE session_migration_repairs AS repair
SET lease_owner=$4,
    lease_until=NOW()+$5::interval,
    attempts=repair.attempts+1,
    updated_at=NOW()
FROM claimable
WHERE repair.migration_id=claimable.migration_id
  AND repair.scope=claimable.scope
  AND repair.scope_key=claimable.scope_key
  AND repair.subject_id=claimable.subject_id
RETURNING repair.migration_id, repair.tenant_id, repair.app_code, repair.scope, repair.scope_key, repair.subject_id,
          repair.route_generation, repair.primary_profile_id, repair.replica_profile_id,
          repair.revision, repair.attempts, repair.lease_owner, repair.lease_until,
          repair.next_attempt_at, repair.last_error, repair.updated_at`, tenantID, migrationID, limit, owner, postgresInterval(leaseTTL))
	if err != nil {
		return nil, fmt.Errorf("claim Session migration repairs: %w", err)
	}
	defer rows.Close()
	repairs := make([]SessionMigrationRepair, 0, limit)
	for rows.Next() {
		repair, scanErr := scanSessionMigrationRepair(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		repairs = append(repairs, repair)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return repairs, nil
}

func (s *PostgresSessionMigrationStore) CompleteSessionMigrationRepair(ctx context.Context, repair SessionMigrationRepair, owner string) (bool, error) {
	repair, err := normalizeSessionMigrationRepair(repair)
	if err != nil {
		return false, err
	}
	owner = strings.TrimSpace(owner)
	if repair.Revision == 0 {
		return false, errors.New("Session migration repair completion requires revision")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, repair.TenantID)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
DELETE FROM session_migration_repairs
WHERE migration_id=$1 AND tenant_id=$2 AND scope=$3 AND scope_key=$4 AND subject_id=$5
  AND revision=$6 AND (($7='' AND lease_owner='') OR lease_owner=$7)`,
		repair.MigrationID, repair.TenantID, repair.Scope, repair.ScopeKey, repair.SubjectID, repair.Revision, owner)
	if err != nil {
		return false, fmt.Errorf("complete Session migration repair: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return rows == 1, nil
}

func (s *PostgresSessionMigrationStore) FailSessionMigrationRepair(ctx context.Context, repair SessionMigrationRepair, owner string, retryAt time.Time, failure string) error {
	repair, err := normalizeSessionMigrationRepair(repair)
	if err != nil {
		return err
	}
	owner = strings.TrimSpace(owner)
	if repair.Revision == 0 || owner == "" || retryAt.IsZero() {
		return errors.New("Session migration repair failure requires revision, owner, and retry time")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, repair.TenantID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE session_migration_repairs
SET lease_owner='', lease_until=NULL, next_attempt_at=$1, last_error=$2, updated_at=NOW()
WHERE migration_id=$3 AND tenant_id=$4 AND scope=$5 AND scope_key=$6 AND subject_id=$7
  AND revision=$8 AND lease_owner=$9`,
		retryAt.UTC(), truncateSessionMigrationRepairError(failure), repair.MigrationID, repair.TenantID,
		repair.Scope, repair.ScopeKey, repair.SubjectID, repair.Revision, owner)
	if err != nil {
		return fmt.Errorf("fail Session migration repair: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrSessionMigrationConflict
	}
	return tx.Commit()
}

func normalizeSessionMigrationRepair(repair SessionMigrationRepair) (SessionMigrationRepair, error) {
	repair.MigrationID = strings.TrimSpace(repair.MigrationID)
	repair.TenantID = strings.TrimSpace(repair.TenantID)
	repair.AppCode = strings.TrimSpace(repair.AppCode)
	repair.Scope = strings.ToLower(strings.TrimSpace(repair.Scope))
	repair.ScopeKey = strings.TrimSpace(repair.ScopeKey)
	repair.SubjectID = strings.TrimSpace(repair.SubjectID)
	repair.PrimaryProfileID = strings.TrimSpace(repair.PrimaryProfileID)
	repair.ReplicaProfileID = strings.TrimSpace(repair.ReplicaProfileID)
	if repair.MigrationID == "" || repair.TenantID == "" || repair.AppCode == "" || repair.ScopeKey == "" {
		return SessionMigrationRepair{}, errors.New("Session migration repair identity is required")
	}
	if repair.RouteGeneration == 0 || repair.PrimaryProfileID == "" || repair.ReplicaProfileID == "" || repair.PrimaryProfileID == repair.ReplicaProfileID {
		return SessionMigrationRepair{}, errors.New("Session migration repair requires route generation and distinct primary/replica profiles")
	}
	switch repair.Scope {
	case SessionRepairScopeSession, SessionRepairScopeUser:
		if repair.SubjectID == "" {
			return SessionMigrationRepair{}, errors.New("Session and user repair require subject identity")
		}
	case SessionRepairScopeApplication:
		repair.SubjectID = ""
	default:
		return SessionMigrationRepair{}, fmt.Errorf("unsupported Session migration repair scope %q", repair.Scope)
	}
	return repair, nil
}

type sessionMigrationRepairScanner interface{ Scan(...any) error }

func scanSessionMigrationRepair(scanner sessionMigrationRepairScanner) (SessionMigrationRepair, error) {
	var repair SessionMigrationRepair
	if err := scanner.Scan(
		&repair.MigrationID, &repair.TenantID, &repair.AppCode, &repair.Scope, &repair.ScopeKey, &repair.SubjectID,
		&repair.RouteGeneration, &repair.PrimaryProfileID, &repair.ReplicaProfileID,
		&repair.Revision, &repair.Attempts, &repair.LeaseOwner, &repair.LeaseUntil,
		&repair.NextAttempt, &repair.LastError, &repair.UpdatedAt,
	); err != nil {
		return SessionMigrationRepair{}, fmt.Errorf("scan Session migration repair: %w", err)
	}
	return repair, nil
}

func postgresInterval(duration time.Duration) string {
	return fmt.Sprintf("%f seconds", duration.Seconds())
}

func truncateSessionMigrationRepairError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 512 {
		return value[:512]
	}
	return value
}

var _ SessionMigrationRepairStore = (*PostgresSessionMigrationStore)(nil)
