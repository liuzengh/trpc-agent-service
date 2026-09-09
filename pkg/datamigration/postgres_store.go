package datamigration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// PostgresStore is the durable Store used by production migration workers.
// Every claim/advance/release checks owner, monotonically increasing lease
// version, and lease expiry in the same SQL statement or transaction.
type PostgresStore struct{ db *sql.DB }

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }

func (s *PostgresStore) Create(ctx context.Context, job Job) error {
	ctx = nonNilMigrationContext(ctx)
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: postgres store is unavailable", ErrMigrationCapability)
	}
	if err := job.Validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO data_migrations (
			id, tenant_id, domain, source_profile, target_profile, phase,
			paused, cursor, snapshot_watermark, applied_watermark,
			lease_version, last_error, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,'PREPARE',FALSE,'',0,0,0,'',COALESCE($6,now()),now())`,
		job.ID, job.TenantID, job.Domain, job.SourceProfile, job.TargetProfile,
		nullTime(job.CreatedAt))
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return ErrMigrationConflict
		}
		return fmt.Errorf("create data migration: %w", err)
	}
	return nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (Job, error) {
	ctx = nonNilMigrationContext(ctx)
	if s == nil || s.db == nil {
		return Job{}, fmt.Errorf("%w: postgres store is unavailable", ErrMigrationCapability)
	}
	row := s.db.QueryRowContext(ctx, migrationSelect+` WHERE id=$1`, id)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrMigrationNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get data migration: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) Claim(ctx context.Context, id, owner string, ttl time.Duration) (Job, error) {
	ctx = nonNilMigrationContext(ctx)
	if s == nil || s.db == nil {
		return Job{}, fmt.Errorf("%w: postgres store is unavailable", ErrMigrationCapability)
	}
	if err := validateMigrationLease(owner, ttl); err != nil {
		return Job{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, fmt.Errorf("claim data migration begin: %w", err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, migrationSelect+` WHERE id=$1 FOR UPDATE`, id)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrMigrationNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("claim data migration load: %w", err)
	}
	now, err := databaseNow(ctx, tx)
	if err != nil {
		return Job{}, fmt.Errorf("claim data migration clock: %w", err)
	}
	if terminalPhase(job.Phase) {
		return Job{}, ErrMigrationTerminal
	}
	if job.LeaseOwner != "" && job.LeaseUntil.After(now) {
		return Job{}, ErrMigrationLeaseHeld
	}
	leaseUntil := now.Add(ttl)
	result, err := tx.ExecContext(ctx, `
		UPDATE data_migrations
		SET lease_owner=$2, lease_version=lease_version+1,
		    lease_until=$3, updated_at=clock_timestamp()
		WHERE id=$1 AND (lease_until IS NULL OR lease_until <= clock_timestamp())`, id, owner, leaseUntil)
	if err != nil {
		return Job{}, fmt.Errorf("claim data migration update: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Job{}, fmt.Errorf("claim data migration rows: %w", err)
	}
	if rows != 1 {
		return Job{}, ErrMigrationLeaseHeld
	}
	job.LeaseOwner = owner
	job.LeaseVersion++
	job.LeaseUntil = leaseUntil
	job.UpdatedAt = now
	if err := tx.Commit(); err != nil {
		return Job{}, fmt.Errorf("claim data migration commit: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) Renew(ctx context.Context, id, owner string, version int64, ttl time.Duration) (Job, error) {
	ctx = nonNilMigrationContext(ctx)
	if s == nil || s.db == nil {
		return Job{}, fmt.Errorf("%w: postgres store is unavailable", ErrMigrationCapability)
	}
	if version <= 0 {
		return Job{}, fmt.Errorf("%w: invalid lease version", ErrInvalidMigration)
	}
	if err := validateMigrationLease(owner, ttl); err != nil {
		return Job{}, err
	}
	// Lock before checking the lease. A direct UPDATE can evaluate its
	// wall-clock predicate before waiting for a concurrent claimant to release
	// the row, allowing an expired owner to renew or overwrite a newer lease.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, fmt.Errorf("renew data migration begin: %w", err)
	}
	defer tx.Rollback()
	job, err := scanJob(tx.QueryRowContext(ctx, migrationSelect+` WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrMigrationNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("renew data migration load: %w", err)
	}
	now, err := databaseNow(ctx, tx)
	if err != nil {
		return Job{}, fmt.Errorf("renew data migration clock: %w", err)
	}
	if err := checkLease(job, owner, version, now); err != nil {
		return Job{}, err
	}
	leaseUntil := now.Add(ttl)
	result, err := tx.ExecContext(ctx, `
		UPDATE data_migrations
		SET lease_until=$2, updated_at=clock_timestamp()
		WHERE id=$1 AND lease_owner=$3 AND lease_version=$4
		  AND lease_until > clock_timestamp()`, id, leaseUntil, owner, version)
	if err != nil {
		return Job{}, fmt.Errorf("renew data migration: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Job{}, fmt.Errorf("renew data migration rows: %w", err)
	}
	if rows != 1 {
		return Job{}, ErrMigrationFence
	}
	job.LeaseUntil = leaseUntil
	job.UpdatedAt = now
	if err := tx.Commit(); err != nil {
		return Job{}, fmt.Errorf("renew data migration commit: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) Advance(ctx context.Context, id, owner string, version int64, patch JobPatch) (Job, error) {
	ctx = nonNilMigrationContext(ctx)
	if s == nil || s.db == nil {
		return Job{}, fmt.Errorf("%w: postgres store is unavailable", ErrMigrationCapability)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, fmt.Errorf("advance data migration begin: %w", err)
	}
	defer tx.Rollback()
	job, err := scanJob(tx.QueryRowContext(ctx, migrationSelect+` WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrMigrationNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("advance data migration load: %w", err)
	}
	now, err := databaseNow(ctx, tx)
	if err != nil {
		return Job{}, fmt.Errorf("advance data migration clock: %w", err)
	}
	if err := checkLease(job, owner, version, now); err != nil {
		return Job{}, err
	}
	previousPhase := job.Phase
	if patch.Phase != nil {
		if !canTransition(job.Phase, *patch.Phase) {
			return Job{}, fmt.Errorf("%w: %s -> %s", ErrInvalidMigration, job.Phase, *patch.Phase)
		}
		job.Phase = *patch.Phase
	}
	if patch.Paused != nil {
		job.Paused = *patch.Paused
	}
	if patch.Cursor != nil {
		// Phase has already been applied above. These are the only legal phase
		// transitions whose cursor is intentionally reset; canTransition keeps
		// the reset from being smuggled into another state.
		boundaryReset := patch.Phase != nil &&
			((previousPhase == PhaseSnapshotCopy && *patch.Phase == PhaseDualWrite) ||
				(previousPhase == PhaseDualWrite && *patch.Phase == PhaseCatchUp))
		if *patch.Cursor == "" && job.Cursor != "" && !boundaryReset {
			return Job{}, fmt.Errorf("%w: migration cursor cannot be cleared outside the snapshot-to-dual-write boundary", ErrInvalidMigration)
		}
		job.Cursor = *patch.Cursor
	}
	if patch.SnapshotWatermark != nil {
		if *patch.SnapshotWatermark < job.SnapshotWatermark {
			return Job{}, fmt.Errorf("%w: snapshot watermark cannot move backwards", ErrInvalidMigration)
		}
		job.SnapshotWatermark = *patch.SnapshotWatermark
	}
	if patch.AppliedWatermark != nil {
		if *patch.AppliedWatermark < job.AppliedWatermark {
			return Job{}, fmt.Errorf("%w: applied watermark cannot move backwards", ErrInvalidMigration)
		}
		job.AppliedWatermark = *patch.AppliedWatermark
	}
	if patch.LastError != nil {
		job.LastError = sanitizeMigrationErrorText(*patch.LastError)
	}
	if err := job.Validate(); err != nil {
		return Job{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE data_migrations
		SET phase=$2, paused=$3, cursor=$4, snapshot_watermark=$5,
		    applied_watermark=$6, last_error=$7, updated_at=now()
		WHERE id=$1 AND lease_owner=$8 AND lease_version=$9
		  AND lease_until > clock_timestamp()`, id, job.Phase, job.Paused, job.Cursor,
		job.SnapshotWatermark, job.AppliedWatermark, job.LastError, owner, version)
	if err != nil {
		return Job{}, fmt.Errorf("advance data migration update: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Job{}, fmt.Errorf("advance data migration rows: %w", err)
	}
	if rows != 1 {
		return Job{}, ErrMigrationFence
	}
	if err := tx.Commit(); err != nil {
		return Job{}, fmt.Errorf("advance data migration commit: %w", err)
	}
	return job, nil
}

func (s *PostgresStore) Release(ctx context.Context, id, owner string, version int64) error {
	ctx = nonNilMigrationContext(ctx)
	if s == nil || s.db == nil {
		return fmt.Errorf("%w: postgres store is unavailable", ErrMigrationCapability)
	}
	// As with Renew, acquire the row lock before consulting the database clock.
	// This prevents a stale releaser from clearing a lease that another worker
	// acquired while the UPDATE was waiting on the row lock.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("release data migration begin: %w", err)
	}
	defer tx.Rollback()
	job, err := scanJob(tx.QueryRowContext(ctx, migrationSelect+` WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMigrationNotFound
	}
	if err != nil {
		return fmt.Errorf("release data migration load: %w", err)
	}
	now, err := databaseNow(ctx, tx)
	if err != nil {
		return fmt.Errorf("release data migration clock: %w", err)
	}
	if err := checkLease(job, owner, version, now); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE data_migrations
		SET lease_owner=NULL, lease_until=NULL, updated_at=clock_timestamp()
		WHERE id=$1 AND lease_owner=$2 AND lease_version=$3
		  AND lease_until > clock_timestamp()`, id, owner, version)
	if err != nil {
		return fmt.Errorf("release data migration: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("release data migration rows: %w", err)
	}
	if rows != 1 {
		return ErrMigrationFence
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("release data migration commit: %w", err)
	}
	return nil
}

const migrationSelect = `
	SELECT id, tenant_id, domain, source_profile, target_profile, phase,
	       paused, cursor, snapshot_watermark, applied_watermark,
	       COALESCE(lease_owner,''), lease_version, lease_until,
	       COALESCE(last_error,''), created_at, updated_at
	FROM data_migrations`

type rowScanner interface{ Scan(...any) error }

func scanJob(row rowScanner) (Job, error) {
	var (
		job                              Job
		domain, phase                    string
		leaseOwner                       string
		leaseUntil, createdAt, updatedAt sql.NullTime
	)
	err := row.Scan(
		&job.ID, &job.TenantID, &domain, &job.SourceProfile, &job.TargetProfile,
		&phase, &job.Paused, &job.Cursor, &job.SnapshotWatermark,
		&job.AppliedWatermark, &leaseOwner, &job.LeaseVersion, &leaseUntil,
		&job.LastError, &createdAt, &updatedAt,
	)
	if err != nil {
		return Job{}, err
	}
	job.Domain = Domain(domain)
	job.Phase = Phase(phase)
	job.LeaseOwner = leaseOwner
	if leaseUntil.Valid {
		job.LeaseUntil = leaseUntil.Time
	}
	if createdAt.Valid {
		job.CreatedAt = createdAt.Time
	}
	if updatedAt.Valid {
		job.UpdatedAt = updatedAt.Time
	}
	if err := job.Validate(); err != nil {
		return Job{}, err
	}
	return job, nil
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

type clockQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func databaseNow(ctx context.Context, q clockQuerier) (time.Time, error) {
	var now time.Time
	if err := q.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now.UTC(), nil
}
