package datamigration

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) (*PostgresStore, error) {
	if pool == nil {
		return nil, errors.New("data migration database pool is required")
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Create(ctx context.Context, job Job) error {
	if !validNewJob(job) {
		return errors.New("data migration job is invalid")
	}
	checkpoint, err := json.Marshal(cloneMap(job.Checkpoint))
	if err != nil {
		return errors.New("encode data migration checkpoint")
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO data_migrations
			(migration_id, tenant_id, app_name, resource_kind, source_backend, target_backend,
			 phase, status, checkpoint)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)`,
		job.MigrationID, job.TenantID, job.AppName, job.ResourceKind, job.Source, job.Target,
		PhasePrepare, StatusPending, checkpoint)
	if err != nil {
		return errors.New("create data migration job")
	}
	return nil
}

func (s *PostgresStore) Get(ctx context.Context, migrationID string) (Job, error) {
	var job Job
	var checkpoint []byte
	err := s.pool.QueryRow(ctx, `
		SELECT migration_id, tenant_id, app_name, resource_kind, source_backend, target_backend,
		       phase, status, generation, checkpoint, copied_count, verified_count,
		       failed_count, mismatch_count, last_error_type
		FROM data_migrations WHERE migration_id = $1`, migrationID).Scan(
		&job.MigrationID, &job.TenantID, &job.AppName, &job.ResourceKind, &job.Source, &job.Target,
		&job.Phase, &job.Status, &job.Generation, &checkpoint, &job.Copied, &job.Verified,
		&job.Failed, &job.Mismatches, &job.LastError,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, errors.New("read data migration job")
	}
	if err := json.Unmarshal(checkpoint, &job.Checkpoint); err != nil {
		return Job{}, errors.New("decode data migration checkpoint")
	}
	return job, nil
}

func (s *PostgresStore) CompareAndSwap(ctx context.Context, expectedGeneration int64, job Job) error {
	checkpoint, err := json.Marshal(cloneMap(job.Checkpoint))
	if err != nil {
		return errors.New("encode data migration checkpoint")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE data_migrations
		SET phase = $1, status = $2, generation = generation + 1, checkpoint = $3::jsonb,
		    copied_count = $4, verified_count = $5, failed_count = $6,
		    mismatch_count = $7, last_error_type = $8, updated_at = clock_timestamp()
		WHERE migration_id = $9 AND generation = $10`,
		job.Phase, job.Status, checkpoint, job.Copied, job.Verified, job.Failed,
		job.Mismatches, job.LastError, job.MigrationID, expectedGeneration)
	if err != nil {
		return errors.New("update data migration job")
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func validNewJob(job Job) bool {
	if job.MigrationID == "" || job.TenantID == "" || job.AppName == "" || job.Source == "" || job.Target == "" {
		return false
	}
	switch job.ResourceKind {
	case "session", "memory", "artifact", "knowledge":
		return true
	default:
		return false
	}
}
