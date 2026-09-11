package background

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

type PostgresRepository struct{ db *sql.DB }

type postgresExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (r *PostgresRepository) sql(ctx context.Context) postgresExecutor {
	if tx := database.Transaction(ctx, r.db); tx != nil {
		return tx
	}
	return r.db
}

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, errors.New("background job database is nil")
	}
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) Enqueue(
	ctx context.Context,
	request EnqueueRequest,
) (EnqueueResult, error) {
	if err := validateEnqueue(&request); err != nil {
		return EnqueueResult{}, err
	}
	id := StableJobID(request.TenantID, request.Type, request.DedupeKey)
	result, err := r.sql(ctx).ExecContext(ctx, `
INSERT INTO background_job(
    job_id, tenant_id, app_id, revision_id, job_type, dedupe_key,
    payload, max_attempts, trace_parent
) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,NULLIF($9,''))
ON CONFLICT (tenant_id, job_type, dedupe_key) DO NOTHING`,
		id, request.TenantID, request.AppID, request.RevisionID, request.Type,
		request.DedupeKey, string(request.Payload), request.MaxAttempts, request.TraceParent,
	)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("enqueue background job: %w", err)
	}
	rows, _ := result.RowsAffected()
	job, err := r.get(ctx, id)
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{Job: job, Duplicate: rows == 0}, nil
}

func (r *PostgresRepository) Claim(
	ctx context.Context,
	workerID string,
	lease time.Duration,
) (Job, error) {
	row := r.sql(ctx).QueryRowContext(ctx, `
WITH candidate AS (
    SELECT job_id FROM background_job
    WHERE status IN ('pending','running')
      AND next_attempt_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
      AND attempt_count < max_attempts
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE background_job j
SET status='running', locked_by=$1, locked_until=now()+$2::interval,
    attempt_count=attempt_count+1
FROM candidate c WHERE j.job_id=c.job_id
RETURNING j.job_id,j.tenant_id,j.app_id,j.revision_id,j.job_type,j.dedupe_key,
	          j.payload,j.status,j.attempt_count,j.max_attempts,COALESCE(j.trace_parent,''),j.created_at,
	          COALESCE(j.last_error,''),j.completed_at`,
		workerID, postgresDuration(lease),
	)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNoJob
	}
	if err != nil {
		return Job{}, fmt.Errorf("claim background job: %w", err)
	}
	return job, nil
}

func (r *PostgresRepository) Complete(ctx context.Context, jobID string, workerID string) error {
	result, err := r.sql(ctx).ExecContext(ctx, `
UPDATE background_job SET status='completed', completed_at=now(),
    locked_by=NULL,locked_until=NULL,last_error=NULL
WHERE job_id=$1 AND locked_by=$2 AND status='running'`, jobID, workerID)
	if err != nil {
		return fmt.Errorf("complete background job: %w", err)
	}
	return requireJobRow(result)
}

func (r *PostgresRepository) Fail(
	ctx context.Context,
	job Job,
	workerID string,
	retryAt time.Time,
	cause error,
) error {
	status := "pending"
	if job.AttemptCount >= job.MaxAttempts {
		status = "dead"
	}
	errorText := ""
	if cause != nil {
		errorText = cause.Error()
		if len(errorText) > 2048 {
			errorText = errorText[:2048]
		}
	}
	result, err := r.sql(ctx).ExecContext(ctx, `
UPDATE background_job SET status=$3,next_attempt_at=$4,last_error=$5,
    locked_by=NULL,locked_until=NULL,completed_at=CASE WHEN $3='dead' THEN now() ELSE NULL END
WHERE job_id=$1 AND locked_by=$2 AND status='running'`,
		job.ID, workerID, status, retryAt, errorText,
	)
	if err != nil {
		return fmt.Errorf("fail background job: %w", err)
	}
	return requireJobRow(result)
}

func (r *PostgresRepository) Get(
	ctx context.Context,
	tenantID string,
	jobID string,
) (Job, error) {
	job, err := scanJob(r.sql(ctx).QueryRowContext(ctx, `
SELECT job_id,tenant_id,app_id,revision_id,job_type,dedupe_key,payload,status,
       attempt_count,max_attempts,COALESCE(trace_parent,''),created_at,
       COALESCE(last_error,''),completed_at
FROM background_job WHERE tenant_id=$1 AND job_id=$2`, tenantID, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}
	return job, err
}

func (r *PostgresRepository) Retry(ctx context.Context, tenantID string, jobID string) error {
	result, err := r.sql(ctx).ExecContext(ctx, `
UPDATE background_job
SET status='pending',attempt_count=0,next_attempt_at=now(),last_error=NULL,
    completed_at=NULL,locked_by=NULL,locked_until=NULL
WHERE tenant_id=$1 AND job_id=$2 AND status='dead'`, tenantID, jobID)
	if err != nil {
		return fmt.Errorf("retry background job: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 1 {
		return nil
	}
	if _, err := r.Get(ctx, tenantID, jobID); err != nil {
		return err
	}
	return ErrJobConflict
}

func (r *PostgresRepository) Ready(ctx context.Context) error { return r.db.PingContext(ctx) }
func (r *PostgresRepository) Close() error                    { return nil }

func (r *PostgresRepository) get(ctx context.Context, id string) (Job, error) {
	return scanJob(r.sql(ctx).QueryRowContext(ctx, `
SELECT job_id,tenant_id,app_id,revision_id,job_type,dedupe_key,payload,status,
       attempt_count,max_attempts,COALESCE(trace_parent,''),created_at,
       COALESCE(last_error,''),completed_at
FROM background_job WHERE job_id=$1`, id))
}

type jobScanner interface{ Scan(...any) error }

func scanJob(row jobScanner) (Job, error) {
	var job Job
	var payload []byte
	var completedAt sql.NullTime
	if err := row.Scan(
		&job.ID, &job.TenantID, &job.AppID, &job.RevisionID, &job.Type,
		&job.DedupeKey, &payload, &job.Status, &job.AttemptCount,
		&job.MaxAttempts, &job.TraceParent, &job.CreatedAt, &job.LastError, &completedAt,
	); err != nil {
		return Job{}, err
	}
	job.Payload = json.RawMessage(payload)
	if completedAt.Valid {
		job.CompletedAt = completedAt.Time
	}
	return job, nil
}

func requireJobRow(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return errors.New("background job ownership mismatch")
	}
	return nil
}

func postgresDuration(value time.Duration) string { return fmt.Sprintf("%f seconds", value.Seconds()) }

var _ Repository = (*PostgresRepository)(nil)
