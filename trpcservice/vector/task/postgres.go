package task

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

const (
	defaultRepositoryTimeout = 5 * time.Second
	taskColumns              = `tenant_id, task_id, source_type, source_id, projection_scope,
        document_id, operation, source_version, source_sequence, content_hash,
        model, model_version, dimension, schema_version, status, attempt,
        max_attempts, next_attempt_at, last_error_category, lease_owner,
        lease_epoch, lease_fence, lease_expires_at, claimed_at, completed_at,
        dead_lettered_at, created_at, updated_at`
)

type RepositoryConfig struct {
	QueryTimeout time.Duration
	MaxAttempts  int
}

type PostgresRepository struct {
	pool        *pgxpool.Pool
	timeout     time.Duration
	maxAttempts int
}

func NewPostgresRepository(pool *pgxpool.Pool, cfg RepositoryConfig) (*PostgresRepository, error) {
	if pool == nil {
		return nil, ErrUnavailable
	}
	if cfg.QueryTimeout == 0 {
		cfg.QueryTimeout = defaultRepositoryTimeout
	}
	if cfg.QueryTimeout <= 0 || cfg.QueryTimeout > time.Minute {
		return nil, ErrInvalidTask
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.MaxAttempts < 1 || cfg.MaxAttempts > 100 {
		return nil, ErrInvalidTask
	}
	return &PostgresRepository{pool: pool, timeout: cfg.QueryTimeout, maxAttempts: cfg.MaxAttempts}, nil
}

func (r *PostgresRepository) queryContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if r == nil || r.pool == nil || ctx == nil {
		return nil, nil, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
	return queryCtx, cancel, nil
}

func normalizedTime(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Now().UTC(), nil
	}
	return value.UTC(), nil
}

func validTenantContext(tc tenant.TenantContext) error {
	if err := tc.Validate(); err != nil {
		return ErrTenantMismatch
	}
	return nil
}

func validTaskCategory(category Category) bool {
	switch category {
	case CategoryCancelled, CategoryDeadline, CategoryInvalidRequest, CategoryStaleTask,
		CategorySchemaMismatch, CategoryDimensionMismatch, CategoryModelMismatch,
		CategoryUnavailable, CategoryRetryable, CategoryPermanent, CategoryUnknown,
		CategoryLeaseLost, CategoryClosed, CategoryFenceRejected, CategorySourceMissing:
		return true
	default:
		return false
	}
}

func dbError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrConflictOwnership
	}
	return ErrUnavailable
}

// queryExecutor is satisfied by both the pool and a PostgreSQL transaction so
// transaction-aware enqueue shares exactly the same SQL and validation as the
// standalone path. It is internal to the durable task boundary.
type queryExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// EnqueueTx inserts the task inside a caller-owned PostgreSQL transaction so a
// Memory mutation and its projection task commit or roll back together. It
// applies the same tenant/ref validation, dedup key, task state defaults and
// query timeout as Enqueue; ownership of commit/rollback stays with the
// caller. It never acquires a lease and never touches Redis or any backend.
func (r *PostgresRepository) EnqueueTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (EnqueueOutcome, error) {
	if tx == nil {
		return EnqueueOutcome{}, ErrInvalidTask
	}
	return r.enqueue(ctx, tx, tc, ref, now)
}

func (r *PostgresRepository) Enqueue(ctx context.Context, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (EnqueueOutcome, error) {
	if err := validTenantContext(tc); err != nil {
		return EnqueueOutcome{}, err
	}
	var outcome EnqueueOutcome
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return EnqueueOutcome{}, err
	}
	defer cancel()
	err = tenantctx.WithTenantContext(queryCtx, r.pool, tc.TenantID, "vector task enqueue", func(ctx context.Context, tx pgx.Tx) error {
		var enqueueErr error
		outcome, enqueueErr = r.enqueue(ctx, tx, tc, ref, now)
		return enqueueErr
	})
	if err != nil {
		return EnqueueOutcome{}, err
	}
	return outcome, nil
}
func (r *PostgresRepository) RedriveTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	if tx == nil {
		return false, ErrInvalidTask
	}
	return r.enqueueRedrive(ctx, tx, tc, ref, now)
}

func (r *PostgresRepository) enqueueRedrive(ctx context.Context, exec queryExecutor, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	if err := validTenantContext(tc); err != nil {
		return false, err
	}
	if err := ref.Validate(); err != nil || ref.TenantID != tc.TenantID {
		return false, ErrTenantMismatch
	}
	taskID, err := TaskID(ref)
	if err != nil {
		return false, ErrInvalidTask
	}
	now, _ = normalizedTime(now)
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return false, err
	}
	defer cancel()
	tag, err := exec.Exec(queryCtx, `INSERT INTO vector_projection_task (
        tenant_id, task_id, source_type, source_id, projection_scope,
        document_id, operation, source_version, source_sequence, content_hash,
        model, model_version, dimension, schema_version, max_attempts,
        next_attempt_at, created_at, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$16,$16)
    ON CONFLICT (tenant_id, task_id) DO UPDATE
        SET status='pending', attempt=0, next_attempt_at=EXCLUDED.next_attempt_at,
            last_error_category=NULL, updated_at=EXCLUDED.updated_at
        WHERE vector_projection_task.status = 'succeeded'`,
		ref.TenantID, taskID, ref.SourceType, ref.SourceID, ref.ProjectionScope,
		ref.DocumentID, ref.Operation, ref.SourceVersion, ref.SourceSequence,
		ref.ContentHash, ref.Model, ref.ModelVersion, ref.Dimension,
		ref.SchemaVersion, r.maxAttempts, now)
	if err != nil {
		return false, dbError(err)
	}
	return tag.RowsAffected() >= 1, nil
}

func (r *PostgresRepository) enqueue(ctx context.Context, exec queryExecutor, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (EnqueueOutcome, error) {
	if err := validTenantContext(tc); err != nil {
		return EnqueueOutcome{}, err
	}
	if err := ref.Validate(); err != nil || ref.TenantID != tc.TenantID {
		return EnqueueOutcome{}, ErrTenantMismatch
	}
	taskID, err := TaskID(ref)
	if err != nil {
		return EnqueueOutcome{}, ErrInvalidTask
	}
	now, _ = normalizedTime(now)
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return EnqueueOutcome{}, err
	}
	defer cancel()
	result, err := exec.Exec(queryCtx, `INSERT INTO vector_projection_task (
        tenant_id, task_id, source_type, source_id, projection_scope,
        document_id, operation, source_version, source_sequence, content_hash,
        model, model_version, dimension, schema_version, max_attempts,
        next_attempt_at, created_at, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$16,$16)
    ON CONFLICT (tenant_id, task_id) DO NOTHING`,
		ref.TenantID, taskID, ref.SourceType, ref.SourceID, ref.ProjectionScope,
		ref.DocumentID, ref.Operation, ref.SourceVersion, ref.SourceSequence,
		ref.ContentHash, ref.Model, ref.ModelVersion, ref.Dimension,
		ref.SchemaVersion, r.maxAttempts, now)
	if err != nil {
		return EnqueueOutcome{}, dbError(err)
	}
	task, err := r.getByKeyExec(queryCtx, exec, ref.TenantID, taskID)
	if err != nil {
		return EnqueueOutcome{}, err
	}
	return EnqueueOutcome{Task: task, Created: result.RowsAffected() == 1}, nil
}

func (r *PostgresRepository) Get(ctx context.Context, tc tenant.TenantContext, taskID string) (Task, error) {
	if err := validTenantContext(tc); err != nil {
		return Task{}, err
	}
	if strings.TrimSpace(taskID) == "" {
		return Task{}, ErrInvalidTask
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return Task{}, err
	}
	defer cancel()
	return r.getByKey(queryCtx, tc.TenantID, taskID)
}

func (r *PostgresRepository) getByKey(ctx context.Context, tenantID, taskID string) (Task, error) {
	var task Task
	err := tenantctx.WithTenantContext(ctx, r.pool, tenantID, "vector task read", func(ctx context.Context, tx pgx.Tx) error {
		var scanErr error
		task, scanErr = r.getByKeyExec(ctx, tx, tenantID, taskID)
		return scanErr
	})
	if err != nil {
		return Task{}, err
	}
	return task, nil
}
func (r *PostgresRepository) getByKeyExec(ctx context.Context, exec queryExecutor, tenantID, taskID string) (Task, error) {
	task, err := scanTask(exec.QueryRow(ctx, `SELECT `+taskColumns+` FROM vector_projection_task WHERE tenant_id=$1 AND task_id=$2`, tenantID, taskID))
	if err != nil {
		return Task{}, dbError(err)
	}
	return task, nil
}

func (r *PostgresRepository) Candidate(ctx context.Context, now time.Time) (Candidate, error) {
	now, _ = normalizedTime(now)
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return Candidate{}, err
	}
	defer cancel()
	var candidate Candidate
	// Global candidate discovery is the task worker's single cross-tenant
	// capability and runs as the fixed SECURITY DEFINER function. It returns
	// only (tenant_id, task_id); every later statement is tenant-bound.
	err = r.pool.QueryRow(queryCtx, `SELECT tenant_id, task_id FROM trpc_vector_task_candidate($1)`, now).Scan(&candidate.TenantID, &candidate.TaskID)
	if err != nil {
		return Candidate{}, dbError(err)
	}
	return candidate, nil
}

func (r *PostgresRepository) Claim(ctx context.Context, candidate Candidate, lease LeaseRef, now time.Time) (Task, error) {
	if strings.TrimSpace(candidate.TenantID) == "" || strings.TrimSpace(candidate.TaskID) == "" || !lease.Valid() {
		return Task{}, ErrInvalidTask
	}
	now, _ = normalizedTime(now)
	if !lease.ExpiresAt.After(now) {
		return Task{}, ErrInvalidTask
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return Task{}, err
	}
	defer cancel()
	var task Task
	err = tenantctx.WithTenantContext(queryCtx, r.pool, candidate.TenantID, "vector task claim", func(ctx context.Context, tx pgx.Tx) error {
		var scanErr error
		task, scanErr = scanTask(tx.QueryRow(ctx, `WITH exhausted AS (
        UPDATE vector_projection_task
        SET status='dead_letter', completed_at=$3, dead_lettered_at=$3,
            last_error_category='unknown', lease_owner=NULL, lease_epoch=NULL,
            lease_fence=NULL, lease_expires_at=NULL, updated_at=$3
        WHERE tenant_id=$1 AND task_id=$2 AND attempt >= max_attempts
          AND ((status IN ('pending','retry_wait') AND next_attempt_at <= $3)
            OR (status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $3))
        RETURNING task_id
    )
    UPDATE vector_projection_task
        SET status='running', attempt=attempt+1, next_attempt_at=$3,
            lease_owner=$4, lease_epoch=$5, lease_fence=$6, lease_expires_at=$7,
            claimed_at=COALESCE(claimed_at,$3), updated_at=$3, last_error_category=NULL
        WHERE tenant_id=$1 AND task_id=$2 AND attempt < max_attempts
          AND ((status IN ('pending','retry_wait') AND next_attempt_at <= $3)
            OR (status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $3))
        RETURNING `+taskColumns, candidate.TenantID, candidate.TaskID, now,
			lease.Owner, int64(lease.Epoch), int64(lease.Fence), lease.ExpiresAt))
		return scanErr
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Task{}, ErrNotClaimable
		}
		return Task{}, dbError(err)
	}
	return task, nil
}
func (r *PostgresRepository) ExtendLease(ctx context.Context, candidate Candidate, lease LeaseRef, now time.Time) error {
	if strings.TrimSpace(candidate.TenantID) == "" || strings.TrimSpace(candidate.TaskID) == "" || !lease.Valid() {
		return ErrInvalidTask
	}
	now, _ = normalizedTime(now)
	if !lease.ExpiresAt.After(now) {
		return ErrInvalidTask
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	err = tenantctx.WithTenantContext(queryCtx, r.pool, candidate.TenantID, "vector task extend", func(ctx context.Context, tx pgx.Tx) error {
		result, execErr := tx.Exec(ctx, `UPDATE vector_projection_task
        SET lease_expires_at=$7, updated_at=$3
        WHERE tenant_id=$1 AND task_id=$2 AND status='running'
          AND lease_owner=$4 AND lease_epoch=$5 AND lease_fence=$6
          AND lease_expires_at IS NOT NULL AND lease_expires_at > $3`,
			candidate.TenantID, candidate.TaskID, now, lease.Owner, int64(lease.Epoch), int64(lease.Fence), lease.ExpiresAt)
		if execErr != nil {
			return execErr
		}
		if result.RowsAffected() != 1 {
			return ErrConflictOwnership
		}
		return nil
	})
	if err != nil && !errors.Is(err, ErrConflictOwnership) {
		return dbError(err)
	}
	return err
}
func (r *PostgresRepository) Complete(ctx context.Context, candidate Candidate, lease LeaseRef, now time.Time) (Task, error) {
	if strings.TrimSpace(candidate.TenantID) == "" || strings.TrimSpace(candidate.TaskID) == "" || !lease.Valid() {
		return Task{}, ErrInvalidTask
	}
	now, _ = normalizedTime(now)
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return Task{}, err
	}
	defer cancel()
	var task Task
	err = tenantctx.WithTenantContext(queryCtx, r.pool, candidate.TenantID, "vector task complete", func(ctx context.Context, tx pgx.Tx) error {
		var scanErr error
		task, scanErr = scanTask(tx.QueryRow(ctx, `UPDATE vector_projection_task v
        SET status='succeeded', completed_at=$3, updated_at=$3,
            next_attempt_at=$3, last_error_category=NULL,
            lease_owner=NULL, lease_epoch=NULL, lease_fence=NULL, lease_expires_at=NULL
        WHERE v.tenant_id=$1 AND v.task_id=$2 AND v.status='running'
          AND v.lease_owner=$4 AND v.lease_epoch=$5 AND v.lease_fence=$6
          AND v.lease_expires_at IS NOT NULL AND v.lease_expires_at > $3
          AND NOT EXISTS (
              SELECT 1 FROM vector_projection_task n
              WHERE n.tenant_id=v.tenant_id AND n.document_id=v.document_id
                AND n.status='succeeded' AND n.task_id<>v.task_id
                AND (n.source_version, n.source_sequence,
                     CASE WHEN n.operation='delete' THEN 1 ELSE 0 END)
                    > (v.source_version, v.source_sequence,
                       CASE WHEN v.operation='delete' THEN 1 ELSE 0 END)
          )
        RETURNING `+taskColumns, candidate.TenantID, candidate.TaskID, now,
			lease.Owner, int64(lease.Epoch), int64(lease.Fence)))
		return scanErr
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Task{}, ErrConflictOwnership
		}
		return Task{}, dbError(err)
	}
	return task, nil
}
func (r *PostgresRepository) Fail(ctx context.Context, candidate Candidate, lease LeaseRef, failure Failure, now time.Time) (Task, error) {
	if strings.TrimSpace(candidate.TenantID) == "" || strings.TrimSpace(candidate.TaskID) == "" || !lease.Valid() {
		return Task{}, ErrInvalidTask
	}
	if !validTaskCategory(failure.Category) || failure.Kind == "" {
		return Task{}, ErrInvalidTask
	}
	now, _ = normalizedTime(now)
	next := failure.NextAttempt
	if failure.Kind == FailureRetry {
		if next.IsZero() || next.Before(now) {
			return Task{}, ErrInvalidTask
		}
		next = next.UTC()
	} else {
		next = now
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return Task{}, err
	}
	defer cancel()
	var task Task
	err = tenantctx.WithTenantContext(queryCtx, r.pool, candidate.TenantID, "vector task fail", func(ctx context.Context, tx pgx.Tx) error {
		var scanErr error
		task, scanErr = scanTask(tx.QueryRow(ctx, `UPDATE vector_projection_task
        SET status = CASE
                WHEN $4='retry' AND attempt < max_attempts THEN 'retry_wait'
                WHEN $4='stale' THEN 'stale'
                WHEN $4='release' THEN 'pending'
                ELSE 'dead_letter'
            END,
            attempt = CASE WHEN $4='release' AND attempt > 0 THEN attempt-1 ELSE attempt END,
            next_attempt_at = CASE
                WHEN ($4='retry' AND attempt < max_attempts) OR $4='release' THEN $3
                ELSE $2
            END,
            last_error_category=$5,
            lease_owner=NULL, lease_epoch=NULL, lease_fence=NULL, lease_expires_at=NULL,
            completed_at = CASE
                WHEN ($4='retry' AND attempt < max_attempts) OR $4='release' THEN NULL
                ELSE $2
            END,
            dead_lettered_at = CASE
                WHEN $4='dead_letter' OR ($4='retry' AND attempt >= max_attempts) THEN $2
                ELSE NULL
            END,
            updated_at=$2
        WHERE tenant_id=$1 AND task_id=$6 AND status='running'
          AND lease_owner=$7 AND lease_epoch=$8 AND lease_fence=$9
          AND lease_expires_at IS NOT NULL AND lease_expires_at > $2
        RETURNING `+taskColumns,
			candidate.TenantID, now, next, string(failure.Kind), string(failure.Category), candidate.TaskID,
			lease.Owner, int64(lease.Epoch), int64(lease.Fence)))
		return scanErr
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Task{}, ErrConflictOwnership
		}
		return Task{}, dbError(err)
	}
	return task, nil
}
func (r *PostgresRepository) Cancel(ctx context.Context, tc tenant.TenantContext, taskID string, now time.Time) (Task, error) {
	if err := validTenantContext(tc); err != nil {
		return Task{}, err
	}
	if strings.TrimSpace(taskID) == "" {
		return Task{}, ErrInvalidTask
	}
	now, _ = normalizedTime(now)
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return Task{}, err
	}
	defer cancel()
	var task Task
	err = tenantctx.WithTenantContext(queryCtx, r.pool, tc.TenantID, "vector task cancel", func(ctx context.Context, tx pgx.Tx) error {
		var scanErr error
		task, scanErr = scanTask(tx.QueryRow(ctx, `UPDATE vector_projection_task
        SET status='cancelled', completed_at=$3, updated_at=$3,
            next_attempt_at=$3, last_error_category='cancelled',
            lease_owner=NULL, lease_epoch=NULL, lease_fence=NULL, lease_expires_at=NULL
        WHERE tenant_id=$1 AND task_id=$2
          AND status IN ('pending','retry_wait','running')
        RETURNING `+taskColumns, tc.TenantID, taskID, now))
		return scanErr
	})
	if err == nil {
		return task, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Task{}, dbError(err)
	}
	existing, getErr := r.getByKey(queryCtx, tc.TenantID, taskID)
	if getErr != nil {
		return Task{}, getErr
	}
	if existing.State != StateCancelled {
		return Task{}, ErrConflictOwnership
	}
	return existing, nil
}
func (r *PostgresRepository) Head(ctx context.Context, tenantID, documentID string) (HeadKey, bool, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(documentID) == "" {
		return HeadKey{}, false, ErrInvalidTask
	}
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return HeadKey{}, false, err
	}
	defer cancel()
	var key HeadKey
	var operation string
	err = tenantctx.WithTenantContext(queryCtx, r.pool, tenantID, "vector head read", func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(queryCtx, `SELECT source_version, source_sequence, operation
        FROM vector_projection_task WHERE tenant_id=$1 AND document_id=$2 AND status='succeeded'
        ORDER BY source_version DESC, source_sequence DESC,
            CASE WHEN operation='delete' THEN 1 ELSE 0 END DESC LIMIT 1`, tenantID, documentID).Scan(&key.Version, &key.Sequence, &operation)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return HeadKey{}, false, nil
	}
	if err != nil {
		return HeadKey{}, false, dbError(err)
	}
	key.Operation = vector.VectorOperation(operation)
	return key, true, nil
}
func (r *PostgresRepository) Counts(ctx context.Context) (map[State]int64, error) {
	queryCtx, cancel, err := r.queryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	rows, err := r.pool.Query(queryCtx, `SELECT status, count(*) FROM vector_projection_task GROUP BY status`)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	result := make(map[State]int64)
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, dbError(err)
		}
		result[State(status)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, dbError(err)
	}
	return result, nil
}

func scanTask(row pgx.Row) (Task, error) {
	var t Task
	var operation, status string
	var lastError, leaseOwner *string
	var leaseEpoch, leaseFence *int64
	var leaseExpires, claimed, completed, deadLettered *time.Time
	err := row.Scan(&t.TenantID, &t.TaskID, &t.SourceType, &t.SourceID, &t.ProjectionScope,
		&t.DocumentID, &operation, &t.SourceVersion, &t.SourceSequence, &t.ContentHash,
		&t.Model, &t.ModelVersion, &t.Dimension, &t.SchemaVersion, &status, &t.Attempt,
		&t.MaxAttempts, &t.NextAttemptAt, &lastError, &leaseOwner, &leaseEpoch,
		&leaseFence, &leaseExpires, &claimed, &completed, &deadLettered, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return Task{}, err
	}
	t.Operation = vector.VectorOperation(operation)
	t.State = State(status)
	if lastError != nil {
		t.LastError = *lastError
	}
	if leaseOwner != nil {
		t.LeaseOwner = *leaseOwner
	}
	if leaseEpoch != nil {
		t.LeaseEpoch = storage.Epoch(*leaseEpoch)
	}
	if leaseFence != nil {
		t.LeaseFence = uint64(*leaseFence)
	}
	if leaseExpires != nil {
		t.LeaseExpiresAt = *leaseExpires
	}
	if claimed != nil {
		t.ClaimedAt = *claimed
	}
	if completed != nil {
		t.CompletedAt = *completed
	}
	if deadLettered != nil {
		t.DeadLetteredAt = *deadLettered
	}
	return t, nil
}

var _ Repository = (*PostgresRepository)(nil)
