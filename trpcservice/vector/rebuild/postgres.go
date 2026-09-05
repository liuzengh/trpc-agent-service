package rebuild

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const runColumns = `tenant_id, run_id, projection_fingerprint, mode, phase, cursor_memory_id,
	attempt, max_attempts, scanned, enqueued, tombstoned, lease_owner, lease_epoch,
	lease_fence, lease_expires_at, last_error_category, deadline_at, created_at, updated_at, completed_at`

// RunRepository is the durable rebuild run boundary. Every mutation is
// tenant-scoped and lease-fenced; zero rows affected is a classified failure.
type RunRepository interface {
	Create(ctx context.Context, tc tenant.TenantContext, run Run, now time.Time) (Run, error)
	Get(ctx context.Context, tc tenant.TenantContext, runID string) (Run, error)
	// Claim moves a pending or expired-lease scanning run into scanning under
	// the lease. Zero rows affected is ErrConflictOwner or ErrNotRunning.
	Claim(ctx context.Context, tc tenant.TenantContext, runID string, lease LeaseRef, maxAttempts int, now time.Time) (Run, error)
	// AdvanceCursor advances the keyset cursor and counters inside the
	// caller-owned transaction, fenced by the lease.
	AdvanceCursorTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, runID string, lease LeaseRef, cursor string, scanned, enqueued, tombstoned int64, now time.Time) error
	// MarkScanned completes the enqueue scan under the lease.
	MarkScanned(ctx context.Context, tc tenant.TenantContext, runID string, lease LeaseRef, now time.Time) (Run, error)
	// Complete moves a scanned run to completed under the lease.
	Complete(ctx context.Context, tc tenant.TenantContext, runID string, lease LeaseRef, now time.Time) (Run, error)
	// Fail marks a terminal failure under the lease with a safe category.
	Fail(ctx context.Context, tc tenant.TenantContext, runID string, lease LeaseRef, category Category, now time.Time) (Run, error)
	// Cancel cancels a non-terminal run without requiring a lease.
	Cancel(ctx context.Context, tc tenant.TenantContext, runID string, now time.Time) (Run, error)
	Counts(ctx context.Context) (map[Phase]int64, error)
}

// PostgresRunRepository is the PostgreSQL implementation of RunRepository.
type PostgresRunRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRunRepository fails closed without a pool.
func NewPostgresRunRepository(pool *pgxpool.Pool) (*PostgresRunRepository, error) {
	if pool == nil {
		return nil, ErrInvalidConfig
	}
	return &PostgresRunRepository{pool: pool}, nil
}

func rebuildError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrConflictOwner
	}
	return ErrUnavailable
}

func (r *PostgresRunRepository) Create(ctx context.Context, tc tenant.TenantContext, run Run, now time.Time) (Run, error) {
	if r == nil || r.pool == nil {
		return Run{}, ErrUnavailable
	}
	if err := tc.Validate(); err != nil {
		return Run{}, ErrTenantMismatch
	}
	if run.TenantID != tc.TenantID || !validRunID(run.RunID) || !validHex64(run.Fingerprint) ||
		run.MaxAttempts < 1 || run.MaxAttempts > 100 || run.DeadlineAt.IsZero() {
		return Run{}, ErrInvalidRun
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	row := r.pool.QueryRow(queryCtx, `INSERT INTO vector_rebuild_run
		(tenant_id, run_id, projection_fingerprint, mode, phase, max_attempts, deadline_at, created_at, updated_at)
		VALUES ($1,$2,$3,'rebuild','pending',$4,$5,$6,$6)
		RETURNING `+runColumns,
		tc.TenantID, run.RunID, run.Fingerprint, run.MaxAttempts, run.DeadlineAt.UTC(), now.UTC())
	return scanRun(row)
}

func (r *PostgresRunRepository) Get(ctx context.Context, tc tenant.TenantContext, runID string) (Run, error) {
	if r == nil || r.pool == nil {
		return Run{}, ErrUnavailable
	}
	if err := tc.Validate(); err != nil {
		return Run{}, ErrTenantMismatch
	}
	if !validRunID(runID) {
		return Run{}, ErrInvalidRun
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return scanRun(r.pool.QueryRow(queryCtx, `SELECT `+runColumns+` FROM vector_rebuild_run WHERE tenant_id=$1 AND run_id=$2`, tc.TenantID, runID))
}

func (r *PostgresRunRepository) Claim(ctx context.Context, tc tenant.TenantContext, runID string, lease LeaseRef, maxAttempts int, now time.Time) (Run, error) {
	if err := r.guard(tc, runID, lease); err != nil {
		return Run{}, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	run, err := scanRun(r.pool.QueryRow(queryCtx, `UPDATE vector_rebuild_run
		SET phase=CASE WHEN phase='pending' THEN 'scanning' ELSE phase END,
			attempt=attempt+1,
			lease_owner=$3, lease_epoch=$4, lease_fence=$5, lease_expires_at=$6,
			last_error_category=NULL, updated_at=$7
		WHERE tenant_id=$1 AND run_id=$2 AND attempt < $8
		  AND (
			(phase='pending')
			OR (phase='scanning' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $7)
			OR (phase='scanned')
		  )
		RETURNING `+runColumns,
		tc.TenantID, runID, lease.Owner, int64(lease.Epoch), int64(lease.Fence), lease.ExpiresAt.UTC(), now.UTC(), maxAttempts))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Run{}, ErrConflictOwner
		}
		return Run{}, err
	}
	return run, nil
}

func (r *PostgresRunRepository) guard(tc tenant.TenantContext, runID string, lease LeaseRef) error {
	if r == nil || r.pool == nil {
		return ErrUnavailable
	}
	if err := tc.Validate(); err != nil {
		return ErrTenantMismatch
	}
	if !validRunID(runID) || !lease.valid() {
		return ErrInvalidRun
	}
	return nil
}

func (r *PostgresRunRepository) AdvanceCursorTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, runID string, lease LeaseRef, cursor string, scanned, enqueued, tombstoned int64, now time.Time) error {
	if err := r.guard(tc, runID, lease); err != nil {
		return err
	}
	if cursor == "" || len(cursor) > 256 {
		return ErrInvalidRun
	}
	tag, err := tx.Exec(ctx, `UPDATE vector_rebuild_run
		SET cursor_memory_id=$3, scanned=scanned+$4, enqueued=enqueued+$5, tombstoned=tombstoned+$6, updated_at=$7
		WHERE tenant_id=$1 AND run_id=$2 AND phase='scanning'
		  AND lease_owner=$8 AND lease_epoch=$9 AND lease_fence=$10
		  AND lease_expires_at IS NOT NULL AND lease_expires_at > $7`,
		tc.TenantID, runID, cursor, scanned, enqueued, tombstoned, now.UTC(),
		lease.Owner, int64(lease.Epoch), int64(lease.Fence))
	if err != nil {
		return rebuildError(err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflictOwner
	}
	return nil
}

func (r *PostgresRunRepository) MarkScanned(ctx context.Context, tc tenant.TenantContext, runID string, lease LeaseRef, now time.Time) (Run, error) {
	if err := r.guard(tc, runID, lease); err != nil {
		return Run{}, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	run, err := scanRun(r.pool.QueryRow(queryCtx, `UPDATE vector_rebuild_run
		SET phase='scanned', updated_at=$3
		WHERE tenant_id=$1 AND run_id=$2 AND phase='scanning'
		  AND lease_owner=$4 AND lease_epoch=$5 AND lease_fence=$6
		  AND lease_expires_at IS NOT NULL AND lease_expires_at > $3
		RETURNING `+runColumns,
		tc.TenantID, runID, now.UTC(), lease.Owner, int64(lease.Epoch), int64(lease.Fence)))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Run{}, ErrConflictOwner
		}
		return Run{}, err
	}
	return run, nil
}

func (r *PostgresRunRepository) Complete(ctx context.Context, tc tenant.TenantContext, runID string, lease LeaseRef, now time.Time) (Run, error) {
	if err := r.guard(tc, runID, lease); err != nil {
		return Run{}, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	run, err := scanRun(r.pool.QueryRow(queryCtx, `UPDATE vector_rebuild_run
		SET phase='completed', completed_at=$3, updated_at=$3,
			lease_owner=NULL, lease_epoch=NULL, lease_fence=NULL, lease_expires_at=NULL
		WHERE tenant_id=$1 AND run_id=$2 AND phase='scanned'
		  AND lease_owner=$4 AND lease_epoch=$5 AND lease_fence=$6
		  AND lease_expires_at IS NOT NULL AND lease_expires_at > $3
		RETURNING `+runColumns,
		tc.TenantID, runID, now.UTC(), lease.Owner, int64(lease.Epoch), int64(lease.Fence)))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Run{}, ErrConflictOwner
		}
		return Run{}, err
	}
	return run, nil
}

func (r *PostgresRunRepository) Fail(ctx context.Context, tc tenant.TenantContext, runID string, lease LeaseRef, category Category, now time.Time) (Run, error) {
	if err := r.guard(tc, runID, lease); err != nil {
		return Run{}, err
	}
	if !validCategory(category) {
		return Run{}, ErrInvalidRun
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	run, err := scanRun(r.pool.QueryRow(queryCtx, `UPDATE vector_rebuild_run
		SET phase='failed', completed_at=$3, updated_at=$3, last_error_category=$4,
			lease_owner=NULL, lease_epoch=NULL, lease_fence=NULL, lease_expires_at=NULL
		WHERE tenant_id=$1 AND run_id=$2 AND phase IN ('pending','scanning','scanned')
		  AND lease_owner=$5 AND lease_epoch=$6 AND lease_fence=$7
		  AND (lease_expires_at IS NULL OR lease_expires_at > $3)
		RETURNING `+runColumns,
		tc.TenantID, runID, now.UTC(), string(category), lease.Owner, int64(lease.Epoch), int64(lease.Fence)))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Run{}, ErrConflictOwner
		}
		return Run{}, err
	}
	return run, nil
}

func (r *PostgresRunRepository) Cancel(ctx context.Context, tc tenant.TenantContext, runID string, now time.Time) (Run, error) {
	if r == nil || r.pool == nil {
		return Run{}, ErrUnavailable
	}
	if err := tc.Validate(); err != nil {
		return Run{}, ErrTenantMismatch
	}
	if !validRunID(runID) {
		return Run{}, ErrInvalidRun
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	run, err := scanRun(r.pool.QueryRow(queryCtx, `UPDATE vector_rebuild_run
		SET phase='cancelled', completed_at=$3, updated_at=$3, last_error_category='cancelled',
			lease_owner=NULL, lease_epoch=NULL, lease_fence=NULL, lease_expires_at=NULL
		WHERE tenant_id=$1 AND run_id=$2 AND phase IN ('pending','scanning','scanned')
		RETURNING `+runColumns,
		tc.TenantID, runID, now.UTC()))
	if err != nil {
		return Run{}, rebuildError(err)
	}
	return run, nil
}

func (r *PostgresRunRepository) Counts(ctx context.Context) (map[Phase]int64, error) {
	if r == nil || r.pool == nil {
		return nil, ErrUnavailable
	}
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := r.pool.Query(queryCtx, `SELECT phase, count(*) FROM vector_rebuild_run GROUP BY phase`)
	if err != nil {
		return nil, rebuildError(err)
	}
	defer rows.Close()
	result := make(map[Phase]int64)
	for rows.Next() {
		var phase string
		var count int64
		if err := rows.Scan(&phase, &count); err != nil {
			return nil, ErrUnavailable
		}
		result[Phase(phase)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, ErrUnavailable
	}
	return result, nil
}

func scanRun(row pgx.Row) (Run, error) {
	var run Run
	var mode, phase string
	var cursor *string
	var leaseOwner *string
	var leaseEpoch, leaseFence *int64
	var leaseExpires, completed *time.Time
	var lastError *string
	err := row.Scan(&run.TenantID, &run.RunID, &run.Fingerprint, &mode, &phase, &cursor,
		&run.Attempt, &run.MaxAttempts, &run.Scanned, &run.Enqueued, &run.Tombstoned,
		&leaseOwner, &leaseEpoch, &leaseFence, &leaseExpires, &lastError,
		&run.DeadlineAt, &run.CreatedAt, &run.UpdatedAt, &completed)
	if err != nil {
		return Run{}, rebuildError(err)
	}
	run.Mode = mode
	run.Phase = Phase(phase)
	if cursor != nil {
		run.Cursor = *cursor
	}
	if leaseOwner != nil {
		run.LeaseOwner = *leaseOwner
	}
	if leaseEpoch != nil {
		run.LeaseEpoch = storage.Epoch(*leaseEpoch)
	}
	if leaseFence != nil {
		run.LeaseFence = uint64(*leaseFence)
	}
	if leaseExpires != nil {
		run.LeaseExpires = *leaseExpires
	}
	if lastError != nil {
		run.LastError = *lastError
	}
	if completed != nil {
		run.CompletedAt = *completed
	}
	return run, nil
}

var _ RunRepository = (*PostgresRunRepository)(nil)
