package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const executionCommitInsert = `
INSERT INTO execution_result (
    tenant_id, execution_id, job_id, session_id, owner_id, epoch, fence_token,
    status, result_version, result_json, committed_at, config_version
)
SELECT $1, $2, $3, $4, l.owner_id, l.epoch, l.fencing_token,
       'succeeded', 1, $8::jsonb, clock_timestamp(), $9
FROM session AS s
JOIN session_lease AS l
  ON l.tenant_id = s.tenant_id AND l.session_id = s.session_id
WHERE s.tenant_id = $1
  AND s.session_id = $4
  AND l.owner_id = $5
  AND l.epoch = $6
  AND l.fencing_token = $7
  AND l.leased_until > clock_timestamp()
ON CONFLICT DO NOTHING
`

const executionCommitExisting = `
SELECT job_id, execution_id, tenant_id, session_id, owner_id, epoch, fence_token,
       status, result_version, result_json, committed_at, config_version
FROM execution_result
WHERE tenant_id = $1 AND (execution_id = $2 OR job_id = $3)
LIMIT 1
`

const executionCommitLease = `
SELECT owner_id, epoch, fencing_token, leased_until
FROM session_lease
WHERE tenant_id = $1 AND session_id = $2
`

type ExecutionResultRepository struct {
	pool *pgxpool.Pool

	// beforeCommit is a private transaction-failure seam used only by tests to
	// verify rollback after the result row has been inserted.
	beforeCommit func() error
}

var _ storage.ExecutionResultRepository = (*ExecutionResultRepository)(nil)

func NewExecutionResultRepository(pool *pgxpool.Pool) (*ExecutionResultRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres: execution result pool is required")
	}
	return &ExecutionResultRepository{pool: pool}, nil
}

func (r *ExecutionResultRepository) CommitExecution(ctx context.Context, record storage.ExecutionCommitRecord) error {
	if err := validateExecutionCommitRecord(ctx, record); err != nil {
		return err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return commitError("begin execution commit", err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '15s'`); err != nil {
		return commitError("configure execution commit", err)
	}

	result, err := tx.Exec(ctx, executionCommitInsert,
		record.TenantID,
		record.ExecutionID,
		record.JobID,
		record.SessionID,
		record.OwnerID,
		record.Epoch,
		record.FenceToken,
		string(record.ResultJSON),
		nullableConfigVersion(record.ConfigVersion),
	)
	if err != nil {
		return commitError("insert execution result", err)
	}
	if result.RowsAffected() == 0 {
		outcome, lookupErr := r.resolveExistingOrFence(ctx, tx, record)
		if lookupErr != nil {
			return lookupErr
		}
		if outcome == commitIdempotent {
			if err := tx.Commit(ctx); err != nil {
				return commitError("commit idempotent execution result", err)
			}
			return nil
		}
		return commitError("execution result conflict", storage.ErrConflict)
	}

	if r.beforeCommit != nil {
		if err := r.beforeCommit(); err != nil {
			return commitError("execution commit hook", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return commitError("commit execution result", err)
	}
	return nil
}

func (r *ExecutionResultRepository) GetExecutionResult(ctx context.Context, tc tenant.TenantContext, jobID, executionID string) (storage.ExecutionResultRecord, error) {
	if err := ctx.Err(); err != nil {
		return storage.ExecutionResultRecord{}, err
	}
	if tc.TenantID == "" || strings.TrimSpace(jobID) == "" || strings.TrimSpace(executionID) == "" {
		return storage.ExecutionResultRecord{}, storage.ErrInvalidArgument
	}
	var record storage.ExecutionResultRecord
	var configVersion *int64
	err := WithTenantContext(ctx, r.pool, tc.TenantID, "execution result read", func(ctx context.Context, tx pgx.Tx) error {
		scanErr := tx.QueryRow(ctx, `
SELECT job_id, execution_id, tenant_id, session_id, owner_id, epoch, fence_token,
       status, result_version, result_json, committed_at, config_version
FROM execution_result
WHERE tenant_id = $1 AND job_id = $2 AND execution_id = $3
`, tc.TenantID, jobID, executionID).Scan(
			&record.JobID,
			&record.ExecutionID,
			&record.TenantID,
			&record.SessionID,
			&record.OwnerID,
			&record.Epoch,
			&record.FenceToken,
			&record.Status,
			&record.ResultVersion,
			&record.ResultJSON,
			&record.CommittedAt,
			&configVersion,
		)
		return scanErr
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ExecutionResultRecord{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.ExecutionResultRecord{}, commitError("read execution result", err)
	}
	if configVersion != nil {
		record.ConfigVersion = *configVersion
	}
	return record, nil
}

type commitOutcome uint8

const (
	commitConflict commitOutcome = iota
	commitIdempotent
)

func (r *ExecutionResultRepository) resolveExistingOrFence(ctx context.Context, tx pgx.Tx, record storage.ExecutionCommitRecord) (commitOutcome, error) {
	var existing storage.ExecutionResultRecord
	var existingConfigVersion *int64
	err := tx.QueryRow(ctx, executionCommitExisting, record.TenantID, record.ExecutionID, record.JobID).Scan(
		&existing.JobID,
		&existing.ExecutionID,
		&existing.TenantID,
		&existing.SessionID,
		&existing.OwnerID,
		&existing.Epoch,
		&existing.FenceToken,
		&existing.Status,
		&existing.ResultVersion,
		&existing.ResultJSON,
		&existing.CommittedAt,
		&existingConfigVersion,
	)
	if err == nil {
		if existingConfigVersion != nil {
			existing.ConfigVersion = *existingConfigVersion
		}
		sameExecution := existing.JobID == record.JobID && existing.ExecutionID == record.ExecutionID &&
			existing.TenantID == record.TenantID && existing.SessionID == record.SessionID
		if sameExecution && (existing.OwnerID != record.OwnerID || existing.Epoch != record.Epoch || existing.FenceToken != record.FenceToken) {
			return commitConflict, commitError("execution fence rejected by existing result", storage.ErrFenceRejected)
		}
		if sameExecution && existing.Status == "succeeded" && existing.ConfigVersion == record.ConfigVersion && equalJSON(existing.ResultJSON, record.ResultJSON) {
			return commitIdempotent, nil
		}
		return commitConflict, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return commitConflict, commitError("read conflicting execution result", err)
	}

	var tenantExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tenant WHERE tenant_id = $1)`, record.TenantID).Scan(&tenantExists); err != nil {
		return commitConflict, commitError("check execution tenant", err)
	}
	if !tenantExists {
		return commitConflict, commitError("execution tenant mismatch", storage.ErrTenantMismatch)
	}
	var sessionExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM session WHERE tenant_id = $1 AND session_id = $2)`, record.TenantID, record.SessionID).Scan(&sessionExists); err != nil {
		return commitConflict, commitError("check execution session", err)
	}
	if !sessionExists {
		return commitConflict, commitError("execution session not found", storage.ErrNotFound)
	}

	var owner string
	var epoch storage.Epoch
	var token uint64
	var expiresAt time.Time
	err = tx.QueryRow(ctx, executionCommitLease, record.TenantID, record.SessionID).Scan(&owner, &epoch, &token, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return commitConflict, commitError("execution lease lost", storage.ErrLeaseLost)
	}
	if err != nil {
		return commitConflict, commitError("read execution lease", err)
	}
	if epoch != record.Epoch {
		return commitConflict, commitError("execution epoch rejected", storage.ErrEpochRejected)
	}
	if !expiresAt.After(time.Now().UTC()) {
		return commitConflict, commitError("execution lease expired", storage.ErrLeaseLost)
	}
	if owner != record.OwnerID || token != record.FenceToken {
		return commitConflict, commitError("execution fence rejected", storage.ErrFenceRejected)
	}
	return commitConflict, commitError("execution commit did not affect a row", storage.ErrFenceRejected)
}

func nullableConfigVersion(version int64) any {
	if version < 1 {
		return nil
	}
	return version
}

func equalJSON(left, right []byte) bool {
	var leftValue any
	var rightValue any
	if err := json.Unmarshal(left, &leftValue); err != nil {
		return false
	}
	if err := json.Unmarshal(right, &rightValue); err != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func validateExecutionCommitRecord(ctx context.Context, record storage.ExecutionCommitRecord) error {
	if ctx == nil {
		return storage.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(record.JobID) == "" || strings.TrimSpace(record.ExecutionID) == "" ||
		strings.TrimSpace(record.TenantID) == "" || strings.TrimSpace(record.SessionID) == "" ||
		strings.TrimSpace(record.OwnerID) == "" || record.Epoch == 0 || record.FenceToken == 0 ||
		!json.Valid(record.ResultJSON) {
		return storage.ErrInvalidArgument
	}
	return nil
}

func commitError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return &CommitError{Operation: operation, Err: err}
}

type CommitError struct {
	Operation string
	Err       error
}

func (e *CommitError) Error() string {
	if e == nil || e.Err == nil {
		return "postgres: execution commit failed"
	}
	return fmt.Sprintf("postgres: %s: %v", e.Operation, e.Err)
}

func (e *CommitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
