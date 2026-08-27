package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const (
	completionLeaseSelect = `
SELECT owner_id, epoch, fencing_token, leased_until, clock_timestamp()
FROM session_lease
WHERE tenant_id = $1 AND session_id = $2
FOR UPDATE`

	completionQueueSelect = `
SELECT execution_id, status, COALESCE(delivery_id, ''),
       COALESCE(last_delivery_id, ''), COALESCE(leased_until, 'epoch'::timestamptz), payload, clock_timestamp()
FROM job_queue
WHERE tenant_id = $1 AND job_id = $2
FOR UPDATE`

	completionResultSelect = `
SELECT job_id, execution_id, tenant_id, session_id, owner_id, epoch,
       fence_token, status, result_version, result_json, committed_at
FROM execution_result
WHERE tenant_id = $1 AND (execution_id = $2 OR job_id = $3)
LIMIT 1`

	completionResultInsert = `
INSERT INTO execution_result (
    tenant_id, execution_id, job_id, session_id, owner_id, epoch, fence_token,
    status, result_version, result_json, committed_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'succeeded', 1, $8::jsonb, clock_timestamp())
ON CONFLICT DO NOTHING`

	completionQueueAck = `
UPDATE job_queue
SET status = 'acked', delivery_id = NULL, leased_until = NULL,
    last_delivery_id = $3, acked_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND job_id = $2 AND status = 'in_flight'
  AND delivery_id = $3 AND leased_until > clock_timestamp()`
)

// CompletionOutcome describes the best-effort read-only state observation made
// after a database connection failed while committing the transaction.
type CompletionOutcome string

const (
	CompletionOutcomeUnknown   CompletionOutcome = "unknown"
	CompletionNeitherCommitted CompletionOutcome = "neither_committed"
	CompletionBothCommitted    CompletionOutcome = "both_committed"
	CompletionResultOnly       CompletionOutcome = "result_only"
	CompletionAckOnly          CompletionOutcome = "ack_only"
	CompletionTokenTakenOver   CompletionOutcome = "delivery_token_taken_over"
)

// CompletionOutcomeError never reports an unknown commit as success. Outcome
// is diagnostic state only; callers must still handle ErrCompletionOutcomeUnknown.
type CompletionOutcomeError struct {
	Operation string
	Outcome   CompletionOutcome
	Err       error
}

func (e *CompletionOutcomeError) Error() string {
	if e == nil {
		return "postgres: completion transaction outcome is unknown"
	}
	if e.Err == nil {
		return fmt.Sprintf("postgres: %s: completion outcome=%s", e.Operation, e.Outcome)
	}
	return fmt.Sprintf("postgres: %s: completion outcome=%s: %v", e.Operation, e.Outcome, e.Err)
}

func (e *CompletionOutcomeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return errors.Join(storage.ErrCompletionOutcomeUnknown, e.Err)
}

// AtomicCompletionCoordinator is the PostgreSQL implementation of the
// storage-neutral atomic result-and-ack contract.
type AtomicCompletionCoordinator struct {
	pool *pgxpool.Pool

	// beforeCommit is a test-only seam. Tests use it to close the transaction's
	// connection immediately before Commit, which models an unknown outcome.
	beforeCommit func(*pgxpool.Conn)
}

var _ storage.AtomicCompletionCoordinator = (*AtomicCompletionCoordinator)(nil)

func NewAtomicCompletionCoordinator(pool *pgxpool.Pool) (*AtomicCompletionCoordinator, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres: atomic completion pool is required")
	}
	return &AtomicCompletionCoordinator{pool: pool}, nil
}

func (c *AtomicCompletionCoordinator) CommitResultAndAck(ctx context.Context, request storage.AtomicCompletionRequest) error {
	if err := validateAtomicCompletionRequest(ctx, request); err != nil {
		return err
	}
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return completionDBError("acquire transaction connection", err)
	}
	released := false
	transactionFinished := false
	tx, err := conn.Conn().BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		conn.Release()
		released = true
		return completionDBError("begin transaction", err)
	}
	defer func() {
		if !transactionFinished {
			_ = tx.Rollback(ctx)
		}
		if !released {
			conn.Release()
		}
	}()
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '15s'`); err != nil {
		return completionDBError("configure transaction", err)
	}

	queueState, err := lockCompletionQueue(ctx, tx, request)
	if err != nil {
		return err
	}
	existing, found, err := readCompletionResult(ctx, tx, request)
	if err != nil {
		return err
	}

	// A completed request may be retried after the worker releases its lease.
	// The durable result plus the matching last delivery token is sufficient for
	// this idempotent read-only success; no new state is written.
	if queueState.status == "acked" {
		if queueState.lastDeliveryID != request.Delivery.DeliveryID {
			return storage.ErrDeliveryFinished
		}
		if !found || !sameCompletionResult(existing, request) {
			return completionConflict("queue is acked but its result is absent or different")
		}
		if !sameCompletionFence(existing, request) {
			return storage.ErrFenceRejected
		}
		return c.commitCompletionTx(ctx, conn, tx, request, &transactionFinished, &released)
	}
	if queueState.status != "in_flight" {
		return storage.ErrDeliveryFinished
	}
	if queueState.deliveryID != request.Delivery.DeliveryID {
		return storage.ErrDeliveryFinished
	}
	if !queueState.leasedUntil.After(queueState.now) {
		return storage.ErrDeliveryExpired
	}
	lease, err := lockCompletionLease(ctx, tx, request)
	if err != nil {
		return err
	}

	if found {
		if !sameCompletionResult(existing, request) {
			return completionConflict("execution result identity or payload conflicts")
		}
	} else {
		result, err := tx.Exec(ctx, completionResultInsert,
			request.Commit.TenantID,
			request.Commit.ExecutionID,
			request.Commit.JobID,
			request.Commit.SessionID,
			lease.ownerID,
			lease.epoch,
			lease.fenceToken,
			string(request.Commit.ResultJSON),
		)
		if err != nil {
			return completionDBError("insert execution result", err)
		}
		if result.RowsAffected() == 0 {
			existing, found, err = readCompletionResult(ctx, tx, request)
			if err != nil {
				return err
			}
			if !found || !sameCompletionResult(existing, request) {
				return completionConflict("execution result insert did not produce the requested result")
			}
		}
	}

	result, err := tx.Exec(ctx, completionQueueAck,
		request.Delivery.TenantID,
		request.Delivery.JobID,
		request.Delivery.DeliveryID,
	)
	if err != nil {
		return completionDBError("ack delivery", err)
	}
	if result.RowsAffected() != 1 {
		return storage.ErrDeliveryExpired
	}
	return c.commitCompletionTx(ctx, conn, tx, request, &transactionFinished, &released)
}

type completionLeaseState struct {
	ownerID    string
	epoch      storage.Epoch
	fenceToken uint64
	expiresAt  time.Time
	now        time.Time
}

func lockCompletionLease(ctx context.Context, tx pgx.Tx, request storage.AtomicCompletionRequest) (completionLeaseState, error) {
	var state completionLeaseState
	err := tx.QueryRow(ctx, completionLeaseSelect, request.Commit.TenantID, request.Commit.SessionID).Scan(
		&state.ownerID, &state.epoch, &state.fenceToken, &state.expiresAt, &state.now)
	if errors.Is(err, pgx.ErrNoRows) {
		var tenantExists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tenant WHERE tenant_id = $1)`, request.Commit.TenantID).Scan(&tenantExists); err != nil {
			return completionLeaseState{}, completionDBError("check completion tenant", err)
		}
		if !tenantExists {
			return completionLeaseState{}, storage.ErrTenantMismatch
		}
		var sessionExists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM session WHERE tenant_id = $1 AND session_id = $2)`, request.Commit.TenantID, request.Commit.SessionID).Scan(&sessionExists); err != nil {
			return completionLeaseState{}, completionDBError("check completion session", err)
		}
		if !sessionExists {
			return completionLeaseState{}, storage.ErrNotFound
		}
		return completionLeaseState{}, storage.ErrLeaseLost
	}
	if err != nil {
		return completionLeaseState{}, completionDBError("lock completion lease", err)
	}
	if state.epoch != request.Commit.Epoch {
		return completionLeaseState{}, storage.ErrEpochRejected
	}
	if !state.expiresAt.After(state.now) {
		return completionLeaseState{}, storage.ErrLeaseLost
	}
	if state.ownerID != request.Commit.OwnerID || state.fenceToken != request.Commit.FenceToken {
		return completionLeaseState{}, storage.ErrFenceRejected
	}
	return state, nil
}

type completionQueueState struct {
	executionID    string
	status         string
	deliveryID     string
	lastDeliveryID string
	leasedUntil    time.Time
	now            time.Time
	payload        []byte
}

func lockCompletionQueue(ctx context.Context, tx pgx.Tx, request storage.AtomicCompletionRequest) (completionQueueState, error) {
	var state completionQueueState
	err := tx.QueryRow(ctx, completionQueueSelect, request.Delivery.TenantID, request.Delivery.JobID).Scan(
		&state.executionID, &state.status, &state.deliveryID, &state.lastDeliveryID,
		&state.leasedUntil, &state.payload, &state.now)
	if errors.Is(err, pgx.ErrNoRows) {
		return completionQueueState{}, storage.ErrDeliveryExpired
	}
	if err != nil {
		return completionQueueState{}, completionDBError("lock delivery", err)
	}
	var stored struct {
		JobID     string `json:"job_id"`
		Execution string `json:"execution_id"`
		Tenant    struct {
			TenantID  string `json:"tenant_id"`
			SessionID string `json:"session_id"`
		} `json:"tenant"`
	}
	if err := json.Unmarshal(state.payload, &stored); err != nil {
		return completionQueueState{}, fmt.Errorf("postgres: decode queued job identity: %w", storage.ErrInvalidArgument)
	}
	if state.executionID != request.Delivery.ExecutionID || stored.JobID != request.Delivery.JobID || stored.Execution != request.Delivery.ExecutionID {
		return completionQueueState{}, storage.ErrInvalidDelivery
	}
	if stored.Tenant.TenantID != request.Delivery.TenantID {
		return completionQueueState{}, storage.ErrTenantMismatch
	}
	if stored.Tenant.SessionID != request.Delivery.SessionID || request.Commit.SessionID != stored.Tenant.SessionID {
		return completionQueueState{}, storage.ErrInvalidDelivery
	}
	return state, nil
}

func readCompletionResult(ctx context.Context, tx pgx.Tx, request storage.AtomicCompletionRequest) (storage.ExecutionResultRecord, bool, error) {
	var record storage.ExecutionResultRecord
	err := tx.QueryRow(ctx, completionResultSelect, request.Commit.TenantID, request.Commit.ExecutionID, request.Commit.JobID).Scan(
		&record.JobID, &record.ExecutionID, &record.TenantID, &record.SessionID,
		&record.OwnerID, &record.Epoch, &record.FenceToken, &record.Status,
		&record.ResultVersion, &record.ResultJSON, &record.CommittedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ExecutionResultRecord{}, false, nil
	}
	if err != nil {
		return storage.ExecutionResultRecord{}, false, completionDBError("read execution result", err)
	}
	return record, true, nil
}

func sameCompletionResult(existing storage.ExecutionResultRecord, request storage.AtomicCompletionRequest) bool {
	return existing.JobID == request.Commit.JobID &&
		existing.ExecutionID == request.Commit.ExecutionID &&
		existing.TenantID == request.Commit.TenantID &&
		existing.SessionID == request.Commit.SessionID &&
		existing.Status == "succeeded" &&
		equalJSON(existing.ResultJSON, request.Commit.ResultJSON)
}

func sameCompletionFence(existing storage.ExecutionResultRecord, request storage.AtomicCompletionRequest) bool {
	return existing.OwnerID == request.Commit.OwnerID &&
		existing.Epoch == request.Commit.Epoch &&
		existing.FenceToken == request.Commit.FenceToken
}

func (c *AtomicCompletionCoordinator) commitCompletionTx(ctx context.Context, conn *pgxpool.Conn, tx pgx.Tx, request storage.AtomicCompletionRequest, transactionFinished, released *bool) error {
	if c.beforeCommit != nil {
		c.beforeCommit(conn)
	}
	if err := tx.Commit(ctx); err != nil {
		_ = tx.Rollback(ctx)
		*transactionFinished = true
		conn.Release()
		*released = true
		return c.unknownOutcome(ctx, request, err)
	}
	*transactionFinished = true
	return nil
}

func (c *AtomicCompletionCoordinator) unknownOutcome(ctx context.Context, request storage.AtomicCompletionRequest, commitErr error) error {
	outcome, reconcileErr := c.reconcileCompletion(ctx, request)
	if reconcileErr != nil {
		commitErr = errors.Join(commitErr, reconcileErr)
	}
	return &CompletionOutcomeError{Operation: "commit result and ack", Outcome: outcome, Err: commitErr}
}

func (c *AtomicCompletionCoordinator) reconcileCompletion(ctx context.Context, request storage.AtomicCompletionRequest) (CompletionOutcome, error) {
	if err := ctx.Err(); err != nil {
		return CompletionOutcomeUnknown, err
	}
	var resultExists bool
	if err := c.pool.QueryRow(ctx, `
SELECT EXISTS(
    SELECT 1 FROM execution_result
    WHERE tenant_id = $1 AND execution_id = $2 AND job_id = $3 AND session_id = $4
)`, request.Commit.TenantID, request.Commit.ExecutionID, request.Commit.JobID, request.Commit.SessionID).Scan(&resultExists); err != nil {
		return CompletionOutcomeUnknown, completionDBError("reconcile execution result", err)
	}
	var status, deliveryID, lastDeliveryID string
	err := c.pool.QueryRow(ctx, `
SELECT status, COALESCE(delivery_id, ''), COALESCE(last_delivery_id, '')
FROM job_queue WHERE tenant_id = $1 AND job_id = $2`, request.Delivery.TenantID, request.Delivery.JobID).Scan(&status, &deliveryID, &lastDeliveryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return CompletionOutcomeUnknown, nil
	}
	if err != nil {
		return CompletionOutcomeUnknown, completionDBError("reconcile queue delivery", err)
	}
	if status == "acked" && lastDeliveryID == request.Delivery.DeliveryID {
		if resultExists {
			return CompletionBothCommitted, nil
		}
		return CompletionAckOnly, nil
	}
	if status == "in_flight" && deliveryID != request.Delivery.DeliveryID {
		return CompletionTokenTakenOver, nil
	}
	if resultExists {
		return CompletionResultOnly, nil
	}
	return CompletionNeitherCommitted, nil
}

func validateAtomicCompletionRequest(ctx context.Context, request storage.AtomicCompletionRequest) error {
	if ctx == nil {
		return storage.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	commit := request.Commit
	delivery := request.Delivery
	if strings.TrimSpace(commit.TenantID) == "" || strings.TrimSpace(commit.SessionID) == "" ||
		strings.TrimSpace(commit.JobID) == "" || strings.TrimSpace(commit.ExecutionID) == "" ||
		strings.TrimSpace(commit.OwnerID) == "" || commit.Epoch == 0 || commit.FenceToken == 0 ||
		!json.Valid(commit.ResultJSON) || strings.TrimSpace(delivery.TenantID) == "" ||
		strings.TrimSpace(delivery.SessionID) == "" || strings.TrimSpace(delivery.JobID) == "" ||
		strings.TrimSpace(delivery.ExecutionID) == "" || strings.TrimSpace(delivery.DeliveryID) == "" {
		return storage.ErrInvalidArgument
	}
	if commit.TenantID != delivery.TenantID {
		return storage.ErrTenantMismatch
	}
	if commit.JobID != delivery.JobID || commit.ExecutionID != delivery.ExecutionID || commit.SessionID != delivery.SessionID {
		return storage.ErrInvalidDelivery
	}
	return nil
}

func completionConflict(message string) error {
	return fmt.Errorf("postgres: atomic completion conflict (%s): %w", message, storage.ErrConflict)
}

func completionDBError(operation string, err error) error {
	return fmt.Errorf("postgres: atomic completion %s: %w", operation, err)
}
