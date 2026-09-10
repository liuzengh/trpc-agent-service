package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	maxExecutionAttempts  = 3
	blockedDispatchDelay  = time.Second
	defaultDispatchBatch  = 32
	executionRetryInitial = time.Second
	executionRetryMax     = 30 * time.Second
	// consumedOutboxRetention bounds how long consumed dispatch records are
	// kept for observability before RecoverDispatches removes them.
	consumedOutboxRetention = 24 * time.Hour
)

// Claim changes one dispatched execution into a worker-owned run. It retains
// PostgreSQL session-lane order even when Redis delivers entries out of order.
func (s *Store) Claim(ctx context.Context, dispatch queue.Dispatch, request queue.ClaimRequest) (queue.Claim, bool, error) {
	if err := s.validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := dispatch.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := request.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return queue.Claim{}, false, fmt.Errorf("begin execution claim: %w", err)
	}
	defer func() { rollback(tx) }()
	stored, active, err := lockExecutionForClaim(ctx, tx, dispatch)
	if err != nil {
		return queue.Claim{}, false, err
	}
	if stored.requestID != "" {
		if err := validateDispatchTraceContext(dispatch, stored); err != nil {
			return queue.Claim{}, false, err
		}
	}
	if !active || stored.status == "WAITING_APPROVAL" || stored.status == "SUCCEEDED" || stored.status == "FAILED" || stored.status == "UNCERTAIN" || stored.status == "CANCELED" {
		if err := consumeDispatch(ctx, tx, dispatch); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit unavailable execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	if stored.status == "RUNNING" && stored.leaseActive {
		if err := consumeDispatch(ctx, tx, dispatch); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit duplicate execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	if stored.attempt >= maxExecutionAttempts {
		if _, err := tx.Exec(ctx, `UPDATE platform.execution
SET status = 'UNCERTAIN',
    last_error = 'execution reached its attempt limit without a terminal result',
    lease_owner = NULL, run_token = NULL, lease_until = NULL,
    finished_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
  AND status IN ('PENDING', 'RUNNING')`, stored.tenantID, stored.appID, stored.requestID); err != nil {
			return queue.Claim{}, false, fmt.Errorf("mark exhausted execution uncertain: %w", err)
		}
		if err := releaseExecutionQuotaTx(ctx, tx, stored.tenantID, stored.appID, stored.requestID); err != nil {
			return queue.Claim{}, false, err
		}
		if err := consumeDispatch(ctx, tx, dispatch); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit exhausted execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	if !stored.attemptReady || stored.hasEarlier {
		if err := deferDispatch(ctx, tx, dispatch, stored.nextAttemptAt); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit deferred execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	var leaseUntil time.Time
	token := uuid.NewString()
	err = tx.QueryRow(ctx, `UPDATE platform.execution
SET status = 'RUNNING', attempt = attempt + 1, lease_owner = $4, run_token = $5,
    lease_until = clock_timestamp() + $6::interval, next_attempt_at = clock_timestamp(),
    started_at = COALESCE(started_at, clock_timestamp()), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
  AND status IN ('PENDING', 'RUNNING')
  AND next_attempt_at <= clock_timestamp()
  AND (status <> 'RUNNING' OR lease_until <= clock_timestamp())
  AND attempt < $7
RETURNING lease_until, attempt`, stored.tenantID, stored.appID, stored.requestID, request.Owner, token, intervalLiteral(request.LeaseDuration), maxExecutionAttempts).Scan(&leaseUntil, &stored.attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		// The eligibility predicate is evaluated with the database clock. Keep
		// the dispatch pending if the lease/attempt window changed between the
		// locked read and this UPDATE; the caller will retry with backoff.
		return queue.Claim{}, false, errors.New("claim execution is not eligible")
	}
	if err != nil {
		return queue.Claim{}, false, fmt.Errorf("claim execution: %w", err)
	}
	if err := consumeDispatch(ctx, tx, dispatch); err != nil {
		return queue.Claim{}, false, err
	}
	job, err := stored.executionJob()
	if err != nil {
		return queue.Claim{}, false, err
	}
	claim := queue.Claim{
		Job: job, TurnSeq: stored.turnSeq, Attempt: stored.attempt,
		FinalAttempt: stored.attempt >= maxExecutionAttempts,
		Lease:        queue.Lease{Owner: request.Owner, Token: token, Until: leaseUntil.UTC()},
	}
	if err := claim.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return queue.Claim{}, false, fmt.Errorf("commit execution claim: %w", err)
	}
	if s.metrics != nil && !stored.createdAt.IsZero() {
		lag := time.Since(stored.createdAt)
		if lag < 0 {
			lag = 0
		}
		s.metrics.RecordQueueLag(ctx, platformmetrics.Labels{
			TenantID:      stored.tenantID,
			AppID:         stored.appID,
			ConfigVersion: stored.configVersion,
		}, lag)
	}
	return claim, true, nil
}

// Renew extends one active run token. A lost or expired claim cannot be renewed.
func (s *Store) Renew(ctx context.Context, claim queue.Claim, duration time.Duration) (queue.Lease, error) {
	if err := s.validate(); err != nil {
		return queue.Lease{}, err
	}
	if err := claim.Validate(); err != nil {
		return queue.Lease{}, err
	}
	if duration <= 0 {
		return queue.Lease{}, errors.New("lease duration must be positive")
	}
	var until time.Time
	err := s.pool.QueryRow(ctx, `UPDATE platform.execution SET lease_until = clock_timestamp() + $6::interval, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'
  AND lease_owner = $4 AND run_token = $5 AND lease_until > clock_timestamp()
RETURNING lease_until`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token, intervalLiteral(duration)).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return queue.Lease{}, fmt.Errorf("renew execution: %w", queue.ErrLeaseLost)
	}
	if err != nil {
		return queue.Lease{}, fmt.Errorf("renew execution: %w", err)
	}
	return queue.Lease{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Until: until.UTC()}, nil
}

// Complete records a terminal result only when the caller still owns its run token.
func (s *Store) Complete(ctx context.Context, claim queue.Claim, status queue.CompletionStatus) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	if err := status.Validate(); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin complete execution: %w", err)
	}
	defer func() { rollback(tx) }()
	tag, err := tx.Exec(ctx, `UPDATE platform.execution SET status = $6, lease_owner = NULL, run_token = NULL,
lease_until = NULL, finished_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'
  AND lease_owner = $4 AND run_token = $5 AND lease_until > clock_timestamp()`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token, status)
	if err != nil {
		return fmt.Errorf("complete execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		var currentStatus string
		if err := tx.QueryRow(ctx, `SELECT status FROM platform.execution WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID()).Scan(&currentStatus); err == nil && isTerminalExecutionStatus(currentStatus) {
			if err := releaseExecutionQuotaTx(ctx, tx, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID()); err != nil {
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit terminal quota cleanup: %w", err)
			}
		}
		return fmt.Errorf("complete execution: %w", queue.ErrLeaseLost)
	}
	if err := releaseExecutionQuotaTx(ctx, tx, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID()); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit complete execution: %w", err)
	}
	return nil
}

// CompleteUncertain records that an external side effect may have happened.
// The durable status prevents idempotent re-admission and lease recovery from
// starting another whole-agent attempt.
func (s *Store) CompleteUncertain(ctx context.Context, claim queue.Claim, cause error) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	lastError := platformlog.SafeError(cause)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin complete uncertain execution: %w", err)
	}
	defer func() { rollback(tx) }()
	tag, err := tx.Exec(ctx, `UPDATE platform.execution
SET status = 'UNCERTAIN', last_error = $6, lease_owner = NULL, run_token = NULL,
    lease_until = NULL, finished_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'
  AND lease_owner = $4 AND run_token = $5 AND lease_until > clock_timestamp()`,
		claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(),
		claim.Lease.Owner, claim.Lease.Token, lastError)
	if err != nil {
		return fmt.Errorf("complete uncertain execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		var currentStatus string
		if err := tx.QueryRow(ctx, `SELECT status FROM platform.execution WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID()).Scan(&currentStatus); err == nil && isTerminalExecutionStatus(currentStatus) {
			if err := releaseExecutionQuotaTx(ctx, tx, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID()); err != nil {
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit terminal quota cleanup: %w", err)
			}
		}
		return fmt.Errorf("complete uncertain execution: %w", queue.ErrLeaseLost)
	}
	if err := releaseExecutionQuotaTx(ctx, tx, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID()); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit complete uncertain execution: %w", err)
	}
	return nil
}

// WaitForApproval parks a running execution until the exact approval record is
// decided. If an administrator decided concurrently, it requeues the
// execution in the same transaction so the next attempt observes that result.
func (s *Store) WaitForApproval(ctx context.Context, claim queue.Claim, approvalID string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	if approvalID == "" {
		return errors.New("approval_id is required")
	}
	jobTenant := claim.Job.Tenant()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin approval wait: %w", err)
	}
	defer func() { rollback(tx) }()

	var requestID, sessionID, configVersion, status string
	var expired bool
	err = tx.QueryRow(ctx, `
SELECT request_id, session_id, config_version, status, expires_at <= clock_timestamp()
FROM platform.tool_approval
WHERE tenant_id = $1 AND app_id = $2 AND approval_id = $3
FOR UPDATE`, jobTenant.TenantID, jobTenant.AppID, approvalID).Scan(
		&requestID, &sessionID, &configVersion, &status, &expired,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("approval was not found")
	}
	if err != nil {
		return fmt.Errorf("lock approval for wait: %w", err)
	}
	if requestID != claim.Job.RequestID() || sessionID != jobTenant.SessionID || configVersion != jobTenant.ConfigVersion {
		return errors.New("approval does not match execution scope")
	}

	if status == "PENDING" && expired {
		if _, err := tx.Exec(ctx, `UPDATE platform.tool_approval
SET status='EXPIRED', decided_at=COALESCE(decided_at, clock_timestamp())
WHERE tenant_id=$1 AND app_id=$2 AND approval_id=$3 AND status='PENDING'`, jobTenant.TenantID, jobTenant.AppID, approvalID); err != nil {
			return fmt.Errorf("expire approval while waiting: %w", err)
		}
		status = "EXPIRED"
	}

	switch status {
	case "PENDING":
		tag, err := tx.Exec(ctx, `UPDATE platform.execution
SET status='WAITING_APPROVAL', last_error='', lease_owner=NULL, run_token=NULL,
    lease_until=NULL, updated_at=clock_timestamp()
WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3 AND status='RUNNING'
  AND lease_owner=$4 AND run_token=$5 AND lease_until > clock_timestamp()`,
			jobTenant.TenantID, jobTenant.AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token)
		if err != nil {
			return fmt.Errorf("park execution for approval: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("park execution for approval: %w", queue.ErrLeaseLost)
		}
	case "APPROVED", "DENIED", "EXPIRED":
		if err := requeueApprovalExecution(ctx, tx, jobTenant.TenantID, jobTenant.AppID, claim.Job.RequestID(), claim.Lease); err != nil {
			return err
		}
	default:
		return fmt.Errorf("approval status %q is invalid", status)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit approval wait: %w", err)
	}
	return nil
}

// Retry returns a failed run to PENDING and creates its next dispatch in the
// same transaction. It terminally fails after a bounded retry count.
func (s *Store) Retry(ctx context.Context, claim queue.Claim, cause error) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin execution retry: %w", err)
	}
	defer func() { rollback(tx) }()
	var attempt int
	retryScheduled := false
	err = tx.QueryRow(ctx, `SELECT attempt FROM platform.execution WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3 AND status='RUNNING' AND lease_owner=$4 AND run_token=$5 AND lease_until > clock_timestamp() FOR UPDATE`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("retry execution: %w", queue.ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock execution retry: %w", err)
	}
	if attempt >= maxExecutionAttempts {
		_, err = tx.Exec(ctx, `UPDATE platform.execution SET status='FAILED', last_error=$4, lease_owner=NULL, run_token=NULL, lease_until=NULL, finished_at=clock_timestamp(), updated_at=clock_timestamp() WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), platformlog.SafeError(cause))
	} else {
		delay := executionRetryDelay(attempt)
		retryScheduled = true
		_, err = tx.Exec(ctx, `UPDATE platform.execution SET status='PENDING', last_error=$4, lease_owner=NULL, run_token=NULL, lease_until=NULL, next_attempt_at=clock_timestamp()+$5::interval, updated_at=clock_timestamp() WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), platformlog.SafeError(cause), intervalLiteral(delay))
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id, app_id, request_id, next_attempt_at) VALUES ($1,$2,$3,clock_timestamp()+$4::interval)`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), intervalLiteral(delay))
		}
	}
	if err != nil {
		return fmt.Errorf("retry execution: %w", err)
	}
	if !retryScheduled {
		if err := releaseExecutionQuotaTx(ctx, tx, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID()); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit execution retry: %w", err)
	}
	if retryScheduled && s.metrics != nil {
		s.metrics.RecordRetry(ctx, platformmetrics.Labels{
			TenantID:      claim.Job.Tenant().TenantID,
			AppID:         claim.Job.Tenant().AppID,
			ConfigVersion: claim.Job.Tenant().ConfigVersion,
			Channel:       claim.Job.Tenant().Channel,
		}, "execution_retry")
	}
	return nil
}

func executionRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := executionRetryInitial
	for attempt > 1 && delay < executionRetryMax {
		delay *= 2
		attempt--
	}
	if delay > executionRetryMax {
		delay = executionRetryMax
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// ClaimDispatches leases pending outbox rows for a relay process.
func (s *Store) ClaimDispatches(ctx context.Context, owner string, leaseDuration time.Duration, limit int) ([]queue.Dispatch, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("dispatcher owner is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("dispatch lease duration must be positive")
	}
	if limit <= 0 {
		limit = defaultDispatchBatch
	}
	rows, err := s.pool.Query(ctx, `WITH candidates AS (
 SELECT o.outbox_id, e.trace_parent, e.trace_state
 FROM platform.dispatch_outbox o JOIN platform.execution e USING (tenant_id,app_id,request_id)
 WHERE o.status='PENDING' AND o.next_attempt_at <= clock_timestamp()
   AND e.status='PENDING' AND e.attempt < $4
 ORDER BY o.outbox_id FOR UPDATE SKIP LOCKED LIMIT $3
) UPDATE platform.dispatch_outbox o SET status='PUBLISHING', lease_owner=$1, lease_until=clock_timestamp()+$2::interval, attempt=attempt+1, updated_at=clock_timestamp()
FROM candidates c WHERE o.outbox_id=c.outbox_id
RETURNING o.outbox_id,o.tenant_id,o.app_id,o.request_id,c.trace_parent,c.trace_state`, owner, intervalLiteral(leaseDuration), limit, maxExecutionAttempts)
	if err != nil {
		return nil, fmt.Errorf("claim dispatch outbox: %w", err)
	}
	defer rows.Close()
	var result []queue.Dispatch
	for rows.Next() {
		var d queue.Dispatch
		if err := rows.Scan(&d.OutboxID, &d.TenantID, &d.AppID, &d.RequestID, &d.TraceParent, &d.TraceState); err != nil {
			return nil, fmt.Errorf("scan dispatch outbox: %w", err)
		}
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dispatch outbox: %w", err)
	}
	return result, nil
}

// CompleteDispatch marks a successfully published stream message as sent.
func (s *Store) CompleteDispatch(ctx context.Context, dispatch queue.Dispatch, owner string) error {
	if err := dispatch.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='SENT', lease_owner=NULL, lease_until=NULL, updated_at=clock_timestamp() WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4 AND status='PUBLISHING' AND lease_owner=$5 AND lease_until > clock_timestamp()`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID, owner)
	if err != nil {
		return fmt.Errorf("complete dispatch: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = s.pool.QueryRow(ctx, `SELECT status FROM platform.dispatch_outbox
WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("complete dispatch: %w", queue.ErrLeaseLost)
		}
		return fmt.Errorf("read dispatch completion: %w", err)
	}
	if status != "CONSUMED" {
		return fmt.Errorf("complete dispatch: %w", queue.ErrLeaseLost)
	}
	return nil
}

// RetryDispatch releases a relay claim so another publisher can retry it with
// bounded exponential backoff and jitter. attempt is incremented while the
// row is leased by ClaimDispatches, so no second retry state is required.
func (s *Store) RetryDispatch(ctx context.Context, dispatch queue.Dispatch, owner string, cause error) error {
	if err := dispatch.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='PENDING', lease_owner=NULL, lease_until=NULL, last_error=$6,
next_attempt_at=clock_timestamp()+interval '1 second' * (LEAST(30::double precision, power(2::double precision, LEAST(attempt - 1, 5))) * (0.5 + random())),
updated_at=clock_timestamp() WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4 AND status='PUBLISHING' AND lease_owner=$5`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID, owner, platformlog.SafeError(cause))
	if err != nil {
		return fmt.Errorf("retry dispatch: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("retry dispatch: %w", queue.ErrLeaseLost)
	}
	return nil
}

// RecoverDispatches makes expired execution and relay leases dispatchable again.
func (s *Store) RecoverDispatches(ctx context.Context, staleAfter time.Duration) error {
	if err := s.validate(); err != nil {
		return err
	}
	if staleAfter <= 0 {
		return errors.New("dispatch stale duration must be positive")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin dispatch recovery: %w", err)
	}
	defer func() { rollback(tx) }()
	if _, err = tx.Exec(ctx, `UPDATE platform.execution
SET status='UNCERTAIN',
    last_error='execution lease expired before completion; external side effect status is unknown',
    lease_owner=NULL, run_token=NULL, lease_until=NULL,
    finished_at=clock_timestamp(), updated_at=clock_timestamp()
WHERE status='RUNNING' AND lease_until <= clock_timestamp() AND attempt >= $1`, maxExecutionAttempts); err != nil {
		return fmt.Errorf("recover expired executions: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE platform.execution SET status='PENDING', lease_owner=NULL, run_token=NULL, lease_until=NULL, next_attempt_at=clock_timestamp(), updated_at=clock_timestamp() WHERE status='RUNNING' AND lease_until <= clock_timestamp() AND attempt < $1`, maxExecutionAttempts); err != nil {
		return fmt.Errorf("requeue recoverable executions: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE platform.dispatch_outbox o SET status='PENDING', lease_owner=NULL, lease_until=NULL, next_attempt_at=clock_timestamp(), updated_at=clock_timestamp()
WHERE ((o.status='PUBLISHING' AND o.lease_until <= clock_timestamp())
    OR (o.status='SENT' AND o.updated_at <= clock_timestamp()-$1::interval))
  AND EXISTS (SELECT 1 FROM platform.execution e
              WHERE e.tenant_id=o.tenant_id AND e.app_id=o.app_id
                AND e.request_id=o.request_id AND e.status='PENDING')`, intervalLiteral(staleAfter)); err != nil {
		return fmt.Errorf("recover dispatch outbox: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM platform.dispatch_outbox WHERE status='CONSUMED' AND updated_at <= clock_timestamp() - $1::interval`, intervalLiteral(consumedOutboxRetention)); err != nil {
		return fmt.Errorf("clean consumed dispatch outbox: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id,app_id,request_id)
SELECT e.tenant_id,e.app_id,e.request_id
FROM platform.execution e
JOIN platform.tenant t ON t.tenant_id=e.tenant_id
JOIN platform.agent_app a ON a.tenant_id=e.tenant_id AND a.app_id=e.app_id
WHERE e.status='PENDING' AND t.status='ACTIVE' AND a.status='ACTIVE'
  AND NOT EXISTS (SELECT 1 FROM platform.dispatch_outbox o WHERE o.tenant_id=e.tenant_id AND o.app_id=e.app_id AND o.request_id=e.request_id AND o.status IN ('PENDING','PUBLISHING','SENT'))`); err != nil {
		return fmt.Errorf("fill missing dispatch outbox: %w", err)
	}
	if err = releaseTerminalQuotaReservationsTx(ctx, tx); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dispatch recovery: %w", err)
	}
	return nil
}

type storedExecution struct {
	tenantID, appID, requestID, sessionPrincipalID, sessionID, userID string
	turnSeq                                                           int64
	configVersion                                                     string
	tenantSource                                                      gateway.TenantSource
	command                                                           []byte
	createdAt                                                         time.Time
	traceID, traceParent, traceState, status                          string
	attempt                                                           int
	nextAttemptAt, leaseUntil                                         time.Time
	leaseActive, attemptReady                                         bool
	hasEarlier                                                        bool
}

func lockExecutionForClaim(ctx context.Context, tx pgx.Tx, d queue.Dispatch) (storedExecution, bool, error) {
	var v storedExecution
	err := tx.QueryRow(ctx, `SELECT e.tenant_id,e.app_id,e.request_id,e.session_principal_id,e.session_id,e.user_id,e.turn_seq,e.config_version,e.tenant_source,e.command,e.created_at,e.trace_id,e.trace_parent,e.trace_state,e.status,e.attempt,e.next_attempt_at,COALESCE(e.lease_until,'epoch'::timestamptz),COALESCE(e.lease_until,'epoch'::timestamptz) > clock_timestamp(),e.next_attempt_at <= clock_timestamp(),EXISTS(SELECT 1 FROM platform.execution x WHERE x.tenant_id=e.tenant_id AND x.app_id=e.app_id AND x.session_principal_id=e.session_principal_id AND x.session_id=e.session_id AND x.turn_seq<e.turn_seq AND x.status IN ('PENDING','RUNNING','WAITING_APPROVAL')) FROM platform.execution e JOIN platform.tenant t ON t.tenant_id=e.tenant_id JOIN platform.agent_app a ON a.tenant_id=e.tenant_id AND a.app_id=e.app_id WHERE e.tenant_id=$1 AND e.app_id=$2 AND e.request_id=$3 FOR UPDATE OF e`, d.TenantID, d.AppID, d.RequestID).Scan(&v.tenantID, &v.appID, &v.requestID, &v.sessionPrincipalID, &v.sessionID, &v.userID, &v.turnSeq, &v.configVersion, &v.tenantSource, &v.command, &v.createdAt, &v.traceID, &v.traceParent, &v.traceState, &v.status, &v.attempt, &v.nextAttemptAt, &v.leaseUntil, &v.leaseActive, &v.attemptReady, &v.hasEarlier)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return v, false, fmt.Errorf("lock dispatched execution: %w", err)
	}
	var active bool
	err = tx.QueryRow(ctx, `SELECT t.status='ACTIVE' AND a.status='ACTIVE' FROM platform.tenant t JOIN platform.agent_app a ON a.tenant_id=t.tenant_id WHERE t.tenant_id=$1 AND a.app_id=$2`, d.TenantID, d.AppID).Scan(&active)
	if err != nil {
		return v, false, fmt.Errorf("check execution availability: %w", err)
	}
	return v, active, nil
}

func validateDispatchTraceContext(dispatch queue.Dispatch, stored storedExecution) error {
	if dispatch.TraceParent != "" && dispatch.TraceParent != stored.traceParent {
		return errors.New("dispatch traceparent does not match stored execution")
	}
	if dispatch.TraceState != "" && dispatch.TraceState != stored.traceState {
		return errors.New("dispatch tracestate does not match stored execution")
	}
	return nil
}

func (v storedExecution) executionJob() (execution.Job, error) {
	var c admissionCommand
	if err := json.Unmarshal(v.command, &c); err != nil {
		return execution.Job{}, fmt.Errorf("unmarshal execution command: %w", err)
	}
	if c.TenantID != v.tenantID || c.AppID != v.appID || c.SessionPrincipalID != v.sessionPrincipalID || c.SessionID != v.sessionID || c.UserID != v.userID {
		return execution.Job{}, errors.New("execution command does not match stored scope")
	}
	return execution.NewJob(v.requestID, v.tenantSource, tenant.RuntimeContext{
		TenantID:           v.tenantID,
		AppID:              v.appID,
		ConfigVersion:      v.configVersion,
		Channel:            c.Channel,
		BindingID:          c.BindingID,
		BindingRevision:    c.BindingRevision,
		SessionPrincipalID: v.sessionPrincipalID,
		SessionID:          v.sessionID,
		UserID:             v.userID,
		TraceID:            v.traceID,
		TraceParent:        v.traceParent,
		TraceState:         v.traceState,
	}, gateway.Message{
		Text:         c.Text,
		ArtifactRefs: c.ArtifactRefs,
	})
}
func consumeDispatch(ctx context.Context, tx pgx.Tx, dispatch queue.Dispatch) error {
	if err := dispatch.Validate(); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='CONSUMED',lease_owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID)
	if err != nil {
		return fmt.Errorf("consume dispatch: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("consume dispatch: outbox identity does not match dispatch")
	}
	return nil
}
func deferDispatch(ctx context.Context, tx pgx.Tx, d queue.Dispatch, next time.Time) error {
	if err := consumeDispatch(ctx, tx, d); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id,app_id,request_id,next_attempt_at)
VALUES ($1,$2,$3,CASE WHEN $4 > clock_timestamp() THEN $4 ELSE clock_timestamp()+$5::interval END)`, d.TenantID, d.AppID, d.RequestID, next, intervalLiteral(blockedDispatchDelay))
	if err != nil {
		return fmt.Errorf("defer dispatch: %w", err)
	}
	return nil
}
func intervalLiteral(v time.Duration) string { return fmt.Sprintf("%d microseconds", v.Microseconds()) }
