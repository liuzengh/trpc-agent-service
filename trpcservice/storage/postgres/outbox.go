package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type OutboxRepositoryConfig struct {
	LockDuration time.Duration
	MaxBatchSize int
}

func (c OutboxRepositoryConfig) withDefaults() (OutboxRepositoryConfig, error) {
	if c.LockDuration == 0 {
		c.LockDuration = storage.DefaultOutboxLockDuration
	}
	if c.MaxBatchSize == 0 {
		c.MaxBatchSize = storage.DefaultOutboxBatchSize
	}
	if c.LockDuration < time.Microsecond || c.MaxBatchSize < 1 || c.MaxBatchSize > storage.DefaultOutboxBatchSize {
		return OutboxRepositoryConfig{}, fmt.Errorf("postgres: invalid outbox repository configuration")
	}
	return c, nil
}

type OutboxRepository struct {
	pool         *pgxpool.Pool
	lockDuration time.Duration
	maxBatchSize int

	// These hooks are private test seams for proving rollback without exposing
	// failure injection as part of the production repository API.
	beforeCommit    func() error
	beforeDLQInsert func() error
	afterDLQInsert  func() error
}

var _ storage.OutboxRepository = (*OutboxRepository)(nil)

type OutboxError struct {
	Operation string
	err       error
}

func (e *OutboxError) Error() string {
	if e == nil {
		return "postgres: outbox operation failed"
	}
	return fmt.Sprintf("postgres: outbox %s: %s", e.Operation, outboxErrorCategory(e.err))
}

func (e *OutboxError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func outboxErrorCategory(err error) string {
	switch {
	case errors.Is(err, storage.ErrTransactionOutcomeUnknown):
		return "transaction outcome unknown"
	case errors.Is(err, storage.ErrBackendUnavailable):
		return "backend unavailable"
	case errors.Is(err, storage.ErrDedupConflict):
		return "deduplication conflict"
	case errors.Is(err, storage.ErrConflict):
		return "conflict"
	default:
		return "operation failed"
	}
}

func NewOutboxRepository(pool *pgxpool.Pool, config OutboxRepositoryConfig) (*OutboxRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres: outbox pool is required")
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &OutboxRepository{pool: pool, lockDuration: config.LockDuration, maxBatchSize: config.MaxBatchSize}, nil
}

func (r *OutboxRepository) Enqueue(ctx context.Context, tc tenant.TenantContext, value storage.OutboxMessage) error {
	if err := validateOutboxContext(ctx, tc); err != nil {
		return err
	}
	if err := storage.ValidateOutboxMessage(value); err != nil {
		return err
	}
	if value.TenantID != tc.TenantID {
		return storage.ErrTenantMismatch
	}
	if value.ConfigVersion != 0 && value.ConfigVersion != tc.ConfigVersion {
		return storage.ErrTenantMismatch
	}
	payload := value.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	} else {
		payload = append([]byte(nil), payload...)
	}
	var nextAttempt any
	if !value.NextAttempt.IsZero() {
		nextAttempt = value.NextAttempt.UTC()
	}
	var dedupKey any
	if value.DedupKey != "" {
		dedupKey = value.DedupKey
	}
	// P1-08 additive: the reply outbox retains the job's immutable config
	// version. Zero (legacy or unknown) is stored as NULL and decodes as 0.
	configVersion := value.ConfigVersion
	if configVersion == 0 {
		configVersion = tc.ConfigVersion
	}
	var configVersionAny any
	if configVersion > 0 {
		configVersionAny = configVersion
	}

	tx, err := r.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := tx.Exec(ctx, `
INSERT INTO outbox_message (
    tenant_id, outbox_id, kind, aggregate_id, dedup_key, payload,
    status, attempt, next_attempt_at, created_at, updated_at, config_version
) VALUES ($1, $2, $3, $4, $5, $6, 'pending', 1,
          COALESCE($7::timestamptz, clock_timestamp()),
          clock_timestamp(), clock_timestamp(), $8)
ON CONFLICT DO NOTHING`,
		value.TenantID, value.ID, value.Kind, value.AggregateID, dedupKey, payload, nextAttempt, configVersionAny)
	if err != nil {
		return outboxDBError("enqueue", err)
	}
	if result.RowsAffected() == 0 {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM outbox_message WHERE tenant_id=$1 AND outbox_id=$2 FOR UPDATE)`, value.TenantID, value.ID).Scan(&exists); err != nil {
			return outboxDBError("inspect enqueue conflict", err)
		}
		if exists {
			return storage.ErrConflict
		}
		if value.DedupKey != "" {
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM outbox_message WHERE tenant_id=$1 AND dedup_key=$2 FOR UPDATE)`, value.TenantID, value.DedupKey).Scan(&exists); err != nil {
				return outboxDBError("inspect dedup conflict", err)
			}
			if exists {
				return fmt.Errorf("%w: %w", storage.ErrDedupConflict, storage.ErrConflict)
			}
		}
		return storage.ErrConflict
	}
	if err := r.runCommitHook("enqueue"); err != nil {
		return err
	}
	return r.commit(ctx, tx, "enqueue")
}

func (r *OutboxRepository) Get(ctx context.Context, tc tenant.TenantContext, id string) (storage.OutboxMessage, error) {
	if err := validateOutboxContext(ctx, tc); err != nil {
		return storage.OutboxMessage{}, err
	}
	if err := validateOutboxID(id); err != nil {
		return storage.OutboxMessage{}, err
	}
	var value storage.OutboxMessage
	var status string
	var dedupKey, lockedBy, lastError *string
	var lockedUntil *time.Time
	var payload []byte
	err := r.pool.QueryRow(ctx, `
SELECT tenant_id, outbox_id, kind, aggregate_id, dedup_key, payload, status,
       attempt, next_attempt_at, locked_by, locked_until, last_error,
       created_at, updated_at
FROM outbox_message
WHERE tenant_id=$1 AND outbox_id=$2`, tc.TenantID, id).Scan(
		&value.TenantID, &value.ID, &value.Kind, &value.AggregateID, &dedupKey, &payload, &status,
		&value.Attempt, &value.NextAttempt, &lockedBy, &lockedUntil, &lastError,
		&value.CreatedAt, &value.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.OutboxMessage{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.OutboxMessage{}, outboxDBError("read", err)
	}
	value.DedupKey = optionalString(dedupKey)
	value.Payload = append([]byte(nil), payload...)
	value.Status = storage.OutboxStatus(status)
	value.LockedBy = optionalString(lockedBy)
	if lockedUntil != nil {
		value.LockedUntil = *lockedUntil
	}
	value.LastError = optionalString(lastError)
	return value, nil
}

func (r *OutboxRepository) ClaimBatch(ctx context.Context, tc tenant.TenantContext, workerID string, limit int) ([]storage.OutboxMessage, error) {
	if err := validateOutboxContext(ctx, tc); err != nil {
		return nil, err
	}
	if err := validateOutboxID(workerID); err != nil {
		return nil, err
	}
	if limit < 1 || limit > r.maxBatchSize {
		return nil, storage.ErrInvalidArgument
	}
	tx, err := r.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `
UPDATE outbox_message
SET status='retry', next_attempt_at=clock_timestamp(), attempt=attempt+1,
    locked_by=NULL, locked_until=NULL, updated_at=clock_timestamp()
WHERE tenant_id=$1 AND status='processing' AND locked_until <= clock_timestamp()`, tc.TenantID); err != nil {
		return nil, outboxDBError("reclaim expired locks", err)
	}
	rows, err := tx.Query(ctx, `
WITH candidates AS (
    SELECT outbox_id
    FROM outbox_message
    WHERE tenant_id=$1
      AND status IN ('pending', 'retry')
      AND next_attempt_at <= clock_timestamp()
    ORDER BY next_attempt_at, created_at, outbox_id
    FOR UPDATE SKIP LOCKED
    LIMIT $2
)
UPDATE outbox_message AS message
SET status='processing', locked_by=$3,
    locked_until=clock_timestamp() + ($4::double precision * interval '1 microsecond'),
    updated_at=clock_timestamp()
FROM candidates
WHERE message.tenant_id=$1 AND message.outbox_id=candidates.outbox_id
RETURNING message.tenant_id, message.outbox_id, message.kind, message.aggregate_id,
          message.dedup_key, message.payload, message.status, message.attempt,
          message.next_attempt_at, message.locked_by, message.locked_until,
          message.last_error, message.created_at, message.updated_at,
          message.config_version`,
		tc.TenantID, limit, workerID, durationMicros(r.lockDuration))
	if err != nil {
		return nil, outboxDBError("claim batch", err)
	}
	defer rows.Close()
	messages := make([]storage.OutboxMessage, 0, limit)
	for rows.Next() {
		value, scanErr := scanOutboxMessage(rows)
		if scanErr != nil {
			return nil, outboxDBError("scan claim batch", scanErr)
		}
		messages = append(messages, value)
	}
	if err := rows.Err(); err != nil {
		return nil, outboxDBError("read claim batch", err)
	}
	if err := r.commit(ctx, tx, "claim batch"); err != nil {
		return nil, err
	}
	return messages, nil
}

func (r *OutboxRepository) MarkCompleted(ctx context.Context, tc tenant.TenantContext, workerID, id string) error {
	if err := validateOutboxMutation(ctx, tc, workerID, id); err != nil {
		return err
	}
	tx, err := r.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := tx.Exec(ctx, `
UPDATE outbox_message
SET status='completed', locked_by=NULL, locked_until=NULL, updated_at=clock_timestamp()
WHERE tenant_id=$1 AND outbox_id=$2 AND status='processing'
  AND locked_by=$3 AND locked_until > clock_timestamp()`, tc.TenantID, id, workerID)
	if err != nil {
		return outboxDBError("complete", err)
	}
	if result.RowsAffected() != 1 {
		if _, err := r.lockAndValidate(ctx, tx, tc.TenantID, id, workerID); err != nil {
			return err
		}
		return storage.ErrConflict
	}
	if err := r.runCommitHook("complete"); err != nil {
		return err
	}
	return r.commit(ctx, tx, "complete")
}

func (r *OutboxRepository) MarkRetry(ctx context.Context, tc tenant.TenantContext, workerID, id string, nextAttempt time.Time, errorType string) error {
	if err := validateOutboxMutation(ctx, tc, workerID, id); err != nil {
		return err
	}
	if nextAttempt.IsZero() {
		return storage.ErrInvalidArgument
	}
	if err := storage.ValidateOutboxFailureCode(errorType); err != nil {
		return err
	}
	tx, err := r.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := tx.Exec(ctx, `
UPDATE outbox_message
SET status='retry', next_attempt_at=$4, attempt=attempt+1, last_error=$5,
    locked_by=NULL, locked_until=NULL, updated_at=clock_timestamp()
WHERE tenant_id=$1 AND outbox_id=$2 AND status='processing'
  AND locked_by=$3 AND locked_until > clock_timestamp()`, tc.TenantID, id, workerID, nextAttempt.UTC(), errorType)
	if err != nil {
		return outboxDBError("retry", err)
	}
	if result.RowsAffected() != 1 {
		if _, err := r.lockAndValidate(ctx, tx, tc.TenantID, id, workerID); err != nil {
			return err
		}
		return storage.ErrConflict
	}
	if err := r.runCommitHook("retry"); err != nil {
		return err
	}
	return r.commit(ctx, tx, "retry")
}

func (r *OutboxRepository) MoveToDLQ(ctx context.Context, tc tenant.TenantContext, workerID, id, reason string) error {
	if err := validateOutboxMutation(ctx, tc, workerID, id); err != nil {
		return err
	}
	if err := storage.ValidateOutboxFailureCode(reason); err != nil {
		return err
	}
	tx, err := r.beginTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	value, err := r.lockAndValidate(ctx, tx, tc.TenantID, id, workerID)
	if err != nil {
		return err
	}
	if r.beforeDLQInsert != nil {
		if err := r.beforeDLQInsert(); err != nil {
			return outboxDBError("dlq insert hook", err)
		}
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO dead_letter (
    tenant_id, dead_letter_id, outbox_id, kind, payload, attempt, reason,
    last_error, failed_at, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, clock_timestamp(), clock_timestamp())
ON CONFLICT (tenant_id, outbox_id) DO NOTHING`,
		value.TenantID, dlqID(value.ID), value.ID, value.Kind, value.Payload, value.Attempt, reason); err != nil {
		return outboxDBError("dlq insert", err)
	}
	if r.afterDLQInsert != nil {
		if err := r.afterDLQInsert(); err != nil {
			return outboxDBError("dlq insert hook", err)
		}
	}
	result, err := tx.Exec(ctx, `
UPDATE outbox_message
SET status='dead', last_error=$3, locked_by=NULL, locked_until=NULL,
    updated_at=clock_timestamp()
WHERE tenant_id=$1 AND outbox_id=$2 AND status='processing'
  AND locked_by=$4 AND locked_until > clock_timestamp()`, tc.TenantID, id, reason, workerID)
	if err != nil {
		return outboxDBError("dlq transition", err)
	}
	if result.RowsAffected() != 1 {
		if _, classifyErr := r.lockAndValidate(ctx, tx, tc.TenantID, id, workerID); classifyErr != nil {
			return classifyErr
		}
		return storage.ErrConflict
	}
	if err := r.runCommitHook("dlq"); err != nil {
		return err
	}
	return r.commit(ctx, tx, "dlq")
}

func (r *OutboxRepository) beginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, outboxDBError("begin transaction", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'`); err != nil {
		_ = tx.Rollback(context.Background())
		return nil, outboxDBError("configure transaction", err)
	}
	return tx, nil
}

func (r *OutboxRepository) commit(ctx context.Context, tx pgx.Tx, operation string) error {
	if err := tx.Commit(ctx); err != nil {
		return &OutboxError{Operation: "commit " + operation, err: fmt.Errorf("%w: %w", storage.ErrTransactionOutcomeUnknown, err)}
	}
	return nil
}

func (r *OutboxRepository) runCommitHook(operation string) error {
	if r.beforeCommit == nil {
		return nil
	}
	if err := r.beforeCommit(); err != nil {
		return outboxDBError(operation+" commit hook", err)
	}
	return nil
}

func (r *OutboxRepository) lockAndValidate(ctx context.Context, tx pgx.Tx, tenantID, id, workerID string) (storage.OutboxMessage, error) {
	var value storage.OutboxMessage
	var status string
	var dedupKey, lockedBy, lastError *string
	var lockedUntil *time.Time
	var payload []byte
	var now time.Time
	err := tx.QueryRow(ctx, `
SELECT tenant_id, outbox_id, kind, aggregate_id, dedup_key, payload, status,
       attempt, next_attempt_at, locked_by, locked_until, last_error,
       created_at, updated_at, clock_timestamp()
FROM outbox_message
WHERE tenant_id=$1 AND outbox_id=$2
FOR UPDATE`, tenantID, id).Scan(
		&value.TenantID, &value.ID, &value.Kind, &value.AggregateID, &dedupKey, &payload, &status,
		&value.Attempt, &value.NextAttempt, &lockedBy, &lockedUntil, &lastError,
		&value.CreatedAt, &value.UpdatedAt, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.OutboxMessage{}, storage.ErrNotFound
	}
	if err != nil {
		return storage.OutboxMessage{}, outboxDBError("lock outbox message", err)
	}
	value.DedupKey = optionalString(dedupKey)
	value.Payload = append([]byte(nil), payload...)
	value.Status = storage.OutboxStatus(status)
	value.LockedBy = optionalString(lockedBy)
	if lockedUntil != nil {
		value.LockedUntil = *lockedUntil
	}
	value.LastError = optionalString(lastError)
	if value.Status == storage.OutboxCompleted {
		return value, storage.ErrAlreadyCompleted
	}
	if value.Status == storage.OutboxDead {
		return value, storage.ErrAlreadyDead
	}
	if value.Status != storage.OutboxProcessing {
		return value, storage.ErrConflict
	}
	if value.LockedBy != workerID {
		return value, fmt.Errorf("%w: %w", storage.ErrOutboxLockLost, storage.ErrFenceRejected)
	}
	if !value.LockedUntil.After(now) {
		return value, fmt.Errorf("%w: %w", storage.ErrOutboxLockExpired, storage.ErrLeaseLost)
	}
	return value, nil
}

func scanOutboxMessage(row interface{ Scan(...any) error }) (storage.OutboxMessage, error) {
	var value storage.OutboxMessage
	var status string
	var dedupKey, lockedBy, lastError *string
	var lockedUntil *time.Time
	var payload []byte
	var configVersion *int64
	if err := row.Scan(
		&value.TenantID, &value.ID, &value.Kind, &value.AggregateID, &dedupKey, &payload, &status,
		&value.Attempt, &value.NextAttempt, &lockedBy, &lockedUntil, &lastError,
		&value.CreatedAt, &value.UpdatedAt, &configVersion); err != nil {
		return storage.OutboxMessage{}, err
	}
	if configVersion != nil {
		value.ConfigVersion = *configVersion
	}
	value.DedupKey = optionalString(dedupKey)
	value.Payload = append([]byte(nil), payload...)
	value.Status = storage.OutboxStatus(status)
	value.LockedBy = optionalString(lockedBy)
	if lockedUntil != nil {
		value.LockedUntil = *lockedUntil
	}
	value.LastError = optionalString(lastError)
	return value, nil
}

func validateOutboxContext(ctx context.Context, tc tenant.TenantContext) error {
	if ctx == nil {
		return storage.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tc.Validate(); err != nil {
		return storage.ErrInvalidArgument
	}
	return nil
}

func validateOutboxMutation(ctx context.Context, tc tenant.TenantContext, workerID, id string) error {
	if err := validateOutboxContext(ctx, tc); err != nil {
		return err
	}
	if err := validateOutboxID(workerID); err != nil {
		return err
	}
	return validateOutboxID(id)
}

func validateOutboxID(value string) error {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return storage.ErrInvalidArgument
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return storage.ErrInvalidArgument
		}
	}
	return nil
}

func outboxDBError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &OutboxError{Operation: operation, err: fmt.Errorf("%w: %w", storage.ErrBackendUnavailable, err)}
}

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func durationMicros(value time.Duration) int64 {
	micros := value.Microseconds()
	if value > 0 && micros == 0 {
		return 1
	}
	return micros
}

func dlqID(outboxID string) string { return outboxID }
