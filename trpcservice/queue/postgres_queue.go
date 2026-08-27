package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrQueueBackend            = errors.New("queue: postgres backend failure")
	ErrQueueOperationAmbiguous = errors.New("queue: postgres operation result is ambiguous")
	ErrEnqueueConflict         = errors.New("queue: enqueue conflicts with an existing job")
)

// PostgresQueueConfig controls durable queue validation and receive polling.
type PostgresQueueConfig struct {
	MaxJobAge              time.Duration
	MaxVisibilityExtension time.Duration
	PollInterval           time.Duration
}

func (c PostgresQueueConfig) withDefaults() (PostgresQueueConfig, error) {
	if c.MaxJobAge == 0 {
		c.MaxJobAge = DefaultJobMaxAge
	}
	if c.MaxVisibilityExtension == 0 {
		c.MaxVisibilityExtension = c.MaxJobAge
	}
	if c.PollInterval == 0 {
		c.PollInterval = 50 * time.Millisecond
	}
	if c.MaxJobAge <= 0 || c.MaxVisibilityExtension <= 0 || c.PollInterval <= 0 {
		return PostgresQueueConfig{}, fmt.Errorf("queue: durations must be positive")
	}
	return c, nil
}

// PostgresQueue is the durable implementation of JobQueue. Queue lifecycle is
// process-local; Close stops new operations but never deletes database rows.
type PostgresQueue struct {
	pool                   *pgxpool.Pool
	maxJobAge              time.Duration
	maxVisibilityExtension time.Duration
	pollInterval           time.Duration

	mu     sync.RWMutex
	closed bool
}

var _ JobQueue = (*PostgresQueue)(nil)

func NewPostgresQueue(pool *pgxpool.Pool, config PostgresQueueConfig) (*PostgresQueue, error) {
	if pool == nil {
		return nil, fmt.Errorf("queue: postgres pool is required")
	}
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &PostgresQueue{
		pool:                   pool,
		maxJobAge:              config.MaxJobAge,
		maxVisibilityExtension: config.MaxVisibilityExtension,
		pollInterval:           config.PollInterval,
	}, nil
}

func (q *PostgresQueue) Enqueue(ctx context.Context, job AgentJob) (QueueReceipt, error) {
	if err := contextError(ctx); err != nil {
		return QueueReceipt{}, err
	}
	if q.isClosed() {
		return QueueReceipt{}, ErrQueueClosed
	}
	now := time.Now().UTC()
	if err := job.ValidateAt(now, q.maxJobAge); err != nil {
		return QueueReceipt{}, err
	}
	payload, err := marshalJob(job)
	if err != nil {
		return QueueReceipt{}, err
	}

	tx, err := q.beginTx(ctx)
	if err != nil {
		return QueueReceipt{}, err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `
		INSERT INTO job_queue (
			tenant_id, job_id, execution_id, schema_version, payload,
			status, available_at, attempt, delivery_count
		) VALUES ($1, $2, $3, $4, $5, 'queued', clock_timestamp(), $6, 0)
		ON CONFLICT DO NOTHING`,
		job.Tenant.TenantID, job.JobID, job.ExecutionID, job.SchemaVersion, payload, job.Attempt)
	if err != nil {
		return QueueReceipt{}, queueDBError("enqueue", err)
	}
	if result.RowsAffected() == 0 {
		var existingPayload []byte
		var existingExecutionID string
		var existingSchemaVersion, existingAttempt int
		err = tx.QueryRow(ctx, `
			SELECT payload, execution_id, schema_version, attempt
			FROM job_queue
			WHERE tenant_id = $1 AND job_id = $2
			FOR UPDATE`, job.Tenant.TenantID, job.JobID).Scan(
			&existingPayload, &existingExecutionID, &existingSchemaVersion, &existingAttempt)
		if errors.Is(err, pgx.ErrNoRows) {
			return QueueReceipt{}, ErrEnqueueConflict
		}
		if err != nil {
			return QueueReceipt{}, queueDBError("inspect enqueue conflict", err)
		}
		if existingExecutionID != job.ExecutionID || existingSchemaVersion != job.SchemaVersion || !jsonEquivalent(existingPayload, payload) {
			return QueueReceipt{}, ErrEnqueueConflict
		}
		return QueueReceipt{Accepted: true, ReceiptID: receiptID(job.JobID), JobID: job.JobID, Attempt: existingAttempt}, nil
	}
	if err := commitQueueTx(ctx, tx, "enqueue"); err != nil {
		return QueueReceipt{}, err
	}
	return QueueReceipt{Accepted: true, ReceiptID: receiptID(job.JobID), JobID: job.JobID, Attempt: job.Attempt}, nil
}

func (q *PostgresQueue) Receive(ctx context.Context, visibilityTimeout time.Duration) (Delivery, error) {
	if visibilityTimeout < time.Microsecond {
		return Delivery{}, fmt.Errorf("%w: visibility timeout must be at least one microsecond", ErrInvalidDelivery)
	}
	for {
		if err := contextError(ctx); err != nil {
			return Delivery{}, err
		}
		if q.isClosed() {
			return Delivery{}, ErrQueueClosed
		}
		delivery, found, err := q.claim(ctx, visibilityTimeout)
		if err != nil {
			return Delivery{}, err
		}
		if found {
			return delivery, nil
		}
		timer := time.NewTimer(q.pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return Delivery{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (q *PostgresQueue) claim(ctx context.Context, visibilityTimeout time.Duration) (Delivery, bool, error) {
	tx, err := q.beginTx(ctx)
	if err != nil {
		return Delivery{}, false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `
		UPDATE job_queue
		SET status = 'queued', delivery_id = NULL, leased_until = NULL,
			available_at = clock_timestamp(), attempt = attempt + 1,
			updated_at = clock_timestamp()
		WHERE status = 'in_flight' AND leased_until <= clock_timestamp()`); err != nil {
		return Delivery{}, false, queueDBError("recover expired deliveries", err)
	}

	var row durableJobRow
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, job_id, execution_id, schema_version, payload, attempt
		FROM job_queue
		WHERE status = 'queued' AND available_at <= clock_timestamp()
		ORDER BY available_at, created_at, job_id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`).Scan(
		&row.tenantID, &row.jobID, &row.executionID, &row.schemaVersion, &row.payload, &row.attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := commitQueueTx(ctx, tx, "recover expired deliveries"); err != nil {
			return Delivery{}, false, err
		}
		return Delivery{}, false, nil
	}
	if err != nil {
		return Delivery{}, false, queueDBError("claim candidate", err)
	}
	job, err := q.decodeStoredJob(row)
	if err != nil {
		return Delivery{}, false, err
	}
	deliveryID, err := newDeliveryID()
	if err != nil {
		return Delivery{}, false, err
	}
	var visibleUntil, receivedAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE job_queue
		SET status = 'in_flight', delivery_id = $3,
			leased_until = clock_timestamp() + ($4::double precision * interval '1 microsecond'),
			delivery_count = delivery_count + 1, updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND job_id = $2
		  AND status = 'queued' AND available_at <= clock_timestamp()
		RETURNING leased_until, clock_timestamp()`,
		row.tenantID, row.jobID, deliveryID, durationMicros(visibilityTimeout)).Scan(&visibleUntil, &receivedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Delivery{}, false, fmt.Errorf("%w: claim candidate disappeared", ErrQueueBackend)
	}
	if err != nil {
		return Delivery{}, false, queueDBError("claim candidate", err)
	}
	if err := commitQueueTx(ctx, tx, "claim"); err != nil {
		return Delivery{}, false, err
	}
	job.Attempt = row.attempt
	payload, err := marshalJob(job)
	if err != nil {
		return Delivery{}, false, err
	}
	return Delivery{
		DeliveryID: deliveryID,
		Envelope:   JobEnvelope{SchemaVersion: row.schemaVersion, JobID: row.jobID, Payload: payload},
		Job:        job, Attempt: row.attempt, ReceivedAt: receivedAt, VisibleUntil: visibleUntil,
	}, true, nil
}

type durableJobRow struct {
	tenantID      string
	jobID         string
	executionID   string
	schemaVersion int
	payload       []byte
	attempt       int
}

func (q *PostgresQueue) decodeStoredJob(row durableJobRow) (AgentJob, error) {
	var job AgentJob
	if err := json.Unmarshal(row.payload, &job); err != nil {
		return AgentJob{}, fmt.Errorf("%w: stored payload: %v", ErrInvalidEnvelope, err)
	}
	if job.JobID != row.jobID || job.ExecutionID != row.executionID || job.SchemaVersion != row.schemaVersion {
		return AgentJob{}, fmt.Errorf("%w: stored identity disagrees with queue row", ErrInvalidEnvelope)
	}
	if err := job.ValidateAt(job.CreatedAt.UTC(), q.maxJobAge); err != nil {
		return AgentJob{}, err
	}
	return job, nil
}

func (q *PostgresQueue) Ack(ctx context.Context, delivery Delivery) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	tx, err := q.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	state, err := q.lockDelivery(ctx, tx, delivery)
	if err != nil {
		return err
	}
	if state.lastDeliveryID == delivery.DeliveryID && state.status == "acked" {
		return nil
	}
	if err := state.requireActive(delivery.DeliveryID); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		UPDATE job_queue
		SET status = 'acked', delivery_id = NULL, leased_until = NULL,
			last_delivery_id = $3, acked_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND job_id = $2 AND status = 'in_flight'
		  AND delivery_id = $3 AND leased_until > clock_timestamp()`,
		delivery.Job.Tenant.TenantID, delivery.Job.JobID, delivery.DeliveryID)
	if err != nil {
		return queueDBError("ack", err)
	}
	if result.RowsAffected() != 1 {
		return ErrDeliveryExpired
	}
	return commitQueueTx(ctx, tx, "ack")
}

func (q *PostgresQueue) Nack(ctx context.Context, delivery Delivery, options NackOptions) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := options.Validate(); err != nil {
		return err
	}
	tx, err := q.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	state, err := q.lockDelivery(ctx, tx, delivery)
	if err != nil {
		return err
	}
	if err := state.requireActive(delivery.DeliveryID); err != nil {
		return err
	}
	var result pgconnCommandTag
	if options.Requeue {
		result, err = tx.Exec(ctx, `
			UPDATE job_queue
			SET status = 'queued', delivery_id = NULL, leased_until = NULL,
				available_at = clock_timestamp() + ($4::double precision * interval '1 microsecond'),
				attempt = attempt + 1, last_delivery_id = NULL, updated_at = clock_timestamp()
			WHERE tenant_id = $1 AND job_id = $2 AND status = 'in_flight'
			  AND delivery_id = $3 AND leased_until > clock_timestamp()`,
			delivery.Job.Tenant.TenantID, delivery.Job.JobID, delivery.DeliveryID, durationMicros(options.RetryAfter))
	} else {
		result, err = tx.Exec(ctx, `
			UPDATE job_queue
			SET status = 'discarded', delivery_id = NULL, leased_until = NULL,
				last_delivery_id = NULL, updated_at = clock_timestamp()
			WHERE tenant_id = $1 AND job_id = $2 AND status = 'in_flight'
			  AND delivery_id = $3 AND leased_until > clock_timestamp()`,
			delivery.Job.Tenant.TenantID, delivery.Job.JobID, delivery.DeliveryID)
	}
	if err != nil {
		return queueDBError("nack", err)
	}
	if result.RowsAffected() != 1 {
		return ErrDeliveryExpired
	}
	return commitQueueTx(ctx, tx, "nack")
}

func (q *PostgresQueue) ExtendVisibility(ctx context.Context, delivery Delivery, extension time.Duration) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if extension < time.Microsecond || extension > q.maxVisibilityExtension {
		return fmt.Errorf("%w: invalid visibility extension", ErrInvalidDelivery)
	}
	tx, err := q.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	state, err := q.lockDelivery(ctx, tx, delivery)
	if err != nil {
		return err
	}
	if err := state.requireActive(delivery.DeliveryID); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		UPDATE job_queue
		SET leased_until = clock_timestamp() + ($3::double precision * interval '1 microsecond'),
			updated_at = clock_timestamp()
		WHERE tenant_id = $1 AND job_id = $2 AND status = 'in_flight'
		  AND delivery_id = $4 AND leased_until > clock_timestamp()`,
		delivery.Job.Tenant.TenantID, delivery.Job.JobID, durationMicros(extension), delivery.DeliveryID)
	if err != nil {
		return queueDBError("extend visibility", err)
	}
	if result.RowsAffected() != 1 {
		return ErrDeliveryExpired
	}
	return commitQueueTx(ctx, tx, "extend visibility")
}

func (q *PostgresQueue) Close() error {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	return nil
}

type lockedDeliveryState struct {
	status         string
	deliveryID     string
	lastDeliveryID string
	leasedUntil    time.Time
	now            time.Time
}

func (q *PostgresQueue) lockDelivery(ctx context.Context, tx pgx.Tx, delivery Delivery) (lockedDeliveryState, error) {
	if delivery.DeliveryID == "" || delivery.Job.JobID == "" || delivery.Job.Tenant.TenantID == "" {
		return lockedDeliveryState{}, ErrInvalidDelivery
	}
	var state lockedDeliveryState
	err := tx.QueryRow(ctx, `
		SELECT status, COALESCE(delivery_id, ''), COALESCE(last_delivery_id, ''),
			COALESCE(leased_until, 'epoch'::timestamptz), clock_timestamp()
		FROM job_queue
		WHERE tenant_id = $1 AND job_id = $2
		FOR UPDATE`, delivery.Job.Tenant.TenantID, delivery.Job.JobID).Scan(
		&state.status, &state.deliveryID, &state.lastDeliveryID, &state.leasedUntil, &state.now)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedDeliveryState{}, ErrDeliveryExpired
	}
	if err != nil {
		return lockedDeliveryState{}, queueDBError("lock delivery", err)
	}
	return state, nil
}

func (s lockedDeliveryState) requireActive(deliveryID string) error {
	if s.status != "in_flight" || s.deliveryID != deliveryID {
		return ErrDeliveryFinished
	}
	if !s.leasedUntil.After(s.now) {
		return ErrDeliveryExpired
	}
	return nil
}

func (q *PostgresQueue) beginTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, queueDBError("begin transaction", err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '15s'`); err != nil {
		_ = tx.Rollback(ctx)
		return nil, queueDBError("configure transaction", err)
	}
	return tx, nil
}

func commitQueueTx(ctx context.Context, tx pgx.Tx, operation string) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrQueueOperationAmbiguous, operation, err)
	}
	return nil
}

func queueDBError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", ErrQueueBackend, operation, err)
}

func newDeliveryID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("queue: generate delivery token: %w", err)
	}
	return "delivery-" + hex.EncodeToString(raw[:]), nil
}

func receiptID(jobID string) string { return "receipt-" + jobID }

func durationMicros(value time.Duration) int64 {
	micros := value.Microseconds()
	if value > 0 && micros == 0 {
		return 1
	}
	return micros
}

func jsonEquivalent(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func (q *PostgresQueue) isClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}

// pgconnCommandTag keeps the queue package independent of pgconn's concrete
// command tag while retaining the RowsAffected contract used by pgx.
type pgconnCommandTag interface {
	RowsAffected() int64
}
