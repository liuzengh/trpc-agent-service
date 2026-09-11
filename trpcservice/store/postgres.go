package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/cyl6/trpc-agent-service/trpcservice/dbbinding"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
)

// Postgres is the durable Store backed by Postgres. Concurrency safety relies
// on SELECT ... FOR UPDATE SKIP LOCKED for leasing and lease_owner fencing on
// every transition, which is what makes multi-replica deployments safe.
type Postgres struct {
	pool *pgxpool.Pool
}

var _ interface{ DatabaseIdentity() string } = (*Postgres)(nil)

// durationInterval preserves sub-second lease and retry durations without
// converting through float64. PostgreSQL intervals have microsecond
// precision, so round any non-zero remainder away from zero: a positive
// deadline must never become shorter merely because it was encoded for the
// database.
func durationInterval(d time.Duration) pgtype.Interval {
	microseconds := d / time.Microsecond
	if remainder := d % time.Microsecond; remainder != 0 {
		if d > 0 {
			microseconds++
		} else {
			microseconds--
		}
	}
	return pgtype.Interval{Microseconds: int64(microseconds), Valid: true}
}

// NewPostgres opens a pool and verifies connectivity. dsn comes from the
// environment; it never appears in logs or errors verbatim.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, errors.New("store: open postgres pool failed")
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, errors.New("store: postgres ping failed")
	}
	return &Postgres{pool: pool}, nil
}

// EnsureSchema explicitly applies the runtime migration. It is retained for
// programmatic migration/test compatibility; business service startup uses
// VerifySchema and therefore never performs DDL.
func (p *Postgres) EnsureSchema(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin schema migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := migrations.ApplyAll(ctx, tx); err != nil {
		return fmt.Errorf("store: ensure schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit schema migration: %w", err)
	}
	return nil
}

// VerifySchema is the read-only business-process startup gate. It fails when
// the independent migrator has not applied the exact migration embedded in
// this binary.
func (p *Postgres) VerifySchema(ctx context.Context) error {
	if p == nil || p.pool == nil {
		return errors.New("store: postgres schema verification unavailable")
	}
	if err := migrations.VerifyAll(ctx, p.pool); err != nil {
		return fmt.Errorf("store: verify schema: %w", err)
	}
	return nil
}

// DatabaseIdentity returns a credential-free identity for the configured
// PostgreSQL server, database, and search path. Callers use it only for exact
// transaction-boundary matching; it must not be treated as a secret or logged.
func (p *Postgres) DatabaseIdentity() string {
	if p == nil {
		return ""
	}
	return dbbinding.FromPool(p.pool)
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

func (p *Postgres) InsertInbox(ctx context.Context, rec *InboxRecord, _ time.Time) (err error) {
	ctx, finish := observability.StartStorage(ctx, "inbox.insert", "postgres", rec.TenantID, "")
	defer func() { finish(err) }()
	var insertedID string
	err = p.pool.QueryRow(ctx, `
		INSERT INTO runtime_inbox
			(inbox_id, tenant_id, channel_type, binding_id, external_message_id,
			 dedup_key, partition_key, pipeline_schema_version, atomic_commit_mode,
			 database_identity, payload, trace_carrier, status, next_attempt_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb,
			'received', clock_timestamp())
		ON CONFLICT (dedup_key) DO NOTHING
		RETURNING inbox_id`,
		rec.InboxID, rec.TenantID, rec.ChannelType, rec.BindingID, rec.ExternalMessageID,
		rec.DedupKey, rec.PartitionKey, rec.PipelineSchemaVersion, rec.AtomicCommitMode,
		rec.DatabaseIdentity, rec.Payload,
		traceCarrierJSON(rec.TraceCarrier)).Scan(&insertedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrDuplicate
	}
	return err
}

func traceCarrierJSON(carrier map[string]string) []byte {
	if len(carrier) == 0 {
		return []byte("{}")
	}
	// The map holds W3C trace header values only; marshal cannot fail.
	data, err := json.Marshal(carrier)
	if err != nil {
		return []byte("{}")
	}
	return data
}

func (p *Postgres) LeaseInbox(ctx context.Context, owner string, _ time.Time, leaseTTL time.Duration, limit int) (records []InboxRecord, err error) {
	ctx, finish := observability.StartStorage(ctx, "inbox.lease", "postgres", "", "")
	defer func() { finish(err) }()
	if limit <= 0 {
		limit = 1
	}
	rows, err := p.pool.Query(ctx, `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.inbox_id
			FROM runtime_inbox AS candidate
			WHERE candidate.status IN ('received', 'retry')
				AND candidate.next_attempt_at <= clock_timestamp()
				AND NOT EXISTS (
					SELECT 1
					FROM runtime_inbox AS earlier
					WHERE earlier.tenant_id = candidate.tenant_id
						AND earlier.channel_type = candidate.channel_type
						AND earlier.binding_id = candidate.binding_id
						AND (
							earlier.partition_key = candidate.partition_key
							OR earlier.partition_key = ''
							OR candidate.partition_key = ''
						)
						AND earlier.status IN ('received', 'retry', 'processing')
						AND (earlier.created_at, earlier.queue_sequence) <
							(candidate.created_at, candidate.queue_sequence)
				)
			ORDER BY candidate.next_attempt_at, candidate.created_at, candidate.queue_sequence
			LIMIT $3
			FOR UPDATE OF candidate SKIP LOCKED
		)
		UPDATE runtime_inbox AS target SET
			status = 'processing', lease_owner = $1,
			lease_expires_at = clock_timestamp() + $2::interval,
			attempt_count = target.attempt_count + 1, updated_at = clock_timestamp()
		FROM candidates
		WHERE target.inbox_id = candidates.inbox_id
		RETURNING target.inbox_id, target.tenant_id, target.channel_type, target.binding_id,
			target.external_message_id, target.dedup_key, target.partition_key,
			target.pipeline_schema_version, target.atomic_commit_mode, target.database_identity,
			target.payload, target.trace_carrier, target.attempt_count, target.last_error_type`,
		owner, durationInterval(leaseTTL), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records = nil
	for rows.Next() {
		var rec InboxRecord
		var carrier []byte
		if err := rows.Scan(&rec.InboxID, &rec.TenantID, &rec.ChannelType, &rec.BindingID,
			&rec.ExternalMessageID, &rec.DedupKey, &rec.PartitionKey,
			&rec.PipelineSchemaVersion, &rec.AtomicCommitMode, &rec.DatabaseIdentity,
			&rec.Payload, &carrier,
			&rec.AttemptCount, &rec.LastErrorType); err != nil {
			return nil, err
		}
		rec.TraceCarrier = decodeCarrier(carrier)
		rec.Status = InboxProcessing
		records = append(records, rec)
	}
	return records, rows.Err()
}

func (p *Postgres) RenewInboxLease(ctx context.Context, inboxID, owner string, _ time.Time, leaseTTL time.Duration) error {
	return p.withInboxLease(ctx, inboxID, owner, func(tx pgx.Tx) (int64, error) {
		tag, err := tx.Exec(ctx, `
			UPDATE runtime_inbox AS target SET
				lease_expires_at = clock_timestamp() + $1::interval,
				updated_at = clock_timestamp()
			WHERE target.inbox_id = $2
				AND target.lease_owner = $3 AND target.status = 'processing'
				AND target.lease_expires_at > clock_timestamp()`,
			durationInterval(leaseTTL), inboxID, owner)
		return tag.RowsAffected(), err
	})
}

func decodeCarrier(data []byte) map[string]string {
	carrier := map[string]string{}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &carrier)
	}
	return carrier
}

// lockInboxLease deliberately does not inspect lease_expires_at. PostgreSQL
// may evaluate scan predicates before a row-lock wait finishes. Callers first
// wait for this lock to be acquired and then issue a separate UPDATE whose
// clock_timestamp() predicate is therefore evaluated after the wait.
func lockInboxLease(ctx context.Context, tx pgx.Tx, inboxID, owner string) (string, error) {
	identity, err := lockInboxLeaseIdentity(ctx, tx, inboxID, owner)
	return identity.PartitionKey, err
}

type inboxLeaseIdentity struct {
	AttemptCount          int
	TenantID              string
	ChannelType           string
	BindingID             string
	DedupKey              string
	PartitionKey          string
	PipelineSchemaVersion int
	AtomicCommitMode      string
	DatabaseIdentity      string
}

func lockInboxLeaseIdentity(
	ctx context.Context,
	tx pgx.Tx,
	inboxID, owner string,
) (inboxLeaseIdentity, error) {
	var identity inboxLeaseIdentity
	err := tx.QueryRow(ctx, `
		SELECT attempt_count, tenant_id, channel_type, binding_id, dedup_key, partition_key,
			pipeline_schema_version, atomic_commit_mode, database_identity
		FROM runtime_inbox
		WHERE inbox_id = $1 AND lease_owner = $2 AND status = 'processing'
		FOR UPDATE`, inboxID, owner).Scan(
		&identity.AttemptCount, &identity.TenantID, &identity.ChannelType,
		&identity.BindingID, &identity.DedupKey, &identity.PartitionKey,
		&identity.PipelineSchemaVersion, &identity.AtomicCommitMode, &identity.DatabaseIdentity,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return inboxLeaseIdentity{}, ErrLeaseLost
	}
	return identity, err
}

func lockOutboxLease(ctx context.Context, tx pgx.Tx, outboxID, owner string) error {
	var lockedID string
	err := tx.QueryRow(ctx, `
		SELECT outbox_id
		FROM runtime_outbox
		WHERE outbox_id = $1 AND lease_owner = $2 AND status = 'sending'
		FOR UPDATE`, outboxID, owner).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	return err
}

func (p *Postgres) withInboxLease(
	ctx context.Context,
	inboxID, owner string,
	mutate func(pgx.Tx) (int64, error),
) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := lockInboxLease(ctx, tx, inboxID, owner); err != nil {
		return err
	}
	affected, err := mutate(tx)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrLeaseLost
	}
	return tx.Commit(ctx)
}

func (p *Postgres) withOutboxLease(
	ctx context.Context,
	outboxID, owner string,
	mutate func(pgx.Tx) (int64, error),
) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockOutboxLease(ctx, tx, outboxID, owner); err != nil {
		return err
	}
	affected, err := mutate(tx)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrLeaseLost
	}
	return tx.Commit(ctx)
}

func (p *Postgres) CompleteInbox(ctx context.Context, inboxID, owner string, outbox *OutboxRecord, now time.Time) error {
	if outbox == nil {
		return p.CompleteInboxBatch(ctx, inboxID, owner, nil, now)
	}
	return p.CompleteInboxBatch(ctx, inboxID, owner, []OutboxRecord{*outbox}, now)
}

// CompleteInboxBatch preserves slice order by issuing each INSERT serially in
// one transaction. PostgreSQL's queue_sequence therefore orders parts exactly
// as planned, while the final lease-deadline check makes the entire batch and
// Inbox completion roll back when the attempt expired during an INSERT wait.
func (p *Postgres) CompleteInboxBatch(
	ctx context.Context,
	inboxID, owner string,
	outboxes []OutboxRecord,
	_ time.Time,
) (err error) {
	ctx, finish := observability.StartStorage(ctx, "inbox.complete", "postgres", "", "")
	defer func() { finish(err) }()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := p.completeInboxBatchTx(ctx, tx, InboxLeaseFence{
		InboxID: inboxID,
		Owner:   owner,
	}, outboxes, false); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CompleteInboxBatchTx performs the queue half of CompleteInboxBatch on a
// caller-owned transaction. This lets strict Session state, canonical replay,
// deterministic delivery operations, and Inbox completion share one COMMIT.
// The final Inbox mutation intentionally remains the last statement issued by
// this method so an Outbox lock wait that crosses the lease deadline rolls the
// whole caller transaction back.
func (p *Postgres) CompleteInboxBatchTx(
	ctx context.Context,
	tx pgx.Tx,
	fence InboxLeaseFence,
	outboxes []OutboxRecord,
) error {
	return p.completeInboxBatchTx(ctx, tx, fence, outboxes, true)
}

func (p *Postgres) completeInboxBatchTx(
	ctx context.Context,
	tx pgx.Tx,
	fence InboxLeaseFence,
	outboxes []OutboxRecord,
	strictFence bool,
) error {
	if tx == nil {
		return errors.New("store: postgres transaction is nil")
	}
	if strictFence && (fence.InboxID == "" || fence.Owner == "" || fence.AttemptCount <= 0 ||
		fence.TenantID == "" || fence.ChannelType == "" || fence.BindingID == "" ||
		fence.DedupKey == "" || fence.PartitionKey == "" || fence.PipelineSchemaVersion <= 0 ||
		fence.AtomicCommitMode == "" || fence.DatabaseIdentity == "") {
		return ErrInboxIdentityMismatch
	}
	if strictFence && (p.DatabaseIdentity() == "" || fence.DatabaseIdentity != p.DatabaseIdentity() ||
		tx.Conn() == nil || fence.DatabaseIdentity != dbbinding.FromConnConfig(tx.Conn().Config())) {
		return ErrInboxIdentityMismatch
	}
	locked, err := lockInboxLeaseIdentity(ctx, tx, fence.InboxID, fence.Owner)
	if err != nil {
		return err
	}
	if strictFence && (locked.AttemptCount != fence.AttemptCount ||
		locked.TenantID != fence.TenantID || locked.ChannelType != fence.ChannelType ||
		locked.BindingID != fence.BindingID || locked.DedupKey != fence.DedupKey ||
		locked.PartitionKey != fence.PartitionKey ||
		locked.PipelineSchemaVersion != fence.PipelineSchemaVersion ||
		locked.AtomicCommitMode != fence.AtomicCommitMode ||
		locked.DatabaseIdentity != fence.DatabaseIdentity) {
		return ErrInboxIdentityMismatch
	}
	if strictFence {
		var leaseValid bool
		if err := tx.QueryRow(ctx, `
			SELECT lease_expires_at > clock_timestamp()
			FROM runtime_inbox
			WHERE inbox_id = $1`, fence.InboxID).Scan(&leaseValid); err != nil {
			return err
		}
		if !leaseValid {
			return ErrLeaseLost
		}
	}
	for i := range outboxes {
		candidate, normalizeErr := normalizeOutboxForInsert(outboxes[i], locked.PartitionKey)
		if normalizeErr != nil {
			return normalizeErr
		}
		if strictFence && (candidate.TenantID != locked.TenantID ||
			candidate.ChannelType != locked.ChannelType || candidate.BindingID != locked.BindingID) {
			return ErrInboxIdentityMismatch
		}
		var insertedID string
		err = tx.QueryRow(ctx, `
				INSERT INTO runtime_outbox
					(outbox_id, operation_key, operation_version, part_index, part_count,
					 payload_hash, tenant_id, channel_type, binding_id, dedup_key,
					 partition_key, payload, payload_bytes, trace_id, trace_carrier,
					 status, delivery_state, next_attempt_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
					$12::jsonb, $13, $14, $15::jsonb, 'pending', 'pending', clock_timestamp())
				ON CONFLICT DO NOTHING
				RETURNING outbox_id`,
			candidate.OutboxID, candidate.OperationKey, candidate.OperationVersion,
			candidate.PartIndex, candidate.PartCount, candidate.PayloadHash,
			candidate.TenantID, candidate.ChannelType, candidate.BindingID,
			candidate.DedupKey, candidate.PartitionKey, candidate.Payload,
			candidate.Payload, candidate.TraceID, traceCarrierJSON(candidate.TraceCarrier)).
			Scan(&insertedID)
		if errors.Is(err, pgx.ErrNoRows) {
			if err := verifyExistingOutbox(ctx, tx, candidate); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
	}
	// Keep the deadline check as the transaction's final data mutation. An
	// Outbox uniqueness/trigger wait may outlive the lease; in that case both
	// the reply insert and completion must roll back.
	tag, err := tx.Exec(ctx, `
		UPDATE runtime_inbox AS target SET
			status = 'processed', lease_owner = '', lease_expires_at = NULL,
			updated_at = clock_timestamp()
		WHERE target.inbox_id = $1
			AND target.lease_owner = $2 AND target.status = 'processing'
			AND target.lease_expires_at > clock_timestamp()
			AND ($3 = 0 OR target.attempt_count = $3)
			AND ($4 = '' OR target.tenant_id = $4)
			AND ($5 = '' OR target.channel_type = $5)
			AND ($6 = '' OR target.binding_id = $6)
			AND ($7 = '' OR target.dedup_key = $7)
			AND ($8 = '' OR target.partition_key = $8)
			AND ($9 = 0 OR target.pipeline_schema_version = $9)
			AND ($10 = '' OR target.atomic_commit_mode = $10)
			AND ($11 = '' OR target.database_identity = $11)`,
		fence.InboxID, fence.Owner, fence.AttemptCount, fence.TenantID,
		fence.ChannelType, fence.BindingID, fence.DedupKey, fence.PartitionKey,
		fence.PipelineSchemaVersion, fence.AtomicCommitMode, fence.DatabaseIdentity)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func normalizeOutboxForInsert(rec OutboxRecord, partitionKey string) (OutboxRecord, error) {
	if rec.OutboxID == "" || rec.DedupKey == "" {
		return OutboxRecord{}, ErrOperationConflict
	}
	if rec.OperationKey == "" {
		rec.OperationKey = "legacy:" + rec.OutboxID
	}
	if rec.PartCount == 0 {
		rec.PartCount = 1
	}
	if rec.OperationVersion < 0 || rec.PartIndex < 0 || rec.PartIndex >= rec.PartCount {
		return OutboxRecord{}, ErrOperationConflict
	}
	computedHash := fmt.Sprintf("%x", sha256.Sum256(rec.Payload))
	if rec.PayloadHash == "" {
		rec.PayloadHash = computedHash
	} else if rec.PayloadHash != computedHash {
		return OutboxRecord{}, ErrOperationConflict
	}
	rec.PartitionKey = partitionKey
	return rec, nil
}

func verifyExistingOutbox(ctx context.Context, tx pgx.Tx, candidate OutboxRecord) error {
	rows, err := tx.Query(ctx, `
		SELECT operation_key, operation_version, part_index, part_count, payload_hash,
			tenant_id, channel_type, binding_id, dedup_key, partition_key, payload_bytes
		FROM runtime_outbox
		WHERE outbox_id = $1 OR operation_key = $2 OR dedup_key = $3
		FOR SHARE`, candidate.OutboxID, candidate.OperationKey, candidate.DedupKey)
	if err != nil {
		return err
	}
	defer rows.Close()
	matched := 0
	for rows.Next() {
		var existing OutboxRecord
		if err := rows.Scan(
			&existing.OperationKey, &existing.OperationVersion, &existing.PartIndex,
			&existing.PartCount, &existing.PayloadHash, &existing.TenantID,
			&existing.ChannelType, &existing.BindingID, &existing.DedupKey,
			&existing.PartitionKey, &existing.Payload,
		); err != nil {
			return err
		}
		matched++
		if existing.OperationKey != candidate.OperationKey ||
			existing.OperationVersion != candidate.OperationVersion ||
			existing.PartIndex != candidate.PartIndex ||
			existing.PartCount != candidate.PartCount ||
			existing.PayloadHash != candidate.PayloadHash ||
			existing.TenantID != candidate.TenantID ||
			existing.ChannelType != candidate.ChannelType ||
			existing.BindingID != candidate.BindingID ||
			existing.DedupKey != candidate.DedupKey ||
			existing.PartitionKey != candidate.PartitionKey ||
			!bytes.Equal(existing.Payload, candidate.Payload) {
			return ErrOperationConflict
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if matched != 1 {
		return ErrOperationConflict
	}
	return nil
}

func (p *Postgres) RetryInbox(ctx context.Context, inboxID, owner, errType string, now, retryAt time.Time) error {
	retryDelay := retryAt.Sub(now)
	return p.withInboxLease(ctx, inboxID, owner, func(tx pgx.Tx) (int64, error) {
		tag, err := tx.Exec(ctx, `
			UPDATE runtime_inbox AS target SET
				status = 'retry', lease_owner = '', lease_expires_at = NULL,
				next_attempt_at = clock_timestamp() + $1::interval,
				last_error_type = $2, updated_at = clock_timestamp()
			WHERE target.inbox_id = $3
				AND target.lease_owner = $4 AND target.status = 'processing'
				AND target.lease_expires_at > clock_timestamp()`,
			durationInterval(retryDelay), errType, inboxID, owner)
		return tag.RowsAffected(), err
	})
}

func (p *Postgres) DeadLetterInbox(ctx context.Context, inboxID, owner, errType string, _ time.Time) error {
	return p.withInboxLease(ctx, inboxID, owner, func(tx pgx.Tx) (int64, error) {
		tag, err := tx.Exec(ctx, `
			UPDATE runtime_inbox AS target SET
				status = 'dead_letter', lease_owner = '', lease_expires_at = NULL,
				last_error_type = $1, updated_at = clock_timestamp()
			WHERE target.inbox_id = $2
				AND target.lease_owner = $3 AND target.status = 'processing'
				AND target.lease_expires_at > clock_timestamp()`,
			errType, inboxID, owner)
		return tag.RowsAffected(), err
	})
}

func (p *Postgres) LeaseOutbox(ctx context.Context, owner string, _ time.Time, leaseTTL time.Duration, limit int) (records []OutboxRecord, err error) {
	ctx, finish := observability.StartStorage(ctx, "outbox.lease", "postgres", "", "")
	defer func() { finish(err) }()
	if limit <= 0 {
		limit = 1
	}
	rows, err := p.pool.Query(ctx, `
		WITH candidates AS MATERIALIZED (
			SELECT candidate.outbox_id
			FROM runtime_outbox AS candidate
			WHERE candidate.status IN ('pending', 'retry')
				AND candidate.next_attempt_at <= clock_timestamp()
				AND NOT EXISTS (
					SELECT 1
					FROM runtime_outbox AS earlier
					WHERE earlier.tenant_id = candidate.tenant_id
						AND earlier.channel_type = candidate.channel_type
						AND earlier.binding_id = candidate.binding_id
						AND (
							earlier.partition_key = candidate.partition_key
							OR earlier.partition_key = ''
							OR candidate.partition_key = ''
						)
						AND earlier.status IN ('pending', 'retry', 'sending')
						AND (earlier.created_at, earlier.queue_sequence) <
							(candidate.created_at, candidate.queue_sequence)
				)
			ORDER BY candidate.next_attempt_at, candidate.created_at, candidate.queue_sequence
			LIMIT $3
			FOR UPDATE OF candidate SKIP LOCKED
		), leased AS (
			UPDATE runtime_outbox AS target SET
				status = 'sending', delivery_state = 'in_flight', lease_owner = $1,
				lease_expires_at = clock_timestamp() + $2::interval,
				attempt_count = target.attempt_count + 1,
				state_version = target.state_version + 1,
				updated_at = clock_timestamp()
			FROM candidates
			WHERE target.outbox_id = candidates.outbox_id
			RETURNING target.*, target.queue_sequence AS leased_sequence
		), attempts AS (
			INSERT INTO runtime_outbox_attempts
				(outbox_id, operation_key, attempt_no, lease_owner, phase, started_at)
			SELECT outbox_id, operation_key, attempt_count, $1, 'leased', clock_timestamp()
			FROM leased
			RETURNING outbox_id, attempt_no, phase
		)
		SELECT leased.outbox_id, leased.operation_key, leased.operation_version,
			leased.part_index, leased.part_count, leased.payload_hash,
			leased.tenant_id, leased.channel_type, leased.binding_id,
			leased.dedup_key, leased.partition_key, leased.payload_bytes,
			leased.trace_id, leased.trace_carrier, leased.status,
			leased.delivery_state, leased.state_version, leased.attempt_count,
			attempts.phase, leased.last_error_type, leased.provider_code,
			leased.provider_message_id, leased.provider_request_id,
			leased.response_hash, leased.lease_expires_at
		FROM leased
		JOIN attempts ON attempts.outbox_id = leased.outbox_id
			AND attempts.attempt_no = leased.attempt_count
		ORDER BY leased.leased_sequence`,
		owner, durationInterval(leaseTTL), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records = nil
	for rows.Next() {
		rec, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

type postgresScanner interface {
	Scan(dest ...any) error
}

func scanOutbox(scanner postgresScanner) (OutboxRecord, error) {
	var rec OutboxRecord
	var carrier []byte
	var lease pgtype.Timestamptz
	err := scanner.Scan(
		&rec.OutboxID, &rec.OperationKey, &rec.OperationVersion,
		&rec.PartIndex, &rec.PartCount, &rec.PayloadHash,
		&rec.TenantID, &rec.ChannelType, &rec.BindingID,
		&rec.DedupKey, &rec.PartitionKey, &rec.Payload,
		&rec.TraceID, &carrier, &rec.Status, &rec.DeliveryState,
		&rec.StateVersion, &rec.AttemptCount, &rec.AttemptPhase,
		&rec.LastErrorType, &rec.ProviderCode, &rec.ProviderMessageID,
		&rec.ProviderRequestID, &rec.ResponseHash, &lease,
	)
	if err != nil {
		return OutboxRecord{}, err
	}
	rec.TraceCarrier = decodeCarrier(carrier)
	if lease.Valid {
		rec.LeaseExpiresAt = lease.Time
	}
	return rec, nil
}

func (p *Postgres) RenewOutboxLease(ctx context.Context, outboxID, owner string, _ time.Time, leaseTTL time.Duration) error {
	return p.withOutboxLease(ctx, outboxID, owner, func(tx pgx.Tx) (int64, error) {
		tag, err := tx.Exec(ctx, `
			UPDATE runtime_outbox AS target SET
				lease_expires_at = clock_timestamp() + $1::interval,
				updated_at = clock_timestamp()
			WHERE target.outbox_id = $2
				AND target.lease_owner = $3 AND target.status = 'sending'
				AND target.lease_expires_at > clock_timestamp()`,
			durationInterval(leaseTTL), outboxID, owner)
		return tag.RowsAffected(), err
	})
}

func (p *Postgres) MarkOutboxDispatched(
	ctx context.Context,
	outboxID, owner string,
	attemptNo int,
	_ time.Time,
) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockOutboxLease(ctx, tx, outboxID, owner); err != nil {
		return err
	}
	var phase, attemptOwner string
	err = tx.QueryRow(ctx, `
		SELECT phase, lease_owner
		FROM runtime_outbox_attempts
		WHERE outbox_id = $1 AND attempt_no = $2
		FOR UPDATE`, outboxID, attemptNo).Scan(&phase, &attemptOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidTransition
	}
	if err != nil {
		return err
	}
	if attemptOwner != owner {
		return ErrInvalidTransition
	}
	switch phase {
	case AttemptLeased:
		if _, err := tx.Exec(ctx, `
			UPDATE runtime_outbox_attempts
			SET phase = 'dispatched', dispatched_at = clock_timestamp()
			WHERE outbox_id = $1 AND attempt_no = $2
				AND lease_owner = $3 AND phase = 'leased'`, outboxID, attemptNo, owner); err != nil {
			return err
		}
	case AttemptDispatched:
		// An ambiguous acknowledgement of the first call is safe to replay: no
		// provider request may have started until this transition committed.
	default:
		return ErrInvalidTransition
	}
	tag, err := tx.Exec(ctx, `
		UPDATE runtime_outbox AS target SET
			state_version = state_version + CASE WHEN $1 THEN 1 ELSE 0 END,
			updated_at = clock_timestamp()
		WHERE target.outbox_id = $2 AND target.lease_owner = $3
			AND target.status = 'sending'
			AND target.lease_expires_at > clock_timestamp()`,
		phase == AttemptLeased, outboxID, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return tx.Commit(ctx)
}

func (p *Postgres) FinishOutboxAttempt(
	ctx context.Context,
	outboxID, owner string,
	attemptNo int,
	result delivery.Result,
	now, retryAt time.Time,
	exhausted bool,
) error {
	return p.finishOutboxAttempt(ctx, outboxID, owner, attemptNo, result, now, retryAt, exhausted, false)
}

func (p *Postgres) finishOutboxAttempt(
	ctx context.Context,
	outboxID, owner string,
	attemptNo int,
	result delivery.Result,
	now, retryAt time.Time,
	exhausted, allowLeased bool,
) error {
	if err := validateDeliveryResult(result); err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockOutboxLease(ctx, tx, outboxID, owner); err != nil {
		return err
	}
	if attemptNo <= 0 {
		if err := tx.QueryRow(ctx, `SELECT attempt_count FROM runtime_outbox WHERE outbox_id = $1`, outboxID).
			Scan(&attemptNo); err != nil {
			return err
		}
	}
	var phase, attemptOwner string
	err = tx.QueryRow(ctx, `
		SELECT phase, lease_owner
		FROM runtime_outbox_attempts
		WHERE outbox_id = $1 AND attempt_no = $2
		FOR UPDATE`, outboxID, attemptNo).Scan(&phase, &attemptOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidTransition
	}
	if err != nil {
		return err
	}
	if attemptOwner != owner {
		return ErrInvalidTransition
	}
	if phase != AttemptDispatched && !(allowLeased && phase == AttemptLeased) {
		return ErrInvalidTransition
	}

	status, state := OutboxSending, DeliveryUnknown
	scheduleRetry := false
	switch result.Outcome {
	case delivery.Confirmed:
		status, state = OutboxSent, DeliveryConfirmed
	case delivery.RetryableNotSent:
		if exhausted {
			status, state = OutboxDead, DeliveryRetryExhausted
		} else {
			status, state = OutboxRetry, DeliveryRetryableNotSent
			scheduleRetry = true
		}
	case delivery.PermanentRejected:
		status, state = OutboxDead, DeliveryPermanentRejected
	case delivery.Unknown:
		// status=sending plus a NULL deadline is intentional: legacy readers
		// still treat the row as a FIFO barrier but cannot reclaim it.
		status, state = OutboxSending, DeliveryUnknown
	}
	retryDelay := retryAt.Sub(now)
	if retryDelay < 0 {
		retryDelay = 0
	}
	_, err = tx.Exec(ctx, `
		UPDATE runtime_outbox_attempts SET
			phase = 'finished', outcome = $1, error_type = $2,
			provider_code = $3, http_status = $4,
			provider_message_id = $5, provider_request_id = $6,
			response_hash = $7,
			finished_at = clock_timestamp()
		WHERE outbox_id = $8 AND attempt_no = $9
			AND lease_owner = $10 AND phase = $11`,
		result.Outcome, result.ErrorType, result.ProviderCode, result.HTTPStatus,
		result.ProviderMessageID, result.ProviderRequestID, result.ResponseHash,
		outboxID, attemptNo, owner, phase)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE runtime_outbox AS target SET
			status = $1, delivery_state = $2,
			lease_owner = '', lease_expires_at = NULL,
			next_attempt_at = CASE WHEN $3 THEN clock_timestamp() + $4::interval ELSE next_attempt_at END,
			last_error_type = $5, provider_code = $6,
			provider_message_id = $7, provider_request_id = $8,
			response_hash = $9, state_version = state_version + 1,
			updated_at = clock_timestamp()
		WHERE target.outbox_id = $10 AND target.lease_owner = $11
			AND target.status = 'sending' AND target.attempt_count = $12
			AND target.lease_expires_at > clock_timestamp()`,
		status, state, scheduleRetry, durationInterval(retryDelay),
		result.ErrorType, result.ProviderCode, result.ProviderMessageID,
		result.ProviderRequestID, result.ResponseHash,
		outboxID, owner, attemptNo)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return tx.Commit(ctx)
}

const (
	maxDeliveryCategoryBytes = 128
	maxDeliveryIDBytes       = 512
	maxResolutionReasonBytes = 1024
)

func validateDeliveryResult(result delivery.Result) error {
	if err := result.Validate(); err != nil {
		return ErrInvalidTransition
	}
	if result.HTTPStatus < 0 || result.HTTPStatus > 999 ||
		!validLedgerIdentifier(result.ErrorType, maxDeliveryCategoryBytes) ||
		!validLedgerIdentifier(result.ProviderCode, maxDeliveryCategoryBytes) ||
		!validLedgerIdentifier(result.ProviderMessageID, maxDeliveryIDBytes) ||
		!validLedgerIdentifier(result.ProviderRequestID, maxDeliveryIDBytes) ||
		!validLedgerIdentifier(result.ResponseHash, maxDeliveryCategoryBytes) {
		return ErrInvalidTransition
	}
	return nil
}

func (p *Postgres) CompleteOutbox(ctx context.Context, outboxID, owner string, _ time.Time) (err error) {
	_, finish := observability.StartStorage(ctx, "outbox.complete", "postgres", "", "")
	defer func() { finish(err) }()
	return p.finishOutboxAttempt(ctx, outboxID, owner, 0,
		delivery.Result{Outcome: delivery.Confirmed}, time.Time{}, time.Time{}, false, true)
}

func (p *Postgres) RetryOutbox(ctx context.Context, outboxID, owner, errType string, now, retryAt time.Time) (err error) {
	_, finish := observability.StartStorage(ctx, "outbox.retry", "postgres", "", "")
	defer func() { finish(err) }()
	return p.finishOutboxAttempt(ctx, outboxID, owner, 0,
		delivery.Result{Outcome: delivery.RetryableNotSent, ErrorType: errType},
		now, retryAt, false, true)
}

func (p *Postgres) DeadLetterOutbox(ctx context.Context, outboxID, owner, errType string, _ time.Time) (err error) {
	_, finish := observability.StartStorage(ctx, "outbox.dead_letter", "postgres", "", "")
	defer func() { finish(err) }()
	return p.finishOutboxAttempt(ctx, outboxID, owner, 0,
		delivery.Result{Outcome: delivery.PermanentRejected, ErrorType: errType},
		time.Time{}, time.Time{}, false, true)
}

func (p *Postgres) ListUncertainOutbox(ctx context.Context, limit int) ([]OutboxRecord, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := p.pool.Query(ctx, `
		SELECT o.outbox_id, o.operation_key, o.operation_version,
			o.part_index, o.part_count, o.payload_hash,
			o.tenant_id, o.channel_type, o.binding_id, o.dedup_key,
			o.partition_key, o.payload_bytes, o.trace_id, o.trace_carrier,
			o.status, o.delivery_state, o.state_version, o.attempt_count,
			COALESCE(a.phase, ''), o.last_error_type, o.provider_code,
			o.provider_message_id, o.provider_request_id, o.response_hash,
			o.lease_expires_at
		FROM runtime_outbox AS o
		LEFT JOIN runtime_outbox_attempts AS a
			ON a.outbox_id = o.outbox_id AND a.attempt_no = o.attempt_count
		WHERE o.status = 'sending' AND o.delivery_state = 'unknown'
			AND o.lease_owner = '' AND o.lease_expires_at IS NULL
		ORDER BY o.updated_at, o.queue_sequence
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]OutboxRecord, 0)
	for rows.Next() {
		rec, err := scanOutbox(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

func (p *Postgres) ResolveOutbox(ctx context.Context, req ResolveRequest) error {
	if req.ResolutionID == "" || req.OutboxID == "" || req.ExpectedVersion < 0 ||
		req.ExpectedAttempt < 1 || strings.TrimSpace(req.Actor) == "" ||
		strings.TrimSpace(req.Reason) == "" || len(req.Actor) > maxDeliveryIDBytes ||
		len(req.Reason) > maxResolutionReasonBytes {
		return ErrInvalidTransition
	}
	switch req.Action {
	case ResolveAssumeDelivered, ResolveRetry, ResolveCancel:
	default:
		return ErrInvalidTransition
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if ok, err := matchingResolution(ctx, tx, req); err != nil || ok {
		return err
	}
	var status, state, leaseOwner string
	var version int64
	var attempt int
	var lease pgtype.Timestamptz
	err = tx.QueryRow(ctx, `
		SELECT status, delivery_state, state_version, attempt_count,
			lease_owner, lease_expires_at
		FROM runtime_outbox WHERE outbox_id = $1 FOR UPDATE`, req.OutboxID).
		Scan(&status, &state, &version, &attempt, &leaseOwner, &lease)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status != OutboxSending || state != DeliveryUnknown || leaseOwner != "" ||
		lease.Valid || version != req.ExpectedVersion || attempt != req.ExpectedAttempt {
		// A concurrent replay of the same resolution may have observed no
		// resolution before waiting on this Outbox row, then wake after the first
		// transaction committed. Re-check the immutable resolution identity so
		// identical requests remain idempotent under concurrency; a genuinely
		// different/stale decision still fails the state CAS below.
		if ok, lookupErr := matchingResolution(ctx, tx, req); lookupErr != nil || ok {
			return lookupErr
		}
		return ErrInvalidTransition
	}
	resultStatus, resultState := OutboxSending, DeliveryUnknown
	switch req.Action {
	case ResolveAssumeDelivered:
		resultStatus, resultState = OutboxSent, DeliveryConfirmed
	case ResolveRetry:
		resultStatus, resultState = OutboxRetry, DeliveryRetryableNotSent
	case ResolveCancel:
		resultStatus, resultState = OutboxDead, DeliveryCanceled
	}
	newVersion := version + 1
	var inserted string
	err = tx.QueryRow(ctx, `
		INSERT INTO runtime_outbox_resolutions
			(resolution_id, outbox_id, expected_version, expected_attempt,
			 action, actor, reason, resulting_status,
			 resulting_delivery_state, resulting_version, resolved_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, clock_timestamp())
		ON CONFLICT (resolution_id) DO NOTHING
		RETURNING resolution_id`, req.ResolutionID, req.OutboxID,
		req.ExpectedVersion, req.ExpectedAttempt, req.Action, req.Actor, req.Reason,
		resultStatus, resultState, newVersion).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		ok, lookupErr := matchingResolution(ctx, tx, req)
		if lookupErr != nil {
			return lookupErr
		}
		if ok {
			return tx.Commit(ctx)
		}
		return ErrOperationConflict
	}
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE runtime_outbox SET
			status = $1, delivery_state = $2,
			next_attempt_at = CASE WHEN $3 THEN clock_timestamp() ELSE next_attempt_at END,
			last_error_type = CASE WHEN $3 THEN 'operator_retry' ELSE last_error_type END,
			state_version = state_version + 1, updated_at = clock_timestamp()
		WHERE outbox_id = $4 AND status = 'sending'
			AND delivery_state = 'unknown' AND lease_owner = ''
			AND lease_expires_at IS NULL AND state_version = $5
			AND attempt_count = $6`,
		resultStatus, resultState, req.Action == ResolveRetry, req.OutboxID,
		req.ExpectedVersion, req.ExpectedAttempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return tx.Commit(ctx)
}

func matchingResolution(ctx context.Context, tx pgx.Tx, req ResolveRequest) (bool, error) {
	var outboxID, action, actor, reason string
	var expectedVersion int64
	var expectedAttempt int
	err := tx.QueryRow(ctx, `
		SELECT outbox_id, expected_version, expected_attempt, action, actor, reason
		FROM runtime_outbox_resolutions WHERE resolution_id = $1`, req.ResolutionID).
		Scan(&outboxID, &expectedVersion, &expectedAttempt, &action, &actor, &reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if outboxID == req.OutboxID && expectedVersion == req.ExpectedVersion &&
		expectedAttempt == req.ExpectedAttempt && action == req.Action &&
		actor == req.Actor && reason == req.Reason {
		return true, nil
	}
	return false, ErrOperationConflict
}

func (p *Postgres) ReclaimExpired(ctx context.Context, _ time.Time) (int, int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var inboxCount int
	err = tx.QueryRow(ctx, `
		WITH inbox_candidates AS MATERIALIZED (
			SELECT inbox_id
			FROM runtime_inbox
			WHERE status = 'processing' AND lease_expires_at <= clock_timestamp()
			ORDER BY lease_expires_at, inbox_id
			FOR UPDATE SKIP LOCKED
		), reclaimed_inbox AS (
			UPDATE runtime_inbox AS target SET
				status = 'retry', lease_owner = '', lease_expires_at = NULL,
				next_attempt_at = clock_timestamp(),
				last_error_type = 'lease_expired', updated_at = clock_timestamp()
			FROM inbox_candidates
			WHERE target.inbox_id = inbox_candidates.inbox_id
				AND target.status = 'processing'
				AND target.lease_expires_at <= clock_timestamp()
			RETURNING 1
		)
		SELECT count(*) FROM reclaimed_inbox`).Scan(&inboxCount)
	if err != nil {
		return 0, 0, err
	}
	rows, err := tx.Query(ctx, `
		SELECT outbox_id, attempt_count
		FROM runtime_outbox
		WHERE status = 'sending' AND delivery_state <> 'unknown'
			AND (lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp())
		ORDER BY lease_expires_at NULLS FIRST, outbox_id
		FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, 0, err
	}
	type expiredOutbox struct {
		id        string
		attemptNo int
	}
	expired := make([]expiredOutbox, 0)
	for rows.Next() {
		var item expiredOutbox
		if err := rows.Scan(&item.id, &item.attemptNo); err != nil {
			rows.Close()
			return 0, 0, err
		}
		expired = append(expired, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, err
	}
	rows.Close()
	outboxCount := 0
	for _, item := range expired {
		phase := ""
		err := tx.QueryRow(ctx, `
			SELECT phase FROM runtime_outbox_attempts
			WHERE outbox_id = $1 AND attempt_no = $2
			FOR UPDATE`, item.id, item.attemptNo).Scan(&phase)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return 0, 0, err
		}
		status, state, outcome, errorType := OutboxSending, DeliveryUnknown, delivery.Unknown, "lease_expired_after_dispatch"
		if phase == AttemptLeased {
			status, state, outcome, errorType = OutboxRetry, DeliveryRetryableNotSent, delivery.RetryableNotSent, "lease_expired_before_dispatch"
		}
		if phase == "" {
			errorType = "legacy_lease_expired_unknown"
		}
		if phase != "" && phase != AttemptFinished {
			if _, err := tx.Exec(ctx, `
				UPDATE runtime_outbox_attempts SET
					phase = 'finished', outcome = $1, error_type = $2,
					finished_at = clock_timestamp()
				WHERE outbox_id = $3 AND attempt_no = $4 AND phase <> 'finished'`,
				outcome, errorType, item.id, item.attemptNo); err != nil {
				return 0, 0, err
			}
		}
		tag, err := tx.Exec(ctx, `
			UPDATE runtime_outbox SET
				status = $1, delivery_state = $2, lease_owner = '', lease_expires_at = NULL,
				next_attempt_at = CASE WHEN $1 = 'retry' THEN clock_timestamp() ELSE next_attempt_at END,
				last_error_type = $3, state_version = state_version + 1,
				updated_at = clock_timestamp()
			WHERE outbox_id = $4 AND status = 'sending'
				AND delivery_state <> 'unknown'
				AND (lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp())`,
			status, state, errorType, item.id)
		if err != nil {
			return 0, 0, err
		}
		outboxCount += int(tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return inboxCount, outboxCount, nil
}

func (p *Postgres) Depths(ctx context.Context) (int, int, int, int, int, error) {
	var runnableInbox, deadInbox, pendingOutbox, deadOutbox, uncertainOutbox int
	err := p.pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM runtime_inbox WHERE status IN ('received', 'retry', 'processing')),
			(SELECT count(*) FROM runtime_inbox WHERE status = 'dead_letter'),
			(SELECT count(*) FROM runtime_outbox
				WHERE status IN ('pending', 'retry', 'sending') AND delivery_state <> 'unknown'),
			(SELECT count(*) FROM runtime_outbox WHERE status = 'dead_letter'),
			(SELECT count(*) FROM runtime_outbox
				WHERE status = 'sending' AND delivery_state = 'unknown')`).
		Scan(&runnableInbox, &deadInbox, &pendingOutbox, &deadOutbox, &uncertainOutbox)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	return runnableInbox, deadInbox, pendingOutbox, deadOutbox, uncertainOutbox, nil
}
