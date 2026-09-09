package reliable

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	_ "github.com/lib/pq"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
)

const (
	defaultInboxMaxAttempts  = 5
	defaultOutboxMaxAttempts = 8
	maxStoredErrorBytes      = 2048
	maxOutboxContentBytes    = 4 << 20
)

// PostgresStore implements Store with PostgreSQL transactions, row locks and
// monotonically increasing lease_version fencing tokens.
type PostgresStore struct {
	db     *sql.DB
	ownsDB bool
}

// NewPostgresStore uses an existing connection pool. Close does not close it.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// OpenPostgresStore opens and verifies a dedicated connection pool.
func OpenPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	ctx = nonNilContext(ctx)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open reliable store: %w", err)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping reliable store: %w", err)
	}
	return &PostgresStore{db: db, ownsDB: true}, nil
}

func (s *PostgresStore) EnqueueInbox(ctx context.Context, msg *InboxMessage) (bool, error) {
	return s.enqueueInbox(ctx, msg, false)
}

// EnqueueInboxWithAdmission performs the operator-owned MaxQueued check in
// the same transaction that allocates the session sequence and inserts Inbox.
func (s *PostgresStore) EnqueueInboxWithAdmission(ctx context.Context, msg *InboxMessage) (bool, error) {
	return s.enqueueInbox(ctx, msg, true)
}

func (s *PostgresStore) enqueueInbox(ctx context.Context, msg *InboxMessage, enforceAdmission bool) (bool, error) {
	if err := prepareInbox(msg); err != nil {
		return false, err
	}
	msg.Status = InboxReceived
	db, err := s.database()
	if err != nil {
		return false, err
	}
	ctx = nonNilContext(ctx)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("enqueue inbox begin: %w", err)
	}
	defer tx.Rollback()
	// Hold a shared row lock through the Inbox commit. Concurrent webhooks for
	// the same tenant remain concurrent, while a suspend/delete UPDATE waits for
	// all accepted ingress transactions. Once that lifecycle mutation returns,
	// no stale Gateway snapshot can enqueue more work.
	var tenantStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT status
		FROM tenants
		WHERE id=$1
		FOR SHARE`, msg.TenantID).Scan(&tenantStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrTenantInactive
		}
		return false, fmt.Errorf("enqueue inbox verify tenant status: %w", err)
	}
	if tenantStatus != "active" {
		return false, ErrTenantInactive
	}
	if enforceAdmission {
		// Keep the schedule row creation in the same transaction as the
		// admission count. Migration 035 seeds existing tenants; this single-row
		// upsert covers tenants created after that migration without reintroducing
		// a queue-wide scan in ClaimInboxFair.
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO tenant_queue_schedule (tenant_id)
			VALUES ($1)
			ON CONFLICT (tenant_id) DO NOTHING`, msg.TenantID); err != nil {
			return false, fmt.Errorf("enqueue inbox initialize queue schedule: %w", err)
		}
		var maxQueued int64
		err := tx.QueryRowContext(ctx, `
			SELECT max_queued
			FROM tenant_queue_schedule
			WHERE tenant_id=$1
			FOR UPDATE`, msg.TenantID).Scan(&maxQueued)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("enqueue inbox read queue policy: %w", err)
		}
		if err == nil && maxQueued > 0 {
			var queued int64
			if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM inbox_messages
			WHERE tenant_id=$1
			  AND status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL')
			  AND NOT (channel_type=$2 AND channel_account_id=$3 AND external_message_id=$4)`,
				msg.TenantID, msg.ChannelType, msg.ChannelAccountID, msg.ExternalMessageID).Scan(&queued); err != nil {
				return false, fmt.Errorf("enqueue inbox count tenant queue: %w", err)
			}
			if queued >= maxQueued {
				// A provider redelivery of an already persisted event is an
				// idempotent read, even when another message fills the tenant
				// budget. Check the unique provider key before rejecting the
				// admission so retries do not turn into a misleading 429.
				stored, duplicateErr := scanInbox(tx.QueryRowContext(ctx, selectInboxByProviderKeyQuery+` FOR KEY SHARE`,
					msg.TenantID, msg.ChannelType, msg.ChannelAccountID, msg.ExternalMessageID))
				// A missing row means this is a genuinely new event and the
				// tenant budget must reject it.
				switch {
				case errors.Is(duplicateErr, sql.ErrNoRows):
					return false, ErrTenantQueueFull
				case duplicateErr != nil:
					return false, fmt.Errorf("enqueue inbox check duplicate at queue limit: %w", duplicateErr)
				}
				if !inboxIdentityMatches(stored, msg) {
					return false, ErrIdempotencyConflict
				}
				if err := tx.Commit(); err != nil {
					return false, fmt.Errorf("enqueue inbox duplicate commit: %w", err)
				}
				*msg = *stored
				return false, nil
			}
		}
	}
	var sessionSequence int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO inbox_session_sequences (
			tenant_id, agent_app_name, session_id, last_sequence
		) VALUES ($1,$2,$3,1)
		ON CONFLICT (tenant_id, agent_app_name, session_id)
		DO UPDATE SET last_sequence=inbox_session_sequences.last_sequence+1
		RETURNING last_sequence`, msg.TenantID, msg.AgentApp, msg.SessionID).
		Scan(&sessionSequence); err != nil {
		return false, fmt.Errorf("allocate inbox session sequence: %w", err)
	}
	const insertQuery = `
		INSERT INTO inbox_messages (
			tenant_id, channel_type, channel_account_id, external_message_id,
			agent_app_name, conversation_id, reply_to_id, user_id, session_id,
			is_group_chat, session_owner_id, routing_version, session_sequence,
			payload_hash, payload, trace_parent, status, max_attempts
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,'RECEIVED',$17)
		ON CONFLICT (tenant_id, channel_type, channel_account_id, external_message_id)
		DO NOTHING
		RETURNING id, session_sequence, created_at, updated_at`
	err = tx.QueryRowContext(ctx, insertQuery,
		msg.TenantID, msg.ChannelType, msg.ChannelAccountID, msg.ExternalMessageID,
		msg.AgentApp, msg.ConversationID, msg.ReplyToID, msg.UserID, msg.SessionID,
		msg.IsGroupChat, msg.SessionOwnerID, msg.RoutingVersion, sessionSequence,
		msg.PayloadHash, msg.Payload, msg.TraceParent, msg.MaxAttempts,
	).Scan(&msg.ID, &msg.SessionSequence, &msg.CreatedAt, &msg.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		// Roll back the sequence allocation. A duplicate must not consume a
		// sequence number, even when two gateways race on the same provider ID.
		_ = tx.Rollback()
		stored, queryErr := scanInbox(db.QueryRowContext(ctx, selectInboxByProviderKeyQuery,
			msg.TenantID, msg.ChannelType, msg.ChannelAccountID, msg.ExternalMessageID,
		))
		if queryErr != nil {
			return false, fmt.Errorf("enqueue inbox read duplicate: %w", queryErr)
		}
		if !inboxIdentityMatches(stored, msg) {
			return false, ErrIdempotencyConflict
		}
		*msg = *stored
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("enqueue inbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("enqueue inbox commit: %w", err)
	}
	msg.SessionSequence = sessionSequence
	return true, nil
}

func (s *PostgresStore) ClaimInbox(ctx context.Context, owner string, leaseDuration time.Duration) (*InboxMessage, error) {
	if err := ValidateLeaseOwner(owner); err != nil {
		return nil, fmt.Errorf("claim inbox: %w", err)
	}
	if leaseDuration < time.Millisecond {
		return nil, fmt.Errorf("claim inbox: lease duration of at least 1ms is required")
	}
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("claim inbox begin: %w", err)
	}
	defer tx.Rollback()

	const selectQuery = `
		SELECT id, tenant_id, channel_type, channel_account_id,
		       external_message_id, agent_app_name, conversation_id, reply_to_id,
		       user_id, session_id,
		       is_group_chat, session_owner_id, routing_version,
		       session_sequence, payload_hash, payload, trace_parent, status, attempt_count,
		       max_attempts, next_attempt_at, approval_deadline,
		       lease_owner, lease_version, lease_until, last_error, created_at, updated_at
		FROM inbox_messages
		WHERE (attempt_count < max_attempts OR status='WAITING_APPROVAL') AND (
			status='RECEIVED'
			OR (status='RETRY_WAIT' AND next_attempt_at <= clock_timestamp())
			OR (status='WAITING_APPROVAL' AND next_attempt_at <= clock_timestamp()
			    AND approval_deadline > clock_timestamp())
			OR (status='PROCESSING' AND lease_until <= clock_timestamp())
		)
		AND NOT EXISTS (
			SELECT 1
			FROM inbox_messages predecessor
			WHERE predecessor.tenant_id = inbox_messages.tenant_id
			  AND predecessor.agent_app_name = inbox_messages.agent_app_name
			  AND predecessor.session_id = inbox_messages.session_id
			  AND predecessor.session_sequence < inbox_messages.session_sequence
			  AND predecessor.status <> 'COMPLETED'
		)
		ORDER BY COALESCE(next_attempt_at, created_at), id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`

	msg, err := scanInbox(tx.QueryRowContext(ctx, selectQuery))
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("claim inbox commit: %w", err)
		}
		return nil, ErrNoWork
	}
	if err != nil {
		return nil, fmt.Errorf("claim inbox select: %w", err)
	}

	const updateQuery = `
		UPDATE inbox_messages
		SET status='PROCESSING',
		    attempt_count=CASE WHEN status='WAITING_APPROVAL' THEN attempt_count ELSE attempt_count+1 END,
		    lease_owner=$2, lease_version=lease_version+1,
		    lease_until=clock_timestamp() + ($3 * interval '1 millisecond'),
		    next_attempt_at=NULL, approval_deadline=NULL, updated_at=now()
		WHERE id=$1
		RETURNING attempt_count, lease_version, lease_until, updated_at`
	if err := tx.QueryRowContext(ctx, updateQuery, msg.ID, owner, leaseDuration.Milliseconds()).
		Scan(&msg.AttemptCount, &msg.Lease.Fence, &msg.Lease.Until, &msg.UpdatedAt); err != nil {
		return nil, fmt.Errorf("claim inbox update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim inbox commit: %w", err)
	}
	msg.Status = InboxProcessing
	// A WAITING_APPROVAL claim resumes the already-authorized invocation. The
	// durable gate is no longer active, so keep the returned snapshot aligned
	// with the row written by the fenced UPDATE.
	msg.ApprovalDeadline = nil
	msg.Lease.Owner = owner
	return msg, nil
}

// UpsertTenantQueuePolicy writes operator-owned fair scheduling state. Tenant
// requests never reach this method; control-plane callers must authorize the
// change and record their audit event before applying it.
func (s *PostgresStore) UpsertTenantQueuePolicy(ctx context.Context, policy TenantQueuePolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	const query = `
		INSERT INTO tenant_queue_schedule
			(tenant_id, weight, max_queued, max_inflight, updated_at)
		VALUES ($1, $2, $3, $4, clock_timestamp())
		ON CONFLICT (tenant_id) DO UPDATE
		SET weight=EXCLUDED.weight,
		    max_queued=EXCLUDED.max_queued,
		    max_inflight=EXCLUDED.max_inflight,
		    updated_at=clock_timestamp()`
	if _, err := db.ExecContext(ctx, query, policy.TenantID, policy.Weight, policy.MaxQueued, policy.MaxInflight); err != nil {
		return fmt.Errorf("upsert tenant queue policy: %w", err)
	}
	return nil
}

func (s *PostgresStore) DeleteTenantQueuePolicy(ctx context.Context, tenantID string) error {
	if tenantID == "" || len(tenantID) > 64 || strings.ContainsAny(tenantID, "\x00\r\n") {
		return fmt.Errorf("%w: tenant id is invalid", ErrInvalidTenantQueuePolicy)
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	// "Delete" resets the operator override to the documented default. Keep
	// service debt and tie-break history so resetting policy cannot buy priority.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO tenant_queue_schedule (tenant_id, weight, max_queued, max_inflight, updated_at)
		VALUES ($1, 1, 0, 0, clock_timestamp())
		ON CONFLICT (tenant_id) DO UPDATE
		SET weight=1, max_queued=0, max_inflight=0,
		    updated_at=clock_timestamp()`, tenantID); err != nil {
		return fmt.Errorf("reset tenant queue policy: %w", err)
	}
	return nil
}

// CheckFairInboxReady verifies the schedule, index and durable dispatch clock.
// It is intentionally a startup probe rather than a claim-path
// fallback: enabling fair scheduling against an older schema must fail closed
// before any consumer acknowledges or claims work.
func (s *PostgresStore) CheckFairInboxReady(ctx context.Context) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	var ready bool
	if err := db.QueryRowContext(ctx, `
		SELECT to_regclass('tenant_queue_schedule') IS NOT NULL
		   AND to_regclass('idx_inbox_fair_tenant_head') IS NOT NULL
		   AND to_regclass('inbox_fair_queue_clock') IS NOT NULL`).Scan(&ready); err != nil {
		return fmt.Errorf("check fair queue schema: %w", err)
	}
	if !ready {
		return fmt.Errorf("%w: queue schedule and dispatch clock migrations are required", ErrFairQueueNotReady)
	}
	var virtualTime int64
	if err := db.QueryRowContext(ctx, `SELECT virtual_time FROM inbox_fair_queue_clock WHERE singleton=TRUE`).Scan(&virtualTime); err != nil {
		return fmt.Errorf("%w: dispatch clock is unavailable: %v", ErrFairQueueNotReady, err)
	}
	if virtualTime < 0 {
		return fmt.Errorf("%w: dispatch clock is invalid", ErrFairQueueNotReady)
	}
	return nil
}

// ClaimInboxFair applies weighted virtual-runtime scheduling over one eligible
// session head per tenant. A durable virtual clock serializes only short claim
// transactions; execution remains concurrent. Schedule/message locks and a
// fresh quota read keep MaxInflight and fencing inside that same boundary.
func (s *PostgresStore) ClaimInboxFair(ctx context.Context, owner string, leaseDuration time.Duration) (*InboxMessage, error) {
	if err := ValidateLeaseOwner(owner); err != nil {
		return nil, fmt.Errorf("claim inbox fair: %w", err)
	}
	if leaseDuration < time.Millisecond {
		return nil, fmt.Errorf("claim inbox fair: lease duration of at least 1ms is required")
	}
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("claim inbox fair begin: %w", err)
	}
	defer tx.Rollback()
	var virtualTime int64
	if err := tx.QueryRowContext(ctx, `
		SELECT virtual_time FROM inbox_fair_queue_clock WHERE singleton=TRUE FOR UPDATE`).Scan(&virtualTime); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: dispatch clock is missing", ErrFairQueueNotReady)
		}
		return nil, fmt.Errorf("claim inbox fair lock clock: %w", err)
	}

	const selectQuery = `
		WITH candidates AS (
			SELECT i.id, i.tenant_id,
			       ROW_NUMBER() OVER (
				   PARTITION BY i.tenant_id
				   ORDER BY COALESCE(i.next_attempt_at, i.created_at), i.id
			       ) AS tenant_rank
			FROM inbox_messages i
			WHERE (i.attempt_count < i.max_attempts OR i.status='WAITING_APPROVAL') AND (
				i.status='RECEIVED'
				OR (i.status='RETRY_WAIT' AND i.next_attempt_at <= clock_timestamp())
				OR (i.status='WAITING_APPROVAL' AND i.next_attempt_at <= clock_timestamp()
				    AND i.approval_deadline > clock_timestamp())
				OR (i.status='PROCESSING' AND i.lease_until <= clock_timestamp())
			) AND NOT EXISTS (
				SELECT 1
				FROM inbox_messages predecessor
				WHERE predecessor.tenant_id = i.tenant_id
				  AND predecessor.agent_app_name = i.agent_app_name
				  AND predecessor.session_id = i.session_id
				  AND predecessor.session_sequence < i.session_sequence
				  AND predecessor.status <> 'COMPLETED'
			)
		), heads AS (
			SELECT id, tenant_id FROM candidates WHERE tenant_rank=1
		), scored AS (
			SELECT h.id, h.tenant_id, s.weight, s.virtual_runtime,
			s.max_inflight, s.last_claimed_at,
			       (SELECT COUNT(*) FROM inbox_messages active
					WHERE active.tenant_id=h.tenant_id
					  AND active.status='PROCESSING'
					  AND active.lease_until > clock_timestamp()) AS inflight
			FROM heads h
			JOIN tenant_queue_schedule s ON s.tenant_id=h.tenant_id
		)
		SELECT i.id, i.tenant_id, i.channel_type, i.channel_account_id,
		       i.external_message_id, i.agent_app_name, i.conversation_id, i.reply_to_id,
		       i.user_id, i.session_id,
		       i.is_group_chat, i.session_owner_id, i.routing_version,
		       i.session_sequence, i.payload_hash, i.payload, i.trace_parent, i.status, i.attempt_count,
		       i.max_attempts, i.next_attempt_at, i.approval_deadline,
		       i.lease_owner, i.lease_version, i.lease_until, i.last_error, i.created_at, i.updated_at
		FROM scored s
		JOIN inbox_messages i ON i.id=s.id
		JOIN tenant_queue_schedule schedule ON schedule.tenant_id=s.tenant_id
		WHERE s.max_inflight=0 OR s.inflight < s.max_inflight
		ORDER BY GREATEST(s.virtual_runtime, $1) ASC,
		         s.last_claimed_at NULLS FIRST, s.tenant_id, i.id
		LIMIT 1
		FOR UPDATE OF i, schedule SKIP LOCKED`
	msg, err := scanInbox(tx.QueryRowContext(ctx, selectQuery, virtualTime))
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("claim inbox fair commit: %w", err)
		}
		return nil, ErrNoWork
	}
	if err != nil {
		return nil, fmt.Errorf("claim inbox fair select: %w", err)
	}

	// Read after obtaining the schedule lock, using a fresh Read Committed
	// snapshot rather than the earlier candidate count after a possible lock wait.
	var weight, maxInflight, tenantRuntime, inflight int64
	if err := tx.QueryRowContext(ctx, `
		SELECT weight, max_inflight, virtual_runtime,
		       (SELECT COUNT(*) FROM inbox_messages
		        WHERE tenant_id=$1 AND status='PROCESSING'
		          AND lease_until > clock_timestamp())
		FROM tenant_queue_schedule WHERE tenant_id=$1`, msg.TenantID).
		Scan(&weight, &maxInflight, &tenantRuntime, &inflight); err != nil {
		return nil, fmt.Errorf("claim inbox fair read locked policy: %w", err)
	}
	if maxInflight > 0 && inflight >= maxInflight {
		return nil, ErrNoWork
	}
	serviceStart := max(tenantRuntime, virtualTime)
	serviceFinish := saturatingAdd(serviceStart, fairQueueServiceCost(weight))
	const updateScheduleQuery = `
		UPDATE tenant_queue_schedule
		SET virtual_runtime=$2,
		    last_claimed_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE tenant_id=$1`
	if _, err := tx.ExecContext(ctx, updateScheduleQuery, msg.TenantID, serviceFinish); err != nil {
		return nil, fmt.Errorf("claim inbox fair update schedule: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE inbox_fair_queue_clock SET virtual_time=$1 WHERE singleton=TRUE`, serviceStart); err != nil {
		return nil, fmt.Errorf("claim inbox fair advance clock: %w", err)
	}
	approvalPending := msg.Status == InboxWaitingApproval
	const updateQuery = `
		UPDATE inbox_messages
		SET status='PROCESSING',
		    attempt_count=CASE WHEN $4 THEN attempt_count ELSE attempt_count+1 END,
		    lease_owner=$2, lease_version=lease_version+1,
		    lease_until=clock_timestamp() + ($3 * interval '1 millisecond'),
		    next_attempt_at=NULL, approval_deadline=NULL, updated_at=clock_timestamp()
		WHERE id=$1
		  AND (attempt_count < max_attempts OR status='WAITING_APPROVAL')
		  AND (
			status='RECEIVED'
			OR (status='RETRY_WAIT' AND next_attempt_at <= clock_timestamp())
			OR (status='WAITING_APPROVAL' AND next_attempt_at <= clock_timestamp()
			    AND approval_deadline > clock_timestamp())
			OR (status='PROCESSING' AND lease_until <= clock_timestamp())
		  )
		RETURNING attempt_count, lease_version, lease_until, updated_at`
	if err := tx.QueryRowContext(ctx, updateQuery, msg.ID, owner, leaseDuration.Milliseconds(), approvalPending).
		Scan(&msg.AttemptCount, &msg.Lease.Fence, &msg.Lease.Until, &msg.UpdatedAt); errors.Is(err, sql.ErrNoRows) {
		// A concurrent claimer/completer may have changed the row after the
		// candidate CTE observed it. Roll back the virtual-runtime increment and
		// report ordinary contention; never resurrect a terminal Inbox row.
		return nil, ErrNoWork
	} else if err != nil {
		return nil, fmt.Errorf("claim inbox fair update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim inbox fair commit: %w", err)
	}
	msg.Status = InboxProcessing
	msg.ApprovalDeadline = nil
	msg.Lease.Owner = owner
	return msg, nil
}

func (s *PostgresStore) RenewInbox(ctx context.Context, id int64, lease Lease, leaseDuration time.Duration) (Lease, error) {
	if leaseDuration < time.Millisecond {
		return Lease{}, fmt.Errorf("renew inbox: lease duration of at least 1ms is required")
	}
	db, err := s.database()
	if err != nil {
		return Lease{}, err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Lease{}, fmt.Errorf("renew inbox begin: %w", err)
	}
	defer tx.Rollback()
	var tenantID string
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM inbox_messages WHERE id=$1`, id).Scan(&tenantID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Lease{}, ErrStaleLease
		}
		return Lease{}, fmt.Errorf("renew inbox tenant: %w", err)
	}
	// Admission must observe a committed renewal before counting active leases.
	// Lock schedule before Inbox, matching enqueue's duplicate-admission order.
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_queue_schedule(tenant_id) VALUES($1) ON CONFLICT (tenant_id) DO NOTHING`, tenantID); err != nil {
		return Lease{}, fmt.Errorf("renew inbox ensure schedule: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM tenant_queue_schedule WHERE tenant_id=$1 FOR UPDATE`, tenantID).Scan(&tenantID); err != nil {
		return Lease{}, fmt.Errorf("renew inbox schedule lock: %w", err)
	}
	if err := lockInboxRow(ctx, tx, id); err != nil {
		return Lease{}, fmt.Errorf("renew inbox lock: %w", err)
	}
	const query = `
		UPDATE inbox_messages
		SET lease_until=clock_timestamp() + ($4 * interval '1 millisecond'), updated_at=now()
		WHERE id=$1 AND status='PROCESSING' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()
		RETURNING lease_version, lease_until`
	var fence int64
	var until time.Time
	if err := tx.QueryRowContext(ctx, query, id, lease.Owner, lease.Fence, leaseDuration.Milliseconds()).
		Scan(&fence, &until); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Lease{}, ErrStaleLease
		}
		return Lease{}, fmt.Errorf("renew inbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, fmt.Errorf("renew inbox commit: %w", err)
	}
	return Lease{Owner: lease.Owner, Fence: fence, Until: until}, nil
}

func (s *PostgresStore) CompleteInbox(ctx context.Context, id int64, lease Lease, reply OutboxReply) (*OutboxMessage, error) {
	return s.completeInbox(ctx, id, lease, reply, nil)
}

// CompleteInboxWithSummary commits the Inbox terminal state, Outbox reply and
// Summary scheduling receipt under one PostgreSQL transaction and row fence.
func (s *PostgresStore) CompleteInboxWithSummary(
	ctx context.Context,
	id int64,
	lease Lease,
	reply OutboxReply,
	request summarycoord.EnqueueRequest,
) (*OutboxMessage, error) {
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("%w: invalid receipt: %v", ErrSummaryCompletionConflict, err)
	}
	// Retry policy and force-requeue are operator-owned controls. A Worker may
	// schedule normal derived work but cannot reset attempts or enlarge them.
	if request.Force || request.MaxAttempts != 0 || request.FilterKey != "" {
		return nil, fmt.Errorf("%w: worker receipt contains operator-only controls", ErrSummaryCompletionConflict)
	}
	return s.completeInbox(ctx, id, lease, reply, &request)
}

func (s *PostgresStore) completeInbox(
	ctx context.Context,
	id int64,
	lease Lease,
	reply OutboxReply,
	summaryRequest *summarycoord.EnqueueRequest,
) (*OutboxMessage, error) {
	if err := prepareOutboxReply(&reply); err != nil {
		return nil, err
	}
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	ctx = nonNilContext(ctx)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("complete inbox begin: %w", err)
	}
	defer tx.Rollback()
	if err := lockInboxRow(ctx, tx, id); err != nil {
		return nil, fmt.Errorf("complete inbox lock: %w", err)
	}

	const completeQuery = `
		UPDATE inbox_messages
		SET status='COMPLETED', lease_owner=NULL, lease_until=NULL,
		    approval_deadline=NULL, last_error='', updated_at=now()
		WHERE id=$1 AND status='PROCESSING' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()
		RETURNING tenant_id, channel_type, channel_account_id,
		          conversation_id, reply_to_id, agent_app_name,
		          session_owner_id, session_id, session_sequence`
	stored := &OutboxMessage{
		InboxID:     id,
		ContentType: reply.ContentType,
		Content:     reply.Content,
		TraceParent: reply.TraceParent,
		MaxAttempts: defaultOutboxMaxAttempts,
		Status:      OutboxPending,
	}
	var storedSessionOwnerID string
	if err := tx.QueryRowContext(ctx, completeQuery, id, lease.Owner, lease.Fence).Scan(
		&stored.TenantID, &stored.ChannelType, &stored.ChannelAccountID,
		&stored.ConversationID, &stored.ReplyToID, &stored.AgentApp,
		&storedSessionOwnerID, &stored.SessionID, &stored.SessionSequence,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrStaleLease
		}
		return nil, fmt.Errorf("complete inbox update: %w", err)
	}

	const insertQuery = `
		INSERT INTO outbox_messages (
			inbox_id, tenant_id, agent_app_name, session_id, session_sequence,
			channel_type, channel_account_id, conversation_id, reply_to_id,
			content_type, content, trace_parent, status, max_attempts
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'REPLY_PENDING',$13)
		ON CONFLICT (inbox_id) DO NOTHING
		RETURNING id, created_at, updated_at`
	err = tx.QueryRowContext(ctx, insertQuery,
		id, stored.TenantID, stored.AgentApp, stored.SessionID, stored.SessionSequence,
		stored.ChannelType, stored.ChannelAccountID, stored.ConversationID,
		stored.ReplyToID, stored.ContentType, stored.Content, stored.TraceParent,
		stored.MaxAttempts,
	).Scan(&stored.ID, &stored.CreatedAt, &stored.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("complete inbox: %w for inbox %d", ErrOutboxConflict, id)
	}
	if err != nil {
		return nil, fmt.Errorf("complete inbox insert outbox: %w", err)
	}
	if summaryRequest != nil {
		if summaryRequest.TenantID != stored.TenantID ||
			summaryRequest.SessionOwnerID != storedSessionOwnerID ||
			summaryRequest.SessionID != stored.SessionID {
			return nil, fmt.Errorf("%w: receipt does not match leased inbox scope", ErrSummaryCompletionConflict)
		}
		var validPinnedIdentity bool
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM agent_apps aa
				JOIN agent_versions av ON av.agent_app_id=aa.id
				WHERE aa.id=$1 AND aa.tenant_id=$2 AND aa.name=$3
				  AND av.id=$4 AND av.status IN ('published','retired')
			)`, summaryRequest.AgentAppID, stored.TenantID, stored.AgentApp,
			summaryRequest.AgentVersionID).Scan(&validPinnedIdentity); err != nil {
			return nil, fmt.Errorf("complete inbox verify summary identity: %w", err)
		}
		if !validPinnedIdentity {
			return nil, fmt.Errorf("%w: receipt app or version does not match leased inbox", ErrSummaryCompletionConflict)
		}
		if _, err := summarycoord.EnqueueTx(ctx, tx, *summaryRequest); err != nil {
			if errors.Is(err, summarycoord.ErrSummaryVersionConflict) ||
				errors.Is(err, summarycoord.ErrInvalidJob) {
				return nil, fmt.Errorf("%w: %v", ErrSummaryCompletionConflict, err)
			}
			return nil, fmt.Errorf("complete inbox enqueue summary: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("complete inbox commit: %w", err)
	}
	return stored, nil
}

func (s *PostgresStore) RetryInbox(ctx context.Context, id int64, lease Lease, cause error, nextAttempt time.Time) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("retry inbox begin: %w", err)
	}
	defer tx.Rollback()
	var lockedID int64
	if err := tx.QueryRowContext(ctx, `
		SELECT id FROM inbox_messages WHERE id=$1 FOR UPDATE`, id).Scan(&lockedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleLease
		}
		return fmt.Errorf("retry inbox lock: %w", err)
	}
	const query = `
		UPDATE inbox_messages
		SET status=CASE WHEN attempt_count >= max_attempts
		                THEN 'DEAD_LETTERED' ELSE 'RETRY_WAIT' END,
		    next_attempt_at=CASE WHEN attempt_count >= max_attempts
		                         THEN NULL ELSE $4::timestamptz END,
		    approval_deadline=NULL, lease_owner=NULL, lease_until=NULL,
		    last_error=$5, updated_at=now()
		WHERE id=$1 AND status='PROCESSING' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, nextAttempt, errorText(cause))
	if err != nil {
		return fmt.Errorf("retry inbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("retry inbox commit: %w", err)
	}
	return nil
}

// RetryInboxAfter derives the deadline from PostgreSQL's clock. Claim and
// retry then use one time authority even when consumer nodes have skewed
// clocks.
func (s *PostgresStore) RetryInboxAfter(ctx context.Context, id int64, lease Lease, cause error, delay time.Duration) error {
	if delay < 0 {
		return fmt.Errorf("retry inbox: negative delay is invalid")
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("retry inbox after begin: %w", err)
	}
	defer tx.Rollback()
	var lockedID int64
	if err := tx.QueryRowContext(ctx, `
		SELECT id FROM inbox_messages WHERE id=$1 FOR UPDATE`, id).Scan(&lockedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleLease
		}
		return fmt.Errorf("retry inbox after lock: %w", err)
	}
	const query = `
		UPDATE inbox_messages
		SET status=CASE WHEN attempt_count >= max_attempts
		                THEN 'DEAD_LETTERED' ELSE 'RETRY_WAIT' END,
		    next_attempt_at=CASE WHEN attempt_count >= max_attempts
		                         THEN NULL ELSE clock_timestamp() + ($4 * interval '1 microsecond') END,
		    approval_deadline=NULL, lease_owner=NULL, lease_until=NULL,
		    last_error=$5, updated_at=now()
		WHERE id=$1 AND status='PROCESSING' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, delay.Microseconds(), errorText(cause))
	if err != nil {
		return fmt.Errorf("retry inbox after: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("retry inbox after commit: %w", err)
	}
	return nil
}

// WaitInboxApproval parks a claimed message until the operator grant is
// visible. The approval path has not executed the dangerous tool, so the
// ordinary transient-attempt budget is preserved. PostgreSQL's clock remains
// authoritative for both the poll deadline and the challenge expiration.
func (s *PostgresStore) WaitInboxApproval(ctx context.Context, id int64, lease Lease, cause error, delay time.Duration, deadline time.Time) error {
	if delay < 0 {
		return fmt.Errorf("wait inbox approval: negative delay is invalid")
	}
	deadline = deadline.UTC()
	if deadline.IsZero() {
		return fmt.Errorf("wait inbox approval: %w", ErrApprovalDeadlineInvalid)
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	valid, err := approvalDeadlineValid(ctx, db, deadline)
	if err != nil {
		return fmt.Errorf("validate approval deadline: %w", err)
	}
	if !valid {
		return fmt.Errorf("wait inbox approval: %w", ErrApprovalDeadlineInvalid)
	}
	tx, err := beginLockedInbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("wait inbox approval lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE inbox_messages
		SET status='WAITING_APPROVAL',
		    next_attempt_at=LEAST(clock_timestamp() + ($4 * interval '1 microsecond'), $5),
		    approval_deadline=$5,
		    lease_owner=NULL, lease_until=NULL, last_error=$6, updated_at=clock_timestamp()
		WHERE id=$1 AND status='PROCESSING' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()
		  AND $5 > clock_timestamp()
		  AND $5 <= clock_timestamp() + ($7 * interval '1 microsecond')`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence,
		delay.Microseconds(), deadline, errorText(cause), MaxApprovalWait.Microseconds())
	if err != nil {
		return fmt.Errorf("wait inbox approval: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		_ = tx.Rollback()
		// The deadline can expire after the preflight and before the fenced
		// UPDATE. Classify that case using PostgreSQL time instead of treating
		// it as an ownership loss and waiting for the lease to expire.
		valid, err := approvalDeadlineValid(ctx, db, deadline)
		if err != nil {
			return fmt.Errorf("revalidate approval deadline: %w", err)
		}
		if !valid {
			return fmt.Errorf("wait inbox approval: %w", ErrApprovalDeadlineInvalid)
		}
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("wait inbox approval commit: %w", err)
	}
	return nil
}

// approvalDeadlineValid evaluates the approval wait window against the
// PostgreSQL clock. It is deliberately separate from Consumer wall time so
// nodes with ordinary clock skew cannot change durable queue state.
func approvalDeadlineValid(ctx context.Context, db *sql.DB, deadline time.Time) (bool, error) {
	const query = `
		SELECT $1 > clock_timestamp()
		   AND $1 <= clock_timestamp() + ($2 * INTERVAL '1 microsecond')`
	var valid bool
	if err := db.QueryRowContext(ctx, query, deadline, MaxApprovalWait.Microseconds()).Scan(&valid); err != nil {
		return false, err
	}
	return valid, nil
}

// BlockInbox removes the processing lease without scheduling another
// automatic attempt. The message remains durable and can only be resumed by
// an audited operator replay after execution reconciliation.
func (s *PostgresStore) BlockInbox(ctx context.Context, id int64, lease Lease, cause error) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedInbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("block inbox lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE inbox_messages
		SET status='WAITING_RECONCILIATION', next_attempt_at=NULL,
		    approval_deadline=NULL, lease_owner=NULL, lease_until=NULL,
		    last_error=$4, updated_at=now()
		WHERE id=$1 AND status='PROCESSING' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, errorText(cause))
	if err != nil {
		return fmt.Errorf("block inbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("block inbox commit: %w", err)
	}
	return nil
}

// DeadLetterInbox records a deterministic, non-retryable failure immediately.
func (s *PostgresStore) DeadLetterInbox(ctx context.Context, id int64, lease Lease, cause error) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedInbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("dead-letter inbox lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE inbox_messages
		SET status='DEAD_LETTERED', next_attempt_at=NULL,
		    approval_deadline=NULL, lease_owner=NULL, lease_until=NULL,
		    last_error=$4, updated_at=now()
		WHERE id=$1 AND status='PROCESSING' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, errorText(cause))
	if err != nil {
		return fmt.Errorf("dead-letter inbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("dead-letter inbox commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) ReplayInbox(ctx context.Context, id int64, actor, reason string) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("replay inbox: actor and reason are required")
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("replay inbox begin: %w", err)
	}
	defer tx.Rollback()
	var tenantID string
	if err := tx.QueryRowContext(ctx, `
		SELECT i.tenant_id
		FROM inbox_messages i
		JOIN tenants t ON t.id=i.tenant_id
		WHERE i.id=$1
		  AND i.status IN ('DEAD_LETTERED','WAITING_RECONCILIATION')
		  AND t.status='active'
		FOR UPDATE OF i, t`, id).Scan(&tenantID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("replay inbox: message is not awaiting replay")
		}
		return fmt.Errorf("replay inbox select: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE inbox_messages
		SET status='RECEIVED', attempt_count=0, next_attempt_at=NULL,
		    approval_deadline=NULL, lease_owner=NULL, lease_version=lease_version+1, lease_until=NULL,
		    last_error='', updated_at=now()
		WHERE id=$1 AND status IN ('DEAD_LETTERED','WAITING_RECONCILIATION')`, id)
	if err != nil {
		return fmt.Errorf("replay inbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("replay inbox: message is not awaiting replay")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO message_replay_audit (queue_type,message_id,tenant_id,requested_by,reason,replay_mode) VALUES ('inbox',$1,$2,$3,$4,'restart')`, id, tenantID, actor, reason); err != nil {
		return fmt.Errorf("replay inbox audit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("replay inbox commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) ClaimOutbox(ctx context.Context, owner string, leaseDuration time.Duration) (*OutboxMessage, error) {
	if err := ValidateLeaseOwner(owner); err != nil {
		return nil, fmt.Errorf("claim outbox: %w", err)
	}
	if leaseDuration < time.Millisecond {
		return nil, fmt.Errorf("claim outbox: lease duration of at least 1ms is required")
	}
	db, err := s.database()
	if err != nil {
		return nil, err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("claim outbox begin: %w", err)
	}
	defer tx.Rollback()

	const selectQuery = `
		SELECT id, inbox_id, tenant_id, agent_app_name, session_id, session_sequence,
		       channel_type, channel_account_id,
		       conversation_id, reply_to_id, content_type, content,
		       trace_parent, delivery_cursor, status, attempt_count, max_attempts,
		       next_attempt_at, lease_owner, lease_version, lease_until,
		       last_error, delivered_at, created_at, updated_at
		FROM outbox_messages AS candidate
		WHERE attempt_count < max_attempts AND (
			status='REPLY_PENDING'
			OR (status='RETRY_WAIT' AND next_attempt_at <= clock_timestamp())
			OR (status='DELIVERING' AND lease_until <= clock_timestamp())
		)
		AND NOT EXISTS (
			SELECT 1
			FROM outbox_messages AS predecessor
			WHERE predecessor.tenant_id = candidate.tenant_id
			  AND predecessor.agent_app_name = candidate.agent_app_name
			  AND predecessor.session_id = candidate.session_id
			  AND predecessor.session_sequence < candidate.session_sequence
			  AND predecessor.status <> 'REPLIED'
		)
		ORDER BY COALESCE(next_attempt_at, created_at), id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`
	msg, err := scanOutbox(tx.QueryRowContext(ctx, selectQuery))
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("claim outbox commit: %w", err)
		}
		return nil, ErrNoWork
	}
	if err != nil {
		return nil, fmt.Errorf("claim outbox select: %w", err)
	}

	const updateQuery = `
		UPDATE outbox_messages
		SET status='DELIVERING', attempt_count=attempt_count+1,
		    lease_owner=$2, lease_version=lease_version+1,
		    lease_until=clock_timestamp() + ($3 * interval '1 millisecond'),
		    next_attempt_at=NULL, updated_at=now()
		WHERE id=$1
		RETURNING attempt_count, lease_version, lease_until, updated_at`
	if err := tx.QueryRowContext(ctx, updateQuery, msg.ID, owner, leaseDuration.Milliseconds()).
		Scan(&msg.AttemptCount, &msg.Lease.Fence, &msg.Lease.Until, &msg.UpdatedAt); err != nil {
		return nil, fmt.Errorf("claim outbox update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim outbox commit: %w", err)
	}
	msg.Status = OutboxDelivering
	msg.Lease.Owner = owner
	return msg, nil
}

// ReapExpired terminalizes bounded batches of expired work outside the Claim
// hot paths. The single Inbox candidate CTE takes at most batchSize row locks
// with SKIP LOCKED, so several reaper replicas can progress without waiting
// for one another. The method
// leaves lease_version unchanged: the terminal status alone rejects a late
// worker commit, while ReplayInbox and ReplayOutbox advance the fence before
// any new execution can begin.
func (s *PostgresStore) ReapExpired(ctx context.Context, batchSize int) (ExpiredWorkReapResult, error) {
	if err := ValidateExpiredWorkReapBatchSize(batchSize); err != nil {
		return ExpiredWorkReapResult{}, err
	}
	db, err := s.database()
	if err != nil {
		return ExpiredWorkReapResult{}, err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ExpiredWorkReapResult{}, fmt.Errorf("reap expired begin: %w", err)
	}
	defer tx.Rollback()

	const inboxQuery = `
		WITH selected AS (
			SELECT id, status
			FROM inbox_messages
			WHERE (status='PROCESSING'
			       AND attempt_count >= max_attempts
			       AND lease_until <= clock_timestamp())
			   OR (status='WAITING_APPROVAL'
			       AND (approval_deadline IS NULL OR approval_deadline <= clock_timestamp()))
			ORDER BY CASE WHEN status='WAITING_APPROVAL' THEN approval_deadline ELSE lease_until END NULLS FIRST, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE inbox_messages AS message
			SET status='DEAD_LETTERED', approval_deadline=NULL,
			    next_attempt_at=NULL, lease_owner=NULL, lease_until=NULL,
			    last_error=CASE
			        WHEN selected.status='WAITING_APPROVAL' THEN 'tool approval expired'
			        ELSE COALESCE(NULLIF(message.last_error,''),'lease expired after final attempt')
			    END,
			    updated_at=now()
			FROM selected
			WHERE message.id=selected.id
			RETURNING selected.status AS previous_status
		)
		SELECT previous_status, COUNT(*)::bigint
		FROM updated
		GROUP BY previous_status`
	rows, err := tx.QueryContext(ctx, inboxQuery, batchSize)
	if err != nil {
		return ExpiredWorkReapResult{}, fmt.Errorf("reap expired inbox: %w", err)
	}
	result := ExpiredWorkReapResult{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return ExpiredWorkReapResult{}, fmt.Errorf("scan reaped inbox: %w", err)
		}
		switch InboxStatus(status) {
		case InboxProcessing:
			result.InboxFinalAttemptExpired += int(count)
		case InboxWaitingApproval:
			result.InboxApprovalExpired += int(count)
		default:
			rows.Close()
			return ExpiredWorkReapResult{}, fmt.Errorf("reap expired inbox: unexpected previous status %q", status)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ExpiredWorkReapResult{}, fmt.Errorf("iterate reaped inbox: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ExpiredWorkReapResult{}, fmt.Errorf("close reaped inbox rows: %w", err)
	}

	const outboxQuery = `
		WITH expired AS (
			SELECT id, status
			FROM outbox_messages
			WHERE ((status='DELIVERING' AND attempt_count >= max_attempts)
			       OR status='DISPATCH_STARTED')
			  AND (lease_until IS NULL OR lease_until <= clock_timestamp())
			ORDER BY lease_until NULLS FIRST, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), updated AS (
			UPDATE outbox_messages AS message
			SET status=CASE WHEN expired.status='DISPATCH_STARTED'
			                THEN 'WAITING_RECONCILIATION' ELSE 'DEAD_LETTERED' END,
			    next_attempt_at=NULL,
			    lease_owner=NULL, lease_until=NULL,
			    last_error=CASE WHEN expired.status='DISPATCH_STARTED'
			                    THEN 'dispatch lease expired; provider outcome unknown'
			                    ELSE COALESCE(NULLIF(message.last_error,''),'lease expired after final attempt') END,
			    updated_at=now()
			FROM expired
			WHERE message.id=expired.id
			RETURNING expired.status AS previous_status
		)
		SELECT previous_status, COUNT(*)::bigint FROM updated GROUP BY previous_status`
	rows, err = tx.QueryContext(ctx, outboxQuery, batchSize)
	if err != nil {
		return ExpiredWorkReapResult{}, fmt.Errorf("reap expired outbox: %w", err)
	}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return ExpiredWorkReapResult{}, fmt.Errorf("scan reaped outbox: %w", err)
		}
		switch OutboxStatus(status) {
		case OutboxDelivering:
			result.OutboxFinalAttemptExpired += int(count)
		case OutboxDispatchStarted:
			result.OutboxDispatchUnknown += int(count)
		default:
			rows.Close()
			return ExpiredWorkReapResult{}, fmt.Errorf("reap expired outbox: unexpected previous status %q", status)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ExpiredWorkReapResult{}, fmt.Errorf("iterate reaped outbox: %w", err)
	}
	if err := rows.Close(); err != nil {
		return ExpiredWorkReapResult{}, fmt.Errorf("close reaped outbox rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ExpiredWorkReapResult{}, fmt.Errorf("reap expired commit: %w", err)
	}
	return result, nil
}

// InspectQueue returns a consistent read-only snapshot of worker-owned queue
// rows. Reconciliation and terminal rows are excluded because they need
// operator action rather than automatic worker capacity.
func (s *PostgresStore) InspectQueue(ctx context.Context) (QueueStats, error) {
	db, err := s.database()
	if err != nil {
		return QueueStats{}, err
	}
	ctx = nonNilContext(ctx)
	const query = `
		SELECT
			(SELECT COUNT(*) FROM inbox_messages
			 WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL')),
			(SELECT MIN(created_at) FROM inbox_messages
			 WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL')),
			(SELECT COUNT(*) FROM outbox_messages
			 WHERE status IN ('REPLY_PENDING','DELIVERING','DISPATCH_STARTED','RETRY_WAIT')),
			(SELECT MIN(created_at) FROM outbox_messages
			 WHERE status IN ('REPLY_PENDING','DELIVERING','DISPATCH_STARTED','RETRY_WAIT')),
			clock_timestamp()`
	var stats QueueStats
	var inboxOldest, outboxOldest sql.NullTime
	if err := db.QueryRowContext(ctx, query).Scan(
		&stats.InboxDepth, &inboxOldest, &stats.OutboxDepth, &outboxOldest, &stats.ObservedAt,
	); err != nil {
		return QueueStats{}, fmt.Errorf("inspect queue: %w", err)
	}
	if inboxOldest.Valid {
		stats.InboxOldest = inboxOldest.Time
	}
	if outboxOldest.Valid {
		stats.OutboxOldest = outboxOldest.Time
	}
	return stats, nil
}

func (s *PostgresStore) RenewOutbox(ctx context.Context, id int64, lease Lease, leaseDuration time.Duration) (Lease, error) {
	if leaseDuration < time.Millisecond {
		return Lease{}, fmt.Errorf("renew outbox: lease duration of at least 1ms is required")
	}
	db, err := s.database()
	if err != nil {
		return Lease{}, err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedOutbox(ctx, db, id)
	if err != nil {
		return Lease{}, fmt.Errorf("renew outbox lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE outbox_messages
		SET lease_until=clock_timestamp() + ($4 * interval '1 millisecond'), updated_at=now()
		WHERE id=$1 AND status IN ('DELIVERING','DISPATCH_STARTED') AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()
		RETURNING lease_version, lease_until`
	var fence int64
	var until time.Time
	if err := tx.QueryRowContext(ctx, query, id, lease.Owner, lease.Fence, leaseDuration.Milliseconds()).
		Scan(&fence, &until); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Lease{}, ErrStaleLease
		}
		return Lease{}, fmt.Errorf("renew outbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, fmt.Errorf("renew outbox commit: %w", err)
	}
	return Lease{Owner: lease.Owner, Fence: fence, Until: until}, nil
}

// MarkDispatchStarted fences the transition across the provider side-effect
// boundary. A marked row is excluded from automatic claims after expiry.
func (s *PostgresStore) MarkDispatchStarted(ctx context.Context, id int64, lease Lease) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedOutbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("mark dispatch started lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE outbox_messages
		SET status='DISPATCH_STARTED', updated_at=now()
		WHERE id=$1 AND status='DELIVERING' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence)
	if err != nil {
		return fmt.Errorf("mark dispatch started: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mark dispatch started commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) MarkDelivered(ctx context.Context, id int64, lease Lease) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedOutbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("mark delivered lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE outbox_messages
		SET status='REPLIED', delivered_at=now(), lease_owner=NULL,
		    lease_until=NULL, last_error='', updated_at=now()
		WHERE id=$1 AND status='DISPATCH_STARTED' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mark delivered commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) AdvanceOutbox(ctx context.Context, id int64, lease Lease, nextCursor int) error {
	if nextCursor <= 0 {
		return fmt.Errorf("advance outbox: positive cursor is required")
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedOutbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("advance outbox lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE outbox_messages
		SET status='REPLY_PENDING', delivery_cursor=$4, attempt_count=0,
		    next_attempt_at=NULL, lease_owner=NULL, lease_until=NULL,
		    last_error='', updated_at=now()
		WHERE id=$1 AND status='DISPATCH_STARTED' AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()
		  AND delivery_cursor < $4`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, nextCursor)
	if err != nil {
		return fmt.Errorf("advance outbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("advance outbox commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) RetryOutbox(ctx context.Context, id int64, lease Lease, cause error, nextAttempt time.Time, cursor int) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedOutbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("retry outbox lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE outbox_messages
		SET status=CASE WHEN attempt_count >= max_attempts
		                THEN 'DEAD_LETTERED' ELSE 'RETRY_WAIT' END,
		    next_attempt_at=CASE WHEN attempt_count >= max_attempts
		                         THEN NULL ELSE $4 END,
		    delivery_cursor=GREATEST(delivery_cursor,$6),
		    lease_owner=NULL, lease_until=NULL, last_error=$5, updated_at=now()
		WHERE id=$1 AND status IN ('DELIVERING','DISPATCH_STARTED') AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, nextAttempt, errorText(cause), cursor)
	if err != nil {
		return fmt.Errorf("retry outbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("retry outbox commit: %w", err)
	}
	return nil
}

// RetryOutboxAfter is the delivery counterpart of RetryInboxAfter.
func (s *PostgresStore) RetryOutboxAfter(ctx context.Context, id int64, lease Lease, cause error, delay time.Duration, cursor int) error {
	if delay < 0 {
		return fmt.Errorf("retry outbox: negative delay is invalid")
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedOutbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("retry outbox after lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE outbox_messages
		SET status=CASE WHEN attempt_count >= max_attempts
		                THEN 'DEAD_LETTERED' ELSE 'RETRY_WAIT' END,
		    next_attempt_at=CASE WHEN attempt_count >= max_attempts
		                         THEN NULL ELSE clock_timestamp() + ($4 * interval '1 microsecond') END,
		    delivery_cursor=GREATEST(delivery_cursor,$6),
		    lease_owner=NULL, lease_until=NULL, last_error=$5, updated_at=now()
		WHERE id=$1 AND status IN ('DELIVERING','DISPATCH_STARTED') AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, delay.Microseconds(), errorText(cause), cursor)
	if err != nil {
		return fmt.Errorf("retry outbox after: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("retry outbox after commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) DeadLetterOutbox(ctx context.Context, id int64, lease Lease, cause error) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedOutbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("dead-letter outbox lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE outbox_messages
		SET status='DEAD_LETTERED', next_attempt_at=NULL,
		    lease_owner=NULL, lease_until=NULL, last_error=$4, updated_at=now()
		WHERE id=$1 AND status IN ('DELIVERING','DISPATCH_STARTED') AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, errorText(cause))
	if err != nil {
		return fmt.Errorf("dead-letter outbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("dead-letter outbox commit: %w", err)
	}
	return nil
}

// BlockOutbox stops automatic delivery until an operator reconciles the
// tenant/provider state and explicitly replays the message.
func (s *PostgresStore) BlockOutbox(ctx context.Context, id int64, lease Lease, cause error) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := beginLockedOutbox(ctx, db, id)
	if err != nil {
		return fmt.Errorf("block outbox lock: %w", err)
	}
	defer tx.Rollback()
	const query = `
		UPDATE outbox_messages
		SET status='WAITING_RECONCILIATION', next_attempt_at=NULL,
		    lease_owner=NULL, lease_until=NULL, last_error=$4, updated_at=now()
		WHERE id=$1 AND status IN ('DELIVERING','DISPATCH_STARTED') AND lease_owner=$2
		  AND lease_version=$3 AND lease_until > clock_timestamp()`
	result, err := tx.ExecContext(ctx, query, id, lease.Owner, lease.Fence, errorText(cause))
	if err != nil {
		return fmt.Errorf("block outbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("block outbox commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) ReplayOutbox(ctx context.Context, id int64, actor, reason string) error {
	return s.replayOutbox(ctx, "", id, actor, reason, OutboxReplayResume)
}

// ReplayOutboxForTenant atomically verifies that the message belongs to the
// requested tenant before changing its delivery state and writing the audit.
// Administrative callers must use this scoped entry point.
func (s *PostgresStore) ReplayOutboxForTenant(ctx context.Context, tenantID string, id int64, actor, reason string) error {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("replay outbox: tenant id is required")
	}
	return s.replayOutbox(ctx, tenantID, id, actor, reason, OutboxReplayResume)
}

func (s *PostgresStore) RestartOutbox(ctx context.Context, id int64, actor, reason string) error {
	return s.replayOutbox(ctx, "", id, actor, reason, OutboxReplayRestart)
}

func (s *PostgresStore) replayOutbox(ctx context.Context, expectedTenant string, id int64, actor, reason string, mode OutboxReplayMode) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("replay outbox: actor and reason are required")
	}
	if err := validateOutboxReplayMode(mode); err != nil {
		return err
	}
	db, err := s.database()
	if err != nil {
		return err
	}
	ctx = nonNilContext(ctx)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("replay outbox begin: %w", err)
	}
	defer tx.Rollback()
	var tenantID string
	if err := tx.QueryRowContext(ctx, `
		SELECT o.tenant_id
		FROM outbox_messages o
		JOIN tenants t ON t.id=o.tenant_id
		WHERE o.id=$1
		  AND ($2='' OR o.tenant_id=$2)
		  AND o.status IN ('DEAD_LETTERED','WAITING_RECONCILIATION')
		  AND t.status='active'
		FOR UPDATE OF o, t`, id, expectedTenant).Scan(&tenantID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("replay outbox: message is not dead-lettered or awaiting reconciliation")
		}
		return fmt.Errorf("replay outbox select: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE outbox_messages
		SET status='REPLY_PENDING', attempt_count=0, next_attempt_at=NULL,
		    delivery_cursor=CASE WHEN $2='restart' THEN 0 ELSE delivery_cursor END,
		    lease_owner=NULL, lease_version=lease_version+1, lease_until=NULL,
		    last_error='', updated_at=now()
		WHERE id=$1 AND ($3='' OR tenant_id=$3)
		  AND status IN ('DEAD_LETTERED','WAITING_RECONCILIATION')`, id, mode, expectedTenant)
	if err != nil {
		return fmt.Errorf("replay outbox: %w", err)
	}
	if ok, err := changedOne(result); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("replay outbox: message is not dead-lettered or awaiting reconciliation")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO message_replay_audit (queue_type,message_id,tenant_id,requested_by,reason,replay_mode) VALUES ('outbox',$1,$2,$3,$4,$5)`, id, tenantID, actor, reason, mode); err != nil {
		return fmt.Errorf("replay outbox audit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("replay outbox commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) PingContext(ctx context.Context) error {
	db, err := s.database()
	if err != nil {
		return err
	}
	return db.PingContext(nonNilContext(ctx))
}

func (s *PostgresStore) Close() error {
	db, err := s.database()
	if err != nil {
		return err
	}
	if s.ownsDB {
		return db.Close()
	}
	return nil
}

func (s *PostgresStore) database() (*sql.DB, error) {
	if s == nil || s.db == nil {
		return nil, ErrStoreUnavailable
	}
	return s.db, nil
}

// beginLockedInbox and beginLockedOutbox establish the row lock before a
// fenced mutation evaluates clock_timestamp(). PostgreSQL can evaluate an
// UPDATE predicate before waiting for a concurrent row lock; separating the
// lock acquisition makes the post-wait lease check deterministic.
func beginLockedInbox(ctx context.Context, db *sql.DB, id int64) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if err := lockInboxRow(ctx, tx, id); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func beginLockedOutbox(ctx context.Context, db *sql.DB, id int64) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if err := lockOutboxRow(ctx, tx, id); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func lockInboxRow(ctx context.Context, tx *sql.Tx, id int64) error {
	var lockedID int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM inbox_messages WHERE id=$1 FOR UPDATE", id).Scan(&lockedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleLease
		}
		return err
	}
	return nil
}

func lockOutboxRow(ctx context.Context, tx *sql.Tx, id int64) error {
	var lockedID int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM outbox_messages WHERE id=$1 FOR UPDATE", id).Scan(&lockedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleLease
		}
		return err
	}
	return nil
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanInbox(row rowScanner) (*InboxMessage, error) {
	msg := &InboxMessage{}
	var nextAttempt, approvalDeadline, leaseUntil sql.NullTime
	var leaseOwner, lastError sql.NullString
	err := row.Scan(
		&msg.ID, &msg.TenantID, &msg.ChannelType, &msg.ChannelAccountID,
		&msg.ExternalMessageID, &msg.AgentApp, &msg.ConversationID, &msg.ReplyToID,
		&msg.UserID, &msg.SessionID, &msg.IsGroupChat, &msg.SessionOwnerID,
		&msg.RoutingVersion,
		&msg.SessionSequence, &msg.PayloadHash, &msg.Payload, &msg.TraceParent, &msg.Status,
		&msg.AttemptCount, &msg.MaxAttempts, &nextAttempt, &approvalDeadline, &leaseOwner,
		&msg.Lease.Fence, &leaseUntil, &lastError, &msg.CreatedAt, &msg.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if nextAttempt.Valid {
		msg.NextAttemptAt = &nextAttempt.Time
	}
	if approvalDeadline.Valid {
		msg.ApprovalDeadline = &approvalDeadline.Time
	}
	if leaseOwner.Valid {
		msg.Lease.Owner = leaseOwner.String
	}
	if leaseUntil.Valid {
		msg.Lease.Until = leaseUntil.Time
	}
	if lastError.Valid {
		msg.LastError = lastError.String
	}
	return msg, nil
}

func scanOutbox(row rowScanner) (*OutboxMessage, error) {
	msg := &OutboxMessage{}
	var nextAttempt, leaseUntil, deliveredAt sql.NullTime
	var leaseOwner, lastError sql.NullString
	err := row.Scan(
		&msg.ID, &msg.InboxID, &msg.TenantID, &msg.AgentApp,
		&msg.SessionID, &msg.SessionSequence, &msg.ChannelType,
		&msg.ChannelAccountID, &msg.ConversationID, &msg.ReplyToID,
		&msg.ContentType, &msg.Content, &msg.TraceParent, &msg.DeliveryCursor, &msg.Status,
		&msg.AttemptCount, &msg.MaxAttempts, &nextAttempt, &leaseOwner,
		&msg.Lease.Fence, &leaseUntil, &lastError, &deliveredAt,
		&msg.CreatedAt, &msg.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if nextAttempt.Valid {
		msg.NextAttemptAt = &nextAttempt.Time
	}
	if leaseOwner.Valid {
		msg.Lease.Owner = leaseOwner.String
	}
	if leaseUntil.Valid {
		msg.Lease.Until = leaseUntil.Time
	}
	if lastError.Valid {
		msg.LastError = lastError.String
	}
	if deliveredAt.Valid {
		msg.DeliveredAt = &deliveredAt.Time
	}
	return msg, nil
}

func prepareInbox(msg *InboxMessage) error {
	if msg == nil {
		return fmt.Errorf("%w: message is required", ErrInvalidInboxMessage)
	}
	if msg.ReplyToID == "" {
		msg.ReplyToID = msg.ExternalMessageID
	}
	if msg.MaxAttempts == 0 {
		msg.MaxAttempts = defaultInboxMaxAttempts
	}
	// A caller that supplies the new owner field but omits the version is
	// treated as a current writer. This keeps the Go API convenient while rows
	// read from pre-migration storage remain version zero because their owner is
	// empty. Old binaries that omit both fields continue to use the legacy
	// payload-proof path in Consumer.
	if msg.RoutingVersion == 0 && msg.SessionOwnerID != "" {
		msg.RoutingVersion = CurrentInboxRoutingVersion
	}
	missing := make([]string, 0, 10)
	if msg.TenantID == "" {
		missing = append(missing, "tenant_id")
	}
	if msg.ChannelType == "" {
		missing = append(missing, "channel_type")
	}
	if msg.ChannelAccountID == "" {
		missing = append(missing, "channel_account_id")
	}
	if msg.ExternalMessageID == "" {
		missing = append(missing, "external_message_id")
	}
	if msg.AgentApp == "" {
		missing = append(missing, "agent_app")
	}
	if msg.ConversationID == "" {
		missing = append(missing, "conversation_id")
	}
	if msg.ReplyToID == "" {
		missing = append(missing, "reply_to_id")
	}
	if msg.UserID == "" {
		missing = append(missing, "user_id")
	}
	if msg.SessionID == "" {
		missing = append(missing, "session_id")
	}
	if msg.PayloadHash == "" {
		missing = append(missing, "payload_hash")
	}
	if len(msg.Payload) == 0 {
		missing = append(missing, "payload")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing %s", ErrInvalidInboxMessage, strings.Join(missing, ", "))
	}
	if msg.RoutingVersion < 0 || msg.RoutingVersion > CurrentInboxRoutingVersion {
		return fmt.Errorf("%w: routing_version is unsupported", ErrInvalidInboxMessage)
	}
	if msg.RoutingVersion >= 1 && msg.SessionOwnerID == "" {
		return fmt.Errorf("%w: session_owner_id is required for routing version %d", ErrInvalidInboxMessage, msg.RoutingVersion)
	}
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{
		{name: "tenant_id", value: msg.TenantID, max: 64},
		{name: "channel_type", value: msg.ChannelType, max: 32},
		{name: "channel_account_id", value: msg.ChannelAccountID, max: 128},
		{name: "agent_app", value: msg.AgentApp, max: 128},
		{name: "external_message_id", value: msg.ExternalMessageID, max: 256},
		{name: "conversation_id", value: msg.ConversationID, max: 256},
		{name: "reply_to_id", value: msg.ReplyToID, max: 256},
		// The installed tRPC PostgreSQL Session backend uses VARCHAR(255) for
		// both user_id and session_id. Reject before enqueue instead of failing
		// later after a durable inbox row has already been accepted.
		{name: "user_id", value: msg.UserID, max: 255},
		{name: "session_id", value: msg.SessionID, max: 255},
		{name: "session_owner_id", value: msg.SessionOwnerID, max: 255},
		{name: "trace_parent", value: msg.TraceParent, max: 256},
	} {
		if !validPersistedIdentifier(field.value) {
			return fmt.Errorf("%w: %s is invalid", ErrInvalidInboxMessage, field.name)
		}
		if len(field.value) > field.max {
			return fmt.Errorf("%w: %s exceeds %d-byte limit", ErrInvalidInboxMessage, field.name, field.max)
		}
	}
	if len(msg.PayloadHash) != sha256.Size*2 {
		return fmt.Errorf("%w: payload_hash must be a SHA-256 hex digest", ErrInvalidInboxMessage)
	}
	if _, err := hex.DecodeString(msg.PayloadHash); err != nil {
		return fmt.Errorf("%w: payload_hash must be a SHA-256 hex digest", ErrInvalidInboxMessage)
	}
	if msg.MaxAttempts < 1 {
		return fmt.Errorf("%w: max_attempts must be positive", ErrInvalidInboxMessage)
	}
	return nil
}

func validPersistedIdentifier(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.In(character, unicode.Cf) {
			return false
		}
	}
	return true
}

func prepareOutboxReply(reply *OutboxReply) error {
	if reply == nil {
		return fmt.Errorf("%w: reply is required", ErrInvalidInboxMessage)
	}
	if reply.ContentType == "" {
		reply.ContentType = "text"
	}
	if reply.Content == "" {
		return fmt.Errorf("%w: reply content is required", ErrInvalidInboxMessage)
	}
	if !utf8.ValidString(reply.ContentType) || strings.ContainsAny(reply.ContentType, "\x00\r\n") || len(reply.ContentType) > 32 {
		return fmt.Errorf("%w: content_type is invalid", ErrInvalidInboxMessage)
	}
	if !utf8.ValidString(reply.Content) || strings.IndexByte(reply.Content, 0) >= 0 || len(reply.Content) > maxOutboxContentBytes {
		return fmt.Errorf("%w: content is invalid or exceeds %d-byte limit", ErrInvalidInboxMessage, maxOutboxContentBytes)
	}
	if !utf8.ValidString(reply.TraceParent) || strings.ContainsAny(reply.TraceParent, "\x00\r\n") || len(reply.TraceParent) > 256 {
		return fmt.Errorf("%w: trace_parent is invalid", ErrInvalidInboxMessage)
	}
	return nil
}

func changedOne(result sql.Result) (bool, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read rows affected: %w", err)
	}
	return rows == 1, nil
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ReplaceAll(err.Error(), "\x00", "\uFFFD")
	text = strings.ToValidUTF8(text, "\uFFFD")
	text = sensitiveURLCredentials.ReplaceAllString(text, `$1<redacted>@`)
	text = sensitiveBearerTokens.ReplaceAllString(text, `$1<redacted>`)
	text = sensitiveAuthorization.ReplaceAllString(text, `$1$2<redacted>`)
	text = sensitiveErrorFields.ReplaceAllString(text, `$1=<redacted>`)
	if len(text) > maxStoredErrorBytes {
		text = text[:maxStoredErrorBytes]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	return text
}

// Queue errors are operator-visible durable data. Redact common credential
// assignments before truncation so a provider or custom adapter cannot turn
// a retry/dead-letter record into a secret exfiltration channel.
var sensitiveErrorFields = regexp.MustCompile(`(?i)(api[_-]?key|access[_-]?token|token|secret|password|dsn)\s*[:=]\s*([^\s,;]+)`)

var sensitiveAuthorization = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)(bearer\s+)?[^\s,;]+`)

// Provider and database drivers commonly embed credentials in connection URLs
// instead of key/value assignments. Keep the scheme and host useful for
// diagnosis while removing the complete userinfo segment.
var sensitiveURLCredentials = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)(?:[^/\s:@]+(?::[^/\s@]*)?@)`)

var sensitiveBearerTokens = regexp.MustCompile(`(?i)(\bbearer\s+)[A-Za-z0-9._~+/=-]+`)
