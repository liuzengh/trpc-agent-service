package idempotency

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// SQLStore implements the production PostgreSQL Inbox claim transaction.
// Its advisory transaction lock allocates a per-session inbox_seq, while the
// primary key enforces delivery idempotency across Gateway nodes.
type SQLStore struct {
	DB          *sql.DB
	Now         func() time.Time
	MaxAttempts int
}

var _ Store = (*SQLStore)(nil)
var _ ReadyStore = (*SQLStore)(nil)
var _ ExecutionStore = (*SQLStore)(nil)

// Cancel marks an exact active SQL claim terminal.
func (store *SQLStore) Cancel(ctx context.Context, claim Claim) error {
	if err := validateSQLClaim(store, claim); err != nil {
		return err
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE inbox_messages SET status='canceled',completed_at=$7 WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 AND claim_owner=$4 AND claim_token=$5 AND status='processing' AND lease_until>$6`, claim.Message.TenantID, claim.Message.BindingID, claim.Message.ExternalMessageID, claim.Owner, claim.ClaimToken, now, now)
	return exactClaimResult(result, err)
}

func (store *SQLStore) Reject(ctx context.Context, claim Claim) error {
	if err := validateSQLClaim(store, claim); err != nil {
		return err
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE inbox_messages SET status='rejected', completed_at=$7, lease_until=NULL WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 AND claim_owner=$4 AND claim_token=$5 AND status='processing' AND lease_until>$6`, claim.Message.TenantID, claim.Message.BindingID, claim.Message.ExternalMessageID, claim.Owner, claim.ClaimToken, now, now)
	return exactClaimResult(result, err)
}

// Renew extends an unexpired SQL claim with an owner/token CAS.
func (store *SQLStore) Renew(ctx context.Context, claim Claim, ttl time.Duration) (Claim, error) {
	if err := validateSQLClaim(store, claim); err != nil {
		return Claim{}, err
	}
	if ttl <= 0 {
		return Claim{}, errors.New("idempotency: positive ttl is required")
	}
	now := store.now().UTC()
	until := now.Add(ttl)
	result, err := store.DB.ExecContext(ctx, `UPDATE inbox_messages SET lease_until=$7 WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 AND claim_owner=$4 AND claim_token=$5 AND status='processing' AND lease_until>$6`, claim.Message.TenantID, claim.Message.BindingID, claim.Message.ExternalMessageID, claim.Owner, claim.ClaimToken, now, until)
	if err := exactClaimResult(result, err); err != nil {
		return Claim{}, err
	}
	claim.LeaseUntil = until
	return claim, nil
}

// Claim atomically inserts or reclaims a delivery under serializable isolation.
func (store *SQLStore) Claim(ctx context.Context, message gateway.InboundMessage, owner string, ttl time.Duration) (Claim, bool, error) {
	if store == nil || store.DB == nil {
		return Claim{}, false, errors.New("idempotency: nil SQL database")
	}
	if message.TenantID == "" || message.AppID == "" || message.BindingID == "" || message.ExternalMessageID == "" || message.UserID == "" || message.SessionID == "" || message.ConfigVersion == 0 || owner == "" || ttl <= 0 {
		return Claim{}, false, errors.New("idempotency: complete tenant message, owner, and positive ttl are required")
	}
	if message.Media != nil {
		return Claim{}, false, errors.New("idempotency: unresolved media reference is not durable")
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Claim{}, false, err
	}
	defer tx.Rollback()
	now := store.now().UTC()
	scopeDigest := sha256.Sum256([]byte(message.TenantID + "\x00" + message.AppID + "\x00" + message.UserID + "\x00" + message.SessionID))
	scope := fmt.Sprintf("%x", scopeDigest)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, scope); err != nil {
		return Claim{}, false, err
	}
	var status Status
	var attempt int
	var inboxSeq uint64
	var leaseUntil, nextAttempt sql.NullTime
	var payload []byte
	var existingOwner, existingToken sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT status, attempts, inbox_seq, lease_until, next_attempt_at, payload_json, claim_owner, claim_token FROM inbox_messages WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 FOR UPDATE`, message.TenantID, message.BindingID, message.ExternalMessageID).Scan(&status, &attempt, &inboxSeq, &leaseUntil, &nextAttempt, &payload, &existingOwner, &existingToken)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Claim{}, false, err
	}
	if err == nil {
		var stored gateway.InboundMessage
		if err := json.Unmarshal(payload, &stored); err != nil {
			return Claim{}, false, err
		}
		existing := Claim{InboxID: InboxID(stored), Owner: existingOwner.String, ClaimToken: existingToken.String, Attempt: attempt, InboxSeq: inboxSeq, Status: status, LeaseUntil: leaseUntil.Time, Message: stored}
		if terminal(status) || (status == StatusProcessing && leaseUntil.Valid && now.Before(leaseUntil.Time)) || (status == StatusRetry && nextAttempt.Valid && now.Before(nextAttempt.Time)) {
			if err := tx.Commit(); err != nil {
				return Claim{}, false, err
			}
			return existing, false, nil
		}
		if attempt >= store.maxAttempts() {
			if _, err := tx.ExecContext(ctx, `UPDATE inbox_messages SET status='dlq', lease_until=NULL, next_attempt_at=NULL WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3`, message.TenantID, message.BindingID, message.ExternalMessageID); err != nil {
				return Claim{}, false, err
			}
			existing.Status = StatusDLQ
			if err := tx.Commit(); err != nil {
				return Claim{}, false, err
			}
			return existing, false, nil
		}
		attempt++
		token := claimToken(existing.InboxID, attempt, now)
		until := now.Add(ttl)
		if _, err := tx.ExecContext(ctx, `UPDATE inbox_messages SET status='processing', attempts=$4, claim_owner=$5, claim_token=$6, lease_until=$7, claimed_at=$8, next_attempt_at=NULL, last_error=NULL WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3`, message.TenantID, message.BindingID, message.ExternalMessageID, attempt, owner, token, until, now); err != nil {
			return Claim{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return Claim{}, false, err
		}
		return Claim{InboxID: existing.InboxID, Owner: owner, ClaimToken: token, Attempt: attempt, InboxSeq: inboxSeq, Status: StatusProcessing, LeaseUntil: until, Message: stored}, true, nil
	}
	var seq uint64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(inbox_seq),0)+1 FROM inbox_messages WHERE tenant_id=$1 AND app_id=$2 AND user_id=$3 AND session_id=$4`, message.TenantID, message.AppID, message.UserID, message.SessionID).Scan(&seq); err != nil {
		return Claim{}, false, err
	}
	payload, err = json.Marshal(message)
	if err != nil {
		return Claim{}, false, err
	}
	id := InboxID(message)
	token := claimToken(id, 1, now)
	until := now.Add(ttl)
	_, err = tx.ExecContext(ctx, `INSERT INTO inbox_messages (tenant_id,binding_id,external_message_id,inbox_id,app_id,user_id,session_id,config_version,inbox_seq,status,attempts,claim_owner,claim_token,lease_until,claimed_at,trace_id,payload_json) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'processing',1,$10,$11,$12,$13,$14,$15)`, message.TenantID, message.BindingID, message.ExternalMessageID, id, message.AppID, message.UserID, message.SessionID, message.ConfigVersion, seq, owner, token, until, now, message.TraceID, payload)
	if err != nil {
		return Claim{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Claim{}, false, err
	}
	return Claim{InboxID: id, Owner: owner, ClaimToken: token, Attempt: 1, InboxSeq: seq, Status: StatusProcessing, LeaseUntil: until, Message: message}, true, nil
}

// ClaimReady atomically reclaims due retries and expired processing leases.
// The NOT EXISTS guard keeps one session's inbox_seq ordered across workers.
func (store *SQLStore) ClaimReady(ctx context.Context, owner string, ttl time.Duration, limit int) ([]Claim, error) {
	if store == nil || store.DB == nil {
		return nil, errors.New("idempotency: nil SQL database")
	}
	if owner == "" || ttl <= 0 || limit <= 0 {
		return nil, errors.New("idempotency: owner, positive ttl, and limit are required")
	}
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := store.now().UTC()
	readyPredicate := `((candidate.status='retry' AND (candidate.next_attempt_at IS NULL OR candidate.next_attempt_at<=$1)) OR (candidate.status='processing' AND (candidate.lease_until IS NULL OR candidate.lease_until<=$1)))`
	if _, err := tx.ExecContext(ctx, `UPDATE inbox_messages AS candidate SET status='dlq',lease_until=NULL,next_attempt_at=NULL WHERE `+readyPredicate+` AND candidate.attempts>=$2`, now, store.maxAttempts()); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT candidate.tenant_id,candidate.binding_id,candidate.external_message_id,candidate.inbox_id,candidate.status,candidate.attempts,candidate.inbox_seq,candidate.payload_json
		FROM inbox_messages AS candidate
		WHERE `+readyPredicate+` AND candidate.attempts<$2
		AND NOT EXISTS (
			SELECT 1 FROM inbox_messages AS earlier
			WHERE earlier.tenant_id=candidate.tenant_id AND earlier.app_id=candidate.app_id
			AND earlier.user_id=candidate.user_id AND earlier.session_id=candidate.session_id
			AND earlier.inbox_seq<candidate.inbox_seq
			AND earlier.status NOT IN ('completed','canceled','rejected','dlq')
		)
		ORDER BY candidate.created_at,candidate.inbox_seq
		FOR UPDATE OF candidate SKIP LOCKED LIMIT $3`, now, store.maxAttempts(), limit)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		tenantID, bindingID, externalMessageID string
		inboxID                                string
		status                                 Status
		attempt                                int
		seq                                    uint64
		message                                gateway.InboundMessage
		invalid                                bool
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		var payload []byte
		if err := rows.Scan(&item.tenantID, &item.bindingID, &item.externalMessageID, &item.inboxID, &item.status, &item.attempt, &item.seq, &payload); err != nil {
			rows.Close()
			return nil, err
		}
		item.invalid = json.Unmarshal(payload, &item.message) != nil ||
			item.message.TenantID != item.tenantID || item.message.BindingID != item.bindingID ||
			item.message.ExternalMessageID != item.externalMessageID || item.message.AppID == "" ||
			item.message.UserID == "" || item.message.SessionID == "" || item.message.ConfigVersion == 0
		candidates = append(candidates, item)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	claims := make([]Claim, 0, len(candidates))
	until := now.Add(ttl)
	for _, item := range candidates {
		if item.invalid {
			if _, err := tx.ExecContext(ctx, `UPDATE inbox_messages SET status='dlq',lease_until=NULL,next_attempt_at=NULL,last_error='invalid durable inbox payload'
				WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3`, item.tenantID, item.bindingID, item.externalMessageID); err != nil {
				return nil, err
			}
			continue
		}
		attempt := item.attempt + 1
		token := claimToken(item.inboxID, attempt, now)
		result, err := tx.ExecContext(ctx, `UPDATE inbox_messages SET status='processing',attempts=$4,claim_owner=$5,claim_token=$6,lease_until=$7,claimed_at=$8,next_attempt_at=NULL,last_error=NULL WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3`, item.tenantID, item.bindingID, item.externalMessageID, attempt, owner, token, until, now)
		if err != nil {
			return nil, err
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			if err != nil {
				return nil, err
			}
			return nil, ErrClaimOwner
		}
		claims = append(claims, Claim{InboxID: item.inboxID, Owner: owner, ClaimToken: token, Attempt: attempt, InboxSeq: item.seq, Status: StatusProcessing, LeaseUntil: until, Message: item.message})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claims, nil
}

// Complete uses tenant scope, claim token, status, and lease in one CAS update.
func (store *SQLStore) Complete(ctx context.Context, claim Claim) error {
	if err := validateSQLClaim(store, claim); err != nil {
		return err
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE inbox_messages SET status='completed', completed_at=$7 WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 AND claim_owner=$4 AND claim_token=$5 AND status='processing' AND lease_until>$6`, claim.Message.TenantID, claim.Message.BindingID, claim.Message.ExternalMessageID, claim.Owner, claim.ClaimToken, now, now)
	return exactClaimResult(result, err)
}

func (store *SQLStore) GetExecution(ctx context.Context, claim Claim) (ExecutionRecord, error) {
	if err := validateSQLClaim(store, claim); err != nil {
		return ExecutionRecord{}, err
	}
	var stage ExecutionStage
	var reply, eventID sql.NullString
	err := store.DB.QueryRowContext(ctx, `SELECT COALESCE(execution_stage,'none'),execution_reply,execution_event_id FROM inbox_messages WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 AND claim_owner=$4 AND claim_token=$5`, claim.Message.TenantID, claim.Message.BindingID, claim.Message.ExternalMessageID, claim.Owner, claim.ClaimToken).Scan(&stage, &reply, &eventID)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionRecord{}, ErrClaimOwner
	}
	if err != nil {
		return ExecutionRecord{}, err
	}
	return ExecutionRecord{Stage: stage, Reply: reply.String, EventID: eventID.String}, nil
}

func (store *SQLStore) SaveExecution(ctx context.Context, claim Claim, execution ExecutionRecord) error {
	if err := validateSQLClaim(store, claim); err != nil {
		return err
	}
	if execution.Stage == ExecutionNone || execution.Reply == "" || execution.EventID == "" {
		return errors.New("idempotency: execution stage, reply, and event ID are required")
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE inbox_messages SET execution_stage=$6,execution_reply=$7,execution_event_id=$8 WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 AND claim_owner=$4 AND claim_token=$5 AND status='processing' AND lease_until>$9 AND (CASE execution_stage WHEN 'outbox_committed' THEN 3 WHEN 'derived_committed' THEN 2 WHEN 'runner_committed' THEN 1 ELSE 0 END < CASE $6 WHEN 'outbox_committed' THEN 3 WHEN 'derived_committed' THEN 2 WHEN 'runner_committed' THEN 1 ELSE 0 END OR (execution_stage=$6 AND execution_reply=$7 AND execution_event_id=$8))`, claim.Message.TenantID, claim.Message.BindingID, claim.Message.ExternalMessageID, claim.Owner, claim.ClaimToken, execution.Stage, execution.Reply, execution.EventID, now)
	return exactClaimResult(result, err)
}

// Fail schedules the exact current claim for retry.
func (store *SQLStore) Fail(ctx context.Context, claim Claim, cause error, retryAt time.Time) error {
	if err := validateSQLClaim(store, claim); err != nil {
		return err
	}
	lastError := ""
	if cause != nil {
		lastError = sanitizeError(cause.Error())
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE inbox_messages SET status='retry', next_attempt_at=$6, last_error=$7, lease_until=NULL WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 AND claim_owner=$4 AND claim_token=$5 AND status='processing' AND lease_until>$8`, claim.Message.TenantID, claim.Message.BindingID, claim.Message.ExternalMessageID, claim.Owner, claim.ClaimToken, retryAt.UTC(), lastError, now)
	return exactClaimResult(result, err)
}

// Defer yields an exact active claim because tenant capacity is full. The next
// claim restores the same attempt number, so healthy backpressure cannot drive
// a message into DLQ.
func (store *SQLStore) Defer(ctx context.Context, claim Claim, retryAt time.Time) error {
	if err := validateSQLClaim(store, claim); err != nil {
		return err
	}
	now := store.now().UTC()
	result, err := store.DB.ExecContext(ctx, `UPDATE inbox_messages SET status='retry',attempts=GREATEST(attempts-1,0),next_attempt_at=$6,last_error=NULL,claim_owner=NULL,claim_token=NULL,lease_until=NULL WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3 AND claim_owner=$4 AND claim_token=$5 AND status='processing' AND lease_until>$7`, claim.Message.TenantID, claim.Message.BindingID, claim.Message.ExternalMessageID, claim.Owner, claim.ClaimToken, retryAt.UTC(), now)
	return exactClaimResult(result, err)
}

func validateSQLClaim(store *SQLStore, claim Claim) error {
	if store == nil || store.DB == nil {
		return errors.New("idempotency: nil SQL database")
	}
	if claim.Message.TenantID == "" || claim.Message.BindingID == "" || claim.Message.ExternalMessageID == "" || claim.Owner == "" || claim.ClaimToken == "" {
		return errors.New("idempotency: tenant-scoped claim identity is required")
	}
	return nil
}
func exactClaimResult(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrClaimOwner
	}
	return nil
}
func claimToken(id string, attempt int, now time.Time) string {
	return fmt.Sprintf("%s#%d#%d", id, attempt, now.UnixNano())
}
func (store *SQLStore) now() time.Time {
	if store.Now != nil {
		return store.Now()
	}
	return time.Now()
}

func (store *SQLStore) maxAttempts() int {
	if store.MaxAttempts > 0 {
		return store.MaxAttempts
	}
	return 5
}
