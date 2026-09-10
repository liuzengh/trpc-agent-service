package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

var (
	// ErrReplyLeaseLost means a sender no longer owns the Reply Outbox row.
	ErrReplyLeaseLost = errors.New("reply outbox lease is lost")
	// ErrReplyTargetExpired means a message-scoped provider target cannot be
	// replaced with a stable user or conversation target.
	ErrReplyTargetExpired = errors.New("reply target is expired")
	errReplyStreamClosed  = errors.New("reply stream is already closed")
)

type storedReplyPayload struct {
	Kind           channels.ReplyKind   `json:"kind,omitempty"`
	StreamID       string               `json:"stream_id,omitempty"`
	StreamPhase    channels.StreamPhase `json:"stream_phase,omitempty"`
	StreamSequence int64                `json:"stream_sequence,omitempty"`
	Text           string               `json:"text,omitempty"`
	Card           *channels.ReplyCard  `json:"card,omitempty"`
}

type storedReplyTarget struct {
	Kind             channels.TargetKind `json:"target_kind"`
	InternalEntityID string              `json:"internal_entity_id"`
}

func insertReplyOutboxTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, requestID string,
	replies []channels.Reply,
) error {
	return insertReplyOutboxTxWithSourceKind(
		ctx, tx, tenantID, appID, bindingID, requestID, "execution", replies,
	)
}

func insertReplyOutboxTxWithSourceKind(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, requestID, sourceKind string,
	replies []channels.Reply,
) error {
	if sourceKind != "execution" && sourceKind != "channel_command" && sourceKind != "channel_failure" {
		return errors.New("reply projection source kind is invalid")
	}
	for _, reply := range replies {
		if reply.TenantID != tenantID || reply.AppID != appID || reply.BindingID != bindingID || reply.RequestID != requestID {
			return errors.New("reply projection scope does not match state")
		}
		if err := normalizeStreamReplyTx(ctx, tx, &reply); errors.Is(err, errReplyStreamClosed) {
			continue
		} else if err != nil {
			return fmt.Errorf("normalize reply stream: %w", err)
		}
		pendingReplyID, pendingPhase, err := pendingLifecycleReplyTx(ctx, tx, reply)
		if err != nil {
			return fmt.Errorf("find pending rich reply: %w", err)
		}
		if pendingReplyID != "" && pendingPhase == channels.StreamPhaseStart && reply.StreamPhase == channels.StreamPhaseUpdate {
			reply.StreamPhase = channels.StreamPhaseStart
		}
		reply.ReplyID = reply.StableID()
		if err := reply.Validate(); err != nil {
			return fmt.Errorf("projected reply: %w", err)
		}
		payload, err := json.Marshal(storedReplyPayload{
			Kind:           reply.ReplyKind(),
			StreamID:       reply.StreamID,
			StreamPhase:    reply.StreamPhase,
			StreamSequence: reply.StreamSequence,
			Text:           reply.Text,
			Card:           reply.Card,
		})
		if err != nil {
			return fmt.Errorf("marshal reply payload: %w", err)
		}
		target, err := json.Marshal(storedReplyTarget{
			Kind:             reply.Target.Kind,
			InternalEntityID: reply.Target.InternalEntityID,
		})
		if err != nil {
			return fmt.Errorf("marshal reply target: %w", err)
		}
		if pendingReplyID != "" {
			// Keep every stream sequence as a durable identity. Coalescing by
			// rewriting the old row would make a later duplicate of that old
			// sequence insertable. SUPERSEDED rows retain the unique key while
			// remaining invisible to the sender and ordering barrier.
			if _, err := tx.Exec(ctx, `
UPDATE platform.reply_outbox
SET status = 'SUPERSEDED', lease_owner = NULL, lease_until = NULL,
    last_error_type = 'stream_coalesced',
    last_error = 'superseded by a newer pending stream snapshot',
    updated_at = clock_timestamp()
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND request_id = $4
  AND reply_kind = $5
  AND stream_id = $6
  AND stream_sequence < $7
  AND status = 'PENDING'`,
				reply.TenantID,
				reply.AppID,
				reply.BindingID,
				reply.RequestID,
				reply.ReplyKind(),
				reply.StreamID,
				reply.StreamSequence,
			); err != nil {
				return fmt.Errorf("coalesce reply outbox: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO platform.reply_outbox (
    reply_id, tenant_id, app_id, binding_id, binding_revision, channel, request_id,
    source_event_id, revision, reply_kind, stream_id, stream_phase, stream_sequence,
    target_ref, payload, source_kind
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT DO NOTHING`,
			reply.ReplyID,
			reply.TenantID,
			reply.AppID,
			reply.BindingID,
			reply.BindingRevision,
			reply.Channel,
			reply.RequestID,
			reply.SourceEventID,
			reply.Revision,
			reply.ReplyKind(),
			reply.StreamID,
			reply.StreamPhase,
			reply.StreamSequence,
			target,
			payload,
			sourceKind,
		); err != nil {
			return fmt.Errorf("insert reply outbox: %w", err)
		}
	}
	return nil
}

func pendingLifecycleReplyTx(
	ctx context.Context,
	tx pgx.Tx,
	reply channels.Reply,
) (string, channels.StreamPhase, error) {
	if !isLifecycleReply(reply) {
		return "", "", nil
	}
	var replyID string
	var phase channels.StreamPhase
	err := tx.QueryRow(ctx, `
SELECT reply_id, stream_phase
FROM platform.reply_outbox
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND request_id = $4
  AND reply_kind = $5
  AND stream_id = $6
  AND stream_sequence < $7
  AND status = 'PENDING'
ORDER BY stream_sequence DESC, created_at DESC
LIMIT 1
FOR UPDATE`,
		reply.TenantID,
		reply.AppID,
		reply.BindingID,
		reply.RequestID,
		reply.ReplyKind(),
		reply.StreamID,
		reply.StreamSequence,
	).Scan(&replyID, &phase)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	return replyID, phase, nil
}

// normalizeStreamReplyTx turns partial model deltas into durable full
// snapshots. The previous snapshot is read in the same transaction as the
// execution event and outbox insert, so a crash cannot expose an unpersisted
// stream frame to the sender.
func normalizeStreamReplyTx(ctx context.Context, tx pgx.Tx, reply *channels.Reply) error {
	if reply == nil || !isLifecycleReply(*reply) {
		return nil
	}
	var closed int
	err := tx.QueryRow(ctx, `
SELECT 1
FROM platform.reply_outbox
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND request_id = $4
  AND reply_kind = $5
  AND stream_id = $6
  AND stream_phase IN ('end', 'abort')
LIMIT 1`,
		reply.TenantID,
		reply.AppID,
		reply.BindingID,
		reply.RequestID,
		reply.ReplyKind(),
		reply.StreamID,
	).Scan(&closed)
	if err == nil {
		return errReplyStreamClosed
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	var previousText string
	err = tx.QueryRow(ctx, `
SELECT COALESCE(payload->>'text', payload->'card'->>'body', '')
FROM platform.reply_outbox
WHERE tenant_id = $1
  AND app_id = $2
  AND binding_id = $3
  AND request_id = $4
  AND reply_kind = $5
  AND stream_id = $6
  AND stream_sequence < $7
ORDER BY stream_sequence DESC, created_at DESC
LIMIT 1`,
		reply.TenantID,
		reply.AppID,
		reply.BindingID,
		reply.RequestID,
		reply.ReplyKind(),
		reply.StreamID,
		reply.StreamSequence,
	).Scan(&previousText)
	hasPrevious := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	currentText := reply.Text
	if reply.ReplyKind() == channels.ReplyKindCard && reply.Card != nil {
		currentText = reply.Card.Body
	}
	if reply.StreamPhase == channels.StreamPhaseEnd && currentText == "" {
		currentText = previousText
		if currentText == "" {
			currentText = "执行完成"
		}
	} else if reply.StreamPhase != channels.StreamPhaseEnd {
		currentText = appendStreamSnapshot(previousText, currentText)
	}
	// Redact after snapshot assembly. A credential split across two model
	// deltas is invisible to per-delta filtering but must not reach durable
	// outbox payloads.
	currentText = sanitizeReplySnapshot(currentText)
	if reply.ReplyKind() == channels.ReplyKindCard && reply.Card != nil {
		reply.Card.Body = currentText
	} else {
		reply.Text = currentText
	}
	if hasPrevious {
		if reply.StreamPhase == channels.StreamPhaseStart {
			reply.StreamPhase = channels.StreamPhaseUpdate
		}
	} else if reply.StreamPhase == channels.StreamPhaseUpdate {
		reply.StreamPhase = channels.StreamPhaseStart
	}
	return nil
}

func sanitizeReplySnapshot(text string) string {
	return guardrail.SanitizeOutput(text).Text
}

func isLifecycleReply(reply channels.Reply) bool {
	return (reply.ReplyKind() == channels.ReplyKindStream || reply.ReplyKind() == channels.ReplyKindCard) && reply.StreamID != ""
}

func appendStreamSnapshot(previous, current string) string {
	if current == "" {
		return previous
	}
	if previous != "" && strings.HasPrefix(current, previous) {
		return current
	}
	return previous + current
}

// ClaimReplies leases ready replies in event order within one request.
func (s *Store) ClaimReplies(
	ctx context.Context,
	owner string,
	leaseDuration time.Duration,
	limit int,
) ([]worker.ReplyDelivery, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("reply sender owner is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("reply lease duration must be positive")
	}
	if limit <= 0 {
		limit = 32
	}
	// Recover an expired sender lease inside the same transaction boundary as
	// claiming. The periodic recovery pass can race with lease expiry, so a
	// stale SENDING row must never be reclassified as a permanently failed
	// binding change before it becomes UNCERTAIN.
	if _, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox
SET status = 'UNCERTAIN', lease_owner = NULL, lease_until = NULL,
    last_error_type = 'provider_result_unknown',
    last_error = 'reply sender lease expired before result was recorded',
    updated_at = clock_timestamp()
WHERE status = 'SENDING' AND lease_until <= clock_timestamp()`); err != nil {
		return nil, fmt.Errorf("recover expired reply leases: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox o
SET status = 'PERMANENTLY_FAILED', lease_owner = NULL, lease_until = NULL,
    last_error_type = 'binding_changed', last_error = 'binding authorization changed',
    updated_at = clock_timestamp()
WHERE o.status = 'PENDING'
  AND EXISTS (
      SELECT 1
      FROM platform.channel_binding b
      WHERE b.tenant_id = o.tenant_id
        AND b.app_id = o.app_id
        AND b.binding_id = o.binding_id
        AND (b.channel <> o.channel OR b.binding_revision <> o.binding_revision)
  )`); err != nil {
		return nil, fmt.Errorf("reject stale reply bindings: %w", err)
	}
	rows, err := s.pool.Query(ctx, `
WITH candidates AS (
    SELECT o.reply_id
    FROM platform.reply_outbox o
    WHERE (
        (o.status = 'PENDING' AND o.next_attempt_at <= clock_timestamp())
        OR (o.status = 'SENDING' AND o.lease_until <= clock_timestamp())
    )
    AND EXISTS (
        SELECT 1
        FROM platform.channel_binding b
        WHERE b.tenant_id = o.tenant_id
          AND b.app_id = o.app_id
          AND b.binding_id = o.binding_id
          AND b.status = 'ACTIVE'
          AND b.channel = o.channel
          AND b.binding_revision = o.binding_revision
    )
    AND NOT EXISTS (
        SELECT 1
        FROM platform.reply_outbox p
        WHERE p.tenant_id = o.tenant_id
          AND p.app_id = o.app_id
          AND p.binding_id = o.binding_id
          AND p.request_id = o.request_id
          AND p.revision < o.revision
          AND (
              p.status IN ('PENDING', 'SENDING', 'UNCERTAIN')
              OR (
                  o.reply_kind IN ('stream', 'card')
                  AND p.reply_kind = o.reply_kind
                  AND p.stream_id = o.stream_id
                  AND p.status = 'PERMANENTLY_FAILED'
              )
          )
    )
    ORDER BY o.created_at, o.reply_id
    FOR UPDATE SKIP LOCKED
    LIMIT $3
)
UPDATE platform.reply_outbox o
SET status = 'SENDING', lease_owner = $1,
    lease_until = clock_timestamp() + $2::interval,
    attempt = attempt + 1,
    updated_at = clock_timestamp()
FROM candidates c
WHERE o.reply_id = c.reply_id
RETURNING o.reply_id, o.tenant_id, o.app_id, o.binding_id,
          o.binding_revision, o.channel, o.request_id, o.source_event_id,
          o.revision, o.reply_kind, o.stream_id, o.stream_phase,
          o.stream_sequence, o.target_ref, o.payload, o.attempt,
          o.lease_owner, o.lease_until,
          CASE WHEN o.reply_kind IN ('stream', 'card') AND o.stream_id <> '' THEN
              COALESCE((SELECT p.provider_message_id
                        FROM platform.reply_outbox p
                        WHERE p.tenant_id = o.tenant_id
                          AND p.app_id = o.app_id
                          AND p.binding_id = o.binding_id
                          AND p.request_id = o.request_id
                          AND p.reply_kind = o.reply_kind
                          AND p.stream_id = o.stream_id
                          AND p.stream_sequence < o.stream_sequence
                          AND p.status = 'SENT'
                          AND p.provider_message_id <> ''
                        ORDER BY p.stream_sequence DESC
                        LIMIT 1), o.provider_message_id)
            ELSE o.provider_message_id END,
          COALESCE((SELECT e.trace_id FROM platform.execution e
           WHERE e.tenant_id = o.tenant_id AND e.app_id = o.app_id AND e.request_id = o.request_id), ''),
          COALESCE((SELECT e.trace_parent FROM platform.execution e
           WHERE e.tenant_id = o.tenant_id AND e.app_id = o.app_id AND e.request_id = o.request_id), ''),
          COALESCE((SELECT e.trace_state FROM platform.execution e
           WHERE e.tenant_id = o.tenant_id AND e.app_id = o.app_id AND e.request_id = o.request_id), '')`,
		owner, intervalLiteral(leaseDuration), limit)
	if err != nil {
		return nil, fmt.Errorf("claim reply outbox: %w", err)
	}
	defer rows.Close()
	result := make([]worker.ReplyDelivery, 0)
	for rows.Next() {
		delivery, err := scanReplyDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reply outbox: %w", err)
	}
	return result, nil
}

type replyRowScanner interface {
	Scan(...any) error
}

func scanReplyDelivery(row replyRowScanner) (worker.ReplyDelivery, error) {
	var (
		replyID, tenantID, appID, bindingID, channel, requestID string
		sourceEventID, replyKind, streamID, streamPhase         string
		leaseOwner, providerMessageID                           string
		traceID, traceParent, traceState                        string
		bindingRevision, revision, streamSequence, attempt      int64
		targetJSON, payloadJSON                                 []byte
		leaseUntil                                              time.Time
	)
	if err := row.Scan(
		&replyID, &tenantID, &appID, &bindingID, &bindingRevision, &channel, &requestID,
		&sourceEventID, &revision, &replyKind, &streamID, &streamPhase, &streamSequence,
		&targetJSON, &payloadJSON, &attempt, &leaseOwner, &leaseUntil,
		&providerMessageID, &traceID, &traceParent, &traceState,
	); err != nil {
		return worker.ReplyDelivery{}, fmt.Errorf("scan reply outbox: %w", err)
	}
	var target storedReplyTarget
	if err := json.Unmarshal(targetJSON, &target); err != nil {
		return worker.ReplyDelivery{}, fmt.Errorf("decode reply target: %w", err)
	}
	var payload storedReplyPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return worker.ReplyDelivery{}, fmt.Errorf("decode reply payload: %w", err)
	}
	reply := channels.Reply{
		TenantID:        tenantID,
		AppID:           appID,
		RequestID:       requestID,
		SourceEventID:   sourceEventID,
		Channel:         channels.Channel(channel),
		BindingID:       bindingID,
		BindingRevision: bindingRevision,
		ReplyID:         replyID,
		Revision:        revision,
		Target: channels.ReplyTarget{
			Kind:             target.Kind,
			InternalEntityID: target.InternalEntityID,
		},
		Text:           payload.Text,
		Kind:           channels.ReplyKind(replyKind),
		StreamID:       streamID,
		StreamPhase:    channels.StreamPhase(streamPhase),
		StreamSequence: streamSequence,
		Card:           payload.Card,
	}
	return worker.ReplyDelivery{
		Reply:             reply,
		Attempt:           int(attempt),
		LeaseOwner:        leaseOwner,
		LeaseUntil:        leaseUntil.UTC(),
		ProviderMessageID: providerMessageID,
		TraceID:           traceID,
		TraceParent:       traceParent,
		TraceState:        traceState,
	}, nil
}

// CompleteReply marks one provider call as sent when its sender lease is
// still valid. A repeated completion after SENT is idempotent.
func (s *Store) CompleteReply(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	receipt channels.ProviderReceipt,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	if err := receipt.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox
SET status = 'SENT', lease_owner = NULL, lease_until = NULL,
    provider_message_id = $3, updated_at = clock_timestamp()
WHERE reply_id = $1 AND status = 'SENDING' AND lease_owner = $2
  AND lease_until > clock_timestamp()`, delivery.Reply.ReplyID, delivery.LeaseOwner, receipt.ProviderMessageID)
	if err != nil {
		return fmt.Errorf("complete reply: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = s.pool.QueryRow(ctx, `SELECT status FROM platform.reply_outbox WHERE reply_id = $1`, delivery.Reply.ReplyID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("complete reply: %w", ErrReplyLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("read reply completion: %w", err)
	}
	if status == "SENT" {
		return nil
	}
	return fmt.Errorf("complete reply: %w", ErrReplyLeaseLost)
}

// MarkReplyUncertain records that the provider may have accepted a reply but
// the sender cannot prove the local SENT transition. Such rows are never
// automatically retried because these providers offer no safe
// result query or idempotency contract here.
func (s *Store) MarkReplyUncertain(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	providerMessageID string,
	cause error,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox
SET status='UNCERTAIN', lease_owner=NULL, lease_until=NULL,
    provider_message_id=CASE WHEN $3 <> '' THEN $3 ELSE provider_message_id END,
    last_error_type='provider_result_unknown', last_error=$4,
    updated_at=clock_timestamp()
WHERE reply_id=$1 AND status='SENDING' AND lease_owner=$2
  AND lease_until > clock_timestamp()`,
		delivery.Reply.ReplyID, delivery.LeaseOwner, providerMessageID, platformlog.SafeError(cause))
	if err != nil {
		return fmt.Errorf("mark uncertain reply: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = s.pool.QueryRow(ctx, `SELECT status FROM platform.reply_outbox WHERE reply_id=$1`, delivery.Reply.ReplyID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("mark uncertain reply: %w", ErrReplyLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("read uncertain reply: %w", err)
	}
	if status == "SENT" || status == "UNCERTAIN" {
		return nil
	}
	return fmt.Errorf("mark uncertain reply: %w", ErrReplyLeaseLost)
}

// RetryReply returns a leased row to PENDING with a bounded retry timestamp.
func (s *Store) RetryReply(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	errorType string,
	delay time.Duration,
	_ error,
) error {
	return s.transitionReplyFailure(ctx, delivery, "PENDING", errorType, delay)
}

// FailReply retains an unrecoverable reply failure for operator inspection.
func (s *Store) FailReply(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	errorType string,
	_ error,
) error {
	return s.transitionReplyFailure(ctx, delivery, "PERMANENTLY_FAILED", errorType, 0)
}

func (s *Store) transitionReplyFailure(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	status, errorType string,
	delay time.Duration,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	if errorType == "" {
		errorType = "provider_send"
	}
	if delay < 0 {
		return errors.New("reply retry delay must not be negative")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox
SET status = $3,
    next_attempt_at = clock_timestamp() + $4::interval,
    lease_owner = NULL, lease_until = NULL,
    last_error_type = $5, last_error = $5,
    updated_at = clock_timestamp()
WHERE reply_id = $1 AND status = 'SENDING' AND lease_owner = $2
  AND lease_until > clock_timestamp()`, delivery.Reply.ReplyID, delivery.LeaseOwner, status,
		intervalLiteral(delay), errorType)
	if err != nil {
		return fmt.Errorf("transition reply failure: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var current string
	err = s.pool.QueryRow(ctx, `SELECT status FROM platform.reply_outbox WHERE reply_id = $1`, delivery.Reply.ReplyID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("transition reply failure: %w", ErrReplyLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("read reply failure transition: %w", err)
	}
	if current == "SENT" || current == status {
		return nil
	}
	return fmt.Errorf("transition reply failure: %w", ErrReplyLeaseLost)
}

// RecoverReplyLeases records expired SENDING rows as uncertain. The provider
// may have accepted the request, so these rows are intentionally not claimable
// for blind retry.
func (s *Store) RecoverReplyLeases(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox
	SET status = 'UNCERTAIN', lease_owner = NULL, lease_until = NULL,
	    last_error_type = 'provider_result_unknown',
	    last_error = 'reply sender lease expired before result was recorded',
	    updated_at = clock_timestamp()
WHERE status = 'SENDING' AND lease_until <= clock_timestamp()`); err != nil {
		return fmt.Errorf("recover reply leases: %w", err)
	}
	return nil
}

// ResolveReplyTarget opens the scoped provider target only for the immediate
// outbound operation. A present but expired message target is terminal and is
// never replaced with a different entity target.
func (s *Store) ResolveReplyTarget(
	ctx context.Context,
	delivery worker.ReplyDelivery,
) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if err := delivery.Validate(); err != nil {
		return "", err
	}
	if s.identityMapper == nil || s.identityMapper.protector == nil {
		return "", errors.New("target protector is required")
	}
	reply := delivery.Reply
	var bindingStatus string
	var bindingChannel string
	var bindingRevision int64
	if err := s.pool.QueryRow(ctx, `
SELECT status, channel, binding_revision
FROM platform.channel_binding
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		reply.TenantID, reply.AppID, reply.BindingID,
	).Scan(&bindingStatus, &bindingChannel, &bindingRevision); err != nil {
		return "", resolveReplyTargetError("reply binding", err)
	}
	if bindingStatus != string(channels.BindingActive) {
		return "", channels.ErrBindingInactive
	}
	if channels.Channel(bindingChannel) != reply.Channel || bindingRevision != reply.BindingRevision {
		return "", errors.New("reply binding authorization changed")
	}
	var targetJSON []byte
	var targetExpired bool
	err := s.pool.QueryRow(ctx, `
SELECT provider_reply_target_envelope,
       COALESCE(reply_target_expires_at <= clock_timestamp(), TRUE)
FROM platform.channel_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND request_id = $4
  AND provider_reply_target_envelope IS NOT NULL
ORDER BY created_at
	LIMIT 1`, reply.TenantID, reply.AppID, reply.BindingID, reply.RequestID).Scan(&targetJSON, &targetExpired)
	if err == nil {
		if targetExpired {
			return "", ErrReplyTargetExpired
		}
		plaintext, err := s.openReplyTarget(ctx, reply, channels.TargetContext{
			Scope:            tenant.Scope{TenantID: reply.TenantID, AppID: reply.AppID},
			BindingID:        reply.BindingID,
			Channel:          reply.Channel,
			EntityType:       channels.TargetEntityInbox,
			InternalEntityID: reply.RequestID,
		}, channels.TargetPurposeReplyMessage, targetJSON)
		if err != nil {
			return "", err
		}
		return plaintext.ProviderTarget, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", retryableReplyTargetError("find message reply target", err)
	}
	scope := tenant.Scope{TenantID: reply.TenantID, AppID: reply.AppID}
	switch reply.Target.Kind {
	case channels.TargetKindUser:
		var identityStatus string
		err = s.pool.QueryRow(ctx, `
	SELECT provider_target_envelope, status
FROM platform.channel_identity
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND user_id = $4`,
			reply.TenantID, reply.AppID, reply.BindingID, reply.Target.InternalEntityID).Scan(&targetJSON, &identityStatus)
		if err != nil {
			return "", resolveReplyTargetError("reply identity target", err)
		}
		if identityStatus != channels.IdentityActive {
			return "", channels.ErrIdentityInactive
		}
		plaintext, err := s.openReplyTarget(ctx, reply, channels.TargetContext{
			Scope: scope, BindingID: reply.BindingID, Channel: reply.Channel,
			EntityType: channels.TargetEntityIdentity, InternalEntityID: reply.Target.InternalEntityID,
		}, channels.TargetPurposeIdentityUser, targetJSON)
		if err != nil {
			return "", err
		}
		return plaintext.ProviderTarget, nil
	case channels.TargetKindConversation, channels.TargetKindTopic:
		err = s.pool.QueryRow(ctx, `
SELECT provider_target_envelope, scope
FROM platform.channel_conversation
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND conversation_id = $4`,
			reply.TenantID, reply.AppID, reply.BindingID, reply.Target.InternalEntityID).Scan(&targetJSON, new(string))
		if err != nil {
			return "", resolveReplyTargetError("reply conversation target", err)
		}
		purpose := channels.TargetPurposeConversationChat
		if reply.Target.Kind == channels.TargetKindTopic {
			purpose = channels.TargetPurposeConversationTopic
		}
		plaintext, err := s.openReplyTarget(ctx, reply, channels.TargetContext{
			Scope: scope, BindingID: reply.BindingID, Channel: reply.Channel,
			EntityType: channels.TargetEntityConversation, InternalEntityID: reply.Target.InternalEntityID,
		}, purpose, targetJSON)
		if err != nil {
			return "", err
		}
		return plaintext.ProviderTarget, nil
	default:
		return "", errors.New("reply target kind is unsupported")
	}
}

func resolveReplyTargetError(entity string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return resolveError(entity, err)
	}
	return retryableReplyTargetError(entity, err)
}

func retryableReplyTargetError(entity string, err error) error {
	return worker.NewRetryableReplyError(fmt.Errorf("%s: %w", entity, err), "target_backend")
}

func (s *Store) openReplyTarget(
	ctx context.Context,
	reply channels.Reply,
	targetContext channels.TargetContext,
	purpose channels.TargetPurpose,
	encoded []byte,
) (channels.TargetPlaintext, error) {
	var envelope channels.TargetEnvelope
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return channels.TargetPlaintext{}, fmt.Errorf("decode reply target envelope: %w", err)
	}
	if err := envelope.Validate(); err != nil {
		return channels.TargetPlaintext{}, err
	}
	plaintext, err := s.identityMapper.protector.Open(ctx, targetContext, purpose, envelope)
	if err != nil {
		return channels.TargetPlaintext{}, fmt.Errorf("open reply target: %w", err)
	}
	if plaintext.Channel != reply.Channel {
		return channels.TargetPlaintext{}, errors.New("reply target channel does not match")
	}
	if err := plaintext.Validate(purpose); err != nil {
		return channels.TargetPlaintext{}, err
	}
	return plaintext, nil
}

var (
	_ worker.ReplyOutbox = (*Store)(nil)
)
