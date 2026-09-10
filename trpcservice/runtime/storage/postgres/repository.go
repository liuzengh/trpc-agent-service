// Package postgres implements the tenant-scoped runtime storage contract on PostgreSQL.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	pgstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
)

// Store persists tenant-scoped runtime state in PostgreSQL.
type Store struct{ db *sql.DB }

const eventColumns = "tenant_id,event_id,session_id,binding_id,external_message_id,idempotency_key,event_seq,status,fencing_token,lease_owner,lease_expires_at,reply_id,segment_count,reply_conversation_kind,reply_receiver_id,reply_thread_id,created_at,updated_at"
const replyColumns = "tenant_id,reply_id,event_id,segment_index,segment_count,payload,reply_kind,attachment_id,attachment_kind,attachment_mime_type,attachment_name,attachment_size,attachment_sha256,attachment_provider,attachment_provider_id,fallback,reply_binding_id,reply_conversation_kind,reply_receiver_id,reply_thread_id,status,attempts,fencing_token,lease_owner,lease_expires_at,provider_message_id,last_error_class,created_at,updated_at"
const insertReplyStatement = "INSERT INTO public.reply_outbox (tenant_id,reply_id,event_id,segment_index,segment_count,payload,reply_kind,attachment_id,attachment_kind,attachment_mime_type,attachment_name,attachment_size,attachment_sha256,attachment_provider,attachment_provider_id,fallback,reply_binding_id,reply_conversation_kind,reply_receiver_id,reply_thread_id,status) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,'pending' WHERE EXISTS (SELECT 1 FROM public.message_event WHERE tenant_id=$1 AND event_id=$3 AND ((reply_conversation_kind='' AND reply_receiver_id='' AND reply_thread_id='' AND $17='' AND $18='' AND $19='' AND $20='') OR (binding_id=$17 AND reply_conversation_kind=$18 AND reply_receiver_id=$19 AND reply_thread_id=$20))) ON CONFLICT (tenant_id,reply_id,segment_index) DO UPDATE SET updated_at=public.reply_outbox.updated_at WHERE public.reply_outbox.event_id=EXCLUDED.event_id AND public.reply_outbox.segment_count=EXCLUDED.segment_count AND public.reply_outbox.payload=EXCLUDED.payload AND public.reply_outbox.reply_kind=EXCLUDED.reply_kind AND public.reply_outbox.attachment_id=EXCLUDED.attachment_id AND public.reply_outbox.attachment_kind=EXCLUDED.attachment_kind AND public.reply_outbox.attachment_mime_type=EXCLUDED.attachment_mime_type AND public.reply_outbox.attachment_name=EXCLUDED.attachment_name AND public.reply_outbox.attachment_size=EXCLUDED.attachment_size AND public.reply_outbox.attachment_sha256=EXCLUDED.attachment_sha256 AND public.reply_outbox.attachment_provider=EXCLUDED.attachment_provider AND public.reply_outbox.attachment_provider_id=EXCLUDED.attachment_provider_id AND public.reply_outbox.fallback=EXCLUDED.fallback AND public.reply_outbox.reply_binding_id=EXCLUDED.reply_binding_id AND public.reply_outbox.reply_conversation_kind=EXCLUDED.reply_conversation_kind AND public.reply_outbox.reply_receiver_id=EXCLUDED.reply_receiver_id AND public.reply_outbox.reply_thread_id=EXCLUDED.reply_thread_id"

// New creates a PostgreSQL runtime store over db.
func New(db *sql.DB) *Store { return &Store{db: db} }

// GetReplyCorrelation loads the durable execution-to-reply correlation.
func (s *Store) GetReplyCorrelation(ctx context.Context, tenantID, eventID string) (runtimestorage.ReplyCorrelation, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.ReplyCorrelation{}, err
	}
	if tenantID == "" || eventID == "" {
		return runtimestorage.ReplyCorrelation{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.ReplyCorrelation
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id,event_id,request_id,trace_id,trace_parent FROM public.runtime_reply_correlation WHERE tenant_id=$1 AND event_id=$2", tenantID, eventID).Scan(&value.TenantID, &value.EventID, &value.RequestID, &value.TraceID, &value.TraceParent)
	if err != nil {
		return runtimestorage.ReplyCorrelation{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	value.TraceParent = observability.NormalizeTraceParent(value.TraceParent)
	return value, nil
}

// GetSession loads a tenant-scoped session.
func (s *Store) GetSession(ctx context.Context, tenantID, sessionID string) (sessionstorage.Session, error) {
	if err := checkStore(ctx, s); err != nil {
		return sessionstorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return sessionstorage.Session{}, err
	}
	var value sessionstorage.Session
	var state []byte
	err := s.db.QueryRowContext(ctx, "SELECT tenant_id, session_id, status, version, state, created_at, updated_at FROM public.runtime_session WHERE tenant_id=$1 AND session_id=$2", tenantID, sessionID).Scan(&value.TenantID, &value.SessionID, &value.Status, &value.Version, &state, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return sessionstorage.Session{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := pgstorage.DecodeJSON(state, &value.State); err != nil {
		return sessionstorage.Session{}, runtimestorage.ErrStorage
	}
	return cloneSession(value), nil
}

// CreateSession persists a new tenant-scoped session.
func (s *Store) CreateSession(ctx context.Context, tenantID, sessionID string, state map[string]any) (sessionstorage.Session, error) {
	if err := checkStore(ctx, s); err != nil {
		return sessionstorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return sessionstorage.Session{}, err
	}
	if state == nil {
		state = map[string]any{}
	}
	encoded, err := pgstorage.EncodeJSON(state)
	if err != nil {
		return sessionstorage.Session{}, runtimestorage.ErrInvalid
	}
	var value sessionstorage.Session
	var persisted []byte
	err = s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_session (tenant_id, session_id, status, version, state) VALUES ($1,$2,'active',1,$3) RETURNING tenant_id,session_id,status,version,state,created_at,updated_at", tenantID, sessionID, encoded).Scan(&value.TenantID, &value.SessionID, &value.Status, &value.Version, &persisted, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		return sessionstorage.Session{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := pgstorage.DecodeJSON(persisted, &value.State); err != nil {
		return sessionstorage.Session{}, runtimestorage.ErrStorage
	}
	return cloneSession(value), nil
}

// UpdateSessionState applies an expected-version session state update.
func (s *Store) UpdateSessionState(ctx context.Context, tenantID, sessionID string, expectedVersion int64, state map[string]any) (sessionstorage.Session, error) {
	if err := checkStore(ctx, s); err != nil {
		return sessionstorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return sessionstorage.Session{}, err
	}
	if state == nil {
		state = map[string]any{}
	}
	encoded, err := pgstorage.EncodeJSON(state)
	if err != nil {
		return sessionstorage.Session{}, runtimestorage.ErrInvalid
	}
	var value sessionstorage.Session
	var persisted []byte
	err = s.db.QueryRowContext(ctx, "UPDATE public.runtime_session SET version=version+1,state=$4,updated_at=now() WHERE tenant_id=$1 AND session_id=$2 AND version=$3 RETURNING tenant_id,session_id,status,version,state,created_at,updated_at", tenantID, sessionID, expectedVersion, encoded).Scan(&value.TenantID, &value.SessionID, &value.Status, &value.Version, &persisted, &value.CreatedAt, &value.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if _, getErr := s.GetSession(ctx, tenantID, sessionID); getErr != nil {
				return sessionstorage.Session{}, getErr
			}
			return sessionstorage.Session{}, runtimestorage.ErrConflict
		}
		return sessionstorage.Session{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if err := pgstorage.DecodeJSON(persisted, &value.State); err != nil {
		return sessionstorage.Session{}, runtimestorage.ErrStorage
	}
	return cloneSession(value), nil
}

// DeleteSession removes a tenant-scoped session.
func (s *Store) DeleteSession(ctx context.Context, tenantID, sessionID string) error {
	if err := checkStore(ctx, s); err != nil {
		return err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, "DELETE FROM public.runtime_session WHERE tenant_id=$1 AND session_id=$2", tenantID, sessionID)
	if err != nil {
		return mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return runtimestorage.ErrStorage
	}
	if rows == 0 {
		return runtimestorage.ErrNotFound
	}
	return nil
}

// RecordMessage inserts or returns an idempotent inbound message event.
func (s *Store) RecordMessage(ctx context.Context, input runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	if runtimestorage.ValidateSession(input.TenantID, input.SessionID) != nil || input.BindingID == "" || input.ExternalMessageID == "" || input.EventID == "" || runtimestorage.ValidateReplyTarget(input.ReplyTarget) != nil || (input.ReplyTarget != (runtimestorage.ReplyTarget{}) && input.ReplyTarget.BindingID != input.BindingID) {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrInvalid
	}
	tx, err := begin(ctx, s.db)
	if err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	defer pgstorage.Rollback(tx)
	var existing runtimestorage.MessageEvent
	err = tx.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM public.message_event WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3", input.TenantID, input.BindingID, input.ExternalMessageID).Scan(eventArgs(&existing)...)
	if err == nil {
		return cloneEvent(existing), true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.MessageEvent{}, false, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	var version int64
	if err := tx.QueryRowContext(ctx, "UPDATE public.runtime_session SET version=version+1,updated_at=now() WHERE tenant_id=$1 AND session_id=$2 RETURNING version", input.TenantID, input.SessionID).Scan(&version); err != nil {
		return runtimestorage.MessageEvent{}, false, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	err = tx.QueryRowContext(ctx, "INSERT INTO public.message_event (tenant_id,event_id,session_id,binding_id,external_message_id,idempotency_key,event_seq,status,reply_conversation_kind,reply_receiver_id,reply_thread_id) VALUES ($1,$2,$3,$4,$5,$6,$7,'received',$8,$9,$10) RETURNING "+eventColumns, input.TenantID, input.EventID, input.SessionID, input.BindingID, input.ExternalMessageID, input.IdempotencyKey, version, input.ReplyTarget.ConversationKind, input.ReplyTarget.ReceiverID, input.ReplyTarget.ThreadID).Scan(eventArgs(&existing)...)
	if err != nil {
		mapped := mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
		if errors.Is(mapped, runtimestorage.ErrDuplicate) {
			pgstorage.Rollback(tx)
			if duplicate, lookupErr := s.lookupMessageByExternal(ctx, input.TenantID, input.BindingID, input.ExternalMessageID); lookupErr == nil {
				return duplicate, true, nil
			}
		}
		return runtimestorage.MessageEvent{}, false, mapped
	}
	if err := commit(ctx, tx); err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	return cloneEvent(existing), false, nil
}

func (s *Store) lookupMessageByExternal(ctx context.Context, tenantID, bindingID, externalID string) (runtimestorage.MessageEvent, error) {
	var value runtimestorage.MessageEvent
	err := s.db.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM public.message_event WHERE tenant_id=$1 AND binding_id=$2 AND external_message_id=$3", tenantID, bindingID, externalID).Scan(eventArgs(&value)...)
	if err != nil {
		return runtimestorage.MessageEvent{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return cloneEvent(value), nil
}

// GetMessage loads a tenant-scoped inbound message event.
func (s *Store) GetMessage(ctx context.Context, tenantID, eventID string) (runtimestorage.MessageEvent, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || eventID == "" {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
	}
	return lookupMessage(ctx, s.db, tenantID, eventID)
}

type messageQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func lookupMessage(ctx context.Context, query messageQuerier, tenantID, eventID string) (runtimestorage.MessageEvent, error) {
	var value runtimestorage.MessageEvent
	err := query.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM public.message_event WHERE tenant_id=$1 AND event_id=$2", tenantID, eventID).Scan(eventArgs(&value)...)
	if err != nil {
		return runtimestorage.MessageEvent{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return cloneEvent(value), nil
}

// TransitionMessage applies a fenced message lifecycle transition.
func (s *Store) TransitionMessage(ctx context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if runtimestorage.ValidateTenant(transition.TenantID) != nil || transition.EventID == "" || transition.Owner == "" {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
	}
	if !runtimestorage.ValidateMessageTransition(transition.From, transition.To) {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrIllegalTransition
	}
	leaseSeconds := int64(0)
	if transition.To == runtimestorage.EventRunning {
		if transition.LeaseDuration <= 0 {
			return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
		}
		leaseSeconds = int64(transition.LeaseDuration / time.Second)
		if leaseSeconds == 0 {
			leaseSeconds = 1
		}
	}
	var value runtimestorage.MessageEvent
	if transition.ReplyID != "" || transition.SegmentCount > 0 {
		return s.transitionMessageWithReply(ctx, transition, leaseSeconds)
	}
	err := s.db.QueryRowContext(ctx, "UPDATE public.message_event SET status=$4,fencing_token=fencing_token+1,lease_owner=CASE WHEN $4='running' THEN $5 ELSE '' END,lease_expires_at=CASE WHEN $6>0 THEN now()+($6 * interval '1 second') ELSE NULL END,updated_at=now() WHERE tenant_id=$1 AND event_id=$2 AND status=$3 AND ($3 <> 'running' OR ($4='execution_reconciling' AND lease_expires_at IS NOT NULL AND lease_expires_at <= now()) OR ($4<>'execution_reconciling' AND lease_owner=$5 AND fencing_token=$7 AND lease_expires_at IS NOT NULL AND lease_expires_at > now())) RETURNING "+eventColumns, transition.TenantID, transition.EventID, transition.From, transition.To, transition.Owner, leaseSeconds, transition.FencingToken).Scan(eventArgs(&value)...)
	if err == nil {
		return cloneEvent(value), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.MessageEvent{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if _, lookupErr := s.GetMessage(ctx, transition.TenantID, transition.EventID); lookupErr != nil {
		return runtimestorage.MessageEvent{}, lookupErr
	}
	return runtimestorage.MessageEvent{}, runtimestorage.ErrConflict
}

func (s *Store) transitionMessageWithReply(ctx context.Context, transition runtimestorage.MessageTransition, leaseSeconds int64) (runtimestorage.MessageEvent, error) {
	var value runtimestorage.MessageEvent
	err := s.db.QueryRowContext(ctx, "UPDATE public.message_event SET status=$4,fencing_token=fencing_token+1,lease_owner=CASE WHEN $4='running' THEN $5 ELSE '' END,lease_expires_at=CASE WHEN $6>0 THEN now()+($6 * interval '1 second') ELSE NULL END,reply_id=COALESCE(NULLIF($8,''),reply_id),segment_count=CASE WHEN $9>0 THEN $9 ELSE segment_count END,updated_at=now() WHERE tenant_id=$1 AND event_id=$2 AND status=$3 AND ($3 <> 'running' OR ($4='execution_reconciling' AND lease_expires_at IS NOT NULL AND lease_expires_at <= now()) OR ($4<>'execution_reconciling' AND lease_owner=$5 AND fencing_token=$7 AND lease_expires_at IS NOT NULL AND lease_expires_at > now())) RETURNING "+eventColumns, transition.TenantID, transition.EventID, transition.From, transition.To, transition.Owner, leaseSeconds, transition.FencingToken, transition.ReplyID, transition.SegmentCount).Scan(eventArgs(&value)...)
	if err == nil {
		return cloneEvent(value), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.MessageEvent{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if _, lookupErr := s.GetMessage(ctx, transition.TenantID, transition.EventID); lookupErr != nil {
		return runtimestorage.MessageEvent{}, lookupErr
	}
	return runtimestorage.MessageEvent{}, runtimestorage.ErrConflict
}

// AppendEventPayload stores an immutable session event payload.
func (s *Store) AppendEventPayload(ctx context.Context, payload sessionstorage.EventPayload) (sessionstorage.EventPayload, error) {
	if err := checkStore(ctx, s); err != nil {
		return sessionstorage.EventPayload{}, err
	}
	if err := validatePayload(payload); err != nil {
		return sessionstorage.EventPayload{}, err
	}
	var value sessionstorage.EventPayload
	err := s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_event_history (tenant_id,session_id,event_id,payload) VALUES ($1,$2,$3,$4::jsonb) ON CONFLICT (tenant_id,session_id,event_id) DO UPDATE SET event_id=public.runtime_event_history.event_id WHERE public.runtime_event_history.payload=EXCLUDED.payload RETURNING tenant_id,session_id,event_id,payload::text,history_seq,created_at", payload.TenantID, payload.SessionID, payload.EventID, payload.Payload).Scan(&value.TenantID, &value.SessionID, &value.EventID, &value.Payload, &value.HistorySeq, &value.CreatedAt)
	if err == nil {
		return clonePayload(value), nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return sessionstorage.EventPayload{}, runtimestorage.ErrConflict
	}
	return sessionstorage.EventPayload{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
}

// ListEventPayloads returns ordered event history for a session.
func (s *Store) ListEventPayloads(ctx context.Context, tenantID, sessionID string) ([]sessionstorage.EventPayload, error) {
	if err := checkStore(ctx, s); err != nil {
		return nil, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT tenant_id,session_id,event_id,payload::text,history_seq,created_at FROM public.runtime_event_history WHERE tenant_id=$1 AND session_id=$2 ORDER BY history_seq", tenantID, sessionID)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	var result []sessionstorage.EventPayload
	for rows.Next() {
		var value sessionstorage.EventPayload
		if err := rows.Scan(&value.TenantID, &value.SessionID, &value.EventID, &value.Payload, &value.HistorySeq, &value.CreatedAt); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		result = append(result, clonePayload(value))
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	if result == nil {
		if _, err := s.GetSession(ctx, tenantID, sessionID); err != nil {
			return nil, err
		}
		result = []sessionstorage.EventPayload{}
	}
	return result, nil
}

// EnqueueReply stores one durable reply segment idempotently.
func (s *Store) EnqueueReply(ctx context.Context, value runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	var normalizeErr error
	value, normalizeErr = runtimestorage.NormalizeReplyOutbox(value)
	if normalizeErr != nil {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if runtimestorage.ValidateTenant(value.TenantID) != nil || value.ReplyID == "" || value.EventID == "" || value.SegmentIndex < 0 || value.SegmentCount <= value.SegmentIndex || runtimestorage.ValidateReplyTarget(value.ReplyTarget) != nil {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if value.Status == "" {
		value.Status = runtimestorage.ReplyPending
	}
	if value.Status != runtimestorage.ReplyPending {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if value.ReplyTarget != (runtimestorage.ReplyTarget{}) {
		event, err := s.GetMessage(ctx, value.TenantID, value.EventID)
		if err != nil {
			return runtimestorage.ReplyOutbox{}, err
		}
		if event.ReplyTarget != value.ReplyTarget {
			return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
		}
	}
	var result runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, insertReplyStatement+" RETURNING "+replyColumns, replyInsertArgs(value)...).Scan(replyArgs(&result)...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if _, lookupErr := s.GetMessage(ctx, value.TenantID, value.EventID); lookupErr != nil {
				return runtimestorage.ReplyOutbox{}, lookupErr
			}
			return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
		}
		return runtimestorage.ReplyOutbox{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return cloneReply(result), nil
}

// EnqueueReplies commits an entire reply's segment set in one transaction.
//
//nolint:gocyclo // The transaction validates and atomically persists the complete reply batch.
func (s *Store) EnqueueReplies(ctx context.Context, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	if err := checkStore(ctx, s); err != nil {
		return nil, err
	}
	var err error
	values, err = normalizeReplyBatch(values)
	if err != nil {
		return nil, err
	}
	if err := validateReplyBatch(values); err != nil {
		return nil, err
	}
	first := values[0]
	if first.ReplyTarget != (runtimestorage.ReplyTarget{}) {
		event, err := s.GetMessage(ctx, first.TenantID, first.EventID)
		if err != nil {
			return nil, err
		}
		if event.ReplyTarget != first.ReplyTarget {
			return nil, runtimestorage.ErrConflict
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := s.insertReplySegments(ctx, tx, values)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return result, nil
}

// EnqueueRepliesWithCorrelation atomically persists execution correlation and
// the complete reply segment batch.
func (s *Store) EnqueueRepliesWithCorrelation(ctx context.Context, correlation runtimestorage.ReplyCorrelation, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	if err := checkStore(ctx, s); err != nil {
		return nil, err
	}
	if correlation.TenantID == "" || correlation.EventID == "" || correlation.RequestID == "" {
		return nil, runtimestorage.ErrInvalid
	}
	correlation.TraceParent = observability.NormalizeTraceParent(correlation.TraceParent)
	var normalizeErr error
	values, normalizeErr = normalizeReplyBatch(values)
	if normalizeErr != nil {
		return nil, normalizeErr
	}
	first, err := validateReplyBatchForCorrelation(correlation, values)
	if err != nil {
		return nil, err
	}
	if first.ReplyTarget != (runtimestorage.ReplyTarget{}) {
		event, err := s.GetMessage(ctx, first.TenantID, first.EventID)
		if err != nil {
			return nil, err
		}
		if event.ReplyTarget != first.ReplyTarget {
			return nil, runtimestorage.ErrConflict
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = tx.Rollback() }()
	if err := tx.QueryRowContext(ctx, "INSERT INTO public.runtime_reply_correlation (tenant_id,event_id,request_id,trace_id,trace_parent) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,event_id) DO UPDATE SET request_id=public.runtime_reply_correlation.request_id, trace_id=public.runtime_reply_correlation.trace_id, trace_parent=public.runtime_reply_correlation.trace_parent WHERE public.runtime_reply_correlation.request_id=EXCLUDED.request_id AND public.runtime_reply_correlation.trace_id=EXCLUDED.trace_id AND public.runtime_reply_correlation.trace_parent=EXCLUDED.trace_parent RETURNING tenant_id", correlation.TenantID, correlation.EventID, correlation.RequestID, correlation.TraceID, correlation.TraceParent).Scan(new(string)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, runtimestorage.ErrConflict
		}
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	result, err := s.insertReplySegments(ctx, tx, values)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return result, nil
}

func validateReplyBatchForCorrelation(correlation runtimestorage.ReplyCorrelation, values []runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	if err := validateReplyBatch(values); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	first := values[0]
	if first.TenantID != correlation.TenantID || first.EventID != correlation.EventID {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	return first, nil
}

func validateReplyBatch(values []runtimestorage.ReplyOutbox) error {
	if len(values) == 0 {
		return runtimestorage.ErrInvalid
	}
	first := values[0]
	seen := make(map[int]struct{}, len(values))
	for _, value := range values {
		if runtimestorage.ValidateTenant(value.TenantID) != nil || value.ReplyID == "" || value.EventID == "" || value.SegmentIndex < 0 || value.SegmentCount <= value.SegmentIndex || value.Status != "" && value.Status != runtimestorage.ReplyPending || runtimestorage.ValidateReplyTarget(value.ReplyTarget) != nil || value.TenantID != first.TenantID || value.ReplyID != first.ReplyID || value.EventID != first.EventID || value.SegmentCount != first.SegmentCount || value.ReplyTarget != first.ReplyTarget {
			return runtimestorage.ErrInvalid
		}
		if _, duplicate := seen[value.SegmentIndex]; duplicate {
			return runtimestorage.ErrInvalid
		}
		seen[value.SegmentIndex] = struct{}{}
	}
	if len(seen) != first.SegmentCount {
		return runtimestorage.ErrInvalid
	}
	for index := 0; index < first.SegmentCount; index++ {
		if _, present := seen[index]; !present {
			return runtimestorage.ErrInvalid
		}
	}
	return nil
}

func normalizeReplyBatch(values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	normalized := make([]runtimestorage.ReplyOutbox, 0, len(values))
	for _, value := range values {
		reply, err := runtimestorage.NormalizeReplyOutbox(value)
		if err != nil {
			return nil, runtimestorage.ErrInvalid
		}
		normalized = append(normalized, reply)
	}
	return normalized, nil
}

func (s *Store) insertReplySegments(ctx context.Context, tx *sql.Tx, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	result := make([]runtimestorage.ReplyOutbox, 0, len(values))
	for _, value := range values {
		var row runtimestorage.ReplyOutbox
		err := tx.QueryRowContext(ctx, insertReplyStatement+" RETURNING "+replyColumns, replyInsertArgs(value)...).Scan(replyArgs(&row)...)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if _, lookupErr := lookupMessage(ctx, tx, value.TenantID, value.EventID); lookupErr != nil {
					return nil, lookupErr
				}
				return nil, runtimestorage.ErrConflict
			}
			return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
		}
		result = append(result, cloneReply(row))
	}
	return result, nil
}

// GetReply loads one durable reply segment.
func (s *Store) GetReply(ctx context.Context, tenantID, replyID string, segment int) (runtimestorage.ReplyOutbox, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || segment < 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, "SELECT "+replyColumns+" FROM public.reply_outbox WHERE tenant_id=$1 AND reply_id=$2 AND segment_index=$3", tenantID, replyID, segment).Scan(replyArgs(&value)...)
	if err != nil {
		return runtimestorage.ReplyOutbox{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	return cloneReply(value), nil
}

// ListReplyCandidates returns reply segments eligible for delivery.
func (s *Store) ListReplyCandidates(ctx context.Context, tenantID string) ([]runtimestorage.ReplyOutbox, error) {
	if err := checkStore(ctx, s); err != nil {
		return nil, err
	}
	if err := runtimestorage.ValidateTenant(tenantID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+replyColumns+" FROM public.reply_outbox WHERE tenant_id=$1 ORDER BY updated_at", tenantID)
	if err != nil {
		return nil, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	defer func() { _ = rows.Close() }()
	result := make([]runtimestorage.ReplyOutbox, 0)
	for rows.Next() {
		var value runtimestorage.ReplyOutbox
		if err := rows.Scan(replyArgs(&value)...); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		result = append(result, cloneReply(value))
	}
	if err := rows.Err(); err != nil {
		return nil, runtimestorage.ErrStorage
	}
	return result, nil
}

// ClaimReply leases one reply segment to a delivery worker.
func (s *Store) ClaimReply(ctx context.Context, tenantID, replyID string, segment int, owner string, leaseDuration time.Duration) (runtimestorage.ReplyOutbox, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || owner == "" || leaseDuration <= 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	seconds := int64(leaseDuration / time.Second)
	if seconds == 0 {
		seconds = 1
	}
	var value runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, "UPDATE public.reply_outbox SET status='sending', attempts=attempts+1, fencing_token=fencing_token+1, lease_owner=$4, lease_expires_at=now()+($5 * interval '1 second'), updated_at=now() WHERE tenant_id=$1 AND reply_id=$2 AND segment_index=$3 AND (status IN ('pending','retryable') OR (status='sending' AND lease_expires_at IS NOT NULL AND lease_expires_at <= now())) RETURNING "+replyColumns, tenantID, replyID, segment, owner, seconds).Scan(replyArgs(&value)...)
	if err == nil {
		return cloneReply(value), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.ReplyOutbox{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if _, lookupErr := s.GetReply(ctx, tenantID, replyID, segment); lookupErr != nil {
		return runtimestorage.ReplyOutbox{}, lookupErr
	}
	return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
}

// RecordReplyReceipt persists a provider acknowledgement without releasing
// the current sending lease. The worker later owns the sent transition.
func (s *Store) RecordReplyReceipt(ctx context.Context, receipt runtimestorage.ReplyReceipt) (runtimestorage.ReplyOutbox, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(receipt.TenantID) != nil || receipt.ReplyID == "" || receipt.SegmentIndex < 0 || receipt.Owner == "" || receipt.FencingToken <= 0 || strings.TrimSpace(receipt.ProviderID) == "" {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	var value runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, "UPDATE public.reply_outbox SET provider_message_id=$6, updated_at=now() WHERE tenant_id=$1 AND reply_id=$2 AND segment_index=$3 AND status='sending' AND lease_owner=$4 AND fencing_token=$5 AND lease_expires_at IS NOT NULL AND lease_expires_at > now() AND (provider_message_id='' OR provider_message_id=$6) RETURNING "+replyColumns, receipt.TenantID, receipt.ReplyID, receipt.SegmentIndex, receipt.Owner, receipt.FencingToken, receipt.ProviderID).Scan(replyArgs(&value)...)
	if err == nil {
		return cloneReply(value), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.ReplyOutbox{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if _, lookupErr := s.GetReply(ctx, receipt.TenantID, receipt.ReplyID, receipt.SegmentIndex); lookupErr != nil {
		return runtimestorage.ReplyOutbox{}, lookupErr
	}
	return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
}

// TransitionReply applies a fenced reply delivery transition.
func (s *Store) TransitionReply(ctx context.Context, transition runtimestorage.ReplyTransition) (runtimestorage.ReplyOutbox, error) {
	if err := checkStore(ctx, s); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(transition.TenantID) != nil || transition.ReplyID == "" || transition.Owner == "" {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if !runtimestorage.ValidateTransition(transition.From, transition.To) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrIllegalTransition
	}
	leaseSeconds := int64(0)
	if transition.LeaseDuration > 0 {
		leaseSeconds = int64(transition.LeaseDuration / time.Second)
		if leaseSeconds == 0 {
			leaseSeconds = 1
		}
	}
	var value runtimestorage.ReplyOutbox
	err := s.db.QueryRowContext(ctx, "UPDATE public.reply_outbox SET status=$5, attempts=attempts+CASE WHEN $5='sending' THEN 1 ELSE 0 END, fencing_token=fencing_token+1, lease_owner=$6, lease_expires_at=CASE WHEN $7>0 THEN now()+($7 * interval '1 second') ELSE NULL END, provider_message_id=COALESCE(NULLIF($8,''),provider_message_id), last_error_class=COALESCE(NULLIF($9,''),last_error_class), updated_at=now() WHERE tenant_id=$1 AND reply_id=$2 AND segment_index=$3 AND status=$4 AND (lease_owner='' OR lease_owner=$6) AND ($10=0 OR fencing_token=$10) AND (status <> 'sending' OR lease_expires_at IS NULL OR lease_expires_at > now()) RETURNING "+replyColumns, transition.TenantID, transition.ReplyID, transition.SegmentIndex, transition.From, transition.To, transition.Owner, leaseSeconds, transition.ProviderID, transition.ErrorClass, transition.FencingToken).Scan(replyArgs(&value)...)
	if err == nil {
		return cloneReply(value), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return runtimestorage.ReplyOutbox{}, mapError(ctx, err, runtimestorage.ErrNotFound, runtimestorage.ErrDuplicate, runtimestorage.ErrConflict, runtimestorage.ErrInvalid)
	}
	if _, lookupErr := s.GetReply(ctx, transition.TenantID, transition.ReplyID, transition.SegmentIndex); lookupErr != nil {
		return runtimestorage.ReplyOutbox{}, lookupErr
	}
	return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
}

// Close releases store resources; the borrowed database remains caller-owned.
func (s *Store) Close() error { return nil }

func checkStore(ctx context.Context, store *Store) error {
	if err := check(ctx); err != nil {
		return err
	}
	if store == nil || store.db == nil {
		return runtimestorage.ErrStorage
	}
	return nil
}

func check(ctx context.Context) error {
	if ctx == nil {
		return runtimestorage.ErrInvalid
	}
	return ctx.Err()
}
func eventArgs(value *runtimestorage.MessageEvent) []any {
	return []any{&value.TenantID, &value.EventID, &value.SessionID, &value.BindingID, &value.ExternalMessageID, &value.IdempotencyKey, &value.EventSeq, &value.Status, &value.FencingToken, &value.LeaseOwner, &value.LeaseExpiresAt, &value.ReplyID, &value.SegmentCount, &value.ReplyTarget.ConversationKind, &value.ReplyTarget.ReceiverID, &value.ReplyTarget.ThreadID, &value.CreatedAt, &value.UpdatedAt}
}
func replyInsertArgs(value runtimestorage.ReplyOutbox) []any {
	return []any{
		value.TenantID, value.ReplyID, value.EventID, value.SegmentIndex, value.SegmentCount, value.Payload,
		value.Kind, value.Attachment.ID, value.Attachment.Kind, value.Attachment.MIMEType, value.Attachment.Name,
		value.Attachment.Size, value.Attachment.SHA256, value.Attachment.Provider, value.Attachment.ProviderID, value.Fallback,
		value.ReplyTarget.BindingID, value.ReplyTarget.ConversationKind, value.ReplyTarget.ReceiverID, value.ReplyTarget.ThreadID,
	}
}
func replyArgs(value *runtimestorage.ReplyOutbox) []any {
	return []any{
		&value.TenantID, &value.ReplyID, &value.EventID, &value.SegmentIndex, &value.SegmentCount, &value.Payload,
		&value.Kind, &value.Attachment.ID, &value.Attachment.Kind, &value.Attachment.MIMEType, &value.Attachment.Name,
		&value.Attachment.Size, &value.Attachment.SHA256, &value.Attachment.Provider, &value.Attachment.ProviderID, &value.Fallback,
		&value.ReplyTarget.BindingID, &value.ReplyTarget.ConversationKind, &value.ReplyTarget.ReceiverID, &value.ReplyTarget.ThreadID,
		&value.Status, &value.Attempts, &value.FencingToken, &value.LeaseOwner, &value.LeaseExpiresAt,
		&value.ProviderMessageID, &value.LastErrorClass, &value.CreatedAt, &value.UpdatedAt,
	}
}
func cloneSession(value sessionstorage.Session) sessionstorage.Session {
	if value.State != nil {
		copy := make(map[string]any, len(value.State))
		for k, v := range value.State {
			copy[k] = v
		}
		value.State = copy
	}
	return value
}
func cloneEvent(value runtimestorage.MessageEvent) runtimestorage.MessageEvent {
	if value.ReplyTarget.ConversationKind != "" {
		value.ReplyTarget.BindingID = value.BindingID
	}
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}
func clonePayload(value sessionstorage.EventPayload) sessionstorage.EventPayload {
	value.Payload = append([]byte(nil), value.Payload...)
	return value
}
func validatePayload(value sessionstorage.EventPayload) error {
	if runtimestorage.ValidateSession(value.TenantID, value.SessionID) != nil || value.EventID == "" || len(value.Payload) == 0 || !json.Valid(value.Payload) {
		return runtimestorage.ErrInvalid
	}
	return nil
}
func cloneReply(value runtimestorage.ReplyOutbox) runtimestorage.ReplyOutbox {
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}

var _ sessionstorage.SessionStateStore = (*Store)(nil)
var _ sessionstorage.EventHistoryStore = (*Store)(nil)
var _ runtimestorage.MessageStore = (*Store)(nil)
var _ runtimestorage.ReplyStore = (*Store)(nil)
var _ runtimestorage.ReplyReceiptRecorder = (*Store)(nil)
