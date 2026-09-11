package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PostgresStateStore persists execution state in PostgreSQL. RecordExecution
// uses one SQL transaction so the session, audit event, and Outbox record either
// all commit or all roll back.
type PostgresStateStore struct {
	database *sql.DB
	audit    AuditContentDigester
	now      func() time.Time
}

// NewPostgresStateStore constructs a durable state store around an injected
// database pool. The composition root owns the pool lifecycle.
func NewPostgresStateStore(database *sql.DB, auditHMACKey []byte) (*PostgresStateStore, error) {
	if database == nil {
		return nil, fmt.Errorf("PostgreSQL database is required")
	}
	digester, err := NewAuditContentDigester(auditHMACKey)
	if err != nil {
		return nil, err
	}
	return &PostgresStateStore{database: database, now: time.Now, audit: digester}, nil
}

// RecordExecution writes the session mutation, redacted audit, and Outbox event
// in a single transaction.
func (s *PostgresStateStore) RecordExecution(ctx context.Context, record ExecutionRecord) (OutboxEvent, error) {
	if err := ctx.Err(); err != nil {
		return OutboxEvent{}, err
	}
	if err := validateExecutionRecord(record); err != nil {
		return OutboxEvent{}, err
	}
	eventID, err := newStateID()
	if err != nil {
		return OutboxEvent{}, err
	}
	auditID, err := newStateID()
	if err != nil {
		return OutboxEvent{}, err
	}
	now := s.now().UTC()
	event := OutboxEvent{
		ID:           eventID,
		TenantID:     record.TenantID,
		AggregateKey: record.SessionKey,
		Type:         record.OutboxType,
		Payload:      append([]byte(nil), record.OutboxPayload...),
		RequestID:    record.OutboxRequestID,
		CreatedAt:    now,
	}

	transaction, err := s.database.BeginTx(ctx, nil)
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("begin execution state transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	completed, err := transaction.ExecContext(ctx, `
UPDATE messages
SET status = 'completed',
    updated_at = $4
WHERE tenant_id = $1 AND channel_type = $2 AND binding_id = $3 AND message_id = $5
  AND status = 'processing' AND trace_id = $6`,
		record.TenantID, record.Channel, record.BindingID, now, record.MessageID, record.TraceID,
	)
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("complete message execution: %w", err)
	}
	rows, err := completed.RowsAffected()
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("read message completion result: %w", err)
	}
	if rows != 1 {
		return OutboxEvent{}, fmt.Errorf("message execution claim not found for completion")
	}

	if record.FencingToken > 0 {
		var currentToken uint64
		var leaseUntil time.Time
		err := transaction.QueryRowContext(ctx, `
SELECT fencing_token, lease_until
FROM session_execution_leases
WHERE tenant_id = $1 AND session_key = $2
FOR UPDATE`, record.TenantID, record.SessionKey).Scan(&currentToken, &leaseUntil)
		if errors.Is(err, sql.ErrNoRows) || err == nil && (currentToken != record.FencingToken || !leaseUntil.After(now)) {
			return OutboxEvent{}, ErrStaleFencingToken
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return OutboxEvent{}, fmt.Errorf("check session execution fence: %w", err)
		}
	}

	if _, err := transaction.ExecContext(ctx, `
INSERT INTO sessions (
    tenant_id, app_code, session_key, last_message_id, revision,
    subject_id, owner_platform_user_id, status, archived_at, updated_at
) VALUES ($1, $2, $3, $4, 1, $6, NULLIF($7, ''), 'active', NULL, $5)
ON CONFLICT (tenant_id, session_key) DO UPDATE
SET app_code = EXCLUDED.app_code,
    last_message_id = EXCLUDED.last_message_id,
    revision = sessions.revision + 1,
    subject_id = CASE WHEN sessions.subject_id = '' THEN EXCLUDED.subject_id ELSE sessions.subject_id END,
    owner_platform_user_id = COALESCE(sessions.owner_platform_user_id, EXCLUDED.owner_platform_user_id),
    status = 'active',
    archived_at = NULL,
    updated_at = EXCLUDED.updated_at`,
		record.TenantID, record.AppCode, record.SessionKey, record.MessageID, now, record.SubjectID, record.OwnerPlatformUserID,
	); err != nil {
		return OutboxEvent{}, fmt.Errorf("upsert session: %w", err)
	}
	if strings.TrimSpace(record.ConversationID) != "" {
		if _, err := transaction.ExecContext(ctx, `
INSERT INTO channel_conversations (
    tenant_id, app_code, channel_type, binding_id, external_conversation_id, external_user_id,
    session_key, scope, started_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)
ON CONFLICT (tenant_id, app_code, channel_type, binding_id, external_conversation_id, session_key)
DO UPDATE SET external_user_id=EXCLUDED.external_user_id, scope=EXCLUDED.scope, updated_at=EXCLUDED.updated_at, ended_at=NULL`,
			record.TenantID, record.AppCode, record.Channel, record.BindingID, record.ConversationID,
			record.ExternalUserID, record.SessionKey, record.ConversationScope, now,
		); err != nil {
			return OutboxEvent{}, fmt.Errorf("upsert channel conversation: %w", err)
		}
	}
	if strings.TrimSpace(record.TriggerType) != "" {
		if _, err := transaction.ExecContext(ctx, `
INSERT INTO inbound_message_routes (
    tenant_id, app_code, channel_type, message_id, session_key, binding_id,
    external_conversation_id, scope, actor_external_user_id, actor_platform_user_id,
    trigger_type, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''),$11,$12)
ON CONFLICT (tenant_id, channel_type, binding_id, message_id) DO NOTHING`,
			record.TenantID, record.AppCode, record.Channel, record.MessageID, record.SessionKey,
			record.BindingID, record.ConversationID, record.ConversationScope, record.ActorExternalUserID,
			record.ActorPlatformUserID, record.TriggerType, now,
		); err != nil {
			return OutboxEvent{}, fmt.Errorf("insert inbound message route: %w", err)
		}
	}
	if err := insertAuditEvent(ctx, transaction, auditEventFromExecution(auditID, record, s.audit.Digest(record.AuditDetail), now)); err != nil {
		return OutboxEvent{}, err
	}
	if _, err := transaction.ExecContext(ctx, `
INSERT INTO outbox_events (
    id, request_id, tenant_id, aggregate_key, event_type, payload, created_at
) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
		event.ID, event.RequestID, event.TenantID, event.AggregateKey, event.Type, string(event.Payload), event.CreatedAt,
	); err != nil {
		return OutboxEvent{}, fmt.Errorf("insert outbox event: %w", err)
	}
	if record.ModelUsage != nil {
		usageID, err := newStateID()
		if err != nil {
			return OutboxEvent{}, err
		}
		var promptTokens, cachedPromptTokens, completionTokens, totalTokens, costMicros any
		if record.ModelUsage.Known {
			promptTokens = record.ModelUsage.PromptTokens
			cachedPromptTokens = record.ModelUsage.CachedPromptTokens
			completionTokens = record.ModelUsage.CompletionTokens
			totalTokens = record.ModelUsage.TotalTokens
			costMicros = record.ModelUsage.CostMicros
		}
		breakdown, err := json.Marshal(record.ModelUsage.Breakdown)
		if err != nil {
			return OutboxEvent{}, fmt.Errorf("encode model usage breakdown: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `
INSERT INTO model_usage_ledger (
    id, tenant_id, app_code, channel_type, binding_id, message_id, trace_id,
    provider_id, model_name, usage_known, prompt_tokens, cached_prompt_tokens, completion_tokens, total_tokens,
    cost_micros, usage_breakdown, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb, $17)
ON CONFLICT (tenant_id, channel_type, binding_id, message_id) DO NOTHING`,
			usageID, record.TenantID, record.AppCode, record.Channel, record.BindingID, record.MessageID, record.TraceID,
			record.ModelUsage.ProviderID, record.ModelUsage.ModelName, record.ModelUsage.Known, promptTokens,
			cachedPromptTokens, completionTokens, totalTokens, costMicros, string(breakdown), now,
		); err != nil {
			return OutboxEvent{}, fmt.Errorf("insert model usage ledger: %w", err)
		}
	}
	if record.ExecutionTrace != nil {
		if err := writeExecutionTrace(ctx, transaction, ExecutionTraceRecord{
			TenantID:  record.TenantID,
			AppCode:   record.AppCode,
			Channel:   record.Channel,
			BindingID: record.BindingID,
			MessageID: record.MessageID,
			TraceID:   record.TraceID,
			Trace:     *record.ExecutionTrace,
		}); err != nil {
			return OutboxEvent{}, err
		}
	}
	if err := transaction.Commit(); err != nil {
		return OutboxEvent{}, fmt.Errorf("commit execution state transaction: %w", err)
	}
	return event, nil
}
