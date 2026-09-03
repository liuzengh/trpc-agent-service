package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

// PostgresJournal stores inbound, conversation, run and queue-outbox records
// in one transaction. The caller owns db.
type PostgresJournal struct {
	db *sql.DB
}

func NewPostgresJournal(db *sql.DB) (*PostgresJournal, error) {
	if db == nil {
		return nil, fmt.Errorf("inbound journal database is nil")
	}
	return &PostgresJournal{db: db}, nil
}

func (j *PostgresJournal) Accept(
	ctx context.Context,
	request InboundRequest,
) (AcceptResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateInbound(&request); err != nil {
		return AcceptResult{}, err
	}
	payloadHash := inboundPayloadHash(request)
	conversationID := stableID(
		"conv_",
		request.Scope.StorageScope,
		request.UserID,
		request.SessionID,
	)
	inboundID := stableID(
		"in_",
		request.Scope.ChannelBindingID,
		request.ExternalMessageID,
	)
	requestID := stableID(
		"req_",
		request.Scope.ChannelBindingID,
		request.ExternalMessageID,
	)

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptResult{}, fmt.Errorf("begin inbound transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO conversation(
    conversation_id, tenant_id, app_id, channel_binding_id, session_id,
    runtime_user_id, chat_type, pinned_revision_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, app_id, runtime_user_id, session_id) DO NOTHING`,
		conversationID,
		request.Scope.TenantID,
		request.Scope.AppID,
		request.Scope.ChannelBindingID,
		request.SessionID,
		request.UserID,
		request.ChatType,
		request.Scope.RevisionID,
	); err != nil {
		return AcceptResult{}, fmt.Errorf("ensure conversation: %w", err)
	}
	var pinnedRevisionID string
	if err := tx.QueryRowContext(ctx, `
SELECT conversation_id, pinned_revision_id
FROM conversation
WHERE tenant_id = $1 AND app_id = $2 AND runtime_user_id = $3 AND session_id = $4
FOR UPDATE`,
		request.Scope.TenantID,
		request.Scope.AppID,
		request.UserID,
		request.SessionID,
	).Scan(&conversationID, &pinnedRevisionID); err != nil {
		return AcceptResult{}, fmt.Errorf("lock conversation: %w", err)
	}
	payload, err := json.Marshal(map[string]any{
		"text":      request.Text,
		"chat_type": request.ChatType,
	})
	if err != nil {
		return AcceptResult{}, fmt.Errorf("marshal inbound payload: %w", err)
	}
	var insertedID string
	err = tx.QueryRowContext(ctx, `
INSERT INTO inbound_message(
    inbound_id, tenant_id, app_id, channel_binding_id, external_message_id,
    request_id, conversation_id, actor_user_id, message_type, payload,
    payload_hash, status
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'text', $9::jsonb, $10, 'queued')
ON CONFLICT (channel_binding_id, external_message_id) DO NOTHING
RETURNING inbound_id`,
		inboundID,
		request.Scope.TenantID,
		request.Scope.AppID,
		request.Scope.ChannelBindingID,
		request.ExternalMessageID,
		requestID,
		conversationID,
		request.UserID,
		string(payload),
		payloadHash,
	).Scan(&insertedID)
	if errors.Is(err, sql.ErrNoRows) {
		result, duplicateErr := duplicateAcceptResult(
			ctx,
			tx,
			request.Scope.ChannelBindingID,
			request.ExternalMessageID,
			payloadHash,
		)
		if duplicateErr != nil {
			return AcceptResult{}, duplicateErr
		}
		if err := tx.Commit(); err != nil {
			return AcceptResult{}, fmt.Errorf("commit duplicate inbound transaction: %w", err)
		}
		return result, nil
	}
	if err != nil {
		return AcceptResult{}, fmt.Errorf("insert inbound message: %w", err)
	}

	var turnSeq int64
	if err := tx.QueryRowContext(ctx, `
UPDATE conversation
SET last_turn_seq = last_turn_seq + 1, updated_at = now()
WHERE conversation_id = $1
RETURNING last_turn_seq`, conversationID).Scan(&turnSeq); err != nil {
		return AcceptResult{}, fmt.Errorf("allocate conversation turn: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO agent_run(
    request_id, tenant_id, app_id, revision_id, conversation_id,
    turn_seq, status, created_at
) VALUES ($1, $2, $3, $4, $5, $6, 'queued', now())`,
		requestID,
		request.Scope.TenantID,
		request.Scope.AppID,
		pinnedRevisionID,
		conversationID,
		turnSeq,
	); err != nil {
		return AcceptResult{}, fmt.Errorf("insert Agent run: %w", err)
	}
	scope := request.Scope
	scope.RevisionID = pinnedRevisionID
	task := workqueue.AgentTask{
		InboundID:      inboundID,
		RequestID:      requestID,
		ConversationID: conversationID,
		Scope:          scope,
		MessageID:      request.ExternalMessageID,
		UserID:         request.UserID,
		SessionID:      request.SessionID,
		Text:           request.Text,
		TurnSeq:        turnSeq,
	}
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return AcceptResult{}, fmt.Errorf("marshal Agent task: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO queue_outbox(outbox_id, topic, partition_key, payload)
VALUES ($1, 'agent.run', $2, $3::jsonb)`,
		stableID("qout_", requestID),
		request.Scope.StorageScope+"|"+request.UserID+"|"+request.SessionID,
		string(taskJSON),
	); err != nil {
		return AcceptResult{}, fmt.Errorf("insert queue outbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return AcceptResult{}, fmt.Errorf("commit inbound transaction: %w", err)
	}
	return AcceptResult{
		InboundID:      inboundID,
		RequestID:      requestID,
		ConversationID: conversationID,
		RevisionID:     pinnedRevisionID,
		TurnSeq:        turnSeq,
	}, nil
}

func duplicateAcceptResult(
	ctx context.Context,
	tx *sql.Tx,
	channelBindingID string,
	externalMessageID string,
	payloadHash string,
) (AcceptResult, error) {
	var result AcceptResult
	var existingHash string
	if err := tx.QueryRowContext(ctx, `
SELECT i.inbound_id, i.request_id, i.conversation_id, i.payload_hash,
       r.revision_id, r.turn_seq
FROM inbound_message i
JOIN agent_run r ON r.request_id = i.request_id
WHERE i.channel_binding_id = $1 AND i.external_message_id = $2`,
		channelBindingID,
		externalMessageID,
	).Scan(
		&result.InboundID,
		&result.RequestID,
		&result.ConversationID,
		&existingHash,
		&result.RevisionID,
		&result.TurnSeq,
	); err != nil {
		return AcceptResult{}, fmt.Errorf("read duplicate inbound message: %w", err)
	}
	if existingHash != payloadHash {
		return AcceptResult{}, ErrMessageConflict
	}
	result.Duplicate = true
	return result, nil
}

func (j *PostgresJournal) Ready(ctx context.Context) error {
	if j == nil || j.db == nil {
		return ErrJournalClosed
	}
	if err := j.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping inbound PostgreSQL: %w", err)
	}
	return nil
}

// Close is a no-op because the control-plane repository owns the shared pool.
func (j *PostgresJournal) Close() error { return nil }

var _ Journal = (*PostgresJournal)(nil)
