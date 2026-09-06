package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
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
		"text":         request.Text,
		"chat_type":    request.ChatType,
		"reply_target": request.ReplyTarget,
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
	traceParent, traceState := outboundTraceHeaders(ctx)
	if request.DirectReply != "" {
		// Commit Inbox, completed control Run and Outbound together. There is
		// no queue_outbox row, so no Worker/model can reinterpret this reply.
		if _, err := tx.ExecContext(ctx, `
UPDATE inbound_message SET status='processed', processed_at=now() WHERE inbound_id=$1`, inboundID); err != nil {
			return AcceptResult{}, fmt.Errorf("complete control inbound: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE agent_run SET status='completed', agent_name='platform-control',
    started_at=now(), completed_at=now(), trace_id=NULLIF($2,'') WHERE request_id=$1`, requestID, audit.TraceID(ctx)); err != nil {
			return AcceptResult{}, fmt.Errorf("complete control run: %w", err)
		}
		outbound, err := json.Marshal(map[string]any{
			"text": request.DirectReply, "reply_target": request.ReplyTarget,
			"agent_name": "platform-control", "event_count": 0, "traceparent": traceParent,
		})
		if err != nil {
			return AcceptResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO outbound_message(outbound_id, tenant_id, app_id, channel_binding_id,
    request_id, conversation_id, payload, status)
VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,'pending')`, stableID("out_", requestID),
			scope.TenantID, scope.AppID, scope.ChannelBindingID, requestID, conversationID, string(outbound)); err != nil {
			return AcceptResult{}, fmt.Errorf("insert control reply: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return AcceptResult{}, fmt.Errorf("commit control reply: %w", err)
		}
		return AcceptResult{InboundID: inboundID, RequestID: requestID, ConversationID: conversationID,
			RevisionID: pinnedRevisionID, TurnSeq: turnSeq}, nil
	}
	task := workqueue.AgentTask{
		InboundID:         inboundID,
		RequestID:         requestID,
		ConversationID:    conversationID,
		Scope:             scope,
		MessageID:         request.ExternalMessageID,
		UserID:            request.UserID,
		SessionID:         request.SessionID,
		Text:              request.Text,
		ReplyTarget:       request.ReplyTarget,
		TurnSeq:           turnSeq,
		TraceParent:       traceParent,
		TraceState:        traceState,
		ApprovedTools:     append([]string(nil), request.ApprovedTools...),
		ApprovedToolCalls: append([]governance.ApprovedToolCall(nil), request.ApprovedToolCalls...),
		ApprovalID:        request.ApprovalID,
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

func (j *PostgresJournal) ClaimQueueOutbox(
	ctx context.Context,
	workerID string,
	limit int,
	lease time.Duration,
) ([]QueueOutboxItem, error) {
	if workerID == "" || limit <= 0 || lease <= 0 {
		return nil, fmt.Errorf("outbox worker, limit and lease are required")
	}
	rows, err := j.db.QueryContext(ctx, `
WITH candidates AS (
    SELECT outbox_id
    FROM queue_outbox
    WHERE status IN ('pending', 'publishing')
      AND next_attempt_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE queue_outbox q
SET status = 'publishing',
    locked_by = $2,
    locked_until = now() + $3::interval,
    attempt_count = attempt_count + 1
FROM candidates c
WHERE q.outbox_id = c.outbox_id
RETURNING q.outbox_id, q.payload`, limit, workerID, postgresInterval(lease))
	if err != nil {
		return nil, fmt.Errorf("claim queue outbox: %w", err)
	}
	defer rows.Close()
	result := make([]QueueOutboxItem, 0, limit)
	for rows.Next() {
		var item QueueOutboxItem
		var payload []byte
		if err := rows.Scan(&item.ID, &payload); err != nil {
			return nil, fmt.Errorf("scan queue outbox: %w", err)
		}
		if err := json.Unmarshal(payload, &item.Task); err != nil {
			return nil, fmt.Errorf("decode queue outbox %q: %w", item.ID, err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queue outbox: %w", err)
	}
	return result, nil
}

func (j *PostgresJournal) MarkQueueOutboxPublished(
	ctx context.Context,
	outboxID string,
	workerID string,
) error {
	result, err := j.db.ExecContext(ctx, `
UPDATE queue_outbox
SET status = 'published', published_at = now(), locked_by = NULL,
    locked_until = NULL, last_error = NULL
WHERE outbox_id = $1 AND locked_by = $2 AND status = 'publishing'`, outboxID, workerID)
	if err != nil {
		return fmt.Errorf("mark queue outbox published: %w", err)
	}
	return requireOneRow(result, "queue outbox publish ownership mismatch")
}

func (j *PostgresJournal) MarkQueueOutboxFailed(
	ctx context.Context,
	outboxID string,
	workerID string,
	retryAt time.Time,
	cause error,
) error {
	errorText := ""
	if cause != nil {
		errorText = platformlog.Redact(cause.Error())
		if len(errorText) > 2048 {
			errorText = errorText[:2048]
		}
	}
	result, err := j.db.ExecContext(ctx, `
UPDATE queue_outbox
SET status = 'pending', next_attempt_at = $3, locked_by = NULL,
    locked_until = NULL, last_error = $4
WHERE outbox_id = $1 AND locked_by = $2 AND status = 'publishing'`,
		outboxID, workerID, retryAt, errorText)
	if err != nil {
		return fmt.Errorf("mark queue outbox failed: %w", err)
	}
	return requireOneRow(result, "queue outbox failure ownership mismatch")
}

func requireOneRow(result sql.Result, message string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read affected rows: %w", err)
	}
	if rows != 1 {
		return errors.New(message)
	}
	return nil
}

func postgresInterval(value time.Duration) string {
	return fmt.Sprintf("%f seconds", value.Seconds())
}

func (j *PostgresJournal) MarkRunRunning(
	ctx context.Context,
	requestID string,
	workerID string,
) error {
	result, err := j.db.ExecContext(ctx, `
UPDATE agent_run
SET status = 'running', worker_id = $2, started_at = COALESCE(started_at, now()),
    error_type = NULL, error_message = NULL
WHERE request_id = $1 AND status NOT IN ('completed','dead')`, requestID, workerID)
	if err != nil {
		return fmt.Errorf("mark Agent run running: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read Agent run affected rows: %w", err)
	}
	if rows == 1 {
		return nil
	}
	var status string
	if err := j.db.QueryRowContext(
		ctx,
		`SELECT status FROM agent_run WHERE request_id = $1`,
		requestID,
	).Scan(&status); err != nil {
		return fmt.Errorf("read Agent run status: %w", err)
	}
	if status == "completed" {
		return nil
	}
	if status == "dead" {
		return ErrRunTerminal
	}
	return fmt.Errorf("Agent run %q cannot start from status %q", requestID, status)
}

func (j *PostgresJournal) CompleteRun(
	ctx context.Context,
	task workqueue.AgentTask,
	result RunResult,
) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Agent completion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	updated, err := tx.ExecContext(ctx, `
UPDATE agent_run
SET status = 'completed', fencing_token = $2, agent_name = $3,
    prompt_tokens = $4, completion_tokens = $5, cost = $6, trace_id = NULLIF($7, ''),
    completed_at = now(), error_type = NULL, error_message = NULL
WHERE request_id = $1 AND fencing_token <= $2 AND status <> 'dead' AND ($8='' OR worker_id=$8 OR status='completed')`,
		task.RequestID,
		result.FencingToken,
		result.AgentName,
		result.PromptTokens,
		result.CompletionTokens,
		result.Cost,
		result.TraceID,
		result.WorkerID,
	)
	if err != nil {
		return fmt.Errorf("complete Agent run: %w", err)
	}
	if err := requireOneRow(updated, "stale or missing Agent run completion"); err != nil {
		return ErrRunSuperseded
	}
	payload, err := json.Marshal(map[string]any{
		"text":         result.Reply,
		"reply_target": task.ReplyTarget,
		"agent_name":   result.AgentName,
		"event_count":  result.EventCount,
		"traceparent":  result.TraceParent,
	})
	if err != nil {
		return fmt.Errorf("marshal outbound payload: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO outbound_message(
    outbound_id, tenant_id, app_id, channel_binding_id, request_id,
    conversation_id, payload, status
) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 'pending')
ON CONFLICT (request_id) DO NOTHING`,
		stableID("out_", task.RequestID),
		task.Scope.TenantID,
		task.Scope.AppID,
		task.Scope.ChannelBindingID,
		task.RequestID,
		task.ConversationID,
		string(payload),
	); err != nil {
		return fmt.Errorf("insert outbound message: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE inbound_message
SET status = 'processed', processed_at = now()
WHERE inbound_id = $1`, task.InboundID); err != nil {
		return fmt.Errorf("mark inbound processed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Agent completion: %w", err)
	}
	return nil
}

func (j *PostgresJournal) FailRun(
	ctx context.Context,
	requestID string,
	errorType string,
	cause error,
	expectedWorker ...string,
) error {
	errorText := ""
	if cause != nil {
		errorText = platformlog.Redact(cause.Error())
		if len(errorText) > 2048 {
			errorText = errorText[:2048]
		}
	}
	owner := ""
	if len(expectedWorker) > 0 {
		owner = expectedWorker[0]
	}
	result, err := j.db.ExecContext(ctx, `
UPDATE agent_run
SET status = 'failed', error_type = $2, error_message = $3, completed_at = now()
WHERE request_id = $1 AND status NOT IN ('completed','dead') AND ($4='' OR worker_id=$4)`, requestID, errorType, errorText, owner)
	if err != nil {
		return fmt.Errorf("fail Agent run: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 && owner != "" {
		var status string
		if err := j.db.QueryRowContext(ctx, `SELECT status FROM agent_run WHERE request_id=$1`, requestID).Scan(&status); err != nil {
			return err
		}
		if status != "completed" && status != "dead" {
			return ErrRunSuperseded
		}
	}
	return nil
}

func (j *PostgresJournal) ClaimOutbound(
	ctx context.Context,
	workerID string,
	limit int,
	lease time.Duration,
) ([]OutboundItem, error) {
	if workerID == "" || limit <= 0 || lease <= 0 {
		return nil, fmt.Errorf("outbound worker, limit and lease are required")
	}
	rows, err := j.db.QueryContext(ctx, `
WITH candidates AS (
    SELECT outbound_id
    FROM outbound_message
    WHERE status IN ('pending', 'sending')
      AND (next_attempt_at IS NULL OR next_attempt_at <= now())
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE outbound_message o
SET status = 'sending', locked_by = $2,
    locked_until = now() + $3::interval,
    attempt_count = attempt_count + 1
FROM candidates c
WHERE o.outbound_id = c.outbound_id
	RETURNING o.outbound_id, o.request_id, o.tenant_id, o.channel_binding_id,
	          o.payload->>'text', COALESCE(o.payload->>'reply_target', ''),
	          o.attempt_count, COALESCE(o.payload->>'traceparent', '')`,
		limit, workerID, postgresInterval(lease))
	if err != nil {
		return nil, fmt.Errorf("claim outbound messages: %w", err)
	}
	defer rows.Close()
	result := make([]OutboundItem, 0, limit)
	for rows.Next() {
		var item OutboundItem
		if err := rows.Scan(
			&item.ID,
			&item.RequestID,
			&item.TenantID,
			&item.ChannelBindingID,
			&item.Text,
			&item.ReplyTarget,
			&item.AttemptCount,
			&item.TraceParent,
		); err != nil {
			return nil, fmt.Errorf("scan outbound message: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbound messages: %w", err)
	}
	return result, nil
}

func (j *PostgresJournal) MarkOutboundSent(
	ctx context.Context,
	outboundID string,
	workerID string,
	providerMessageID string,
) error {
	result, err := j.db.ExecContext(ctx, `
UPDATE outbound_message
SET status = 'sent', provider_message_id = $3, sent_at = now(),
    locked_by = NULL, locked_until = NULL,
    last_error_type = NULL, last_error_message = NULL
WHERE outbound_id = $1 AND locked_by = $2 AND status = 'sending'`,
		outboundID, workerID, providerMessageID)
	if err != nil {
		return fmt.Errorf("mark outbound sent: %w", err)
	}
	return requireOneRow(result, "outbound send ownership mismatch")
}

func (j *PostgresJournal) MarkOutboundFailed(
	ctx context.Context,
	outboundID string,
	workerID string,
	retryAt time.Time,
	terminal bool,
	cause error,
) error {
	status := "pending"
	if terminal {
		status = "dead"
	}
	errorType := "delivery"
	errorText := ""
	if cause != nil {
		errorText = platformlog.Redact(cause.Error())
		if len(errorText) > 2048 {
			errorText = errorText[:2048]
		}
	}
	result, err := j.db.ExecContext(ctx, `
UPDATE outbound_message
SET status = $3, next_attempt_at = $4, locked_by = NULL,
    locked_until = NULL, last_error_type = $5, last_error_message = $6
WHERE outbound_id = $1 AND locked_by = $2 AND status = 'sending'`,
		outboundID, workerID, status, retryAt, errorType, errorText)
	if err != nil {
		return fmt.Errorf("mark outbound failed: %w", err)
	}
	return requireOneRow(result, "outbound failure ownership mismatch")
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
