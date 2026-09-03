package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type PostgresWriter struct {
	db *sql.DB
}

func NewPostgresWriter(db *sql.DB) (*PostgresWriter, error) {
	if db == nil {
		return nil, fmt.Errorf("audit database is nil")
	}
	return &PostgresWriter{db: db}, nil
}

func (w *PostgresWriter) Record(ctx context.Context, event Event) error {
	event = sanitizeEvent(event)
	details, err := json.Marshal(event.Details)
	if err != nil {
		return fmt.Errorf("marshal audit details: %w", err)
	}
	_, err = w.db.ExecContext(ctx, `
INSERT INTO audit_log(
    audit_id, occurred_at, tenant_id, channel, channel_binding_id,
    user_id, session_id, message_id, request_id, trace_id, agent_name,
    revision_id, tool_name, decision, latency_ms, error_type, cost, details
) VALUES (
    $1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''),
    NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''),
    NULLIF($11, ''), NULLIF($12, ''), NULLIF($13, ''), $14, $15,
    NULLIF($16, ''), $17, $18::jsonb
)`, "audit-"+uuid.NewString(), event.OccurredAt, event.TenantID,
		event.Channel, event.ChannelBindingID, event.UserID, event.SessionID,
		event.MessageID, event.RequestID, event.TraceID, event.AgentName,
		event.RevisionID, event.ToolName, event.Decision, event.Latency.Milliseconds(),
		event.ErrorType, event.Cost, string(details))
	if err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	return nil
}

func (w *PostgresWriter) Ready(ctx context.Context) error {
	return w.db.PingContext(ctx)
}

func (w *PostgresWriter) Close() error { return nil }

var _ Writer = (*PostgresWriter)(nil)
