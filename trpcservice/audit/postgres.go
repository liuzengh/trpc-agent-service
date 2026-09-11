package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
	"time"
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
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal audit details: %w", err)
	}
	var accepted bool
	if tx := database.Transaction(ctx, w.db); tx != nil {
		err = tx.QueryRowContext(ctx, "SELECT platform_audit_append($1::jsonb)", string(payload)).Scan(&accepted)
	} else {
		err = w.db.QueryRowContext(ctx, "SELECT platform_audit_append($1::jsonb)", string(payload)).Scan(&accepted)
	}
	if err != nil {
		return fmt.Errorf("audit persistence unavailable")
	}
	if !accepted {
		return ErrEventConflict
	}
	return nil
}

func (w *PostgresWriter) Prune(ctx context.Context, tenantID string, _ time.Time, limit int) (int, error) {
	var n int
	if err := w.db.QueryRowContext(ctx, "SELECT platform_audit_prune($1,$2)", tenantID, limit).Scan(&n); err != nil {
		return 0, fmt.Errorf("audit retention unavailable")
	}
	return n, nil
}

func (w *PostgresWriter) Ready(ctx context.Context) error {
	var ready bool
	if err := w.db.QueryRowContext(ctx, "SELECT to_regprocedure('platform_audit_append(jsonb)') IS NOT NULL").Scan(&ready); err != nil || !ready {
		return fmt.Errorf("audit schema unavailable")
	}
	return nil
}

func (w *PostgresWriter) Query(ctx context.Context, query Query) ([]Event, error) {
	limit := query.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var before any
	if !query.BeforeTime.IsZero() {
		before = query.BeforeTime
	}
	rows, err := w.db.QueryContext(ctx, `
SELECT audit_id,occurred_at,tenant_id,COALESCE(channel,''),COALESCE(channel_binding_id,''),
       COALESCE(user_id,''),COALESCE(session_id,''),COALESCE(message_id,''),
       COALESCE(request_id,''),COALESCE(trace_id,''),COALESCE(agent_name,''),
       COALESCE(revision_id,''),COALESCE(tool_name,''),decision,latency_ms,
       COALESCE(error_type,''),cost,details
FROM audit_log
WHERE tenant_id=$1 AND ($2='' OR decision=$2) AND ($3='' OR trace_id=$3)
AND ($5='' OR details->>'app_id'=$5) AND ($6='' OR request_id=$6)
AND (NOT $7 OR decision IN ('admin_revision_published','admin_draft_published','admin_rollout_policy_updated'))
AND ($8::timestamptz IS NULL OR (occurred_at,audit_id)<($8,$9))
ORDER BY occurred_at DESC,audit_id DESC LIMIT $4`, query.TenantID, query.Decision, query.TraceID, limit, query.AppID, query.RequestID, query.ReleaseOnly, before, query.BeforeID)
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(rows)
	result := make([]Event, 0, limit)
	for rows.Next() {
		var event Event
		var latencyMS int64
		var details []byte
		if err := rows.Scan(
			&event.ID, &event.OccurredAt, &event.TenantID, &event.Channel, &event.ChannelBindingID,
			&event.UserID, &event.SessionID, &event.MessageID, &event.RequestID,
			&event.TraceID, &event.AgentName, &event.RevisionID, &event.ToolName,
			&event.Decision, &latencyMS, &event.ErrorType, &event.Cost, &details,
		); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		event.Latency = time.Duration(latencyMS) * time.Millisecond
		if err := json.Unmarshal(details, &event.Details); err != nil {
			return nil, fmt.Errorf("decode audit details: %w", err)
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (w *PostgresWriter) Close() error { return nil }

var _ Writer = (*PostgresWriter)(nil)
var _ Reader = (*PostgresWriter)(nil)
