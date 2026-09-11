package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

func (s *PostgresStateStore) ListAudit(ctx context.Context, tenantID, traceID string) ([]AuditEvent, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("audit tenant ID is required")
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT id, tenant_id, trace_id, request_id, channel, user_id, session_id, agent_name, tool_name,
       action, result, decision, latency_ms, error_type, cost_micros, redacted_detail, created_at
FROM audit_events
WHERE tenant_id = $1 AND ($2 = '' OR trace_id = $2)
ORDER BY created_at, id`, tenantID, traceID)
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	defer rows.Close()

	events := make([]AuditEvent, 0)
	for rows.Next() {
		var event AuditEvent
		if err := rows.Scan(
			&event.ID, &event.TenantID, &event.TraceID, &event.RequestID, &event.Channel,
			&event.UserID, &event.SessionID, &event.AgentName, &event.ToolName,
			&event.Action, &event.Result, &event.Decision, &event.LatencyMS, &event.ErrorType,
			&event.CostMicros, &event.Detail, &event.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return events, nil
}

func (s *PostgresStateStore) RecordAudit(ctx context.Context, event AuditEvent) error {
	if strings.TrimSpace(event.TenantID) == "" || strings.TrimSpace(event.TraceID) == "" || strings.TrimSpace(event.Action) == "" {
		return fmt.Errorf("audit tenant, trace, and action are required")
	}
	if event.ID == "" {
		id, err := newStateID()
		if err != nil {
			return err
		}
		event.ID = id
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now().UTC()
	}
	event.Detail = s.audit.Digest(event.Detail)
	return insertAuditEvent(ctx, s.database, event)
}

type sqlExecuter interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertAuditEvent(ctx context.Context, executer sqlExecuter, event AuditEvent) error {
	_, err := executer.ExecContext(ctx, `
INSERT INTO audit_events (
    id, tenant_id, trace_id, request_id, channel, user_id, session_id, agent_name, tool_name,
    action, result, decision, latency_ms, error_type, cost_micros, redacted_detail, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		event.ID, event.TenantID, event.TraceID, event.RequestID, event.Channel, event.UserID, event.SessionID, event.AgentName, event.ToolName,
		event.Action, event.Result, event.Decision, event.LatencyMS, event.ErrorType, event.CostMicros, event.Detail, event.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	return nil
}

func (s *PostgresStateStore) PurgeAuditBefore(ctx context.Context, tenantID string, before time.Time) (int64, error) {
	if strings.TrimSpace(tenantID) == "" {
		return 0, fmt.Errorf("audit tenant ID is required")
	}
	if before.IsZero() {
		return 0, fmt.Errorf("audit cutoff is required")
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, s.database, tenantID)
	if err != nil {
		return 0, fmt.Errorf("purge audit events: begin retention transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Approval rows are durable lifecycle evidence. If an old pending row was
	// stranded after its Redis coordination state disappeared, turn the elapsed
	// deadline into an explicit terminal state before retention evaluates it.
	if _, err := tx.ExecContext(ctx, `
UPDATE tool_approvals
SET status='expired', resolved_at=expires_at, updated_at=NOW()
WHERE tenant_id=$1 AND status='pending' AND expires_at < NOW()`, tenantID); err != nil {
		return 0, fmt.Errorf("expire stale approval records: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM tool_approvals
WHERE tenant_id=$1 AND status <> 'pending'
  AND COALESCE(resolved_at, updated_at) < $2`, tenantID, before); err != nil {
		return 0, fmt.Errorf("purge approval evidence: %w", err)
	}

	// Execution traces are observability projections only. They do not carry
	// the exactly-once contract, so the tenant audit retention window applies.
	if _, err := tx.ExecContext(ctx, `
DELETE FROM execution_traces
WHERE tenant_id=$1 AND updated_at < $2`, tenantID, before); err != nil {
		return 0, fmt.Errorf("purge execution traces: %w", err)
	}

	// Completed tool results may contain encrypted business payloads. Delete
	// them only after the parent message is durably completed and both records
	// are beyond the retention window. Keep failed/running/outcome_unknown rows:
	// those records still participate in retry or duplicate-side-effect safety.
	if _, err := tx.ExecContext(ctx, `
DELETE FROM tool_executions AS tool
WHERE tool.tenant_id=$1 AND tool.status='completed' AND tool.completed_at < $2
  AND EXISTS (
      SELECT 1
      FROM messages AS message
      WHERE message.tenant_id=tool.tenant_id
        AND message.message_id=tool.request_id
        AND message.trace_id=tool.trace_id
        AND message.status='completed'
        AND message.updated_at < $2
  )`, tenantID, before); err != nil {
		return 0, fmt.Errorf("purge completed tool evidence: %w", err)
	}

	result, err := tx.ExecContext(ctx, "DELETE FROM audit_events WHERE tenant_id=$1 AND created_at < $2", tenantID, before)
	if err != nil {
		return 0, fmt.Errorf("purge audit events: %w", err)
	}
	removedAudit, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("read purged audit row count: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit audit retention: %w", err)
	}
	return removedAudit, nil
}
