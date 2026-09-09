package telemetry

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// SQLAuditWriter persists the same redacted JSON audit event emitted to logs.
// It implements io.Writer for the authoritative side of NewDurableAuditSink.
type SQLAuditWriter struct {
	db      *sql.DB
	timeout time.Duration
}

// NewSQLAuditWriter creates a synchronous durability sink. Synchronous writes
// intentionally make an audit-store failure visible before success is returned.
func NewSQLAuditWriter(db *sql.DB) *SQLAuditWriter {
	return &SQLAuditWriter{db: db, timeout: 2 * time.Second}
}

func (w *SQLAuditWriter) Write(data []byte) (int, error) {
	if w == nil || w.db == nil {
		return 0, fmt.Errorf("audit database is not configured")
	}
	var entry AuditLog
	if err := json.Unmarshal(bytes.TrimSpace(data), &entry); err != nil {
		return 0, fmt.Errorf("decode audit event: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO audit_logs (
			tenant_id, channel, user_id, session_id, agent_name, tool_name,
			decision, latency_ms, error_type, token_count, cost_usd,
			trace_id, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		entry.TenantID, entry.ChannelType, entry.UserID, entry.SessionID,
		entry.AgentName, entry.ToolName, entry.Decision, entry.LatencyMS,
		entry.ErrorType, entry.TokenCount, entry.CostUSD, entry.TraceID,
		entry.Timestamp,
	)
	if err != nil {
		return 0, fmt.Errorf("persist audit event: %w", err)
	}
	return len(data), nil
}
