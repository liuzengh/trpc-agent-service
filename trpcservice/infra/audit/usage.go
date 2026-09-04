// Usage metering for cost attribution.
//
// usage_records meters per-tenant consumption across dimensions (token, tool,
// sandbox, artifact, skill). The worker writes a token-dimension record per
// turn; amounts are metered only — the platform does NOT price them (no unit
// cost table exists yet). RecordUsage is synchronous best-effort and idempotent
// on record_id (INSERT IGNORE).
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Usage dimensions for usage_records.dimension.
const (
	UsageDimensionToken    = "token"
	UsageDimensionTool     = "tool"
	UsageDimensionSandbox  = "sandbox"
	UsageDimensionArtifact = "artifact"
	UsageDimensionSkill    = "skill"
)

// UsageEntry is one metering record (the write side).
type UsageEntry struct {
	RecordID  string         // unique; derive as messageID + ":" + dimension
	TenantID  string
	AgentID   string
	Dimension string
	Amount    float64
	Meta      map[string]any
}

// UsageQuery filters the read side.
type UsageQuery struct {
	TenantID  string
	AgentID   string
	Dimension string
	From      time.Time // zero = no lower bound
	To        time.Time // zero = no upper bound
	Limit     int       // rows returned (0/negative -> default)
}

// UsageRow is one persisted usage record (read side).
type UsageRow struct {
	RecordID  string          `json:"record_id"`
	TenantID  string          `json:"tenant_id"`
	AgentID   string          `json:"agent_id,omitempty"`
	Dimension string          `json:"dimension"`
	Amount    float64         `json:"amount"`
	Meta      json.RawMessage `json:"meta,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// UsageSummary aggregates amounts per dimension.
type UsageSummary struct {
	Dimension string  `json:"dimension"`
	Total     float64 `json:"total"`
	Count     int64   `json:"count"`
}

// RecordUsage persists one metering record, idempotently (INSERT IGNORE on
// the unique record_id). It is synchronous but best-effort: callers ignore
// the error on the hot path.
func (r *MySQLRecorder) RecordUsage(ctx context.Context, e UsageEntry) error {
	if e.RecordID == "" || e.TenantID == "" || e.Dimension == "" {
		return fmt.Errorf("audit: usage requires record_id, tenant_id and dimension")
	}
	var meta any
	if len(e.Meta) > 0 {
		b, err := json.Marshal(e.Meta)
		if err != nil {
			return fmt.Errorf("audit: usage meta: %w", err)
		}
		meta = string(b)
	}
	_, err := r.db.ExecContext(ctx,
		`INSERT IGNORE INTO usage_records (record_id, tenant_id, agent_id, dimension, amount, meta)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		e.RecordID, e.TenantID, nullString(e.AgentID), e.Dimension, e.Amount, meta)
	if err != nil {
		return fmt.Errorf("audit: record usage: %w", err)
	}
	return nil
}

// UsageRows returns recent usage rows matching the query, newest first.
func (r *MySQLRecorder) UsageRows(ctx context.Context, q UsageQuery) ([]UsageRow, error) {
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	where, args := usageWhere(q)
	rows, err := r.db.QueryContext(ctx,
		`SELECT record_id, tenant_id, COALESCE(agent_id,''), dimension, amount,
		        COALESCE(meta,''), created_at
		 FROM usage_records `+where+` ORDER BY id DESC LIMIT `+fmt.Sprint(q.Limit),
		args...)
	if err != nil {
		return nil, fmt.Errorf("audit: usage rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]UsageRow, 0, 32)
	for rows.Next() {
		var u UsageRow
		var meta string
		if err := rows.Scan(&u.RecordID, &u.TenantID, &u.AgentID, &u.Dimension,
			&u.Amount, &meta, &u.CreatedAt); err != nil {
			return nil, err
		}
		if meta != "" {
			u.Meta = json.RawMessage(meta)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// UsageSummary aggregates amounts per dimension for the query (cost-attribution
// view, amounts only — no pricing).
func (r *MySQLRecorder) UsageSummary(ctx context.Context, q UsageQuery) ([]UsageSummary, error) {
	where, args := usageWhere(q)
	rows, err := r.db.QueryContext(ctx,
		`SELECT dimension, SUM(amount), COUNT(*) FROM usage_records `+where+
			` GROUP BY dimension ORDER BY dimension`, args...)
	if err != nil {
		return nil, fmt.Errorf("audit: usage summary: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]UsageSummary, 0, 8)
	for rows.Next() {
		var s UsageSummary
		if err := rows.Scan(&s.Dimension, &s.Total, &s.Count); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// usageWhere builds the WHERE clause and args for a UsageQuery.
func usageWhere(q UsageQuery) (string, []any) {
	var clauses []string
	var args []any
	if q.TenantID != "" {
		clauses = append(clauses, "tenant_id = ?")
		args = append(args, q.TenantID)
	}
	if q.AgentID != "" {
		clauses = append(clauses, "agent_id = ?")
		args = append(args, q.AgentID)
	}
	if q.Dimension != "" {
		clauses = append(clauses, "dimension = ?")
		args = append(args, q.Dimension)
	}
	if !q.From.IsZero() {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, q.From)
	}
	if !q.To.IsZero() {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, q.To)
	}
	if len(clauses) == 0 {
		return "", args
	}
	where := "WHERE " + clauses[0]
	for _, c := range clauses[1:] {
		where += " AND " + c
	}
	return where, args
}
