package platform

import (
	"context"
	"encoding/json"
	"time"
)

// AuditStore is an append-only tenant-scoped fact store. Implementations must
// deduplicate IDs so retrying a failed local snapshot never duplicates audits.
type AuditStore interface {
	AppendAuditEvents(context.Context, []AuditEvent) error
	ListAuditEvents(context.Context, AuditQuery) ([]AuditEvent, error)
}

func (s *SQLStore) AppendAuditEvents(ctx context.Context, items []AuditEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, item := range items {
		data, err := json.Marshal(item)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, s.q(`INSERT INTO audit_events(tenant_id,audit_id,occurred_at,payload) VALUES(?,?,?,?) ON CONFLICT(tenant_id,audit_id) DO NOTHING`), item.TenantID, item.ID, item.OccurredAt.UTC().Format(time.RFC3339Nano), string(data))
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *SQLStore) ListAuditEvents(ctx context.Context, query AuditQuery) ([]AuditEvent, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT payload FROM audit_events WHERE tenant_id=? ORDER BY occurred_at DESC,audit_id DESC`), query.TenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	limit := query.Limit
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	items := []AuditEvent{}
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var item AuditEvent
		if err := json.Unmarshal([]byte(data), &item); err != nil {
			return nil, err
		}
		if query.Channel != "" && item.Channel != query.Channel || query.AgentName != "" && item.AgentName != query.AgentName || query.Decision != "" && item.Decision != query.Decision || query.ErrorType != "" && item.ErrorType != query.ErrorType || query.UserID != "" && item.UserID != query.UserID || query.SessionID != "" && item.SessionID != query.SessionID || query.RequestID != "" && item.RequestID != query.RequestID || query.TraceID != "" && item.TraceID != query.TraceID || !query.From.IsZero() && item.OccurredAt.Before(query.From) || !query.To.IsZero() && item.OccurredAt.After(query.To) {
			continue
		}
		if query.Offset > 0 {
			query.Offset--
			continue
		}
		items = append(items, item)
		if len(items) == limit {
			break
		}
	}
	return items, rows.Err()
}
func (g *GovernanceCenter) ConfigureAuditStore(store AuditStore) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.auditStore = store
}
func (g *GovernanceCenter) QueryAuditEvents(ctx context.Context, query AuditQuery) ([]AuditEvent, error) {
	g.mu.Lock()
	store := g.auditStore
	retention := g.auditPolicyLocked(query.TenantID).RetentionDays
	cutoff := g.now().UTC().AddDate(0, 0, -retention)
	g.mu.Unlock()
	if store == nil {
		return g.AuditEvents(query), nil
	}
	if query.From.IsZero() || query.From.Before(cutoff) {
		query.From = cutoff
	}
	return store.ListAuditEvents(ctx, query)
}
