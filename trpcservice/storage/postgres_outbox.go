package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func (s *PostgresStateStore) PurgeDeliveredOutboxBefore(ctx context.Context, tenantID string, before time.Time, limit int) (int64, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return 0, fmt.Errorf("outbox retention tenant ID is required")
	}
	if before.IsZero() {
		return 0, fmt.Errorf("outbox retention cutoff is required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("outbox retention limit must be positive")
	}
	result, err := s.database.ExecContext(ctx, `
WITH doomed AS (
    SELECT id
    FROM outbox_events
    WHERE tenant_id=$1 AND delivered_at IS NOT NULL AND delivered_at < $2
    ORDER BY delivered_at, id
    LIMIT $3
)
DELETE FROM outbox_events AS event
USING doomed
WHERE event.tenant_id=$1 AND event.id=doomed.id`, tenantID, before, limit)
	if err != nil {
		return 0, fmt.Errorf("purge delivered outbox events: %w", err)
	}
	return result.RowsAffected()
}

func (s *PostgresStateStore) OutboxBacklog(ctx context.Context, tenantID, eventType string) (OutboxBacklog, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(eventType) == "" {
		return OutboxBacklog{}, fmt.Errorf("outbox backlog tenant and event type are required")
	}
	var pending int64
	var oldest sql.NullTime
	if err := s.database.QueryRowContext(ctx, `
SELECT COUNT(*), MIN(created_at)
FROM outbox_events
WHERE tenant_id = $1 AND event_type = $2 AND delivered_at IS NULL`, tenantID, eventType).Scan(&pending, &oldest); err != nil {
		return OutboxBacklog{}, fmt.Errorf("read outbox backlog: %w", err)
	}
	backlog := OutboxBacklog{Pending: pending}
	if oldest.Valid {
		backlog.OldestCreatedAt = oldest.Time
	}
	return backlog, nil
}

func (s *PostgresStateStore) ListPendingOutbox(ctx context.Context, tenantID string, limit int) ([]OutboxEvent, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("outbox tenant ID is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("outbox limit must be positive")
	}
	rows, err := s.database.QueryContext(ctx, `
SELECT id, tenant_id, aggregate_key, event_type, payload::text, created_at, delivered_at
FROM outbox_events
WHERE tenant_id = $1 AND delivered_at IS NULL AND available_at <= NOW()
ORDER BY available_at, created_at, id
LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox: %w", err)
	}
	defer rows.Close()

	events := make([]OutboxEvent, 0)
	for rows.Next() {
		var event OutboxEvent
		var payload string
		var deliveredAt sql.NullTime
		if err := rows.Scan(&event.ID, &event.TenantID, &event.AggregateKey, &event.Type, &payload, &event.CreatedAt, &deliveredAt); err != nil {
			return nil, fmt.Errorf("scan pending outbox: %w", err)
		}
		event.Payload = []byte(payload)
		if deliveredAt.Valid {
			value := deliveredAt.Time
			event.DeliveredAt = &value
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox: %w", err)
	}
	return events, nil
}

func (s *PostgresStateStore) MarkOutboxDelivered(ctx context.Context, tenantID, eventID string) error {
	if strings.TrimSpace(tenantID) == "" {
		return fmt.Errorf("outbox tenant ID is required")
	}
	if strings.TrimSpace(eventID) == "" {
		return fmt.Errorf("outbox event ID is required")
	}
	result, err := s.database.ExecContext(ctx, `
UPDATE outbox_events
SET delivered_at = COALESCE(delivered_at, NOW())
WHERE tenant_id = $1 AND id = $2`, tenantID, eventID)
	if err != nil {
		return fmt.Errorf("mark outbox delivered: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox delivery result: %w", err)
	}
	if updated == 0 {
		return ErrOutboxEventNotFound
	}
	return nil
}
