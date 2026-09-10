package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// OutboxDeliveryStore extends StateStore with a durable dispatch lease. A lease
// serializes dispatchers across workers; it deliberately provides at-least-once
// delivery because an external IM acknowledgement cannot share a transaction
// with PostgreSQL.
type OutboxDeliveryStore interface {
	StateStore
	ClaimPendingOutbox(context.Context, string, string, time.Duration, int) ([]OutboxEvent, error)
	ClaimPendingOutboxByType(context.Context, string, string, time.Duration, int, string) ([]OutboxEvent, error)
	RenewOutboxDelivery(context.Context, string, string, string, time.Duration) error
	CompleteOutboxDelivery(context.Context, string, string, string, string) error
	FailOutboxDelivery(context.Context, string, string, string, error) error
	FindOutboxByRequestID(context.Context, string, string) (OutboxEvent, error)
}

func (s *MemoryStateStore) ClaimPendingOutbox(ctx context.Context, tenantID, owner string, lease time.Duration, limit int) ([]OutboxEvent, error) {
	return s.claimPendingOutbox(ctx, tenantID, owner, lease, limit, "")
}

func (s *MemoryStateStore) ClaimPendingOutboxByType(ctx context.Context, tenantID, owner string, lease time.Duration, limit int, eventType string) ([]OutboxEvent, error) {
	return s.claimPendingOutbox(ctx, tenantID, owner, lease, limit, eventType)
}

func (s *MemoryStateStore) claimPendingOutbox(ctx context.Context, tenantID, owner string, lease time.Duration, limit int, eventType string) ([]OutboxEvent, error) {
	if err := validateOutboxLease(ctx, tenantID, owner, lease, limit); err != nil {
		return nil, err
	}
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.outbox))
	for id, event := range s.outbox {
		if event.TenantID == tenantID && (eventType == "" || event.Type == eventType) && event.DeliveredAt == nil && !event.AvailableAt.After(now) && (event.LeaseExpiresAt == nil || !event.LeaseExpiresAt.After(now)) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	claimed := make([]OutboxEvent, 0, limit)
	for _, id := range ids {
		if len(claimed) == limit {
			break
		}
		event := s.outbox[id]
		expires := now.Add(lease)
		event.DeliveryOwner, event.LeaseExpiresAt = owner, &expires
		event.DeliveryAttempts++
		s.outbox[id] = event
		claimed = append(claimed, cloneOutboxEvent(event))
	}
	return claimed, nil
}

func (s *MemoryStateStore) RenewOutboxDelivery(ctx context.Context, tenantID, eventID, owner string, lease time.Duration) error {
	if err := validateOutboxLease(ctx, tenantID, owner, lease, 1); err != nil {
		return err
	}
	now := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	event, ok := s.outbox[eventID]
	if !ok || event.TenantID != tenantID {
		return ErrOutboxEventNotFound
	}
	if event.DeliveryOwner != owner || event.DeliveredAt != nil || event.LeaseExpiresAt == nil || !event.LeaseExpiresAt.After(now) {
		return fmt.Errorf("outbox delivery lease is not owned")
	}
	expires := now.Add(lease)
	event.LeaseExpiresAt = &expires
	s.outbox[eventID] = event
	return nil
}

func (s *MemoryStateStore) CompleteOutboxDelivery(ctx context.Context, tenantID, eventID, owner, receipt string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event, ok := s.outbox[eventID]
	if !ok || event.TenantID != tenantID {
		return ErrOutboxEventNotFound
	}
	if event.DeliveryOwner != owner || event.DeliveredAt != nil {
		return fmt.Errorf("outbox delivery lease is not owned")
	}
	deliveredAt := s.now().UTC()
	event.DeliveredAt, event.DeliveryReceipt, event.DeliveryOwner, event.LeaseExpiresAt, event.LastDeliveryError = &deliveredAt, receipt, "", nil, ""
	s.outbox[eventID] = event
	return nil
}

func (s *MemoryStateStore) FailOutboxDelivery(ctx context.Context, tenantID, eventID, owner string, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event, ok := s.outbox[eventID]
	if !ok || event.TenantID != tenantID {
		return ErrOutboxEventNotFound
	}
	if event.DeliveryOwner != owner || event.DeliveredAt != nil {
		return fmt.Errorf("outbox delivery lease is not owned")
	}
	event.DeliveryOwner, event.LeaseExpiresAt = "", nil
	event.AvailableAt = s.now().UTC().Add(outboxRetryDelay(event.DeliveryAttempts))
	if cause != nil {
		event.LastDeliveryError = cause.Error()
	}
	s.outbox[eventID] = event
	return nil
}

func (s *MemoryStateStore) FindOutboxByRequestID(ctx context.Context, tenantID, requestID string) (OutboxEvent, error) {
	if err := ctx.Err(); err != nil {
		return OutboxEvent{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, event := range s.outbox {
		if event.TenantID == tenantID && event.RequestID == requestID {
			return cloneOutboxEvent(event), nil
		}
	}
	return OutboxEvent{}, ErrOutboxEventNotFound
}

func (s *PostgresStateStore) ClaimPendingOutbox(ctx context.Context, tenantID, owner string, lease time.Duration, limit int) ([]OutboxEvent, error) {
	return s.claimPendingOutbox(ctx, tenantID, owner, lease, limit, "")
}

func (s *PostgresStateStore) ClaimPendingOutboxByType(ctx context.Context, tenantID, owner string, lease time.Duration, limit int, eventType string) ([]OutboxEvent, error) {
	return s.claimPendingOutbox(ctx, tenantID, owner, lease, limit, eventType)
}

func (s *PostgresStateStore) claimPendingOutbox(ctx context.Context, tenantID, owner string, lease time.Duration, limit int, eventType string) ([]OutboxEvent, error) {
	if err := validateOutboxLease(ctx, tenantID, owner, lease, limit); err != nil {
		return nil, err
	}
	rows, err := s.database.QueryContext(ctx, `
WITH candidates AS (
    SELECT id FROM outbox_events
    WHERE tenant_id = $1 AND delivered_at IS NULL
      AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())
	      AND available_at <= NOW() AND ($5 = '' OR event_type = $5)
    ORDER BY available_at, created_at, id FOR UPDATE SKIP LOCKED LIMIT $4
)
UPDATE outbox_events event SET delivery_owner = $2,
    lease_expires_at = NOW() + make_interval(secs => $3),
    delivery_attempts = event.delivery_attempts + 1, last_delivery_error = ''
FROM candidates WHERE event.id = candidates.id
RETURNING event.id, event.request_id, event.tenant_id, event.aggregate_key, event.event_type,
          event.payload::text, event.created_at, event.delivered_at, event.delivery_owner,
          event.lease_expires_at, event.delivery_receipt, event.delivery_attempts, event.last_delivery_error`, tenantID, owner, int64(lease.Seconds()), limit, eventType)
	if err != nil {
		return nil, fmt.Errorf("claim pending outbox: %w", err)
	}
	defer rows.Close()
	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		event, scanErr := scanOutboxEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed outbox: %w", err)
	}
	return events, nil
}

func (s *PostgresStateStore) CompleteOutboxDelivery(ctx context.Context, tenantID, eventID, owner, receipt string) error {
	result, err := s.database.ExecContext(ctx, `UPDATE outbox_events
SET delivered_at = NOW(), delivery_receipt = $4, delivery_owner = NULL,
    lease_expires_at = NULL, last_delivery_error = ''
WHERE tenant_id = $1 AND id = $2 AND delivery_owner = $3 AND delivered_at IS NULL`, tenantID, eventID, owner, receipt)
	if err != nil {
		return fmt.Errorf("complete outbox delivery: %w", err)
	}
	return expectOutboxDelivery(result)
}

func (s *PostgresStateStore) RenewOutboxDelivery(ctx context.Context, tenantID, eventID, owner string, lease time.Duration) error {
	if err := validateOutboxLease(ctx, tenantID, owner, lease, 1); err != nil {
		return err
	}
	result, err := s.database.ExecContext(ctx, `UPDATE outbox_events
SET lease_expires_at = NOW() + make_interval(secs => $4)
WHERE tenant_id = $1 AND id = $2 AND delivery_owner = $3
  AND delivered_at IS NULL AND lease_expires_at > NOW()`, tenantID, eventID, owner, int64(lease.Seconds()))
	if err != nil {
		return fmt.Errorf("renew outbox delivery: %w", err)
	}
	return expectOutboxDelivery(result)
}

func (s *PostgresStateStore) FailOutboxDelivery(ctx context.Context, tenantID, eventID, owner string, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	result, err := s.database.ExecContext(ctx, `UPDATE outbox_events
SET delivery_owner = NULL,
    lease_expires_at = NULL,
    last_delivery_error = $4,
    available_at = NOW() + make_interval(secs => LEAST(300, CAST(power(2, LEAST(delivery_attempts - 1, 8)) AS INTEGER)))
WHERE tenant_id = $1 AND id = $2 AND delivery_owner = $3 AND delivered_at IS NULL`, tenantID, eventID, owner, message)
	if err != nil {
		return fmt.Errorf("release outbox delivery: %w", err)
	}
	return expectOutboxDelivery(result)
}

func (s *PostgresStateStore) FindOutboxByRequestID(ctx context.Context, tenantID, requestID string) (OutboxEvent, error) {
	row := s.database.QueryRowContext(ctx, `SELECT id, request_id, tenant_id, aggregate_key, event_type,
       payload::text, created_at, delivered_at, delivery_owner, lease_expires_at,
       delivery_receipt, delivery_attempts, last_delivery_error
FROM outbox_events WHERE tenant_id = $1 AND request_id = $2`, tenantID, requestID)
	event, err := scanOutboxEvent(row)
	if err == sql.ErrNoRows {
		return OutboxEvent{}, ErrOutboxEventNotFound
	}
	if err != nil {
		return OutboxEvent{}, err
	}
	return event, nil
}

type outboxScanner interface{ Scan(...any) error }

func scanOutboxEvent(row outboxScanner) (OutboxEvent, error) {
	var event OutboxEvent
	var payload string
	var deliveredAt, leaseExpiresAt sql.NullTime
	var owner, receipt, lastError sql.NullString
	if err := row.Scan(&event.ID, &event.RequestID, &event.TenantID, &event.AggregateKey, &event.Type,
		&payload, &event.CreatedAt, &deliveredAt, &owner, &leaseExpiresAt, &receipt,
		&event.DeliveryAttempts, &lastError); err != nil {
		return OutboxEvent{}, err
	}
	event.Payload = []byte(payload)
	if deliveredAt.Valid {
		event.DeliveredAt = &deliveredAt.Time
	}
	if leaseExpiresAt.Valid {
		event.LeaseExpiresAt = &leaseExpiresAt.Time
	}
	event.DeliveryOwner, event.DeliveryReceipt, event.LastDeliveryError = owner.String, receipt.String, lastError.String
	return event, nil
}

func validateOutboxLease(ctx context.Context, tenantID, owner string, lease time.Duration, limit int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" {
		return fmt.Errorf("outbox tenant and owner are required")
	}
	if lease <= 0 || limit <= 0 {
		return fmt.Errorf("outbox lease and limit must be positive")
	}
	return nil
}

func outboxRetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	shift := attempts - 1
	if shift > 8 {
		shift = 8
	}
	delay := time.Second * time.Duration(1<<shift)
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func expectOutboxDelivery(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox delivery result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("outbox delivery lease is not owned")
	}
	return nil
}

var _ OutboxDeliveryStore = (*MemoryStateStore)(nil)
var _ OutboxDeliveryStore = (*PostgresStateStore)(nil)
