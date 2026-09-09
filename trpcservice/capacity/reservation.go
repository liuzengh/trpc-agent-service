// Package capacity implements the WS-8 cross-process durable capacity
// reservation boundary using PostgreSQL-authoritative atomic budget rows.
//
// The budget row is the authoritative active count. Acquire uses a single
// conditional UPDATE that increments active_count only when below the limit,
// which is atomic under PostgreSQL row-level locking. Two concurrent
// transactions cannot both succeed because the second UPDATE blocks on the
// row lock until the first commits, then re-evaluates the condition.
//
// Reservation lifecycle:
//
//	ingress Acquire → reservation active (same tx as Queue enqueue)
//	Queue terminal Ack/complete → Release (same tx)
//	requeue/Nack → reservation stays active (budget not released)
//	permanent failure/DLQ → Release (same tx as terminal state)
//	process crash → stale reservation expires or is reconciled
//	unknown tx outcome → reservation unresolved, reconciliation handles
package capacity

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrCapacityFull       = fmt.Errorf("capacity: budget exhausted")
	ErrReservationInvalid = fmt.Errorf("capacity: invalid reservation parameters")
	ErrBudgetNotFound     = fmt.Errorf("capacity: budget row not found")
	ErrBudgetDisabled     = fmt.Errorf("capacity: budget disabled")
)

// ReservationStore provides atomic capacity management via PostgreSQL.
// All methods operate within a caller-owned transaction to ensure that
// the budget increment and the business operation (e.g. Queue enqueue)
// are atomic.
type ReservationStore struct{ tx pgx.Tx }

func NewReservationStore(tx pgx.Tx) *ReservationStore { return &ReservationStore{tx: tx} }

// Acquire atomically increments the budget active_count and inserts a
// reservation row, both within the caller's transaction. If the budget is
// full, ErrCapacityFull is returned and no state is changed.
func Acquire(ctx context.Context, tx pgx.Tx, tenantID, scope, ownerID string, ttl time.Duration) (string, error) {
	if tenantID == "" || scope == "" || ownerID == "" || ttl <= 0 {
		return "", ErrReservationInvalid
	}
	// Step 1: Atomic conditional increment of the budget counter.
	// This single UPDATE is atomic under PostgreSQL row-level locking.
	// If active_count >= budget_limit, zero rows are affected.
	tag, err := tx.Exec(ctx, `
		UPDATE capacity_budget
		SET active_count = active_count + 1, updated_at = now()
		WHERE tenant_id = $1 AND scope = $2 AND enabled = true AND active_count < budget_limit
	`, tenantID, scope)
	if err != nil {
		return "", fmt.Errorf("capacity: budget increment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", ErrCapacityFull
	}
	// Step 2: Insert the reservation audit row.
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return "", fmt.Errorf("capacity: id generation: %w", err)
	}
	reservationID := hex.EncodeToString(id)
	_, err = tx.Exec(ctx,
		`INSERT INTO capacity_reservation (reservation_id, tenant_id, scope, owner_id, budget_scope, expires_at)
		 VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))`,
		reservationID, tenantID, scope, ownerID, scope, ttl.Seconds())
	if err != nil {
		// Rollback budget increment by decrementing (the caller's tx will rollback anyway,
		// but we do it here for clarity in case the caller uses a savepoint)
		_, _ = tx.Exec(ctx, `
			UPDATE capacity_budget SET active_count = active_count - 1
			WHERE tenant_id = $1 AND scope = $2 AND active_count > 0`, tenantID, scope)
		return "", fmt.Errorf("capacity: reservation insert: %w", err)
	}
	return reservationID, nil
}

// Release atomically decrements the budget active_count and marks the
// reservation as released. Only the owner can release. Idempotent: if the
// reservation is already released, this is a no-op.
func Release(ctx context.Context, tx pgx.Tx, tenantID, scope, reservationID, ownerID string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE capacity_reservation SET state = 'released', released_at = now()
		WHERE tenant_id = $1 AND reservation_id = $2 AND owner_id = $3 AND state = 'active'`,
		tenantID, reservationID, ownerID)
	if err != nil {
		return fmt.Errorf("capacity: release reservation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil // already released or not found — idempotent
	}
	_, err = tx.Exec(ctx, `
		UPDATE capacity_budget SET active_count = active_count - 1, updated_at = now()
		WHERE tenant_id = $1 AND scope = $2 AND active_count > 0`,
		tenantID, scope)
	if err != nil {
		return fmt.Errorf("capacity: budget decrement: %w", err)
	}
	return nil
}

// ReconcileStale releases expired active reservations and decrements their
// budget counters. Returns the number of stale reservations released. This
// is a bounded, idempotent sweep that can be called periodically.
func ReconcileStale(ctx context.Context, tx pgx.Tx) (int64, error) {
	var released int64
	err := tx.QueryRow(ctx, `
		WITH expired AS (
			UPDATE capacity_reservation SET state = 'expired', released_at = now()
			WHERE state = 'active' AND expires_at <= now()
			RETURNING tenant_id, budget_scope
		),
		dec AS (
			UPDATE capacity_budget b SET active_count = b.active_count - e.cnt, updated_at = now()
			FROM (SELECT tenant_id, budget_scope, count(*) AS cnt FROM expired GROUP BY tenant_id, budget_scope) e
			WHERE b.tenant_id = e.tenant_id AND b.scope = e.budget_scope AND b.active_count >= e.cnt
			RETURNING 1
		)
		SELECT count(*) FROM dec`).Scan(&released)
	if err != nil {
		return 0, fmt.Errorf("capacity: reconcile stale: %w", err)
	}
	return released, nil
}
