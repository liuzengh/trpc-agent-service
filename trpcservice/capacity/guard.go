package capacity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
)

// Reservation scopes. Each scope carries its own budget row per tenant, so
// admission (ingress), processing (worker) and outbound dispatch (sender)
// are budgeted independently.
const (
	ScopeIngress = "ingress"
	ScopeWorker  = "worker"
	ScopeSender  = "sender"
)

// ErrInvalidScopeRequest reports a malformed AcquireScope call.
var ErrInvalidScopeRequest = errors.New("capacity: invalid scope request")

// ScopeGuard turns the transaction-bound reservation core into the runtime
// budget interface consumed by the ingress, queue and dispatcher wiring.
//
// Fail-open contract: when no enabled budget row exists for the tenant and
// scope, AcquireScope returns a nil release function and nil error — the
// caller MUST treat a nil release as "not enforced" (a nil-func defer would
// panic; callers use the provided helper). Capacity is therefore enforced
// only for tenants that have an explicit budget row, which keeps every
// existing path byte-for-byte identical until budgets are seeded and keeps
// production capacity enforcement inactive by default.
//
// Release is best-effort with its own bounded deadline: a lost release is
// healed by ReconcileStale (TTL expiry), never blocks the caller and never
// turns a completed unit of work into a failure.
type ScopeGuard struct {
	pool       *pgxpool.Pool
	defaultTTL time.Duration
}

// NewScopeGuard validates its dependencies and returns a runtime guard.
func NewScopeGuard(pool *pgxpool.Pool, defaultTTL time.Duration) (*ScopeGuard, error) {
	if pool == nil {
		return nil, ErrInvalidScopeRequest
	}
	if defaultTTL <= 0 {
		return nil, ErrInvalidScopeRequest
	}
	return &ScopeGuard{pool: pool, defaultTTL: defaultTTL}, nil
}

// AcquireScope holds one durable reservation for the tenant and scope in its
// own short transaction. The returned release function is idempotent and
// safe to call more than once; it is nil exactly when the tenant has no
// budget row for this scope (not enforced).
func (g *ScopeGuard) AcquireScope(ctx context.Context, tenantID, scope, ownerID string, ttl time.Duration) (func(), error) {
	if g == nil || tenantID == "" || scope == "" || ownerID == "" {
		return nil, ErrInvalidScopeRequest
	}
	if ttl <= 0 {
		ttl = g.defaultTTL
	}
	tx, err := g.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("capacity: guard begin: %w", err)
	}
	defer tx.Rollback(ctx)
	if err := tenantctx.SetTenantContext(ctx, tx, tenantID); err != nil {
		return nil, fmt.Errorf("capacity: guard tenant context: %w", err)
	}
	var enforced bool
	if err := tx.QueryRow(ctx,
		`SELECT true FROM capacity_budget WHERE tenant_id = $1 AND scope = $2 AND enabled = true`,
		tenantID, scope).Scan(&enforced); err != nil {
		// No row: not enforced (fail-open). Any real error is a dependency
		// failure and propagates.
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("capacity: guard budget lookup: %w", err)
	}
	reservationID, err := Acquire(ctx, tx, tenantID, scope, ownerID, ttl)
	if err != nil {
		return nil, err // ErrCapacityFull propagates; wrapped dependency errors too
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("capacity: guard commit: %w", err)
	}
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rtx, err := g.pool.Begin(rctx)
		if err != nil {
			return // ReconcileStale heals the leaked slot after TTL
		}
		defer rtx.Rollback(rctx)
		if err := tenantctx.SetTenantContext(rctx, rtx, tenantID); err != nil {
			return
		}
		if err := Release(rctx, rtx, tenantID, scope, reservationID, ownerID); err != nil {
			return
		}
		_ = rtx.Commit(rctx)
	}
	return release, nil
}
