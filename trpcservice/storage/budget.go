package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// Budget tracks per-tenant daily token consumption in Redis
// (budget:{tenant}:{yyyymmdd}). The counter lives 48h —
// it is read before the run (deny when already over) and incremented with the
// actual usage after. One day of slack is enough for a budget gate; the
// audit_log carries the row-level truth for billing reconciliation.
type Budget struct {
	rdb *redis.Client
}

// NewBudget creates the tracker on an established Redis client.
func NewBudget(rdb *redis.Client) *Budget {
	return &Budget{rdb: rdb}
}

func budgetKey(tenantID string) string {
	return fmt.Sprintf("budget:%s:%s", tenantID, time.Now().UTC().Format("20060102"))
}

// Allow reports whether the tenant is under its daily budget. maxPerDay <= 0
// means unlimited. Redis failures fail open: a budget check must not take
// down the message path (overruns stay visible in the audit trail).
func (b *Budget) Allow(ctx context.Context, tenantID string, maxPerDay int64) (bool, error) {
	if maxPerDay <= 0 {
		return true, nil
	}
	n, err := b.rdb.Get(ctx, budgetKey(tenantID)).Int64()
	if errors.Is(err, redis.Nil) {
		return true, nil
	}
	if err != nil {
		return true, fmt.Errorf("budget read: %w", err)
	}
	return n < maxPerDay, nil
}

// Record adds the run's token usage to the tenant's daily counter.
func (b *Budget) Record(ctx context.Context, tenantID string, tokens int64) {
	if tenantID == "" || tokens <= 0 {
		return
	}
	key := budgetKey(tenantID)
	if err := b.rdb.IncrBy(ctx, key, tokens).Err(); err != nil {
		plog.Warnf("budget record %s: %v", key, err)
		return
	}
	// Bound the counter's life: set the TTL once, on first increment.
	if err := b.rdb.ExpireNX(ctx, key, 48*time.Hour).Err(); err != nil {
		plog.Warnf("budget ttl %s: %v", key, err)
	}
}
