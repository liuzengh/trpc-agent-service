package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Record is a fire-and-forget write: empty tenants and non-positive token
// counts are no-ops, and a Redis failure must never panic the caller.
func TestBudgetRecordEdgeCases(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	b := NewBudget(rdb)

	// No-ops must not create any key.
	b.Record(ctx, "", 100)
	b.Record(ctx, "t-budget-noop", 0)
	b.Record(ctx, "t-budget-noop", -5)
	for _, tenantID := range []string{"", "t-budget-noop"} {
		if n, err := rdb.Exists(ctx, budgetKey(tenantID)).Result(); err != nil || n != 0 {
			t.Fatalf("no-op Record must not create a key for %q (n=%d err=%v)", tenantID, n, err)
		}
	}

	// A positive record still lands.
	tenantID := fmt.Sprintf("t-budget-record-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, budgetKey(tenantID)) })
	b.Record(ctx, tenantID, 7)
	n, err := rdb.Get(ctx, budgetKey(tenantID)).Int64()
	if err != nil || n != 7 {
		t.Fatalf("recorded tokens mismatch: n=%d err=%v", n, err)
	}
}

// Redis failures fail open: the budget gate must not take down the message
// path, and Record must swallow the error.
func TestBudgetFailsOpenOnRedisError(t *testing.T) {
	dead := redis.NewClient(&redis.Options{
		Addr:        "localhost:1",
		DialTimeout: 500 * time.Millisecond,
		ReadTimeout: 500 * time.Millisecond,
		MaxRetries:  -1,
	})
	defer func() { _ = dead.Close() }()

	b := NewBudget(dead)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ok, err := b.Allow(ctx, "t-budget-dead", 100)
	if !ok {
		t.Fatal("budget must fail open on a Redis error")
	}
	if err == nil {
		t.Fatal("the Redis error must be surfaced to the caller")
	}
	// Record swallows the error (warn only) and must not panic.
	b.Record(ctx, "t-budget-dead", 42)
}
