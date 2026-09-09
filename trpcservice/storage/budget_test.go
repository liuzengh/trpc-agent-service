package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestBudget(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	tenantID := fmt.Sprintf("t-budget-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, budgetKey(tenantID)) })

	b := NewBudget(rdb)
	// Unlimited when maxPerDay <= 0.
	if ok, err := b.Allow(ctx, tenantID, 0); !ok || err != nil {
		t.Fatal("maxPerDay<=0 must be unlimited")
	}
	// Under budget → allowed; record usage.
	if ok, err := b.Allow(ctx, tenantID, 100); !ok || err != nil {
		t.Fatal("under budget must be allowed")
	}
	b.Record(ctx, tenantID, 60)
	if ok, _ := b.Allow(ctx, tenantID, 100); !ok {
		t.Fatal("60/100 must still be allowed")
	}
	b.Record(ctx, tenantID, 50)
	if ok, _ := b.Allow(ctx, tenantID, 100); ok {
		t.Fatal("110/100 must be denied")
	}
	// TTL set on the counter.
	ttl, err := rdb.TTL(ctx, budgetKey(tenantID)).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("budget key must carry a TTL: %v %v", ttl, err)
	}
}
