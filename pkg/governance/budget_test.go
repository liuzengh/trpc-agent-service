package governance

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestBudgetTrackerRejectsInvalidUsageValues(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{ID: "tenant-invalid-usage"})
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		tokens int64
		cost   float64
	}{
		{name: "negative tokens", tokens: -1},
		{name: "oversized tokens", tokens: maxBudgetExactInteger + 1},
		{name: "negative cost", cost: -1},
		{name: "nan cost", cost: math.NaN()},
		{name: "infinite cost", cost: math.Inf(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tracker.RecordUsage(ctx, tc.tokens, tc.cost); err == nil {
				t.Fatal("invalid usage was accepted")
			}
		})
	}
}

func TestBudgetTrackerSnapshotsTenantBudget(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tn := &tenant.Tenant{ID: "tenant-budget-snapshot", Budget: tenant.BudgetConfig{
		MaxTokensPerDay: 100, MaxTokensPerRequest: 50,
	}}
	tracker := NewBudgetTracker(client, tn)
	tn.Budget.MaxTokensPerRequest = 1
	reservation, err := tracker.ReserveTokenBudget(context.Background())
	if err != nil {
		t.Fatalf("snapshot reservation failed: %v", err)
	}
	if reservation.Reserved != 50 {
		t.Fatalf("reservation used mutable tenant state: %d", reservation.Reserved)
	}
}

func TestBudgetTrackerRejectsUnrepresentableReservationBudget(t *testing.T) {
	mr := setupTestRedis(t)
	tracker := NewBudgetTracker(createRedisClient(t, mr), &tenant.Tenant{
		ID: "tenant-unrepresentable-budget",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     maxBudgetExactInteger + 1,
			MaxTokensPerRequest: 1,
		},
	})

	if _, err := tracker.ReserveTokenBudget(context.Background()); err == nil {
		t.Fatal("reservation accepted a budget outside Redis Lua's exact integer range")
	}
}

// setupTestRedis creates a miniredis instance for testing.
func setupTestRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return mr
}

func TestBudgetTrackerTokenReservationsPreventOverspend(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID: "tenant-reserve",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     100,
			MaxTokensPerRequest: 60,
		},
	})
	ctx := context.Background()

	first, err := tracker.ReserveTokenBudget(ctx)
	if err != nil || first.ID == "" || first.Reserved != 60 {
		t.Fatalf("first reservation=%+v err=%v", first, err)
	}
	if _, err := tracker.ReserveTokenBudget(ctx); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("concurrent reservation exceeded daily budget: %v", err)
	}
	if err := tracker.DispatchTokenBudget(ctx, first); err != nil {
		t.Fatalf("dispatch first reservation: %v", err)
	}
	if err := tracker.SettleTokenBudget(ctx, first, 40); err != nil {
		t.Fatalf("settle first reservation: %v", err)
	}

	second, err := tracker.ReserveTokenBudget(ctx)
	if err != nil || second.ID == "" || second.ID == first.ID {
		t.Fatalf("capacity was not released after settlement: reservation=%+v err=%v", second, err)
	}
	if err := tracker.DispatchTokenBudget(ctx, second); err != nil {
		t.Fatalf("dispatch second reservation: %v", err)
	}
	if err := tracker.SettleTokenBudget(ctx, second, 60); err != nil {
		t.Fatalf("settle second reservation: %v", err)
	}
	usage, err := tracker.getDailyTokenUsage(ctx)
	if err != nil || usage != 100 {
		t.Fatalf("committed usage=%d err=%v, want 100", usage, err)
	}
}

func TestBudgetTrackerSettlementIsIdempotent(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID: "tenant-idempotent-settle",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     100,
			MaxTokensPerRequest: 50,
		},
	})
	ctx := context.Background()
	reservation, err := tracker.ReserveTokenBudget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.DispatchTokenBudget(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SettleTokenBudget(ctx, reservation, 30); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SettleTokenBudget(ctx, reservation, 30); err != nil {
		t.Fatalf("idempotent settlement failed: %v", err)
	}
	if err := tracker.SettleTokenBudget(ctx, reservation, 31); !errors.Is(err, ErrBudgetSettlementConflict) {
		t.Fatalf("conflicting settlement error=%v", err)
	}
	usage, err := tracker.getDailyTokenUsage(ctx)
	if err != nil || usage != 30 {
		t.Fatalf("idempotent usage=%d err=%v, want 30", usage, err)
	}
}

func TestBudgetTrackerDispatchAuthorizationIsSingleUse(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID: "tenant-single-dispatch",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     100,
			MaxTokensPerRequest: 50,
		},
	})
	ctx := context.Background()
	reservation, err := tracker.ReserveTokenBudget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.DispatchTokenBudget(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	if err := tracker.DispatchTokenBudget(ctx, reservation); !errors.Is(err, ErrBudgetSettlementConflict) {
		t.Fatalf("duplicate dispatch error=%v", err)
	}
	if err := tracker.ReleaseTokenBudget(ctx, reservation); !errors.Is(err, ErrBudgetSettlementConflict) {
		t.Fatalf("dispatched reservation was released: %v", err)
	}
}

func TestBudgetTrackerReleaseReturnsUnusedReservation(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID: "tenant-release",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     50,
			MaxTokensPerRequest: 50,
		},
	})
	ctx := context.Background()
	reservation, err := tracker.ReserveTokenBudget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.ReleaseTokenBudget(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	if err := tracker.ReleaseTokenBudget(ctx, reservation); err != nil {
		t.Fatalf("idempotent release failed: %v", err)
	}
	if _, err := tracker.ReserveTokenBudget(ctx); err != nil {
		t.Fatalf("released budget remained reserved: %v", err)
	}
}

func TestBudgetTrackerRecordsReservationOverrunAndFailsClosed(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID: "tenant-overrun",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     100,
			MaxTokensPerRequest: 50,
		},
	})
	ctx := context.Background()
	reservation, err := tracker.ReserveTokenBudget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.DispatchTokenBudget(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	if err := tracker.SettleTokenBudget(ctx, reservation, 70); !errors.Is(err, ErrBudgetReservationExceeded) {
		t.Fatalf("reservation overrun error=%v", err)
	}
	usage, err := tracker.getDailyTokenUsage(ctx)
	if err != nil || usage != 70 {
		t.Fatalf("overrun usage=%d err=%v, want 70", usage, err)
	}
}

func TestBudgetTrackerExpiredUndispatchedReservationIsReclaimed(t *testing.T) {
	mr := setupTestRedis(t)
	base := time.Date(2026, time.August, 25, 1, 0, 0, 0, time.UTC)
	mr.SetTime(base)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID: "tenant-late-settlement",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     100,
			MaxTokensPerRequest: 60,
		},
	})
	ctx := context.Background()

	_, err := tracker.ReserveTokenBudget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mr.SetTime(base.Add(tokenReservationLease + time.Second))
	if _, err := tracker.ReserveTokenBudget(ctx); err != nil {
		t.Fatalf("expired reservation was not reclaimed: %v", err)
	}
}

func TestBudgetTrackerExpiredDispatchedReservationRemainsHeld(t *testing.T) {
	mr := setupTestRedis(t)
	base := time.Date(2026, time.August, 25, 1, 0, 0, 0, time.UTC)
	mr.SetTime(base)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID: "tenant-uncertain-settlement",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     100,
			MaxTokensPerRequest: 60,
		},
	})
	ctx := context.Background()

	reservation, err := tracker.ReserveTokenBudget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.DispatchTokenBudget(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	mr.SetTime(base.Add(tokenReservationLease + time.Second))
	if _, err := tracker.ReserveTokenBudget(ctx); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("uncertain provider work released its hold: %v", err)
	}
	if err := tracker.SettleTokenBudget(ctx, reservation, 40); err != nil {
		t.Fatalf("uncertain reservation could not settle: %v", err)
	}
	if _, err := tracker.ReserveTokenBudget(ctx); err != nil {
		t.Fatalf("settlement did not return unused capacity: %v", err)
	}
}

func TestBudgetTrackerReservationDayUsesRedisClockAcrossReplicas(t *testing.T) {
	mr := setupTestRedis(t)
	firstDay := time.Date(2026, time.August, 25, 23, 55, 0, 0, time.UTC)
	mr.SetTime(firstDay)
	tenantConfig := &tenant.Tenant{ID: "tenant-redis-clock", Budget: tenant.BudgetConfig{
		MaxTokensPerDay: 100, MaxTokensPerRequest: 100,
	}}
	first := NewBudgetTracker(createRedisClient(t, mr), tenantConfig)
	second := NewBudgetTracker(createRedisClient(t, mr), tenantConfig)
	ctx := context.Background()

	reservation, err := first.ReserveTokenBudget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.day != budgetDay(firstDay) {
		t.Fatalf("reservation day=%q, want Redis day %q", reservation.day, budgetDay(firstDay))
	}
	if _, err := second.ReserveTokenBudget(ctx); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("second replica bypassed the shared Redis ledger: %v", err)
	}

	secondDay := firstDay.AddDate(0, 0, 1)
	mr.SetTime(secondDay)
	reservation, err = second.ReserveTokenBudget(ctx)
	if err != nil {
		t.Fatalf("new Redis UTC day did not receive a fresh reservation: %v", err)
	}
	if reservation.day != budgetDay(secondDay) {
		t.Fatalf("reservation day=%q, want Redis day %q", reservation.day, budgetDay(secondDay))
	}
}

func TestBudgetTrackerDispatchRejectsReservationExpiredByRedisClock(t *testing.T) {
	mr := setupTestRedis(t)
	mr.SetTime(time.Date(2026, time.August, 25, 1, 0, 0, 0, time.UTC))
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{ID: "tenant-redis-expiry", Budget: tenant.BudgetConfig{
		MaxTokensPerDay: 100, MaxTokensPerRequest: 50,
	}})

	reservation, err := tracker.ReserveTokenBudget(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mr.SetTime(time.Date(2026, time.August, 25, 1, 0, 0, 0, time.UTC).Add(tokenReservationLease + time.Millisecond))
	if err := tracker.DispatchTokenBudget(context.Background(), reservation); !errors.Is(err, ErrBudgetReservationInvalid) {
		t.Fatalf("dispatch after Redis lease expiration error=%v", err)
	}
}

func TestBudgetTrackerCorruptExpiredReservationsDoNotPartiallyReconcile(t *testing.T) {
	mr := setupTestRedis(t)
	base := time.Date(2026, time.August, 25, 1, 0, 0, 0, time.UTC)
	mr.SetTime(base)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{ID: "tenant-corrupt-expiry", Budget: tenant.BudgetConfig{
		MaxTokensPerDay: 100, MaxTokensPerRequest: 50,
	}})
	ctx := context.Background()
	day, _, _ := budgetDayBounds(base)
	keys := tracker.tokenBudgetKeys(day)
	expiredScore := float64(base.Add(-time.Second).UnixMilli())
	const validID = "expired-reserved"
	const corruptID = "expired-corrupt"
	if err := client.HSet(ctx, keys[2], validID, 40).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, keys[4], validID, "reserved", corruptID, "reserved").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, keys[1], &redis.Z{Score: expiredScore, Member: validID}, &redis.Z{Score: expiredScore, Member: corruptID}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, keys[3], 40, 0).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := tracker.ReserveTokenBudget(ctx); err == nil {
		t.Fatal("corrupt expired reservation state was accepted")
	}
	state, err := client.HGet(ctx, keys[4], validID).Result()
	if err != nil || state != "reserved" {
		t.Fatalf("valid record was changed during failed validation: state=%q err=%v", state, err)
	}
	if _, err := client.ZScore(ctx, keys[1], validID).Result(); err != nil {
		t.Fatalf("valid record was removed from active lease set: %v", err)
	}
	pending, err := client.Get(ctx, keys[3]).Int64()
	if err != nil || pending != 40 {
		t.Fatalf("pending tokens changed during failed validation: pending=%d err=%v", pending, err)
	}
}

// createRedisClient creates a Redis client connected to miniredis.
func createRedisClient(t *testing.T, mr *miniredis.Miniredis) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() { client.Close() })
	return client
}

func TestBudgetTracker_CheckBudget_WithinLimits(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-1",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay: 10000,
			MaxCostPerDay:   5.0,
		},
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record some usage within limits
	err := tracker.RecordUsage(ctx, 5000, 2.5)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	// Check budget - should pass
	err = tracker.CheckBudget(ctx)
	if err != nil {
		t.Errorf("CheckBudget should pass within limits, got: %v", err)
	}
}

func TestBudgetTracker_CheckBudget_TokenLimitExceeded(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-2",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay: 1000,
			MaxCostPerDay:   0, // No cost limit
		},
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record usage at the limit
	err := tracker.RecordUsage(ctx, 1000, 0)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	// Check budget - should fail (at limit)
	err = tracker.CheckBudget(ctx)
	if err != ErrBudgetExceeded {
		t.Errorf("CheckBudget should return ErrBudgetExceeded, got: %v", err)
	}
}

func TestBudgetTracker_CheckBudget_CostLimitExceeded(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-3",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay: 0, // No token limit
			MaxCostPerDay:   1.0,
		},
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record usage above the limit
	err := tracker.RecordUsage(ctx, 0, 1.5)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	// Check budget - should fail
	err = tracker.CheckBudget(ctx)
	if err != ErrBudgetExceeded {
		t.Errorf("CheckBudget should return ErrBudgetExceeded, got: %v", err)
	}
}

func TestBudgetTracker_CheckBudget_NoBudgetConfigured(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-4",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay: 0, // No limits
			MaxCostPerDay:   0,
		},
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record usage
	err := tracker.RecordUsage(ctx, 100000, 100.0)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	// Check budget - should pass (no limits configured)
	err = tracker.CheckBudget(ctx)
	if err != nil {
		t.Errorf("CheckBudget should pass with no limits, got: %v", err)
	}
}

func TestBudgetTracker_RecordUsage_IncrementsCorrectly(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-5",
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay: 10000,
			MaxCostPerDay:   10.0,
		},
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record multiple usages
	err := tracker.RecordUsage(ctx, 100, 0.5)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	err = tracker.RecordUsage(ctx, 200, 1.5)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	// Check that usage was accumulated
	tokens, err := tracker.getDailyTokenUsage(ctx)
	if err != nil {
		t.Fatalf("getDailyTokenUsage failed: %v", err)
	}
	if tokens != 300 {
		t.Errorf("Expected 300 tokens, got %d", tokens)
	}

	cost, err := tracker.getDailyCostUsage(ctx)
	if err != nil {
		t.Fatalf("getDailyCostUsage failed: %v", err)
	}
	if cost < 2.0 || cost > 2.001 { // Allow for float precision
		t.Errorf("Expected ~2.0 cost, got %f", cost)
	}
}

func TestBudgetTracker_DailyKeyFormat(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-6",
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record usage
	err := tracker.RecordUsage(ctx, 500, 1.0)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	// The production ledger uses Redis TIME, not the process-local clock.
	now, err := tracker.redisTime(ctx)
	if err != nil {
		t.Fatalf("read Redis clock: %v", err)
	}
	expectedTokenKey := tracker.dailyTokenKey(budgetDay(now))
	expectedCostKey := tracker.dailyCostKey(budgetDay(now))

	// Check that keys exist in Redis
	tokenVal := client.Get(ctx, expectedTokenKey)
	if tokenVal.Err() != nil {
		t.Errorf("Token key not found: %v", tokenVal.Err())
	}

	costVal := client.Get(ctx, expectedCostKey)
	if costVal.Err() != nil {
		t.Errorf("Cost key not found: %v", costVal.Err())
	}
}

func TestBudgetTracker_TTLSetCorrectly(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-7",
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record usage
	err := tracker.RecordUsage(ctx, 500, 1.0)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	// Verify TTL is set against the same Redis-day partition used by writes.
	now, err := tracker.redisTime(ctx)
	if err != nil {
		t.Fatalf("read Redis clock: %v", err)
	}
	tokenKey := tracker.dailyTokenKey(budgetDay(now))

	ttl, err := client.TTL(ctx, tokenKey).Result()
	if err != nil {
		t.Fatalf("TTL check failed: %v", err)
	}

	// TTL should be between 47 and 48 hours
	if ttl < 47*time.Hour || ttl > 48*time.Hour {
		t.Errorf("Expected TTL around 48h, got %v", ttl)
	}
}

func TestBudgetTracker_ZeroTokensNoError(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-8",
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record zero tokens - should not create a key
	err := tracker.RecordUsage(ctx, 0, 1.0)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	now, err := tracker.redisTime(ctx)
	if err != nil {
		t.Fatalf("read Redis clock: %v", err)
	}
	tokenKey := tracker.dailyTokenKey(budgetDay(now))

	// Token key should not exist
	val := client.Get(ctx, tokenKey)
	if val.Err() != redis.Nil {
		t.Errorf("Token key should not exist for zero tokens, got: %v", val.Err())
	}
}

func TestBudgetTracker_ZeroCostNoError(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)

	testTenant := &tenant.Tenant{
		ID: "tenant-9",
	}

	tracker := NewBudgetTracker(client, testTenant)
	ctx := context.Background()

	// Record zero cost - should not create a key
	err := tracker.RecordUsage(ctx, 100, 0)
	if err != nil {
		t.Fatalf("RecordUsage failed: %v", err)
	}

	now, err := tracker.redisTime(ctx)
	if err != nil {
		t.Fatalf("read Redis clock: %v", err)
	}
	costKey := tracker.dailyCostKey(budgetDay(now))

	// Cost key should not exist
	val := client.Get(ctx, costKey)
	if val.Err() != redis.Nil {
		t.Errorf("Cost key should not exist for zero cost, got: %v", val.Err())
	}
}

func TestBudgetTracker_ConcurrencySlotIsFailClosedAndReleasable(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID:     "tenant-concurrency",
		Budget: tenant.BudgetConfig{MaxConcurrentSessions: 1},
	})
	ctx := context.Background()

	first, err := tracker.AcquireSessionSlot(ctx)
	if err != nil || first == nil {
		t.Fatalf("first slot=%v err=%v", first, err)
	}
	if _, err := tracker.AcquireSessionSlot(ctx); err != ErrBudgetExceeded {
		t.Fatalf("second slot must be rejected, got %v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	second, err := tracker.AcquireSessionSlot(ctx)
	if err != nil || second == nil || second == first {
		t.Fatalf("slot was not reusable with a unique lease: slot=%v err=%v", second, err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatalf("release second slot: %v", err)
	}
}

func TestBudgetTracker_ConcurrencySlotRenewsBeyondOriginalTTL(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID:     "tenant-concurrency-renewal",
		Budget: tenant.BudgetConfig{MaxConcurrentSessions: 1},
	})
	tracker.sessionSlotTTL = 90 * time.Millisecond
	tracker.sessionSlotRenewInterval = 20 * time.Millisecond

	slot, err := tracker.AcquireSessionSlot(context.Background())
	if err != nil || slot == nil {
		t.Fatalf("acquire slot=%v err=%v", slot, err)
	}
	t.Cleanup(func() { _ = slot.Release(context.Background()) })

	time.Sleep(140 * time.Millisecond)
	if _, err := tracker.AcquireSessionSlot(context.Background()); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("second slot acquired after original TTL despite renewal: %v", err)
	}
}

func TestBudgetTracker_StaleConcurrencySlotCannotReleaseNewOwner(t *testing.T) {
	mr := setupTestRedis(t)
	// Lease ownership test uses a fixed Redis clock; renewal timing is tested separately.
	mr.SetTime(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC))
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID:     "tenant-concurrency-stale-owner",
		Budget: tenant.BudgetConfig{MaxConcurrentSessions: 1},
	})
	tracker.sessionSlotTTL = 90 * time.Millisecond
	tracker.sessionSlotRenewInterval = 20 * time.Millisecond
	ctx := context.Background()

	first, err := tracker.AcquireSessionSlot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Release(context.Background()) })
	stale := first.(*sessionSlotLease)
	if err := client.ZRem(ctx, tracker.sessionSlotKey(), stale.token).Err(); err != nil {
		t.Fatal(err)
	}
	second, err := tracker.AcquireSessionSlot(ctx)
	if err != nil {
		t.Fatalf("new owner could not acquire vacated slot: %v", err)
	}
	t.Cleanup(func() { _ = second.Release(context.Background()) })

	select {
	case <-first.Done():
	case <-time.After(time.Second):
		t.Fatal("stale owner did not detect lost slot")
	}
	if err := first.Release(ctx); !errors.Is(err, ErrSessionSlotLost) {
		t.Fatalf("stale release error=%v", err)
	}
	newOwner := second.(*sessionSlotLease)
	if _, err := client.ZScore(ctx, tracker.sessionSlotKey(), newOwner.token).Result(); err != nil {
		t.Fatalf("stale release removed the new owner's record: %v", err)
	}
	third, err := tracker.AcquireSessionSlot(ctx)
	if third != nil {
		t.Cleanup(func() { _ = third.Release(context.Background()) })
	}
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("live new owner did not retain the concurrency slot: %v", err)
	}
}

func TestBudgetTracker_ConcurrencySlotReleaseDetectsMissingOwnerImmediately(t *testing.T) {
	mr := setupTestRedis(t)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID:     "tenant-concurrency-missing-owner",
		Budget: tenant.BudgetConfig{MaxConcurrentSessions: 1},
	})
	tracker.sessionSlotTTL = time.Minute
	tracker.sessionSlotRenewInterval = 30 * time.Second

	slot, err := tracker.AcquireSessionSlot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Del(context.Background(), tracker.sessionSlotKey()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := slot.Release(context.Background()); !errors.Is(err, ErrSessionSlotLost) {
		t.Fatalf("release error=%v, want lost owner", err)
	}
}

func TestBudgetTracker_ConcurrencySlotReleaseRejectsExpiredOwner(t *testing.T) {
	mr := setupTestRedis(t)
	base := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	mr.SetTime(base)
	client := createRedisClient(t, mr)
	tracker := NewBudgetTracker(client, &tenant.Tenant{
		ID:     "tenant-concurrency-expired-owner",
		Budget: tenant.BudgetConfig{MaxConcurrentSessions: 1},
	})
	tracker.sessionSlotTTL = time.Minute
	tracker.sessionSlotRenewInterval = 30 * time.Minute

	slot, err := tracker.AcquireSessionSlot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The sorted-set key still exists for two lease periods, but this member's
	// score is already expired. Release must not turn that stale membership into
	// proof that the invocation retained its slot.
	mr.SetTime(base.Add(90 * time.Second))
	if err := slot.Release(context.Background()); !errors.Is(err, ErrSessionSlotLost) {
		t.Fatalf("release error=%v, want expired owner loss", err)
	}
	if count, err := client.ZCard(context.Background(), tracker.sessionSlotKey()).Result(); err != nil || count != 0 {
		t.Fatalf("expired owner remained in slot set: count=%d err=%v", count, err)
	}
}

func TestBudgetTrackerKeysShareOpaqueRedisClusterHashTag(t *testing.T) {
	tracker := NewBudgetTracker(nil, &tenant.Tenant{ID: "tenant-with-{unsafe}-id"})
	keys := append(tracker.tokenBudgetKeys("2026-08-25"), tracker.sessionSlotKey())
	var hashTag string
	for _, key := range keys {
		open := strings.IndexByte(key, '{')
		close := strings.IndexByte(key, '}')
		if open < 0 || close <= open+1 {
			t.Fatalf("key %q has no Redis Cluster hash tag", key)
		}
		got := key[open+1 : close]
		if hashTag == "" {
			hashTag = got
		} else if got != hashTag {
			t.Fatalf("keys do not share one hash tag: %q != %q", got, hashTag)
		}
		if strings.Contains(key, tracker.tenant.ID) {
			t.Fatalf("key exposes raw tenant ID: %q", key)
		}
	}
}
