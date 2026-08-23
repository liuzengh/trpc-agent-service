package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
)

func redisTestContext() tenant.TenantContext {
	return tenant.TenantContext{TenantID: "p006-test", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"}}
}

func redisClaimFixture(t *testing.T, ttl time.Duration) (*Backend, *redis.Client, context.Context, tenant.TenantContext, storage.DedupKey) {
	t.Helper()
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL is not set")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		client.Close()
		t.Fatalf("redis unavailable: %v", err)
	}
	prefix := fmt.Sprintf("p006-test-%d-", time.Now().UnixNano())
	b, err := NewBackendWithClient(client, Config{URL: url, KeyPrefix: prefix, ClaimTTL: ttl, SessionLeaseTTL: 2 * time.Second})
	if err != nil {
		cancel()
		client.Close()
		t.Fatal(err)
	}
	tc := redisTestContext()
	claimKey := storage.DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: prefix + "message"}
	redisKey, err := dedupKey(prefix, claimKey.TenantID, claimKey.Channel, claimKey.BindingID, claimKey.ExternalMessageID)
	if err != nil {
		cancel()
		client.Close()
		t.Fatal(err)
	}
	fenceKey, _ := key(prefix, "fence", "claim", tc.TenantID, tc.Channel, tc.BindingID, claimKey.ExternalMessageID)
	attemptKey, _ := key(prefix, "attempt", "claim", tc.TenantID, tc.Channel, tc.BindingID, claimKey.ExternalMessageID)
	t.Cleanup(func() { client.Del(context.Background(), redisKey, fenceKey, attemptKey); cancel(); client.Close() })
	return b, client, ctx, tc, claimKey
}

func redisLeaseFixture(t *testing.T, ttl time.Duration) (*Store, *redis.Client, context.Context, tenant.TenantContext, string) {
	t.Helper()
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL is not set; Redis lease not verified")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		client.Close()
		t.Fatalf("redis unavailable: %v", err)
	}
	prefix := fmt.Sprintf("p006-lease-%d-", time.Now().UnixNano())
	backend, err := NewBackendWithClient(client, Config{URL: url, KeyPrefix: prefix, SessionLeaseTTL: ttl})
	if err != nil {
		cancel()
		client.Close()
		t.Fatal(err)
	}
	store := NewStore(backend)
	tc := redisTestContext()
	sessionID := prefix + "session"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		var cursor uint64
		for {
			keys, next, scanErr := client.Scan(cleanupCtx, cursor, prefix+"*", 100).Result()
			if scanErr != nil {
				break
			}
			if len(keys) > 0 {
				_ = client.Del(cleanupCtx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		cancel()
		client.Close()
	})
	return store, client, ctx, tc, sessionID
}

func TestRedisLeaseRenewalRunner(t *testing.T) {
	store, _, ctx, tc, sessionID := redisLeaseFixture(t, 3*time.Second)
	lease, err := store.Acquire(ctx, tc, sessionID, "runner-owner", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(ctx)
	runner, err := storage.NewLeaseRenewalRunner(parent, store, tc, lease, 3*time.Second, 500*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	initial := lease.ExpiresAt
	deadline := time.Now().Add(5 * time.Second)
	for !runner.Lease().ExpiresAt.After(initial) {
		if time.Now().After(deadline) {
			t.Fatalf("Redis runner did not renew: initial=%s latest=%s", initial, runner.Lease().ExpiresAt)
		}
		time.Sleep(10 * time.Millisecond)
	}
	latest := runner.Lease()
	if latest.OwnerID != lease.OwnerID || latest.FenceToken != lease.FenceToken || latest.Epoch != lease.Epoch {
		t.Fatalf("Redis lease identity changed: initial=%+v latest=%+v", lease, latest)
	}
	cancel()
	if err := runner.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Redis runner cancellation: %v", err)
	}
}

func TestRedisLeaseAcquireRenewReleaseValidate(t *testing.T) {
	store, _, ctx, tc, sessionID := redisLeaseFixture(t, 2*time.Second)
	lease, err := store.Acquire(ctx, tc, sessionID, "owner-1", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.TenantID != tc.TenantID || lease.SessionID != sessionID || lease.ResourceID != sessionID || lease.OwnerID != "owner-1" || lease.FenceToken == 0 || lease.ExpiresAt.IsZero() || lease.Backend != storage.BackendRedis || lease.Epoch == 0 {
		t.Fatalf("invalid lease: %+v", lease)
	}
	originalExpiry := lease.ExpiresAt
	if err := store.Validate(ctx, tc, lease); err != nil {
		t.Fatalf("valid lease failed validation: %v", err)
	}
	renewed, err := store.Renew(ctx, tc, lease, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !renewed.ExpiresAt.After(originalExpiry) || renewed.FenceToken != lease.FenceToken || renewed.OwnerID != lease.OwnerID {
		t.Fatalf("renew changed invalid fields: before=%+v after=%+v", lease, renewed)
	}
	if _, err := store.Renew(ctx, tc, storage.Lease{TenantID: tc.TenantID, SessionID: sessionID, ResourceID: sessionID, OwnerID: "wrong-owner", FenceToken: lease.FenceToken, Backend: storage.BackendRedis, Epoch: lease.Epoch}, time.Second); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("wrong owner classification: %v", err)
	}
	if _, err := store.Renew(ctx, tc, storage.Lease{TenantID: tc.TenantID, SessionID: sessionID, ResourceID: sessionID, OwnerID: lease.OwnerID, FenceToken: lease.FenceToken - 1, Backend: storage.BackendRedis, Epoch: lease.Epoch}, time.Second); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old token classification: %v", err)
	}
	if _, err := store.Renew(ctx, tc, storage.Lease{TenantID: tc.TenantID, SessionID: sessionID, ResourceID: sessionID, OwnerID: lease.OwnerID, FenceToken: lease.FenceToken, Backend: storage.BackendRedis, Epoch: lease.Epoch + 1}, time.Second); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("bad epoch classification: %v", err)
	}
	if err := store.Release(ctx, tc, renewed); err != nil {
		t.Fatalf("valid release failed: %v", err)
	}
	if err := store.Validate(ctx, tc, renewed); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("released lease validation: %v", err)
	}
	if err := store.Release(ctx, tc, renewed); !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("repeat release classification: %v", err)
	}
}

func TestRedisClaimIntegration(t *testing.T) {
	b, _, ctx, tc, key := redisClaimFixture(t, time.Second)
	c, err := b.claim(ctx, tc, key, time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	if c.Status != storage.ClaimAcquired || c.OwnerID != "owner-1" || c.Epoch == 0 || c.FenceToken == 0 || c.Backend != storage.BackendRedis || c.Key != key {
		t.Fatalf("invalid claim: %+v", c)
	}
}

func TestRedisClaimRepeatAcquire(t *testing.T) {
	b, _, ctx, tc, key := redisClaimFixture(t, 2*time.Second)
	first, err := b.claim(ctx, tc, key, 2*time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{"owner-2", "owner-3", "owner-4"} {
		c, err := b.claim(ctx, tc, key, 2*time.Second, owner)
		if err != nil {
			t.Fatal(err)
		}
		if c.OwnerID != first.OwnerID || c.Attempt != first.Attempt || c.FenceToken != first.FenceToken || c.Epoch != first.Epoch {
			t.Fatalf("unstable repeated claim: first=%+v repeated=%+v", first, c)
		}
	}
}

func TestRedisClaimConcurrency(t *testing.T) {
	b, _, ctx, tc, key := redisClaimFixture(t, 2*time.Second)
	type result struct {
		requested string
		claim     storage.Claim
		err       error
	}
	const n = 100
	start := make(chan struct{})
	results := make(chan result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		owner := fmt.Sprintf("owner-%d", i)
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			<-start
			c, err := b.claim(ctx, tc, key, 2*time.Second, owner)
			results <- result{owner, c, err}
		}(owner)
	}
	close(start)
	wg.Wait()
	close(results)
	owners, attempts, fences, epochs := map[string]int{}, map[int]int{}, map[uint64]int{}, map[storage.Epoch]int{}
	for r := range results {
		if r.err != nil {
			t.Fatalf("request owner=%s error=%v", r.requested, r.err)
		}
		owners[r.claim.OwnerID]++
		attempts[r.claim.Attempt]++
		fences[r.claim.FenceToken]++
		epochs[r.claim.Epoch]++
	}
	t.Logf("owners=%v attempts=%v fences=%v epochs=%v", owners, attempts, fences, epochs)
	if len(owners) != 1 || len(attempts) != 1 || len(fences) != 1 || len(epochs) != 1 {
		t.Fatalf("claim metadata split: owners=%v attempts=%v fences=%v epochs=%v", owners, attempts, fences, epochs)
	}
}

func TestRedisClaimTakeover(t *testing.T) {
	b, _, ctx, tc, key := redisClaimFixture(t, 1*time.Second)
	first, err := b.claim(ctx, tc, key, 1*time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	early, err := b.claim(ctx, tc, key, 1*time.Second, "owner-2")
	if err != nil {
		t.Fatal(err)
	}
	if early.OwnerID != first.OwnerID {
		t.Fatalf("early takeover: %+v", early)
	}
	time.Sleep(1200 * time.Millisecond)
	next, err := b.claim(ctx, tc, key, 2*time.Second, "owner-2")
	if err != nil {
		t.Fatal(err)
	}
	if next.OwnerID != "owner-2" || next.Attempt <= first.Attempt || next.FenceToken <= first.FenceToken {
		t.Fatalf("invalid takeover first=%+v next=%+v", first, next)
	}
	if err := b.writeClaimOwner(ctx, tc, key, "completed", "stale", first.OwnerID, first.FenceToken); err == nil {
		t.Fatal("stale owner accepted")
	}
}

func TestRedisClaimFencing(t *testing.T) {
	b, _, ctx, tc, key := redisClaimFixture(t, 1*time.Second)
	first, err := b.claim(ctx, tc, key, 1*time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	next, err := b.claim(ctx, tc, key, 2*time.Second, "owner-2")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, owner string
		fence       uint64
	}{{"old-owner", first.OwnerID, next.FenceToken}, {"old-token", next.OwnerID, first.FenceToken}, {"wrong-owner", "wrong-owner", next.FenceToken}} {
		if err := b.writeClaimOwner(ctx, tc, key, "completed", "bad", test.owner, test.fence); err == nil {
			t.Fatalf("%s accepted", test.name)
		}
	}
	if err := b.writeClaimOwner(ctx, tc, key, "completed", "ok", next.OwnerID, next.FenceToken); err != nil {
		t.Fatalf("valid fencing operation rejected: %v", err)
	}
}
