package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
)

type fakeRunRedis struct {
	mu      sync.Mutex
	members map[string]map[string]bool
}

type delayedRunLimiter struct {
	mu       sync.Mutex
	denials  int
	attempts int
}

func (limiter *delayedRunLimiter) TryAcquire(_ context.Context, tenantID, _, _ string, _ int, _ time.Duration) (RunPermit, bool, error) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	limiter.attempts++
	if limiter.attempts <= limiter.denials {
		return RunPermit{}, false, nil
	}
	return RunPermit{TenantID: tenantID, member: "permit"}, true, nil
}
func (*delayedRunLimiter) Renew(context.Context, RunPermit, time.Duration) error { return nil }
func (*delayedRunLimiter) Release(RunPermit)                                     {}

type canceledStore struct{ requested bool }

func (store canceledStore) Requested(context.Context, string, string) bool { return store.requested }

func (redis *fakeRunRedis) Eval(_ context.Context, script string, keys []string, args ...any) (any, error) {
	redis.mu.Lock()
	defer redis.mu.Unlock()
	if redis.members == nil {
		redis.members = make(map[string]map[string]bool)
	}
	members := redis.members[keys[0]]
	if members == nil {
		members = make(map[string]bool)
		redis.members[keys[0]] = members
	}
	member := args[0].(string)
	switch {
	case strings.Contains(script, "ZREMRANGEBYSCORE"):
		if members[member] {
			return int64(1), nil
		}
		limit := args[1].(int)
		if len(members) >= limit {
			return int64(0), nil
		}
		members[member] = true
		return int64(1), nil
	case strings.Contains(script, "not redis.call('ZSCORE'"):
		if !members[member] {
			return int64(0), nil
		}
		return int64(1), nil
	case strings.Contains(script, "ZREM"):
		if !members[member] {
			return int64(0), nil
		}
		delete(members, member)
		return int64(1), nil
	default:
		return nil, errors.New("unexpected script")
	}
}

func TestRedisRunLimiterTenantIsolationDynamicQuotaAndStaleRelease(t *testing.T) {
	backend := &fakeRunRedis{}
	firstNode := &RedisRunLimiter{Redis: backend, Prefix: "test"}
	secondNode := &RedisRunLimiter{Redis: backend, Prefix: "test"}
	ctx := context.Background()
	ttl := time.Minute
	first, acquired, err := firstNode.TryAcquire(ctx, "tenant-a", "request-1", "claim-1", 1, ttl)
	if err != nil || !acquired {
		t.Fatalf("first=%+v acquired=%v err=%v", first, acquired, err)
	}
	if _, acquired, err := secondNode.TryAcquire(ctx, "tenant-a", "request-2", "claim-2", 1, ttl); err != nil || acquired {
		t.Fatalf("over quota acquired=%v err=%v", acquired, err)
	}
	other, acquired, err := secondNode.TryAcquire(ctx, "tenant-b", "request-2", "claim-2", 1, ttl)
	if err != nil || !acquired {
		t.Fatalf("tenant isolation acquired=%v err=%v", acquired, err)
	}
	secondNode.Release(other)
	firstNode.Release(first)
	second, acquired, err := secondNode.TryAcquire(ctx, "tenant-a", "request-2", "claim-2", 1, ttl)
	if err != nil || !acquired {
		t.Fatalf("after release acquired=%v err=%v", acquired, err)
	}
	firstNode.Release(first) // stale exact-member release cannot remove request-2.
	if err := secondNode.Renew(ctx, second, ttl); err != nil {
		t.Fatalf("stale release removed current permit: %v", err)
	}
	// A published quota increase is used on the next acquisition.
	third, acquired, err := firstNode.TryAcquire(ctx, "tenant-a", "request-3", "claim-3", 2, ttl)
	if err != nil || !acquired {
		t.Fatalf("dynamic increase acquired=%v err=%v", acquired, err)
	}
	secondNode.Release(second)
	firstNode.Release(third)
}

func TestRedisRunLimiterValidatesAndFailsClosed(t *testing.T) {
	limiter := &RedisRunLimiter{}
	if _, _, err := limiter.TryAcquire(context.Background(), "tenant", "request", "claim", 1, time.Minute); err == nil {
		t.Fatal("nil Redis limiter did not fail closed")
	}
	limiter.Redis = &fakeRunRedis{}
	if _, _, err := limiter.TryAcquire(context.Background(), "tenant", "request", "claim", 257, time.Minute); err == nil {
		t.Fatal("invalid quota accepted")
	}
}

func TestProcessorWaitForQuotaRenewsInboxAndHonorsCancellation(t *testing.T) {
	inbox := idempotency.NewMemoryStore()
	now := time.Now().UTC()
	message := gateway.InboundMessage{TenantID: "tenant", AppID: "app", BindingID: "binding", ExternalMessageID: "external", UserID: "user", SessionID: "session", Text: "hello", ConfigVersion: 1, ReceivedAt: now}
	claim, won, err := inbox.Claim(context.Background(), message, "gateway", 60*time.Millisecond)
	if err != nil || !won {
		t.Fatalf("claim=%+v won=%v err=%v", claim, won, err)
	}
	request := claim.RunRequest()
	limiter := &delayedRunLimiter{denials: 4}
	processor := &Processor{Inbox: inbox, RunLimiter: limiter, LeaseTTL: 60 * time.Millisecond}
	_, renewed, err := processor.waitForRunPermit(context.Background(), request, claim, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !renewed.LeaseUntil.After(claim.LeaseUntil) {
		t.Fatalf("inbox claim was not renewed while waiting: old=%s new=%s", claim.LeaseUntil, renewed.LeaseUntil)
	}

	processor.Cancellations = canceledStore{requested: true}
	processor.RunLimiter = &delayedRunLimiter{denials: 1}
	if _, _, err := processor.waitForRunPermit(context.Background(), request, renewed, 1); !errors.Is(err, errCanceledWhileWaitingForQuota) {
		t.Fatalf("cancellation error=%v", err)
	}
}

func TestProcessorQuotaWaitIsBoundedForTenantFairness(t *testing.T) {
	inbox := idempotency.NewMemoryStore()
	message := gateway.InboundMessage{TenantID: "tenant", AppID: "app", BindingID: "binding", ExternalMessageID: "busy", UserID: "user", SessionID: "session", Text: "hello", ConfigVersion: 1, ReceivedAt: time.Now().UTC()}
	claim, _, err := inbox.Claim(context.Background(), message, "gateway", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	processor := &Processor{Inbox: inbox, RunLimiter: &delayedRunLimiter{denials: 100}, LeaseTTL: time.Minute, QuotaWait: 20 * time.Millisecond}
	started := time.Now()
	if _, _, err := processor.waitForRunPermit(context.Background(), claim.RunRequest(), claim, 1); !errors.Is(err, errTenantQuotaBusy) {
		t.Fatalf("quota error=%v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("quota wait blocked worker for %s", elapsed)
	}
	if err := inbox.Defer(context.Background(), claim, time.Now()); err != nil {
		t.Fatal(err)
	}
	ready, err := inbox.ClaimReady(context.Background(), "worker", time.Minute, 1)
	if err != nil || len(ready) != 1 || ready[0].Attempt != 1 {
		t.Fatalf("capacity defer consumed retry budget: ready=%+v err=%v", ready, err)
	}
}
