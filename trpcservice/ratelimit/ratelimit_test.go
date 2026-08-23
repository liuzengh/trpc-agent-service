package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type fakeExecutor struct {
	mu      sync.Mutex
	counts  map[string]int64
	failure error
	resets  int64
}

func newFakeExecutor() *fakeExecutor { return &fakeExecutor{counts: make(map[string]int64)} }

func (f *fakeExecutor) Eval(ctx context.Context, _ string, keys []string, args ...interface{}) (interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failure != nil {
		return nil, f.failure
	}
	cost := args[1].(int64)
	limits := []int64{args[2].(int64), args[3].(int64), args[4].(int64)}
	for i, key := range keys {
		if f.counts[key]+cost > limits[i] {
			return []interface{}{int64(0), int64(1), limits[i] - f.counts[key], int64(i + 1)}, nil
		}
	}
	remaining := limits[0]
	for i, key := range keys {
		f.counts[key] += cost
		if available := limits[i] - f.counts[key]; available < remaining {
			remaining = available
		}
	}
	return []interface{}{int64(1), int64(0), remaining, int64(0)}, nil
}

func (f *fakeExecutor) reset() {
	f.mu.Lock()
	f.counts = make(map[string]int64)
	f.resets++
	f.mu.Unlock()
}
func (f *fakeExecutor) count(key string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[key]
}

func newTestLimiter(f *fakeExecutor, tenant, binding, chat int64) *RateLimiter {
	return &RateLimiter{executor: f, prefix: "test", policy: LimitPolicy{TenantLimit: tenant, BindingLimit: binding, ChatLimit: chat, Window: time.Minute}, windowSecond: 60}
}
func testRequest() LimitRequest {
	return LimitRequest{TenantID: "tenant-a", Channel: "web", BindingID: "binding-a", ExternalChatID: "chat-a", Cost: 1}
}

func TestRateLimitAllowsWithinAllDimensions(t *testing.T) {
	limiter := newTestLimiter(newFakeExecutor(), 3, 3, 3)
	decision, err := limiter.Allow(context.Background(), testRequest())
	if err != nil || !decision.Allowed || decision.Remaining != 2 || decision.Scope != "" {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestRateLimitRejectsWhenAnyDimensionExhausted(t *testing.T) {
	f := newFakeExecutor()
	limiter := newTestLimiter(f, 2, 5, 5)
	request := testRequest()
	if _, err := limiter.Allow(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Allow(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	decision, err := limiter.Allow(context.Background(), request)
	if !errors.Is(err, ErrRateLimited) || decision.Allowed || decision.Scope != ScopeTenant || decision.Remaining != 0 {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestRateLimitJointConsumeIsAtomic(t *testing.T) {
	f := newFakeExecutor()
	limiter := newTestLimiter(f, 5, 1, 5)
	request := testRequest()
	if _, err := limiter.Allow(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request.ExternalChatID = "chat-b"
	decision, err := limiter.Allow(context.Background(), request)
	if !errors.Is(err, ErrRateLimited) || decision.Scope != ScopeBinding {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	keys := limiter.keys(request)
	if got := f.count(keys[0]); got != 1 {
		t.Fatalf("tenant partial consume=%d", got)
	}
	if got := f.count(keys[2]); got != 0 {
		t.Fatalf("chat partial consume=%d", got)
	}
}

func TestRateLimitConcurrentRequestsDoNotExceedLimit(t *testing.T) {
	f := newFakeExecutor()
	limiter := newTestLimiter(f, 40, 100, 100)
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if decision, err := limiter.Allow(context.Background(), testRequest()); err == nil && decision.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 40 {
		t.Fatalf("allowed=%d", allowed.Load())
	}
}

func TestRateLimitScopesAreIsolated(t *testing.T) {
	f := newFakeExecutor()
	limiter := newTestLimiter(f, 1, 1, 1)
	first := testRequest()
	if _, err := limiter.Allow(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.TenantID = "tenant-b"
	second.BindingID = "binding-b"
	second.ExternalChatID = "chat-b"
	if decision, err := limiter.Allow(context.Background(), second); err != nil || !decision.Allowed {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestRateLimitWindowOrRefillBoundary(t *testing.T) {
	f := newFakeExecutor()
	limiter := newTestLimiter(f, 1, 1, 1)
	if _, err := limiter.Allow(context.Background(), testRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := limiter.Allow(context.Background(), testRequest()); !errors.Is(err, ErrRateLimited) {
		t.Fatal(err)
	}
	f.reset()
	if decision, err := limiter.Allow(context.Background(), testRequest()); err != nil || !decision.Allowed {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestRateLimitContextCancellation(t *testing.T) {
	f := newFakeExecutor()
	limiter := newTestLimiter(f, 1, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := limiter.Allow(ctx, testRequest())
	if !errors.Is(err, ErrRateLimitBackendUnavailable) || errors.Is(err, storage.ErrRateLimited) {
		t.Fatalf("err=%v", err)
	}
}

func TestRateLimitErrorClassification(t *testing.T) {
	f := newFakeExecutor()
	limiter := newTestLimiter(f, 1, 1, 1)
	f.failure = storage.ErrBackendUnavailable
	if _, err := limiter.Allow(context.Background(), testRequest()); !errors.Is(err, ErrRateLimitBackendUnavailable) || errors.Is(err, ErrRateLimited) {
		t.Fatalf("backend err=%v", err)
	}
	f.failure = nil
	if _, err := limiter.Allow(context.Background(), LimitRequest{TenantID: "", Channel: "web", BindingID: "b", ExternalChatID: "c", Cost: 1}); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("invalid err=%v", err)
	}
}
