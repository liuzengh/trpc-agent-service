package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	// Aliased because this test file's own package is also named agent.
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// countResolver builds a fake runner once per call and counts how many times
// it was asked, so a test can distinguish "cached" from "rebuilt every time".
//
// runner.Runner is an interface; a bare stub is enough because nothing here
// executes a conversation — this file is about cache identity, not about
// whether a Runner works. That execution path is exercised by the Registry
// tests in agent_test.go and, from P2 on, by the worker tests.
type countResolver struct {
	mu    sync.Mutex
	calls int
}

func (r *countResolver) resolve(key PlanKey, _ *tenant.Context, _ session.Service) (runner.Runner, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return stubRunner{label: key.String()}, nil
}

func (r *countResolver) called() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

type stubRunner struct{ label string }

func (stubRunner) Run(context.Context, string, string, model.Message, ...trpcagent.RunOption) (<-chan *event.Event, error) {
	return nil, nil
}

func (stubRunner) Close() error { return nil }

func validKey() PlanKey {
	return PlanKey{
		TenantID:              "acme",
		AppID:                 1,
		RevisionID:            10,
		ModelProfileVersion:   1,
		BackendProfileVersion: 1,
	}
}

func TestPlanKeyValidateRejectsPartialKeys(t *testing.T) {
	base := validKey()
	cases := []struct {
		name   string
		mutate func(*PlanKey)
	}{
		{"empty tenant", func(k *PlanKey) { k.TenantID = "" }},
		{"zero app", func(k *PlanKey) { k.AppID = 0 }},
		{"zero revision", func(k *PlanKey) { k.RevisionID = 0 }},
		{"zero model version", func(k *PlanKey) { k.ModelProfileVersion = 0 }},
		{"zero backend version", func(k *PlanKey) { k.BackendProfileVersion = 0 }},
	}
	for _, c := range cases {
		k := base
		c.mutate(&k)
		if err := k.Validate(); err == nil {
			t.Fatalf("%s: a partial plan key must not validate", c.name)
		}
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("the complete key must validate: %v", err)
	}
}

// TestPlanCacheDistinguishesEveryDimension is the acceptance behind "五维缓存
// 隔离" in the approved plan: a cached plan must never be reused for a key
// that differs in any one of the five components.
func TestPlanCacheDistinguishesEveryDimension(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*PlanKey)
	}{
		{"different tenant", func(k *PlanKey) { k.TenantID = "globex" }},
		{"different app", func(k *PlanKey) { k.AppID = 2 }},
		{"different revision", func(k *PlanKey) { k.RevisionID = 11 }},
		{"different model profile version", func(k *PlanKey) { k.ModelProfileVersion = 2 }},
		{"different backend profile version", func(k *PlanKey) { k.BackendProfileVersion = 2 }},
	}
	for _, c := range cases {
		res := &countResolver{}
		cache := newPlanCache(nil, res.resolve)

		first := validKey()
		if _, err := cache.RunnerFor(first, &tenant.Context{ID: first.TenantID}); err != nil {
			t.Fatalf("%s: first resolve: %v", c.name, err)
		}
		second := validKey()
		c.mutate(&second)
		if _, err := cache.RunnerFor(second, &tenant.Context{ID: second.TenantID}); err != nil {
			t.Fatalf("%s: second resolve: %v", c.name, err)
		}
		if res.called() != 2 {
			t.Fatalf("%s: the cache served one plan for two different keys (resolver ran %d times, want 2)",
				c.name, res.called())
		}
		if cache.Len() != 2 {
			t.Fatalf("%s: cache holds %d plans, want 2", c.name, cache.Len())
		}
	}
}

func TestPlanCacheReusesAnIdenticalKey(t *testing.T) {
	res := &countResolver{}
	cache := newPlanCache(nil, res.resolve)

	key := validKey()
	first, err := cache.RunnerFor(key, &tenant.Context{ID: key.TenantID})
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := cache.RunnerFor(key, &tenant.Context{ID: key.TenantID})
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if res.called() != 1 {
		t.Fatalf("resolver ran %d times for an identical key, want 1", res.called())
	}
	if first != second {
		t.Fatal("an identical key returned a different runner instance")
	}
}

func TestPlanCacheRejectsAnInvalidKeyWithoutCallingTheResolver(t *testing.T) {
	res := &countResolver{}
	cache := newPlanCache(nil, res.resolve)

	bad := validKey()
	bad.TenantID = ""
	if _, err := cache.RunnerFor(bad, &tenant.Context{}); err == nil {
		t.Fatal("an invalid key must not resolve")
	}
	if res.called() != 0 {
		t.Fatalf("resolver ran %d times for a rejected key, want 0", res.called())
	}
}

func TestPlanCachePropagatesAResolverError(t *testing.T) {
	sentinel := errors.New("boom")
	cache := newPlanCache(nil, func(PlanKey, *tenant.Context, session.Service) (runner.Runner, error) {
		return nil, sentinel
	})
	if _, err := cache.RunnerFor(validKey(), &tenant.Context{ID: "acme"}); !errors.Is(err, sentinel) {
		t.Fatalf("error lost on the way out: %v, want %v", err, sentinel)
	}
	if cache.Len() != 0 {
		t.Fatal("a failed resolve must not leave a half-built plan in the cache")
	}
}

func TestPlanCacheConcurrentMissesConvergeOnOneEntry(t *testing.T) {
	res := &countResolver{}
	cache := newPlanCache(nil, res.resolve)

	// A cold start where many messages arrive at once for the same key. The
	// resolve happens outside the write lock by design, so several goroutines
	// may each build a Runner; exactly one survives, and the cache ends up
	// with one entry, not one per racer.
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cache.RunnerFor(validKey(), &tenant.Context{ID: "acme"}); err != nil {
				t.Errorf("concurrent resolve: %v", err)
			}
		}()
	}
	wg.Wait()

	if cache.Len() != 1 {
		t.Fatalf("cache holds %d entries after %d concurrent misses on one key, want 1", cache.Len(), n)
	}
	if res.called() < 1 {
		t.Fatalf("resolver never ran: %d calls", res.called())
	}
}

func TestPlanCacheEvictOnlyTouchesTheNamedApp(t *testing.T) {
	res := &countResolver{}
	cache := newPlanCache(nil, res.resolve)

	keep := validKey()
	goAway := validKey()
	goAway.AppID = 2
	if _, err := cache.RunnerFor(keep, &tenant.Context{ID: keep.TenantID}); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.RunnerFor(goAway, &tenant.Context{ID: goAway.TenantID}); err != nil {
		t.Fatal(err)
	}
	if removed := cache.Evict("acme", 2); removed != 1 {
		t.Fatalf("evicted %d plans, want 1", removed)
	}
	if cache.Len() != 1 {
		t.Fatalf("cache holds %d plans after evicting one of two, want 1", cache.Len())
	}
	// A second eviction of the same key is a no-op, not an error.
	if removed := cache.Evict("acme", 2); removed != 0 {
		t.Fatalf("second eviction removed %d plans, want 0", removed)
	}
}
