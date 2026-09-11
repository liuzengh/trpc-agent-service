package agent

import (
	"errors"
	"fmt"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// PlanKey identifies one cached runtime plan. It is exactly the five-tuple
// the approved plan names ("tenant_id + app_id + revision_id +
// model_profile_version + backend_profile_version") and nothing more: an
// instruction or tool list that differs *because* a revision changed will
// land under a different revision_id, so it does not need its own component.
//
// Version is a number, not a name: two profiles that share a public_id but
// differ in content always differ in version, because bumping a profile
// inserts a new row rather than updating one. Keying on the name would let a
// rotated API key or a changed model name silently hit a stale plan.
type PlanKey struct {
	TenantID              string
	AppID                 int64
	RevisionID            int64
	ModelProfileVersion   uint32
	BackendProfileVersion uint32
}

// Validate rejects a key with any zero component before it can be used as a
// cache slot. Every part of this key is a real id, so a zero always means a
// caller forgot to fill something in — and a partially-filled key is exactly
// how one tenant's plan gets served to another.
func (k PlanKey) Validate() error {
	if k.TenantID == "" {
		return errors.New("agent: plan key needs a tenant id")
	}
	if k.AppID == 0 || k.RevisionID == 0 {
		return errors.New("agent: plan key needs a real app and revision id")
	}
	if k.ModelProfileVersion == 0 || k.BackendProfileVersion == 0 {
		return errors.New("agent: plan key needs resolved profile versions")
	}
	return nil
}

// Resolver builds the Runner for one resolved revision. It is a function
// rather than a method on Registry so a caller (a test, or the P2 worker that
// resolves a revision out of MySQL) can supply its own construction without
// replacing the cache.
type Resolver func(key PlanKey, t *tenant.Context, sess session.Service) (runner.Runner, error)

// planCache holds one Runner per PlanKey. It is a plain map, not an LRU: the
// number of live (tenant, app, revision) combinations is bounded by what an
// operator publishes, not by traffic, and evicting a plan that a session is
// still fixed to would recreate the exact "config drifts out from under an
// in-flight session" problem this cache exists to prevent.
type planCache struct {
	mu       sync.RWMutex
	byKey    map[PlanKey]runner.Runner
	resolver Resolver
	sess     session.Service
}

func newPlanCache(sess session.Service, resolver Resolver) *planCache {
	return &planCache{
		byKey:    make(map[PlanKey]runner.Runner),
		resolver: resolver,
		sess:     sess,
	}
}

// RunnerFor returns the cached plan for key, building it once through the
// resolver if this is the first request for that exact five-tuple.
func (c *planCache) RunnerFor(key PlanKey, t *tenant.Context) (runner.Runner, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	rr, ok := c.byKey[key]
	c.mu.RUnlock()
	if ok {
		return rr, nil
	}

	// Resolve outside the write lock: building a Runner touches no shared
	// state and can be slow; holding the write lock across it would make
	// every cache miss serialize on every other miss. Two concurrent misses
	// for the same key may each build a Runner, and the loser's copy is
	// discarded — wasteful, not wrong, and the common "cold start with N
	// simultaneous first messages" case this is built for.
	built, err := c.resolver(key, t, c.sess)
	if err != nil {
		return nil, fmt.Errorf("agent: resolve plan %s: %w", key, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.byKey[key]; ok {
		return existing, nil
	}
	c.byKey[key] = built
	return built, nil
}

// Evict drops cached plans for one app, across every revision. It is not
// called on publish — a session fixed to an old revision must keep using the
// plan it was created with — but a tenant being deleted or a leaked profile
// rotation needs a way to stop serving a no-longer-valid Runner.
func (c *planCache) Evict(tenantID string, appID int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for k := range c.byKey {
		if k.TenantID == tenantID && k.AppID == appID {
			delete(c.byKey, k)
			removed++
		}
	}
	return removed
}

// Len reports how many distinct plans are cached; used by tests and a future
// metric.
func (c *planCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.byKey)
}

func (k PlanKey) String() string {
	return fmt.Sprintf("tenant=%s/app=%d/revision=%d/model=%d/backend=%d",
		k.TenantID, k.AppID, k.RevisionID, k.ModelProfileVersion, k.BackendProfileVersion)
}
