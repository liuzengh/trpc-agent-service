// Package admission implements the P2-03 process-local inbound admission
// gate: a bounded slot budget with global, per-tenant and per-binding
// scopes. It protects the serving process from overload by rejecting new
// webhook work before any dedup claim, durable enqueue or downstream
// execution happens.
//
// Contract:
//   - acquire order is always global -> tenant -> binding; release is the
//     exact reverse. All paths use the same order, so no acquisition
//     ordering deadlock is possible, and each acquire either takes all
//     three scopes or none (fast reject, no partial holds).
//   - Acquire never blocks: when any scope is exhausted it fails fast with
//     ErrCapacityExhausted, which the ingress maps to a bounded 429. There
//     is no unbounded queue, no unbounded channel and no
//     goroutine-per-request waiting.
//   - Release is idempotent and panic-safe: the ingress holds the returned
//     ReleaseFunc in a defer, so context cancellation, handler error,
//     panic recovery, enqueue failure, draining and fast-ACK all release
//     exactly once.
//   - Keys come only from the server-resolved TenantContext (tenant id and
//     channel-binding id). Caller-supplied values are never used.
//   - The per-tenant map is bounded by construction: an active tenant entry
//     always holds at least one global slot, so entries can never exceed
//     the global bound; entries are deleted when their counts reach zero.
//   - Active counts can never go negative or above the configured bound;
//     high-water marks are tracked for bounded capacity observation.
//
// The gate is process-local evidence only. Cross-process queue depth, global
// tenant fairness and production capacity limits stay NOT PROVEN.
package admission

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrCapacityExhausted reports that a configured scope is full. The ingress
// maps it to a stable bounded 429 (capacity_exhausted), never to a claim.
var ErrCapacityExhausted = errors.New("admission: capacity exhausted")

// ErrContextDone reports that the request context was already cancelled or
// expired when admission was attempted.
var ErrContextDone = errors.New("admission: context done")

// ReleaseFunc releases one acquisition exactly once. It is safe to call
// multiple times, after panics, after cancellation and after draining.
type ReleaseFunc func()

// MaxSlots bounds every configured scope. Values above it fail closed.
const MaxSlots = 100000

// Gate is the bounded admission budget. All state is guarded by one mutex;
// acquire and release are O(1).
type Gate struct {
	globalLimit      int
	perTenantLimit   int
	perBindingLimit  int
	mu               sync.Mutex
	globalActive     int
	tenants          map[string]*tenantSlots
	highWaterGlobal  int
	highWaterTenant  int
	highWaterBinding int
}

type tenantSlots struct {
	active   int
	bindings map[string]int
}

// New validates the budget. per-tenant must not exceed global and
// per-binding must not exceed per-tenant, so one tenant or one binding can
// never occupy the whole process budget when other tenants exist.
func New(global, perTenant, perBinding int) (*Gate, error) {
	if global < 1 || perTenant < 1 || perBinding < 1 {
		return nil, fmt.Errorf("%w: admission limits must be positive", ErrCapacityExhausted)
	}
	if global > MaxSlots || perTenant > MaxSlots || perBinding > MaxSlots {
		return nil, fmt.Errorf("%w: admission limits above the supported bound", ErrCapacityExhausted)
	}
	if perTenant > global {
		return nil, fmt.Errorf("%w: per-tenant limit exceeds the global limit", ErrCapacityExhausted)
	}
	if perBinding > perTenant {
		return nil, fmt.Errorf("%w: per-binding limit exceeds the per-tenant limit", ErrCapacityExhausted)
	}
	return &Gate{
		globalLimit:     global,
		perTenantLimit:  perTenant,
		perBindingLimit: perBinding,
		tenants:         make(map[string]*tenantSlots),
	}, nil
}

// Acquire takes one slot in each scope (global -> tenant -> binding) or
// fails fast without holding anything. A non-nil context is honoured before
// acquisition; because Acquire never waits, no later cancellation can leave
// a slot held by a gone request: the caller releases via defer on every
// return path.
func (g *Gate) Acquire(ctx context.Context, tenantID, bindingID string) (ReleaseFunc, error) {
	if g == nil {
		return nil, ErrCapacityExhausted
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ErrContextDone
	}
	if tenantID == "" || bindingID == "" {
		return nil, ErrCapacityExhausted
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.globalActive >= g.globalLimit {
		return nil, ErrCapacityExhausted
	}
	slots, ok := g.tenants[tenantID]
	if !ok {
		slots = &tenantSlots{bindings: make(map[string]int)}
		g.tenants[tenantID] = slots
	}
	if slots.active >= g.perTenantLimit {
		g.gcTenantLocked(tenantID, slots)
		return nil, ErrCapacityExhausted
	}
	if slots.bindings[bindingID] >= g.perBindingLimit {
		g.gcTenantLocked(tenantID, slots)
		return nil, ErrCapacityExhausted
	}
	// Committed: all three scopes move together.
	g.globalActive++
	slots.active++
	slots.bindings[bindingID]++
	if g.globalActive > g.highWaterGlobal {
		g.highWaterGlobal = g.globalActive
	}
	if slots.active > g.highWaterTenant {
		g.highWaterTenant = slots.active
	}
	if slots.bindings[bindingID] > g.highWaterBinding {
		g.highWaterBinding = slots.bindings[bindingID]
	}
	released := false
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if released {
			return
		}
		released = true
		g.globalActive--
		current, ok := g.tenants[tenantID]
		if !ok {
			// Unreachable by construction; never go negative regardless.
			if g.globalActive < 0 {
				g.globalActive = 0
			}
			return
		}
		current.active--
		current.bindings[bindingID]--
		if current.bindings[bindingID] <= 0 {
			delete(current.bindings, bindingID)
		}
		if current.active <= 0 {
			if len(current.bindings) == 0 {
				delete(g.tenants, tenantID)
			} else {
				current.active = 0
			}
		}
	}, nil
}

// gcTenantLocked removes an empty tenant entry created by a rejected
// acquire so rejected bursts do not leave map residue.
func (g *Gate) gcTenantLocked(tenantID string, slots *tenantSlots) {
	if slots.active <= 0 && len(slots.bindings) == 0 {
		delete(g.tenants, tenantID)
	}
}

// ActiveGlobal reports the current global active count (diagnostics).
func (g *Gate) ActiveGlobal() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.globalActive
}

// ActiveTenant reports the current active count of one tenant.
func (g *Gate) ActiveTenant(tenantID string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if slots, ok := g.tenants[tenantID]; ok {
		return slots.active
	}
	return 0
}

// ActiveBinding reports the current active count of one binding inside a
// tenant.
func (g *Gate) ActiveBinding(tenantID, bindingID string) int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if slots, ok := g.tenants[tenantID]; ok {
		return slots.bindings[bindingID]
	}
	return 0
}

// HighWater reports the observed maximum active counts per scope. These are
// bounded-intensity diagnostics for the capacity drill, not production
// telemetry labels.
func (g *Gate) HighWater() (global, tenant, binding int) {
	if g == nil {
		return 0, 0, 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.highWaterGlobal, g.highWaterTenant, g.highWaterBinding
}

// Limits reports the configured bounds (diagnostics).
func (g *Gate) Limits() (global, perTenant, perBinding int) {
	if g == nil {
		return 0, 0, 0
	}
	return g.globalLimit, g.perTenantLimit, g.perBindingLimit
}

// TenantCount reports the number of tenants with active slots (bounded by
// the global limit by construction).
func (g *Gate) TenantCount() int {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.tenants)
}
