package channels

import (
	"sync"
	"time"
)

// budgetSnapshot is one tenant's current spending within the reset window.
type budgetSnapshot struct {
	promptTokens uint64
	compTokens   uint64
	costCents    uint64
	calls        uint64
	resetAt      time.Time
}

// BudgetTracker tracks per-tenant spending against budget limits defined in
// tenant.Guardrails. The in-memory implementation is a lock-protected map;
// the same interface can wrap Redis when cross-replica coordination is needed.
// A nil *BudgetTracker is a valid no-op (budgeting disabled).
type BudgetTracker struct {
	mu sync.Mutex
	// key: tenantID
	m map[string]*budgetSnapshot
}

// NewBudgetTracker builds an in-memory budget tracker.
func NewBudgetTracker() *BudgetTracker {
	return &BudgetTracker{m: make(map[string]*budgetSnapshot)}
}

// Spend atomically checks and records token/cost/call usage for one tenant.
//
// The caller provides the spend (promptTokens, completionTokens) and the
// budget limits (maxPrompt, maxComp, maxCost, maxCalls) plus the reset
// interval. If any limit is exceeded after this spend the method returns
// false (the caller should reject the request). A nil receiver always
// returns true (budgeting disabled).
//
// The reset window is per-tenant and per‑call‑site: the first Spend after an
// elapsed timeout resets the counters, keeping the window aligned to wall
// clock rather than to the tenant's first call.
func (b *BudgetTracker) Spend(tenantID string,
	promptTokens, completionTokens int,
	costCents int64,
	maxPrompt, maxComp, maxCost, maxCalls uint64,
	resetInterval time.Duration,
) (ok bool) {
	if b == nil {
		return true
	}
	if maxPrompt == 0 && maxComp == 0 && maxCost == 0 && maxCalls == 0 {
		return true // no budget configured
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	bs, exists := b.m[tenantID]
	if !exists || (resetInterval > 0 && now.After(bs.resetAt)) {
		bs = &budgetSnapshot{resetAt: now.Add(resetInterval)}
		b.m[tenantID] = bs
	}

	// Check limits before committing.
	nextPrompt := bs.promptTokens + uint64(promptTokens)
	nextComp := bs.compTokens + uint64(completionTokens)
	nextCost := bs.costCents + uint64(costCents)
	nextCalls := bs.calls + 1

	if maxPrompt > 0 && nextPrompt >= maxPrompt {
		return false
	}
	if maxComp > 0 && nextComp >= maxComp {
		return false
	}
	if maxCost > 0 && nextCost >= maxCost {
		return false
	}
	if maxCalls > 0 && nextCalls >= maxCalls {
		return false
	}

	bs.promptTokens = nextPrompt
	bs.compTokens = nextComp
	bs.costCents = nextCost
	bs.calls = nextCalls
	return true
}

// Record unconditionally records actual usage after a model call (the
// pre‑flight Spend already booked the call count). Limits are not checked
// here — the caller is past the point of no return. A nil receiver is a
// no-op.
func (b *BudgetTracker) Record(tenantID string, promptTokens, completionTokens int, costCents int64) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bs, exists := b.m[tenantID]
	if !exists {
		return
	}
	bs.promptTokens += uint64(promptTokens)
	bs.compTokens += uint64(completionTokens)
	bs.costCents += uint64(costCents)
}
