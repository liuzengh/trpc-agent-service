//go:build integration

package capacity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIngressBudgetStressBoundedUnderLoad is the WS-8 joint stress probe for
// the durable admission budget: many concurrent workers contend for a small
// per-tenant budget. Invariants:
//  1. max concurrent active per tenant never exceeds the budget;
//  2. rejections are classified ErrCapacityFull (no other errors);
//  3. after all releases the stored active_count is exactly 0.
func TestIngressBudgetStressBoundedUnderLoad(t *testing.T) {
	pool, guard := newGuardFixture(t)

	tenants := []string{"stress-a", "stress-b", "stress-c", "stress-d"}
	const budget = 3
	for _, tn := range tenants {
		seedBudget(t, pool, tn, ScopeIngress, budget)
	}

	var full, otherErr, admitted atomic.Int64
	maxInFlight := make(map[string]*atomic.Int64, len(tenants))
	inFlight := make(map[string]*atomic.Int64, len(tenants))
	for _, tn := range tenants {
		maxInFlight[tn] = &atomic.Int64{}
		inFlight[tn] = &atomic.Int64{}
	}

	const workersPerTenant = 15
	const rounds = 8
	var wg sync.WaitGroup
	for _, tn := range tenants {
		for w := 0; w < workersPerTenant; w++ {
			wg.Add(1)
			go func(tenant string) {
				defer wg.Done()
				for r := 0; r < rounds; r++ {
					release, err := guard.AcquireScope(context.Background(), tenant, ScopeIngress, fmt.Sprintf("stress-%s", tenant), 30*time.Second)
					if err != nil {
						if errors.Is(err, ErrCapacityFull) {
							full.Add(1)
						} else {
							otherErr.Add(1)
						}
						continue
					}
					admitted.Add(1)
					cur := inFlight[tenant].Add(1)
					for {
						max := maxInFlight[tenant].Load()
						if cur <= max || maxInFlight[tenant].CompareAndSwap(max, cur) {
							break
						}
					}
					time.Sleep(2 * time.Millisecond) // hold the admitted slot
					inFlight[tenant].Add(-1)
					release()
				}
			}(tn)
		}
	}
	wg.Wait()

	for _, tn := range tenants {
		if got := maxInFlight[tn].Load(); got > budget {
			t.Fatalf("tenant %s max concurrent=%d exceeds budget=%d", tn, got, budget)
		}
	}
	if otherErr.Load() != 0 {
		t.Fatalf("non-capacity errors under load: %d", otherErr.Load())
	}
	if full.Load() == 0 {
		t.Fatal("budget never bound under load: no ErrCapacityFull observed")
	}
	for _, tn := range tenants {
		var active int64
		if err := pool.QueryRow(context.Background(),
			`SELECT active_count FROM capacity_budget WHERE tenant_id=$1 AND scope='ingress'`, tn).Scan(&active); err != nil {
			t.Fatal(err)
		}
		if active != 0 {
			t.Fatalf("tenant %s active_count=%d after stress, want 0", tn, active)
		}
	}
	t.Logf("stress evidence: admitted=%d rejected=%d budget=%d tenants=%d workers/tenant=%d rounds=%d",
		admitted.Load(), full.Load(), budget, len(tenants), workersPerTenant, rounds)
}
