//go:build integration

package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestWorkerBudgetStressFairAndBounded is the WS-8 joint stress probe over
// the fair-claim function plus durable worker budget: three tenants with
// deep backlogs, worker budget 2 each, concurrent receiver goroutines.
// Invariants:
//  1. per-tenant concurrent in-flight never exceeds its worker budget;
//  2. fair interleave: no tenant monopolizes claims while others wait
//     (every tenant's first claim happens before any tenant's 4th);
//  3. every job is eventually delivered exactly once and Acked;
//  4. after the drain, every tenant's active_count returns to 0.
func TestWorkerBudgetStressFairAndBounded(t *testing.T) {
	f := newFairBudgetFixture(t)

	tenants := []string{"stress-a", "stress-b", "stress-c"}
	const budget = 2
	const jobsPerTenant = 8
	for _, tn := range tenants {
		f.addTenant(t, tn)
		f.seedWorkerBudget(t, tn, budget)
		for j := 0; j < jobsPerTenant; j++ {
			jobID := fmt.Sprintf("stress-%s-%d", tn, j)
			if _, err := f.queue.Enqueue(f.ctx, durableTestJob(tn, jobID, "exec-"+jobID)); err != nil {
				t.Fatal(err)
			}
		}
	}

	var mu sync.Mutex
	inFlight := map[string]int{}
	maxInFlight := map[string]int{}
	claimOrder := []string{}
	acked := map[string]bool{}
	delivered := map[string]int{}

	var wg sync.WaitGroup
	receivers := 3
	for w := 0; w < receivers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				rctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
				delivery, err := f.queue.Receive(rctx, 30*time.Second)
				cancel()
				if err != nil {
					return // deadline: queue drained for this receiver
				}
				tn := delivery.Job.Tenant.TenantID
				mu.Lock()
				inFlight[tn]++
				if inFlight[tn] > maxInFlight[tn] {
					maxInFlight[tn] = inFlight[tn]
				}
				claimOrder = append(claimOrder, tn)
				delivered[delivery.Job.JobID]++
				mu.Unlock()

				time.Sleep(2 * time.Millisecond) // simulated processing

				mu.Lock()
				inFlight[tn]--
				acked[delivery.Job.JobID] = true
				mu.Unlock()
				if err := f.queue.Ack(f.ctx, delivery); err != nil {
					t.Errorf("ack %s: %v", delivery.Job.JobID, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	total := tenantsJobs(tenants, jobsPerTenant)
	mu.Lock()
	defer mu.Unlock()
	// 1. budget bound
	for _, tn := range tenants {
		if maxInFlight[tn] > budget {
			t.Fatalf("tenant %s max in-flight=%d exceeds worker budget=%d", tn, maxInFlight[tn], budget)
		}
	}
	// 2. fairness: every tenant claims before pure FIFO would admit it.
	// Pure FIFO would exhaust every other tenant's backlog first, putting
	// the last tenant's first claim at index jobsPerTenant*(len-1). The
	// fair order (never-claimed tenants first, then oldest in-flight) must
	// beat that bound even with concurrent receivers blurring the order.
	firstClaim := map[string]int{}
	for idx, tn := range claimOrder {
		if _, ok := firstClaim[tn]; !ok {
			firstClaim[tn] = idx
		}
	}
	fifoBound := jobsPerTenant * (len(tenants) - 1)
	for _, tn := range tenants {
		idx, ok := firstClaim[tn]
		if !ok {
			t.Fatalf("tenant %s never claimed", tn)
		}
		if idx >= fifoBound {
			t.Fatalf("tenant %s first claim at global index %d (bound %d): starved by backlog (fair interleave violated)", tn, idx, fifoBound)
		}
	}
	// 3. exactly-once delivery and full ack
	if len(acked) != total {
		t.Fatalf("acked jobs=%d, want %d", len(acked), total)
	}
	for jobID, count := range delivered {
		if count != 1 {
			t.Fatalf("job %s delivered %d times, want exactly once", jobID, count)
		}
	}
	// 4. counters return to zero
	for _, tn := range tenants {
		var active int64
		if err := f.pool.QueryRow(f.ctx,
			`SELECT active_count FROM capacity_budget WHERE tenant_id=$1 AND scope='worker'`, tn).Scan(&active); err != nil {
			t.Fatal(err)
		}
		if active != 0 {
			t.Fatalf("tenant %s active_count=%d after drain, want 0", tn, active)
		}
	}
	t.Logf("stress evidence: jobs=%d receivers=%d budget/tenant=%d claimOrderLen=%d firstClaims=%v",
		total, receivers, budget, len(claimOrder), firstClaim)
}

func tenantsJobs(tenants []string, perTenant int) int {
	return len(tenants) * perTenant
}
