//go:build integration

package queue

import (
	"context"
	"testing"
	"time"
)

// fairBudgetFixture wraps the standard fixture with a worker-budget-aware
// queue (WorkerBudgetAccounting on) and budget seeding helpers.
type fairBudgetFixture struct {
	*postgresQueueFixture
	queue *PostgresQueue
}

func newFairBudgetFixture(t *testing.T) *fairBudgetFixture {
	t.Helper()
	base := newPostgresQueueFixture(t)
	q, err := NewPostgresQueue(base.pool, PostgresQueueConfig{
		MaxJobAge:              2 * time.Hour,
		MaxVisibilityExtension: 2 * time.Hour,
		PollInterval:           5 * time.Millisecond,
		WorkerBudgetAccounting: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	return &fairBudgetFixture{postgresQueueFixture: base, queue: q}
}

func (f *fairBudgetFixture) seedWorkerBudget(t *testing.T, tenantID string, limit int64) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `
		INSERT INTO capacity_budget (tenant_id, scope, budget_limit, active_count)
		VALUES ($1, 'worker', $2, 0)
		ON CONFLICT (tenant_id, scope) DO UPDATE SET budget_limit = $2, active_count = 0, enabled = true`,
		tenantID, limit); err != nil {
		t.Fatal(err)
	}
}

func (f *fairBudgetFixture) workerActiveCount(t *testing.T, tenantID string) int64 {
	t.Helper()
	var count int64
	if err := f.pool.QueryRow(f.ctx,
		`SELECT active_count FROM capacity_budget WHERE tenant_id = $1 AND scope = 'worker'`,
		tenantID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (f *fairBudgetFixture) backdateJob(t *testing.T, tenantID, jobID string, age time.Duration) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE job_queue SET available_at = clock_timestamp() - ($3::double precision * interval '1 microsecond') WHERE tenant_id = $1 AND job_id = $2`,
		tenantID, jobID, age.Microseconds()); err != nil {
		t.Fatal(err)
	}
}

// TestFairClaimPrefersStarvedTenant proves the fair interleave: a tenant
// with a deep backlog cannot starve a fresh tenant. Tenant A enqueues three
// jobs backdated minutes ago; tenant B enqueues one job now. The first claim
// goes to B (never claimed, NULLS FIRST), the remaining claims to A.
func TestFairClaimPrefersStarvedTenant(t *testing.T) {
	f := newFairBudgetFixture(t)
	f.addTenant(t, "fair-tenant-b")

	for i, id := range []string{"job-a1", "job-a2", "job-a3"} {
		if _, err := f.queue.Enqueue(f.ctx, durableTestJob("queue-tenant-a", id, "exec-"+id)); err != nil {
			t.Fatal(err)
		}
		f.backdateJob(t, "queue-tenant-a", id, time.Duration(3-i)*time.Minute)
	}
	if _, err := f.queue.Enqueue(f.ctx, durableTestJob("fair-tenant-b", "job-b1", "exec-b1")); err != nil {
		t.Fatal(err)
	}

	// Fair interleave: claim 1 goes to A's oldest job (both tenants are
	// unclaimed, so the tie breaks FIFO by age). After that claim, A has an
	// in-flight job with a fresh updated_at while B has never been claimed
	// (NULL) — the fair order must hand claim 2 to B. Pure FIFO would keep
	// feeding A's backdated backlog (A,A,A,B) and starve B.
	first, err := f.queue.Receive(f.ctx, 30*time.Second)
	if err != nil || first.DeliveryID == "" {
		t.Fatalf("first receive: empty=%v err=%v", first.DeliveryID == "", err)
	}
	if first.Job.Tenant.TenantID != "queue-tenant-a" || first.Job.JobID != "job-a1" {
		t.Fatalf("first claim %s/%s, want queue-tenant-a/job-a1 (oldest)", first.Job.Tenant.TenantID, first.Job.JobID)
	}
	second, err := f.queue.Receive(f.ctx, 30*time.Second)
	if err != nil || second.DeliveryID == "" {
		t.Fatalf("second receive: empty=%v err=%v", second.DeliveryID == "", err)
	}
	if second.Job.Tenant.TenantID != "fair-tenant-b" {
		t.Fatalf("second claim went to %s, want fair-tenant-b (starved-tenant preference over A's backlog)", second.Job.Tenant.TenantID)
	}
	third, err := f.queue.Receive(f.ctx, 30*time.Second)
	if err != nil || third.DeliveryID == "" {
		t.Fatalf("third receive: empty=%v err=%v", third.DeliveryID == "", err)
	}
	if third.Job.Tenant.TenantID != "queue-tenant-a" {
		t.Fatalf("third claim went to %s, want queue-tenant-a (B drained, A resumes FIFO)", third.Job.Tenant.TenantID)
	}
}

// TestWorkerBudgetGatesClaimAndReleasesOnAck proves the durable worker
// budget: claims stop at the limit, other tenants proceed, and the terminal
// path (Ack) releases the slot.
func TestWorkerBudgetGatesClaimAndReleasesOnAck(t *testing.T) {
	f := newFairBudgetFixture(t)
	f.addTenant(t, "budget-tenant-b")
	f.seedWorkerBudget(t, "queue-tenant-a", 1)

	if _, err := f.queue.Enqueue(f.ctx, durableTestJob("queue-tenant-a", "job-a1", "exec-a1")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.queue.Enqueue(f.ctx, durableTestJob("queue-tenant-a", "job-a2", "exec-a2")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.queue.Enqueue(f.ctx, durableTestJob("budget-tenant-b", "job-b1", "exec-b1")); err != nil {
		t.Fatal(err)
	}

	first, err := f.queue.Receive(f.ctx, 30*time.Second)
	if err != nil || first.DeliveryID == "" {
		t.Fatalf("receive 1: empty=%v err=%v", first.DeliveryID == "", err)
	}
	if first.Job.Tenant.TenantID != "queue-tenant-a" || f.workerActiveCount(t, "queue-tenant-a") != 1 {
		t.Fatalf("claim 1 tenant=%s active=%d", first.Job.Tenant.TenantID, f.workerActiveCount(t, "queue-tenant-a"))
	}

	// A is at its limit: the next claim must go to B.
	second, err := f.queue.Receive(f.ctx, 30*time.Second)
	if err != nil || second.DeliveryID == "" {
		t.Fatalf("receive 2: empty=%v err=%v", second.DeliveryID == "", err)
	}
	if second.Job.Tenant.TenantID != "budget-tenant-b" {
		t.Fatalf("claim 2 went to %s, want budget-tenant-b (tenant-a budget exhausted)", second.Job.Tenant.TenantID)
	}

	// B is unlimited and empty now; A is still budget-blocked: nothing to
	// claim. Receive polls until its context is done, so bound the context
	// and only reject a real delivery.
	emptyCtx, emptyCancel := context.WithTimeout(f.ctx, 150*time.Millisecond)
	defer emptyCancel()
	if extra, err := f.queue.Receive(emptyCtx, 30*time.Second); err == nil && extra.DeliveryID != "" {
		t.Fatalf("receive 3 delivered tenant=%s job=%s active_a=%d (want nothing: tenant-a over budget, b drained)",
			extra.Job.Tenant.TenantID, extra.Job.JobID, f.workerActiveCount(t, "queue-tenant-a"))
	}

	// Ack releases A's slot: a2 becomes claimable.
	if err := f.queue.Ack(f.ctx, first); err != nil {
		t.Fatal(err)
	}
	if got := f.workerActiveCount(t, "queue-tenant-a"); got != 0 {
		t.Fatalf("active after ack: %d, want 0", got)
	}
	third, err := f.queue.Receive(f.ctx, 30*time.Second)
	if err != nil || third.DeliveryID == "" {
		t.Fatalf("receive 4 after ack: empty=%v err=%v", third.DeliveryID == "", err)
	}
	if third.Job.JobID != "job-a2" {
		t.Fatalf("claim 4 job=%s, want job-a2", third.Job.JobID)
	}
	if got := f.workerActiveCount(t, "queue-tenant-a"); got != 1 {
		t.Fatalf("active after re-claim: %d, want 1", got)
	}
}

// TestWorkerBudgetRequeueReleasesSlot proves a requeue (retryable Nack)
// releases the claimed slot; the job re-acquires it on the next claim.
func TestWorkerBudgetRequeueReleasesSlot(t *testing.T) {
	f := newFairBudgetFixture(t)
	f.seedWorkerBudget(t, "queue-tenant-a", 1)

	if _, err := f.queue.Enqueue(f.ctx, durableTestJob("queue-tenant-a", "job-r1", "exec-r1")); err != nil {
		t.Fatal(err)
	}
	delivery, err := f.queue.Receive(f.ctx, 30*time.Second)
	if err != nil || delivery.DeliveryID == "" {
		t.Fatalf("receive: empty=%v err=%v", delivery.DeliveryID == "", err)
	}
	if err := f.queue.Nack(f.ctx, delivery, NackOptions{Requeue: true, RetryAfter: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if got := f.workerActiveCount(t, "queue-tenant-a"); got != 0 {
		t.Fatalf("active after requeue: %d, want 0", got)
	}
	// The requeued job is claimable again (budget re-acquired).
	reclaimed, err := f.queue.Receive(f.ctx, 30*time.Second)
	if err != nil || reclaimed.DeliveryID == "" {
		t.Fatalf("re-receive: empty=%v err=%v", reclaimed.DeliveryID == "", err)
	}
	if reclaimed.Job.JobID != "job-r1" {
		t.Fatalf("reclaimed job=%s, want job-r1", reclaimed.Job.JobID)
	}
	if got := f.workerActiveCount(t, "queue-tenant-a"); got != 1 {
		t.Fatalf("active after reclaim: %d, want 1", got)
	}
}

// TestWorkerBudgetAccountingDisabledKeepsLegacyBehavior proves the flag
// default: without WorkerBudgetAccounting, Ack/Nack never touch
// capacity_budget even when a budget row exists. (The claim side accounts
// inside migration 000015's function; only the terminal-path decrement is
// flag-gated.)
func TestWorkerBudgetAccountingDisabledKeepsLegacyBehavior(t *testing.T) {
	f := newFairBudgetFixture(t)
	f.seedWorkerBudget(t, "queue-tenant-a", 5)

	// The legacy queue from the base fixture has the flag OFF.
	legacy := f.postgresQueueFixture.queue
	if _, err := legacy.Enqueue(f.ctx, durableTestJob("queue-tenant-a", "job-legacy", "exec-legacy")); err != nil {
		t.Fatal(err)
	}
	delivery, err := legacy.Receive(f.ctx, 30*time.Second)
	if err != nil || delivery.DeliveryID == "" {
		t.Fatalf("receive: empty=%v err=%v", delivery.DeliveryID == "", err)
	}
	if got := f.workerActiveCount(t, "queue-tenant-a"); got != 1 {
		t.Fatalf("claim-side accounting missing: active=%d, want 1", got)
	}
	if err := legacy.Ack(f.ctx, delivery); err != nil {
		t.Fatal(err)
	}
	if got := f.workerActiveCount(t, "queue-tenant-a"); got != 1 {
		t.Fatalf("accounting disabled must not decrement: active=%d, want 1", got)
	}
	// Cleanup the held slot so the fixture teardown stays consistent.
	f.pool.Exec(f.ctx, `UPDATE capacity_budget SET active_count = 0 WHERE tenant_id = 'queue-tenant-a' AND scope = 'worker'`)
}
