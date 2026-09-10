// Package purgebusiness drives the guarded retention of expired business-
// library audit facts and their exported Outbox rows, gated by the relay
// watermark so nothing is destroyed before the compliance copy exists.
package purgebusiness

import (
	"context"
	"errors"
	"time"
)

// State is the durable batch state machine, mirrored by the
// business_audit_purge_batch guard trigger.
type State string

const (
	StatePlanned     State = "planned"
	StateExecuting   State = "executing"
	StateCompleted   State = "completed"
	StateFailed      State = "failed"
	StateQuarantined State = "quarantined"
)

var (
	ErrNotAuthorized = errors.New("business audit purge principal is not authorized")
	ErrDryRun        = errors.New("business audit purge is in dry-run mode")
)

// TransitionOK mirrors the SQL guard's legal transition set.
func TransitionOK(from, to State) bool {
	if from == to {
		switch from {
		case StatePlanned, StateExecuting, StateFailed:
			return true
		}
		return false
	}
	switch from {
	case StatePlanned:
		return to == StateExecuting || to == StateFailed
	case StateExecuting:
		return to == StateCompleted || to == StateFailed || to == StateQuarantined
	case StateFailed:
		return to == StateExecuting || to == StateQuarantined
	}
	return false
}

// Batch is a durable retention batch row. WatermarkAt is zero when every
// audit Outbox row was already exported; SafeCutoffAt is the effective
// deletion window min(cutoff, watermark).
type Batch struct {
	TenantID      string
	BatchID       string
	State         State
	CutoffAt      time.Time
	WatermarkAt   time.Time
	SafeCutoffAt  time.Time
	PlannedEvents int64
	PlannedOutbox int64
	PlannedDigest string
	DeletedEvents int64
	DeletedOutbox int64
	DeleteAttempt int
	LastError     string
	ClaimOwner    string
	ClaimUntil    time.Time
	NotBefore     time.Time
	CreatedAt     time.Time
	Version       int64
}

// PlanInput is the durable intent to purge one tenant's expired audit facts.
type PlanInput struct {
	TenantID string
	CutoffAt time.Time
	Actor    string
	Reason   string
	Now      time.Time
}

// Store is the retention orchestration surface. Every implementation must
// enforce the same watermark gate: un-exported Outbox rows are never deleted
// and any candidate drift from the plan fails closed.
type Store interface {
	Plan(context.Context, PlanInput) (string, error)
	Execute(context.Context, string, string, string, int64) (string, error)
	Quarantine(context.Context, string, string, string) error
	Get(context.Context, string, string) (Batch, error)
	ActiveBatches(context.Context) ([]Batch, error)
	// DueTenants returns tenants that still hold expired audit facts or
	// published audit Outbox rows older than cutoff.
	DueTenants(context.Context, time.Time) ([]string, error)
}

// Stats summarizes one reconciliation pass.
type Stats struct {
	Planned     int
	Executed    int
	Completed   int
	Skipped     int
	Claimed     int
	Failed      int
	Quarantined int
	Deleted     int64
}

// Reconciler plans and executes retention batches with dry-run and retry
// gates, reading durable batch state as the source of truth.
type Reconciler struct {
	Store        Store
	Owner        string
	Retention    time.Duration
	DryRun       bool
	MaxAttempts  int
	MaxBatchSize int64
	Now          func() time.Time
}

func (r Reconciler) ProcessOnce(ctx context.Context) (Stats, error) {
	var stats Stats
	now := r.now()
	active, err := r.Store.ActiveBatches(ctx)
	if err != nil {
		return stats, err
	}
	activeKeys := make(map[string]struct{}, len(active))
	for _, batch := range active {
		activeKeys[batch.TenantID] = struct{}{}
		stats = r.processBatch(ctx, batch, stats, now)
	}
	cutoff := now.Add(-r.retention()).UTC()
	tenants, err := r.Store.DueTenants(ctx, cutoff)
	if err != nil {
		return stats, err
	}
	for _, tenant := range tenants {
		if _, exists := activeKeys[tenant]; exists {
			continue
		}
		batchID, err := r.Store.Plan(ctx, PlanInput{TenantID: tenant, CutoffAt: cutoff,
			Actor: r.Owner, Reason: "scheduled", Now: now.UTC()})
		if err != nil {
			return stats, err
		}
		stats.Planned++
		batch, err := r.Store.Get(ctx, tenant, batchID)
		if err != nil {
			return stats, err
		}
		stats = r.processBatch(ctx, batch, stats, now)
	}
	return stats, nil
}

func (r Reconciler) processBatch(ctx context.Context, batch Batch, stats Stats, now time.Time) Stats {
	switch batch.State {
	case StateCompleted, StateQuarantined:
		return stats
	case StateFailed:
		if batch.DeleteAttempt >= r.maxAttempts() {
			if err := r.Store.Quarantine(ctx, batch.TenantID, batch.BatchID, r.Owner); err != nil {
				stats.Failed++
			} else {
				stats.Quarantined++
			}
			return stats
		}
		if !batch.NotBefore.IsZero() && batch.NotBefore.After(now) {
			return stats
		}
	case StatePlanned, StateExecuting:
	}
	return r.execute(ctx, batch.TenantID, batch.BatchID, stats)
}

func (r Reconciler) execute(ctx context.Context, tenantID, batchID string, stats Stats) Stats {
	if r.DryRun {
		stats.Skipped++
		return stats
	}
	batch, err := r.Store.Get(ctx, tenantID, batchID)
	if err != nil {
		stats.Failed++
		return stats
	}
	if batch.State == StateExecuting && batch.ClaimOwner != "" && batch.ClaimOwner != r.Owner &&
		(batch.ClaimUntil.IsZero() || batch.ClaimUntil.After(r.now())) {
		stats.Claimed++
		return stats
	}
	result, err := r.Store.Execute(ctx, tenantID, batchID, r.Owner, r.maxBatchSize())
	if err != nil {
		stats.Failed++
		return stats
	}
	switch result {
	case "completed":
		completed, getErr := r.Store.Get(ctx, tenantID, batchID)
		if getErr != nil {
			stats.Failed++
			return stats
		}
		stats.Deleted += completed.DeletedEvents + completed.DeletedOutbox
		stats.Completed++
	case "claimed_by_another":
		stats.Claimed++
	case "watermark_drift", "divergence":
		stats.Failed++
	default:
		stats.Skipped++
	}
	return stats
}

func (r Reconciler) retention() time.Duration {
	if r.Retention > 0 {
		return r.Retention
	}
	return 180 * 24 * time.Hour
}

func (r Reconciler) maxAttempts() int {
	if r.MaxAttempts > 0 {
		return r.MaxAttempts
	}
	return 3
}

func (r Reconciler) maxBatchSize() int64 {
	if r.MaxBatchSize > 0 {
		return r.MaxBatchSize
	}
	return 1000
}

func (r Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
