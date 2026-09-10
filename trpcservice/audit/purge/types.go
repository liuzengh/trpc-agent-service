// Package purge drives the guarded destruction of expired compliance audit
// facts through durable, approved batches with a destruction certificate.
package purge

import (
	"context"
	"errors"
	"time"
)

// State is the durable batch state machine. Transitions are mirrored by the
// compliance.audit_purge_batch guard trigger.
type State string

const (
	StatePlanned     State = "planned"
	StateApproved    State = "approved"
	StateExecuting   State = "executing"
	StateCompleted   State = "completed"
	StateFailed      State = "failed"
	StateQuarantined State = "quarantined"
)

var (
	ErrNotAuthorized = errors.New("purge principal is not authorized")
	ErrDryRun        = errors.New("purge is in dry-run mode")
	ErrNotApproved   = errors.New("purge batch is not approved")
)

// TransitionOK mirrors the SQL guard's legal transition set.
func TransitionOK(from, to State) bool {
	if from == to {
		switch from {
		case StatePlanned, StateApproved, StateExecuting, StateFailed:
			return true
		}
		return false
	}
	switch from {
	case StatePlanned:
		return to == StateApproved
	case StateApproved:
		return to == StateExecuting
	case StateExecuting:
		return to == StateCompleted || to == StateFailed || to == StateQuarantined
	case StateFailed:
		return to == StateExecuting || to == StateQuarantined
	}
	return false
}

// Batch is a durable purge batch row.
type Batch struct {
	TenantID      string
	BatchID       string
	State         State
	CutoffAt      time.Time
	Class         string
	PlannedCount  int64
	PlannedDigest string
	DeletedCount  int64
	AlertCount    int64
	DeleteAttempt int
	LastError     string
	PolicyVersion int64
	FloorVersion  int64
	ClaimOwner    string
	ClaimUntil    time.Time
	NotBefore     time.Time
	TTLUntil      time.Time
}

// Candidate is a tenant/class pair that may hold expired audit facts.
type Candidate struct {
	TenantID string
	Class    string
}

// PlanInput is the durable intent to purge one tenant/class.
type PlanInput struct {
	TenantID     string
	Class        string
	CutoffAt     time.Time
	Actor        string
	Reason       string
	TTL          time.Duration
	MaxBatchSize int64
	Now          time.Time
}

// Store is the purge orchestration surface.
type Store interface {
	EffectiveRetention(context.Context, string, string) (time.Duration, error)
	DueCandidates(context.Context, time.Time) ([]Candidate, error)
	ActiveBatches(context.Context) ([]Batch, error)
	Plan(context.Context, PlanInput) (string, error)
	Approve(context.Context, string, string, string, string) error
	Execute(context.Context, string, string, string) error
	Quarantine(context.Context, string, string, string, string) error
	Get(context.Context, string, string) (Batch, error)
	Gauge(context.Context, time.Time) (Gauge, error)
}

// Gauge reports point-in-time retention observability.
type Gauge struct {
	OverdueTenants int
	LegalHolds     int
}

// Stats summarizes one reconciliation pass.
type Stats struct {
	Planned        int
	Approved       int
	Executed       int
	Skipped        int
	Claimed        int
	Failed         int
	Quarantined    int
	Deleted        int64
	OverdueTenants int
	LegalHolds     int
}

// Reconciler plans and executes approved purge batches with dry-run and
// approval gates, retry and terminal quarantine. It reads durable batch state
// as the source of truth instead of classifying transient errors.
type Reconciler struct {
	Store           Store
	Owner           string
	DryRun          bool
	RequireApproval bool
	MaxAttempts     int
	MaxBatchSize    int64
	Now             func() time.Time
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
		activeKeys[batch.TenantID+"\x00"+batch.Class] = struct{}{}
		stats = r.processBatch(ctx, batch, stats, now)
	}
	candidates, err := r.Store.DueCandidates(ctx, now.UTC())
	if err != nil {
		return stats, err
	}
	for _, candidate := range candidates {
		if _, exists := activeKeys[candidate.TenantID+"\x00"+candidate.Class]; exists {
			continue
		}
		retention, err := r.Store.EffectiveRetention(ctx, candidate.TenantID, candidate.Class)
		if err != nil {
			return stats, err
		}
		cutoff := now.Add(-retention)
		batchID, err := r.Store.Plan(ctx, PlanInput{TenantID: candidate.TenantID, Class: candidate.Class,
			CutoffAt: cutoff.UTC(), Actor: r.Owner, Reason: "scheduled", TTL: 24 * time.Hour,
			MaxBatchSize: r.MaxBatchSize, Now: now.UTC()})
		if err != nil {
			return stats, err
		}
		stats.Planned++
		batch, err := r.Store.Get(ctx, candidate.TenantID, batchID)
		if err != nil {
			return stats, err
		}
		stats = r.processBatch(ctx, batch, stats, now)
	}
	if gauge, err := r.Store.Gauge(ctx, now.UTC()); err == nil {
		stats.OverdueTenants = gauge.OverdueTenants
		stats.LegalHolds = gauge.LegalHolds
	}
	return stats, nil
}

func (r Reconciler) processBatch(ctx context.Context, batch Batch, stats Stats, now time.Time) Stats {
	if batch.PlannedCount == 0 {
		return stats
	}
	switch batch.State {
	case StateCompleted, StateQuarantined:
		return stats
	case StatePlanned:
		if r.RequireApproval {
			return stats
		}
		if err := r.Store.Approve(ctx, batch.TenantID, batch.BatchID, r.Owner, "auto-approved"); err != nil {
			stats.Failed++
			return stats
		}
		stats.Approved++
	case StateFailed:
		if batch.DeleteAttempt >= r.MaxAttempts {
			if err := r.Store.Quarantine(ctx, batch.TenantID, batch.BatchID, r.Owner, batch.LastError); err != nil {
				stats.Failed++
				return stats
			}
			stats.Quarantined++
			return stats
		}
		if !batch.NotBefore.IsZero() && batch.NotBefore.After(now) {
			return stats
		}
	case StateApproved, StateExecuting:
	}
	return r.execute(ctx, batch.TenantID, batch.BatchID, stats)
}

func (r Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
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
	if err := r.Store.Execute(ctx, tenantID, batchID, r.Owner); err != nil {
		stats.Failed++
		return stats
	}
	completed, err := r.Store.Get(ctx, tenantID, batchID)
	if err != nil {
		stats.Failed++
		return stats
	}
	if completed.State == StateCompleted {
		stats.Deleted += completed.DeletedCount
		stats.Executed++
	} else if completed.State == StateExecuting {
		stats.Claimed++
	} else {
		stats.Skipped++
	}
	return stats
}
