package relay

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

// ReconciliationHandler must call the same typed transition used by the
// normal path. It must not update Session or Outbox tables directly.
type ReconciliationHandler interface {
	Reconcile(context.Context, messaging.ReconciliationIssue) error
}

type reconciliationIdentity struct {
	Kind                         messaging.ReconciliationIssueKind
	TenantID, AggregateID, RefID string
}

type Reconciler struct {
	Store   messaging.ReconciliationStore
	Handler ReconciliationHandler
	// Now makes the stale-watermark query deterministic in tests and avoids
	// coupling the reconciliation contract to the process clock.
	Now          func() time.Time
	StuckAfter   time.Duration
	BatchSize    int
	PollInterval time.Duration
}

func (r Reconciler) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	interval := r.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		_, _ = r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r Reconciler) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	stuckAfter := r.StuckAfter
	if stuckAfter <= 0 {
		stuckAfter = time.Minute
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = 100
	}
	issues, err := r.Store.FindReconciliationIssues(ctx, r.now().Add(-stuckAfter), limit)
	if err != nil {
		return 0, err
	}
	latest := make(map[reconciliationIdentity]messaging.ReconciliationIssue, len(issues))
	order := make([]reconciliationIdentity, 0, len(issues))
	for _, issue := range issues {
		key := reconciliationIdentity{Kind: issue.Kind, TenantID: issue.TenantID, AggregateID: issue.AggregateID, RefID: issue.RefID}
		current, exists := latest[key]
		if !exists {
			order = append(order, key)
		}
		// A reconciliation issue describes the current durable record. If one
		// page contains a replay or an older out-of-order watermark, dispatch
		// only its newest version to the typed transition handler.
		if !exists || issue.Version > current.Version {
			latest[key] = issue
		}
	}
	handled := 0
	for _, key := range order {
		issue := latest[key]
		if err := r.Handler.Reconcile(ctx, issue); err != nil {
			return handled, err
		}
		handled++
	}
	return handled, nil
}

func (r Reconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r Reconciler) validate() error {
	if r.Store == nil || r.Handler == nil {
		return runtime.ErrInvariantViolation
	}
	return nil
}
