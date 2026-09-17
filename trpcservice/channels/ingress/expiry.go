package ingress

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// CandidateExpiryReconciler bounds candidate retention. Expired verified
// candidates are burned as well: a process crash between verification and
// promotion must not leave a durable, reusable-looking capability behind.
type CandidateExpiryReconciler struct {
	Store        Store
	Now          func() time.Time
	BatchSize    int
	PollInterval time.Duration
	OnError      func(context.Context, error)
}

func (r CandidateExpiryReconciler) Run(ctx context.Context) error {
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
		if _, err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			if r.OnError == nil {
				return err
			}
			r.OnError(ctx, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r CandidateExpiryReconciler) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	now := r.now()
	if now.IsZero() {
		return 0, runtime.ErrInvariantViolation
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = 100
	}
	return r.Store.BurnExpiredCandidates(ctx, now, limit)
}

func (r CandidateExpiryReconciler) validate() error {
	if r.Store == nil {
		return runtime.ErrInvariantViolation
	}
	return nil
}

func (r CandidateExpiryReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}
