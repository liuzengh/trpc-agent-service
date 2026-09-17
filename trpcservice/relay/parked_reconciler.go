package relay

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// ParkedInputReconciler repairs a lost Wakeup Stream event and enforces park
// deadlines even when no further session commit occurs.
type ParkedInputReconciler struct {
	// Activator is retained for compatibility. New callers should set Store,
	// Dispatch, and ShardCount directly so the repair scan is statically typed.
	Activator    WakeupActivator
	Store        gateway.ParkedInputStore
	Dispatch     broker.Broker
	ShardCount   uint32
	BatchSize    int
	PollInterval time.Duration
	OnBlocked    func(context.Context, gateway.ExecutionKey)
	OnError      func(context.Context, error)
}

func (r ParkedInputReconciler) Run(ctx context.Context) error {
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

func (r ParkedInputReconciler) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}
	limit := r.BatchSize
	if limit <= 0 {
		limit = 100
	}
	store, activator, err := r.components()
	if err != nil {
		return 0, err
	}
	keys, err := store.ListActionableParkedInputs(ctx, time.Now().UTC(), limit)
	if err != nil {
		return 0, err
	}
	handled := 0
	var joined error
	for _, key := range keys {
		disposition, err := activator.Activate(ctx, key)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if disposition == WakeupBlocked && r.OnBlocked != nil {
			r.OnBlocked(ctx, key)
		}
		handled++
	}
	return handled, joined
}

func (r ParkedInputReconciler) validate() error {
	_, _, err := r.components()
	return err
}

func (r ParkedInputReconciler) components() (gateway.ParkedInputStore, WakeupActivator, error) {
	if r.Store != nil || r.Dispatch != nil || r.ShardCount != 0 {
		if r.Store == nil || r.Dispatch == nil || r.ShardCount == 0 {
			return nil, WakeupActivator{}, runtime.ErrInvariantViolation
		}
		return r.Store, WakeupActivator{Store: r.Store, Dispatch: r.Dispatch, ShardCount: r.ShardCount}, nil
	}
	store, ok := r.Activator.Store.(gateway.ParkedInputStore)
	if !ok || r.Activator.Dispatch == nil || r.Activator.ShardCount == 0 {
		return nil, WakeupActivator{}, runtime.ErrInvariantViolation
	}
	return store, r.Activator, nil
}
