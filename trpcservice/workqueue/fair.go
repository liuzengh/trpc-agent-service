package workqueue

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

type pollQueue interface {
	Queue
	TryReceive(context.Context) (Delivery, error)
}

// FairQueue composes the existing transport; it does not replace Redis lease,
// reclaim or ACK logic. Four foreground opportunities per one backlog opportunity,
// with immediate borrowing when either queue is empty, prevent backlog starvation.
type FairQueue struct {
	recent, backlog pollQueue
	turn            atomic.Uint64
}

// Reserve a Worker slot for recent traffic; the remaining slots share both lanes.
func (q *FairQueue) ForegroundQueue() Queue { return q.recent }

func (q *FairQueue) Publish(ctx context.Context, task AgentTask) error {
	old := !task.Lifetime.OccurredAt.IsZero() && time.Since(task.Lifetime.OccurredAt) > 2*time.Minute
	if task.Background || old {
		return q.backlog.Publish(ctx, task)
	}
	return q.recent.Publish(ctx, task)
}
func (q *FairQueue) Receive(ctx context.Context) (Delivery, error) {
	first, second := q.recent, q.backlog
	if q.turn.Add(1)%5 == 0 {
		first, second = second, first
	}
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		for _, source := range []pollQueue{first, second} {
			d, err := source.TryReceive(ctx)
			if !errors.Is(err, ErrNoMessage) {
				return d, err
			}
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-timer.C:
		}
	}
}
func (q *FairQueue) Ready(ctx context.Context) error {
	return errors.Join(q.recent.Ready(ctx), q.backlog.Ready(ctx))
}
func (q *FairQueue) Close() error { return errors.Join(q.recent.Close(), q.backlog.Close()) }
