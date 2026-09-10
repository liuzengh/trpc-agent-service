// Package relay publishes committed execution outbox rows to worker streams.
package relay

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

const (
	defaultLease      = 30 * time.Second
	defaultPoll       = time.Second
	relayRetryInitial = 250 * time.Millisecond
	relayRetryMax     = 30 * time.Second
)

// Store leases committed outbox rows and records relay outcomes.
type Store interface {
	ClaimDispatches(context.Context, string, time.Duration, int) ([]queue.Dispatch, error)
	CompleteDispatch(context.Context, queue.Dispatch, string) error
	RetryDispatch(context.Context, queue.Dispatch, string, error) error
	RecoverDispatches(context.Context, time.Duration) error
}

// Relay publishes PostgreSQL outbox rows to the shared worker stream. Duplicate
// stream entries are safe because workers condition claims on execution state.
type Relay struct {
	store     Store
	publisher queue.Publisher
	owner     string
	lease     time.Duration
	poll      time.Duration
}

// New creates a transactional-outbox relay.
func New(store Store, publisher queue.Publisher, owner string) (*Relay, error) {
	if store == nil {
		return nil, errors.New("dispatch store is required")
	}
	if publisher == nil {
		return nil, errors.New("dispatch publisher is required")
	}
	if owner == "" {
		return nil, errors.New("dispatcher owner is required")
	}
	return &Relay{store: store, publisher: publisher, owner: owner, lease: defaultLease, poll: defaultPoll}, nil
}

// Run continuously recovers stale rows and publishes ready outbox entries.
// PostgreSQL and Redis faults use bounded exponential backoff with jitter so
// one dependency outage cannot turn this loop into a restart or busy-loop.
func (r *Relay) Run(ctx context.Context) error {
	if r == nil || r.store == nil || r.publisher == nil {
		return errors.New("relay is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 0; ; {
		items, err := r.runPass(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("dispatch relay pass failed: %s", platformlog.SafeError(err))
			if err := waitForRelay(ctx, relayRetryDelay(attempt)); err != nil {
				return err
			}
			attempt++
			continue
		}
		attempt = 0
		if len(items) != 0 {
			continue
		}
		if err := waitForRelay(ctx, r.poll); err != nil {
			return err
		}
	}
}

func (r *Relay) runPass(ctx context.Context) ([]queue.Dispatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.store.RecoverDispatches(ctx, r.lease); err != nil {
		return nil, fmt.Errorf("recover dispatches: %w", err)
	}
	items, err := r.store.ClaimDispatches(ctx, r.owner, r.lease, 0)
	if err != nil {
		return nil, fmt.Errorf("claim dispatches: %w", err)
	}
	for _, item := range items {
		if err := r.publisher.Publish(ctx, item); err != nil {
			// A canceled relay must not keep a database operation alive. If the
			// retry transition cannot be persisted, its expiring outbox lease is
			// recovered by a later relay pass.
			if retryErr := r.store.RetryDispatch(ctx, item, r.owner, err); retryErr != nil {
				return nil, fmt.Errorf("retry dispatch: %w", retryErr)
			}
			continue
		}
		if err := r.store.CompleteDispatch(ctx, item, r.owner); err != nil {
			return nil, fmt.Errorf("complete dispatch: %w", err)
		}
	}
	return items, nil
}

func relayRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := relayRetryInitial
	for attempt > 0 && delay < relayRetryMax {
		delay *= 2
		attempt--
	}
	if delay > relayRetryMax {
		delay = relayRetryMax
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func waitForRelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		delay = defaultPoll
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
