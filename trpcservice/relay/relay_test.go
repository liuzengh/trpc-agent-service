package relay

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

func TestRelayRetriesBackendFailureWithoutReturning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &retryingStore{cancel: cancel}
	relay, err := New(store, publisherFunc(func(context.Context, queue.Dispatch) error { return nil }), "relay-1")
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	if err := relay.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.recoverCalls.Load() < 2 {
		t.Fatalf("recover calls=%d, want retry after temporary backend failure", store.recoverCalls.Load())
	}
}

func TestRelayRetryDelayUsesBoundedExponentialJitter(t *testing.T) {
	for attempt, base := range map[int]time.Duration{
		0: relayRetryInitial,
		1: 2 * relayRetryInitial,
		2: 4 * relayRetryInitial,
		7: relayRetryMax,
	} {
		for range 10 {
			delay := relayRetryDelay(attempt)
			if delay < base/2 || delay > base {
				t.Fatalf("attempt=%d delay=%s, want [%s,%s]", attempt, delay, base/2, base)
			}
		}
	}
}

func TestRelayDoesNotDetachRetryFromCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dispatch := queue.Dispatch{OutboxID: 1, TenantID: "tenant-a", AppID: "support", RequestID: "request-1"}
	store := &canceledRetryStore{dispatch: dispatch}
	relay, err := New(store, publisherFunc(func(context.Context, queue.Dispatch) error {
		cancel()
		return errors.New("redis unavailable")
	}), "relay-1")
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	if _, err := relay.runPass(ctx); err == nil {
		t.Fatal("run pass succeeded after retry transition was canceled")
	}
	if !store.retrySawCanceled {
		t.Fatal("retry dispatch was detached from relay cancellation")
	}
}

type retryingStore struct {
	recoverCalls atomic.Int32
	cancel       context.CancelFunc
}

func (s *retryingStore) RecoverDispatches(context.Context, time.Duration) error {
	if s.recoverCalls.Add(1) == 1 {
		return errors.New("postgres temporarily unavailable")
	}
	s.cancel()
	return nil
}
func (*retryingStore) ClaimDispatches(context.Context, string, time.Duration, int) ([]queue.Dispatch, error) {
	return nil, nil
}
func (*retryingStore) CompleteDispatch(context.Context, queue.Dispatch, string) error { return nil }
func (*retryingStore) RetryDispatch(context.Context, queue.Dispatch, string, error) error {
	return nil
}

type canceledRetryStore struct {
	dispatch         queue.Dispatch
	retrySawCanceled bool
}

func (*canceledRetryStore) RecoverDispatches(context.Context, time.Duration) error { return nil }
func (s *canceledRetryStore) ClaimDispatches(context.Context, string, time.Duration, int) ([]queue.Dispatch, error) {
	return []queue.Dispatch{s.dispatch}, nil
}
func (*canceledRetryStore) CompleteDispatch(context.Context, queue.Dispatch, string) error {
	return nil
}
func (s *canceledRetryStore) RetryDispatch(ctx context.Context, _ queue.Dispatch, _ string, _ error) error {
	s.retrySawCanceled = errors.Is(ctx.Err(), context.Canceled)
	return ctx.Err()
}

type publisherFunc func(context.Context, queue.Dispatch) error

func (f publisherFunc) Publish(ctx context.Context, dispatch queue.Dispatch) error {
	return f(ctx, dispatch)
}
