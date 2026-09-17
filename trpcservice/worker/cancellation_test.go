package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestCancelHintHubBroadcastsAndUnsubscribes(t *testing.T) {
	hub := &CancelHintHub{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := hub.SubscribeCancellation(ctx)
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	second := hub.SubscribeCancellation(secondCtx)
	hub.PublishCancellation(gateway.ExecutionKey{TenantID: "tenant", RequestID: "request"})
	for i, ch := range []<-chan gateway.ExecutionKey{first, second} {
		select {
		case key := <-ch:
			if key.TenantID != "tenant" || key.RequestID != "request" {
				t.Fatalf("subscriber %d key=%#v", i, key)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d did not receive hint", i)
		}
	}
	cancel()
	select {
	case _, ok := <-first:
		if ok {
			t.Fatal("subscription remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription did not close")
	}
}

type cancellationStatusStub struct{}

func (cancellationStatusStub) GetExecution(_ context.Context, key gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	return gateway.ExecutionStatus{Envelope: runtime.ExecutionEnvelope{TenantID: key.TenantID, RequestID: key.RequestID}}, nil
}

func TestWatchCancellationAcceptsAccelerationHint(t *testing.T) {
	hub := &CancelHintHub{}
	c := Consumer{Statuses: cancellationStatusStub{}, CancelHints: hub, CancelPollInterval: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requested := &atomic.Bool{}
	done := make(chan error, 1)
	key := runtime.ExecutionEnvelope{TenantID: "tenant", RequestID: "request"}
	go c.watchCancellation(ctx, key, requested, cancel, done)
	deadline := time.Now().Add(time.Second)
	subscribed := false
	for time.Now().Before(deadline) {
		hub.mu.Lock()
		subscribed = len(hub.subscribers) > 0
		hub.mu.Unlock()
		if subscribed {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !subscribed {
		t.Fatal("watcher did not subscribe")
	}
	hub.PublishCancellation(gateway.ExecutionKey{TenantID: key.TenantID, RequestID: key.RequestID})
	if err := <-done; err != nil || !requested.Load() {
		t.Fatalf("done=%v requested=%t", err, requested.Load())
	}
}
