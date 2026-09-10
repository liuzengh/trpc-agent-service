package worker

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type CancelHintSource interface {
	SubscribeCancellation(context.Context) <-chan gateway.ExecutionKey
}

// CancelHintHub fans out Redis/relay acceleration hints to all local workers.
// Hints are lossy by design; the Consumer's authoritative status poll remains
// the correctness path.
type CancelHintHub struct {
	mu          sync.Mutex
	subscribers map[chan gateway.ExecutionKey]struct{}
}

func (h *CancelHintHub) SubscribeCancellation(ctx context.Context) <-chan gateway.ExecutionKey {
	ch := make(chan gateway.ExecutionKey, 8)
	if h == nil {
		close(ch)
		return ch
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		close(ch)
		return ch
	}
	h.mu.Lock()
	if h.subscribers == nil {
		h.subscribers = make(map[chan gateway.ExecutionKey]struct{})
	}
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()
	go func() {
		<-ctx.Done()
		h.mu.Lock()
		if _, ok := h.subscribers[ch]; ok {
			delete(h.subscribers, ch)
			close(ch)
		}
		h.mu.Unlock()
	}()
	return ch
}

func (h *CancelHintHub) PublishCancellation(key gateway.ExecutionKey) {
	if h == nil || key.TenantID == "" || key.RequestID == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subscribers {
		select {
		case ch <- key:
		default:
		}
	}
}

// ConsumeExecutionControl adapts a relay delivery to the local hint hub. The
// event is intentionally not treated as authority; TaskStore polling in the
// Consumer still decides whether cancellation is valid.
func (h *CancelHintHub) ConsumeExecutionControl(_ context.Context, delivery relay.ExecutionControlDelivery) error {
	if delivery.Event.Kind != "execution-control" || delivery.Event.TenantID == "" || delivery.Event.AggregateID == "" || delivery.Event.Version < 1 {
		return runtime.ErrInvariantViolation
	}
	h.PublishCancellation(gateway.ExecutionKey{TenantID: delivery.Event.TenantID, RequestID: delivery.Event.AggregateID})
	return nil
}
