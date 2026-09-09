// Package channels adapts IM platforms into the shared Gateway pipeline.
package channels

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	platformmetrics "github.com/Violet2314/trpc-agent-service/trpcservice/metrics"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

// Sink is the adapter-facing Gateway entrypoint.
type Sink func(context.Context, gateway.InboundMessage) (gateway.Result, error)

// Adapter owns platform-specific inbound parsing and reply construction.
type Adapter interface {
	Type() string
	Run(context.Context, *http.ServeMux, Sink) error
	NewReplier(tenant.Snapshot) Replier
}

// Replier converts Worker events into platform-specific output and must always
// drain the event channel.
type Replier interface {
	Reply(
		context.Context,
		string,
		gateway.InboundMessage,
		<-chan reply.Event,
	) error
}

// Registry stores channel adapters and implements Gateway ReplyDispatcher.
type Registry struct {
	mu       sync.RWMutex
	adapters map[string]Adapter
	metrics  platformmetrics.Recorder
}

// SetMetrics enables IM delivery metrics.
func (r *Registry) SetMetrics(recorder platformmetrics.Recorder) {
	r.mu.Lock()
	r.metrics = recorder
	r.mu.Unlock()
}

// NewRegistry constructs an empty channel registry.
func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]Adapter)}
}

// Register adds one uniquely typed adapter.
func (r *Registry) Register(adapter Adapter) error {
	if adapter == nil || adapter.Type() == "" {
		return errors.New("channel adapter and type are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.adapters[adapter.Type()]; exists {
		return fmt.Errorf("channel adapter %q is already registered", adapter.Type())
	}
	r.adapters[adapter.Type()] = adapter
	return nil
}

// Run registers webhook handlers or starts adapter receive loops.
func (r *Registry) Run(ctx context.Context, mux *http.ServeMux, sink Sink) error {
	r.mu.RLock()
	adapters := make([]Adapter, 0, len(r.adapters))
	for _, adapter := range r.adapters {
		adapters = append(adapters, adapter)
	}
	r.mu.RUnlock()
	for _, adapter := range adapters {
		if err := adapter.Run(ctx, mux, sink); err != nil {
			return fmt.Errorf("run %s adapter: %w", adapter.Type(), err)
		}
	}
	return nil
}

// Dispatch implements gateway.ReplyDispatcher.
func (r *Registry) Dispatch(
	ctx context.Context,
	snapshot tenant.Snapshot,
	sessionID string,
	message gateway.InboundMessage,
	events <-chan reply.Event,
) error {
	r.mu.RLock()
	adapter := r.adapters[message.Channel]
	recorder := r.metrics
	r.mu.RUnlock()
	if adapter == nil {
		drain(events)
		if recorder != nil {
			recorder.ObserveDelivery(snapshot.Tenant.ID, message.Channel, true)
		}
		return fmt.Errorf("channel adapter %q is not registered", message.Channel)
	}
	replier := adapter.NewReplier(snapshot)
	if replier == nil {
		drain(events)
		if recorder != nil {
			recorder.ObserveDelivery(snapshot.Tenant.ID, message.Channel, true)
		}
		return fmt.Errorf("channel adapter %q returned nil replier", message.Channel)
	}
	err := replier.Reply(ctx, sessionID, message, events)
	if err != nil {
		log.Printf("channel %s reply failed: %v", message.Channel, err)
	}
	if recorder != nil {
		recorder.ObserveDelivery(snapshot.Tenant.ID, message.Channel, err != nil)
	}
	return err
}

func drain(events <-chan reply.Event) {
	for range events {
	}
}
