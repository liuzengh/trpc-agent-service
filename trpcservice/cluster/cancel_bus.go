package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// PubSubBackend is the shared transient notification surface. Durable intent
// remains in PostgreSQL and is checked by Workers independently of Pub/Sub.
type PubSubBackend interface {
	Publish(context.Context, string, []byte) error
	Subscribe(context.Context, string) (<-chan []byte, func() error, error)
}

// DurableCanceler atomically records cancellation for an active request.
type DurableCanceler interface {
	Cancel(string, string) bool
}

// LocalCancel stops a request only if this node currently owns its Runner.
type LocalCancel func(string, string) bool

// CancelBus persists cancellation before broadcasting it to every Worker.
type CancelBus struct {
	backend PubSubBackend
	store   DurableCanceler
	local   LocalCancel
	channel string
	close   func() error
	done    chan struct{}
	once    sync.Once
}

type cancelCommand struct {
	TenantID  string `json:"tenant_id"`
	RequestID string `json:"request_id"`
}

// CancelPublisher is the Gateway-only half of cancellation control. It writes
// durable intent and publishes the low-latency notification without opening a
// Worker subscription in an ingress-only process.
type CancelPublisher struct {
	backend PubSubBackend
	store   DurableCanceler
	channel string
}

func NewCancelPublisher(backend PubSubBackend, store DurableCanceler, prefix string) (*CancelPublisher, error) {
	if backend == nil || store == nil {
		return nil, errors.New("cluster: cancel backend and store are required")
	}
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	return &CancelPublisher{backend: backend, store: store, channel: prefix + ":run-control:cancel"}, nil
}

func (publisher *CancelPublisher) Cancel(tenantID, requestID string) bool {
	return publishCancellation(publisher.backend, publisher.store, publisher.channel, tenantID, requestID)
}

// NewCancelBus subscribes this node before accepting cancellation requests.
func NewCancelBus(ctx context.Context, backend PubSubBackend, store DurableCanceler, local LocalCancel, prefix string) (*CancelBus, error) {
	if ctx == nil || backend == nil || store == nil || local == nil {
		return nil, errors.New("cluster: cancel backend, store, callback, and context are required")
	}
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	bus := &CancelBus{backend: backend, store: store, local: local, channel: prefix + ":run-control:cancel", done: make(chan struct{})}
	input, closeSubscription, err := backend.Subscribe(ctx, bus.channel)
	if err != nil {
		return nil, err
	}
	bus.close = closeSubscription
	go bus.consume(input)
	return bus, nil
}

// Cancel records durable intent and then sends a low-latency notification.
func (bus *CancelBus) Cancel(tenantID, requestID string) bool {
	if bus == nil {
		return false
	}
	return publishCancellation(bus.backend, bus.store, bus.channel, tenantID, requestID)
}

func publishCancellation(backend PubSubBackend, store DurableCanceler, channel, tenantID, requestID string) bool {
	if backend == nil || store == nil || tenantID == "" || requestID == "" || !store.Cancel(tenantID, requestID) {
		return false
	}
	payload, err := json.Marshal(cancelCommand{TenantID: tenantID, RequestID: requestID})
	if err != nil {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = backend.Publish(ctx, channel, payload)
	cancel()
	return true
}

func (bus *CancelBus) consume(input <-chan []byte) {
	defer close(bus.done)
	for payload := range input {
		var command cancelCommand
		if json.Unmarshal(payload, &command) == nil && command.TenantID != "" && command.RequestID != "" {
			bus.local(command.TenantID, command.RequestID)
		}
	}
}

// Close stops this node's subscription.
func (bus *CancelBus) Close() error {
	if bus == nil {
		return nil
	}
	var err error
	bus.once.Do(func() {
		if bus.close != nil {
			err = bus.close()
		}
	})
	return err
}
