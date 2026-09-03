// Package channels adapts IM platforms (WeCom, WeChat, Telegram, etc.) into
// normalized inbound messages and outbound delivery receipts.
package channels

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// OutboundMessage is independent from any provider-specific send API.
type OutboundMessage struct {
	OutboundID string
	RequestID  string
	Text       string
}

// DeliveryReceipt records the provider acknowledgement for one send.
type DeliveryReceipt struct {
	ProviderMessageID string
	SentAt            time.Time
}

// Capabilities describes delivery constraints used by Reply Sender.
type Capabilities struct {
	MaxTextRunes int
	SupportsEdit bool
	SupportsCard bool
	SupportsFile bool
}

// Adapter sends normalized replies using one channel protocol.
type Adapter interface {
	Type() string
	Send(
		ctx context.Context,
		binding controlplane.ChannelBinding,
		message OutboundMessage,
	) (DeliveryReceipt, error)
	Capabilities() Capabilities
}

// DeliveryError classifies retryable provider failures.
type DeliveryError struct {
	Cause      error
	Retryable  bool
	RetryAfter time.Duration
}

func (e *DeliveryError) Error() string {
	if e == nil || e.Cause == nil {
		return "channel delivery failed"
	}
	return e.Cause.Error()
}

func (e *DeliveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Registry resolves channel type to a concurrency-safe Adapter.
type Registry struct {
	mu       sync.RWMutex
	adapters map[string]Adapter
}

func NewRegistry(adapters ...Adapter) (*Registry, error) {
	registry := &Registry{adapters: make(map[string]Adapter)}
	for _, adapter := range adapters {
		if adapter == nil || adapter.Type() == "" {
			return nil, fmt.Errorf("channel adapter and type are required")
		}
		if _, exists := registry.adapters[adapter.Type()]; exists {
			return nil, fmt.Errorf("channel adapter %q is duplicated", adapter.Type())
		}
		registry.adapters[adapter.Type()] = adapter
	}
	return registry, nil
}

func (r *Registry) Get(channelType string) (Adapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	adapter := r.adapters[channelType]
	if adapter == nil {
		return nil, fmt.Errorf("channel adapter %q: %w", channelType, ErrAdapterNotFound)
	}
	return adapter, nil
}

var ErrAdapterNotFound = errors.New("channel adapter not found")
