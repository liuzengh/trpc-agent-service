// Package channels adapts IM platforms (WeCom, WeChat, Telegram, etc.) into
// normalized inbound messages and outbound delivery receipts.
package channels

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// OutboundMessage is independent from any provider-specific send API.
type OutboundMessage struct {
	OutboundID  string
	RequestID   string
	Text        string
	ReplyTarget string
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

// InboundEnvelope is the protocol-neutral callback message passed to Gateway.
type InboundEnvelope struct {
	Media             *MediaReference
	ExternalMessageID string
	ExternalUserID    string
	ExternalChatID    string
	ExternalThreadID  string
	ChatType          string
	MessageType       string
	Edited            bool
	Text              string
	ReplyTarget       string
	OccurredAt        time.Time
}

// CallbackResult includes both normalized messages and the immediate provider
// acknowledgement. Agent execution never blocks the callback response.
type CallbackResult struct {
	Messages    []InboundEnvelope
	StatusCode  int
	ContentType string
	Body        []byte
}

// CallbackAdapter verifies and decodes one provider webhook request.
type CallbackAdapter interface {
	Adapter
	Callback(
		ctx context.Context,
		binding controlplane.ChannelBinding,
		request *http.Request,
	) (CallbackResult, error)
}

// DeliveryError classifies retryable provider failures.
type DeliveryError struct {
	Cause      error
	Retryable  bool
	RetryAfter time.Duration
	// Unknown means the provider may have delivered; do not blindly resend.
	Unknown     bool
	Diagnostics *DeliveryDiagnostics
}

func (e *DeliveryError) Error() string {
	if e == nil || e.Cause == nil {
		return "channel delivery failed"
	}
	if e.Diagnostics == nil {
		return e.Cause.Error()
	}
	d := e.Diagnostics.Safe()
	return fmt.Sprintf("%s [kind=%s phase=%s http_status=%d]", e.Cause.Error(), d.Kind, d.Phase, d.HTTPStatus)
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
