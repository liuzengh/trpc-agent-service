package channels

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

var (
	ErrInvalidSignature = errors.New("invalid webhook signature")
	ErrUnsupportedEvent = errors.New("unsupported channel event")
	ErrBindingMismatch  = errors.New("channel event does not match the configured binding")
)

// ParsedWebhook contains either a provider challenge or normalized messages.
type ParsedWebhook struct {
	Challenge string
	Messages  []domain.InboundMessage
}

// Adapter converts one IM provider's protocol to and from the canonical model.
type Adapter interface {
	Name() string
	Verify(r *http.Request, body []byte, binding config.ChannelConfig) error
	Parse(body []byte, binding config.ChannelConfig) (ParsedWebhook, error)
	// Plan deterministically splits one logical reply into provider-sized parts.
	// It performs no I/O and does not read credentials, allowing callers to
	// persist every part before attempting any external side effect.
	Plan(binding config.ChannelConfig, msg domain.OutboundMessage) ([]delivery.Part, error)
	// Deliver performs exactly one planned message operation. It must never
	// loop over or implicitly split request.Message. Credential preflight is
	// allowed, and an explicitly rejected authentication request may be
	// replaced after refresh, but no request whose acceptance is uncertain may
	// be repeated; operation-level retry belongs to the durable ledger.
	Deliver(ctx context.Context, binding config.ChannelConfig, request delivery.Request) delivery.Result
}

// URLVerifier is implemented by adapters whose provider verifies the callback
// URL with a GET challenge (e.g. WeCom echoes a decrypted random string). The
// returned plaintext must be written back as text/plain.
type URLVerifier interface {
	VerifyURL(r *http.Request, binding config.ChannelConfig) (string, error)
}

type Registry struct {
	mu       sync.RWMutex
	adapters map[string]Adapter
}

func NewRegistry(adapters ...Adapter) *Registry {
	r := &Registry{adapters: make(map[string]Adapter, len(adapters))}
	for _, adapter := range adapters {
		if adapter != nil {
			r.adapters[adapter.Name()] = adapter
		}
	}
	return r
}

func (r *Registry) Get(name string) (Adapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	adapter, ok := r.adapters[name]
	if !ok {
		return nil, fmt.Errorf("channel adapter %q is not registered", name)
	}
	return adapter, nil
}
