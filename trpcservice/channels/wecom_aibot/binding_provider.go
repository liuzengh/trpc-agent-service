package wecom_aibot

import (
	"context"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// BindingProvider selects the manager fixed by a durable ReplyTarget. It does
// not accept a BotID, destination, or credential from a worker caller.
type BindingProvider struct {
	mu        sync.RWMutex
	providers map[string]*Provider
}

var _ outbox.Provider = (*BindingProvider)(nil)

// NewBindingProvider registers the supplied manager set by trusted BindingID.
// The caller owns manager lifecycle; this provider only owns reply routing.
func NewBindingProvider(store DeliveryStore, managers ...*Manager) (*BindingProvider, error) {
	if store == nil || len(managers) == 0 {
		return nil, ErrInvalid
	}
	providers := make(map[string]*Provider, len(managers))
	for _, manager := range managers {
		if manager == nil || manager.target.BindingID == "" {
			return nil, ErrInvalid
		}
		if _, exists := providers[manager.target.BindingID]; exists {
			return nil, ErrInvalid
		}
		provider, err := NewProvider(manager, store)
		if err != nil {
			return nil, err
		}
		providers[manager.target.BindingID] = provider
	}
	return &BindingProvider{providers: providers}, nil
}

// Deliver routes a durable reply to the manager selected by its BindingID.
func (p *BindingProvider) Deliver(ctx context.Context, value storage.ReplyOutbox) (string, error) {
	provider, err := p.provider(value)
	if err != nil {
		return "", err
	}
	return provider.Deliver(ctx, value)
}

// Reconcile returns the persisted delivery state for the selected manager.
func (p *BindingProvider) Reconcile(ctx context.Context, value storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	provider, err := p.provider(value)
	if err != nil {
		return outbox.DeliveryUnknown, "", err
	}
	return provider.Reconcile(ctx, value)
}

func (p *BindingProvider) provider(value storage.ReplyOutbox) (*Provider, error) {
	if p == nil || value.ReplyTarget.BindingID == "" {
		return nil, &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	p.mu.RLock()
	provider := p.providers[value.ReplyTarget.BindingID]
	p.mu.RUnlock()
	if provider == nil {
		return nil, &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	return provider, nil
}
