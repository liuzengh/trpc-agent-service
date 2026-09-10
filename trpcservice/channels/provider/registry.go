// Package provider owns the composition-time registry for channel delivery
// providers. The channel domain package remains independent from durable reply
// delivery; only this adapter boundary knows how a provider sends an outbox
// segment.
package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
)

var (
	// ErrProviderUnavailable is the redacted result of an unknown channel provider.
	ErrProviderUnavailable = errors.New("channel provider unavailable")
	// ErrProviderRegistryClosed reports use after close.
	ErrProviderRegistryClosed = errors.New("channel provider registry is closed")
)

// Factory constructs a protocol-neutral outbox provider for one tenant-scoped
// Binding. Concrete Telegram/WeCom packages implement this contract without
// being imported by the shared channel model.
type Factory interface {
	New(context.Context, channels.Binding) (outbox.Provider, error)
}

type providerKey struct {
	tenantID, channel, account string
}

// Registry resolves a tenant/channel/account tuple to an adapter factory. It
// contains no tokens or protocol credentials and is owned by composition code.
type Registry struct {
	mu        sync.RWMutex
	factories map[providerKey]Factory
	closed    bool
}

// NewRegistry creates an empty channel provider registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[providerKey]Factory)}
}

// Register installs or replaces one tenant/channel/provider-account factory.
func (registry *Registry) Register(tenantID string, channel channels.Channel, providerAccountID string, factory Factory) error {
	providerAccountID = strings.TrimSpace(providerAccountID)
	if registry == nil || channels.ValidateTenantID(tenantID) != nil || channel.Validate() != nil || providerAccountID == "" || factory == nil {
		return fmt.Errorf("%w: invalid channel provider registration", channels.ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrProviderRegistryClosed
	}
	registry.factories[providerKey{tenantID: tenantID, channel: string(channel), account: providerAccountID}] = factory
	return nil
}

// Resolve finds a provider factory without falling back across tenants.
func (registry *Registry) Resolve(ctx context.Context, binding channels.Binding) (Factory, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", channels.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if registry == nil || binding.Validate() != nil {
		return nil, ErrProviderUnavailable
	}
	registry.mu.RLock()
	factory := registry.factories[providerKey{tenantID: binding.TenantID, channel: string(binding.Channel), account: strings.TrimSpace(binding.ProviderAccountID)}]
	closed := registry.closed
	registry.mu.RUnlock()
	if closed || factory == nil {
		return nil, ErrProviderUnavailable
	}
	return factory, nil
}

// Remove deletes one registration.
func (registry *Registry) Remove(tenantID string, channel channels.Channel, providerAccountID string) error {
	if registry == nil || channels.ValidateTenantID(tenantID) != nil {
		return fmt.Errorf("%w: invalid channel provider scope", channels.ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrProviderRegistryClosed
	}
	delete(registry.factories, providerKey{tenantID: tenantID, channel: string(channel), account: strings.TrimSpace(providerAccountID)})
	return nil
}

// Close prevents new resolutions and drops factory references. Existing
// adapter instances remain owned by their caller.
func (registry *Registry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil
	}
	registry.closed = true
	clear(registry.factories)
	return nil
}
