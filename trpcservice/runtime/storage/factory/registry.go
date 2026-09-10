package factory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	backendprofile "github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

var (
	// ErrProviderUnavailable is the redacted result of an unknown backend provider.
	ErrProviderUnavailable = errors.New("backend provider unavailable")
	// ErrRegistryClosed reports use after a backend registry has been closed.
	ErrRegistryClosed = errors.New("backend provider registry is closed")
)

// CapabilityProvider constructs one runtime capability from one normalized
// binding. The provider receives a temporary secret only for this call.
type CapabilityProvider interface {
	New(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error)
}

type providerRegistryKey struct {
	tenantID   string
	capability Capability
	provider   string
}

// ProviderRegistry is a tenant-scoped backend capability factory registry.
// Registration is replaceable to support rotation without mutating plans.
type ProviderRegistry struct {
	mu        sync.RWMutex
	providers map[providerRegistryKey]CapabilityProvider
	closed    bool
}

// NewProviderRegistry creates an empty backend provider registry.
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{providers: make(map[providerRegistryKey]CapabilityProvider)}
}

// Register installs or replaces one tenant/capability/provider implementation.
func (registry *ProviderRegistry) Register(tenantID string, capability Capability, provider string, value CapabilityProvider) error {
	if registry == nil || backendprofile.ValidateTenantID(tenantID) != nil || !validCapability(capability) || value == nil {
		return fmt.Errorf("%w: invalid backend provider registration", ErrInvalid)
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		return fmt.Errorf("%w: provider is required", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	registry.providers[providerRegistryKey{tenantID: tenantID, capability: capability, provider: provider}] = value
	return nil
}

// Remove deletes one tenant/capability/provider registration.
func (registry *ProviderRegistry) Remove(tenantID string, capability Capability, provider string) error {
	if registry == nil || backendprofile.ValidateTenantID(tenantID) != nil {
		return fmt.Errorf("%w: invalid backend provider scope", ErrInvalid)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRegistryClosed
	}
	delete(registry.providers, providerRegistryKey{tenantID: tenantID, capability: capability, provider: strings.ToLower(strings.TrimSpace(provider))})
	return nil
}

// Resolve returns a registered provider after validating the explicit tenant
// and binding identity. It never falls back to another tenant's provider.
func (registry *ProviderRegistry) Resolve(ctx context.Context, input StorageFactoryInput, binding CapabilityBinding) (CapabilityProvider, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if registry == nil || input.TenantID == "" {
		return nil, ErrProviderUnavailable
	}
	registry.mu.RLock()
	provider := registry.providers[providerRegistryKey{tenantID: input.TenantID, capability: binding.Capability, provider: strings.ToLower(strings.TrimSpace(binding.Provider))}]
	closed := registry.closed
	registry.mu.RUnlock()
	if closed || provider == nil {
		return nil, ErrProviderUnavailable
	}
	return provider, nil
}

// Close prevents future resolution and removes all factory references. It does
// not close capabilities already materialized by a caller.
func (registry *ProviderRegistry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil
	}
	registry.closed = true
	clear(registry.providers)
	return nil
}
