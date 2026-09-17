package channels

import (
	"context"
	"sync"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type BindingRuntime struct {
	TenantID, ChannelBindingID, Channel, ExternalAccountID string
	Adapter                                                channel.Adapter
}

// Registry is the provider-neutral, tenant-scoped Adapter registry. Runtime
// configuration may select a registered Adapter, but cannot register code or
// resolve a binding from another tenant.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]BindingRuntime
}

func NewRegistry(entries ...BindingRuntime) (*Registry, error) {
	r := &Registry{entries: make(map[string]BindingRuntime)}
	for _, entry := range entries {
		if err := r.Register(entry); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Registry) Register(entry BindingRuntime) error {
	if r == nil || entry.TenantID == "" || entry.ChannelBindingID == "" || entry.Channel == "" ||
		entry.ExternalAccountID == "" || entry.Adapter == nil || entry.Adapter.ID() != entry.Channel {
		return runtime.ErrInvariantViolation
	}
	key := registryKey(entry.TenantID, entry.ChannelBindingID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.entries == nil {
		r.entries = make(map[string]BindingRuntime)
	}
	if _, ok := r.entries[key]; ok {
		return runtime.ErrIdempotencyCollision
	}
	r.entries[key] = entry
	return nil
}

func (r *Registry) ResolveAdapter(ctx context.Context, tenantID, bindingID string) (channel.Adapter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entry, err := r.ResolveBinding(ctx, tenantID, bindingID)
	if err != nil {
		return nil, err
	}
	return entry.Adapter, nil
}

func (r *Registry) ResolveBinding(ctx context.Context, tenantID, bindingID string) (BindingRuntime, error) {
	if err := ctx.Err(); err != nil {
		return BindingRuntime{}, err
	}
	if r == nil || tenantID == "" || bindingID == "" {
		return BindingRuntime{}, runtime.ErrTenantScope
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.entries[registryKey(tenantID, bindingID)]
	if !ok {
		return BindingRuntime{}, runtime.ErrNotFound
	}
	return entry, nil
}

func registryKey(tenantID, bindingID string) string { return tenantID + "\x00" + bindingID }
