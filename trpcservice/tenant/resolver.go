package tenant

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrBindingNotFound      = errors.New("channel binding not found")
	ErrBindingConflict      = errors.New("channel binding conflicts with another tenant")
	ErrTenantInactive       = errors.New("tenant is not active")
	ErrTenantMismatch       = errors.New("tenant context does not allow resource tenant")
	ErrCallerTenantOverride = errors.New("caller tenant or binding override is not allowed")
)

type ResolveRequest struct {
	TenantID         string
	BindingID        string
	Channel          string
	ExternalAppID    string
	ExternalUser     string
	ExternalChat     string
	ExternalChatType string
	ExternalThreadID string
	RequestID        string
	MessageID        string
	TraceID          string
}

type TenantResolver interface {
	Resolve(context.Context, ResolveRequest) (TenantContext, error)
}

type Registry interface {
	Tenant(context.Context, string) (Tenant, error)
	Agent(context.Context, string, string) (AgentApp, error)
	Binding(context.Context, string, string) (ChannelBinding, error)
}

type RegistryResolver struct {
	Registry       Registry
	SecretResolver SecretResolver
}

func (r RegistryResolver) Resolve(ctx context.Context, req ResolveRequest) (TenantContext, error) {
	if ctx == nil {
		return TenantContext{}, errors.New("tenant resolver requires context")
	}
	if err := ctx.Err(); err != nil {
		return TenantContext{}, err
	}
	if req.TenantID != "" || req.BindingID != "" {
		return TenantContext{}, ErrCallerTenantOverride
	}
	if req.Channel != ChannelLark && req.Channel != ChannelTelegram {
		return TenantContext{}, ErrUnsupportedBindingChannel
	}
	if r.Registry == nil {
		return TenantContext{}, errors.New("tenant registry is not configured")
	}
	binding, err := r.Registry.Binding(ctx, req.Channel, req.ExternalAppID)
	if err != nil {
		return TenantContext{}, fmt.Errorf("resolve binding: %w", err)
	}
	if binding.Channel != req.Channel || binding.ExternalAppID != req.ExternalAppID {
		return TenantContext{}, ErrBindingNotFound
	}
	if err := binding.Validate(); err != nil {
		return TenantContext{}, fmt.Errorf("%w: %w", ErrBindingNotFound, err)
	}
	if err := binding.IsUsableAt(time.Now().UTC()); err != nil {
		return TenantContext{}, fmt.Errorf("%w: %w", ErrBindingNotFound, err)
	}
	if binding.ExternalTargetType == BindingTargetUser && binding.ExternalTargetID != req.ExternalUser {
		return TenantContext{}, ErrBindingNotFound
	}
	if binding.ExternalTargetType == BindingTargetChat && binding.ExternalTargetID != req.ExternalChat {
		return TenantContext{}, ErrBindingNotFound
	}
	if r.SecretResolver != nil {
		for _, ref := range []string{binding.SecretRef, binding.VerifyTokenRef} {
			if ref == "" {
				continue
			}
			if _, secretErr := r.SecretResolver.Resolve(ctx, ref); secretErr != nil {
				if errors.Is(secretErr, context.Canceled) || errors.Is(secretErr, context.DeadlineExceeded) {
					return TenantContext{}, secretErr
				}
				return TenantContext{}, ErrBindingNotFound
			}
		}
	}
	t, err := r.Registry.Tenant(ctx, binding.TenantID)
	if err != nil {
		return TenantContext{}, fmt.Errorf("resolve tenant: %w", err)
	}
	if err := t.Validate(); err != nil {
		return TenantContext{}, fmt.Errorf("validate tenant: %w", err)
	}
	if t.Status != StatusActive {
		return TenantContext{}, ErrTenantInactive
	}
	agent, err := r.Registry.Agent(ctx, t.ID, t.DefaultAgentID)
	if err != nil {
		return TenantContext{}, fmt.Errorf("resolve agent: %w", err)
	}
	if err := agent.Validate(); err != nil {
		return TenantContext{}, fmt.Errorf("validate agent: %w", err)
	}
	if agent.TenantID != t.ID {
		return TenantContext{}, ErrTenantMismatch
	}
	tc := TenantContext{TenantID: t.ID, AgentAppID: agent.ID, BindingID: binding.ID, Channel: binding.Channel, ExternalUser: req.ExternalUser, ExternalChat: req.ExternalChat, ExternalChatType: req.ExternalChatType, ExternalThreadID: req.ExternalThreadID, RequestID: req.RequestID, MessageID: req.MessageID, TraceID: req.TraceID, ConfigVersion: t.ConfigVersion, BackendPolicy: t.Backend}
	if err := tc.Validate(); err != nil {
		return TenantContext{}, err
	}
	return tc, nil
}

// MemoryRegistry is a development and test registry. Production uses the same Registry contract with PostgreSQL.
type MemoryRegistry struct {
	mu       sync.RWMutex
	tenants  map[string]Tenant
	agents   map[string]AgentApp
	bindings map[string]ChannelBinding
}

func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{tenants: map[string]Tenant{}, agents: map[string]AgentApp{}, bindings: map[string]ChannelBinding{}}
}
func registryKey(a, b string) string { return a + "\x00" + b }
func (r *MemoryRegistry) PutTenant(t Tenant) error {
	if err := t.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenants[t.ID] = t
	return nil
}
func (r *MemoryRegistry) PutAgent(a AgentApp) error {
	if err := a.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.agents[registryKey(a.TenantID, a.ID)] = a
	return nil
}
func (r *MemoryRegistry) PutBinding(b ChannelBinding) error {
	if err := b.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := registryKey(b.Channel, b.ExternalAppID)
	if existing, ok := r.bindings[key]; ok && existing.TenantID != b.TenantID {
		return ErrBindingConflict
	}
	r.bindings[key] = b
	return nil
}
func (r *MemoryRegistry) Tenant(ctx context.Context, id string) (Tenant, error) {
	if err := ctx.Err(); err != nil {
		return Tenant{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.tenants[id]
	if !ok {
		return Tenant{}, ErrBindingNotFound
	}
	return v, nil
}
func (r *MemoryRegistry) Agent(ctx context.Context, tenantID, id string) (AgentApp, error) {
	if err := ctx.Err(); err != nil {
		return AgentApp{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.agents[registryKey(tenantID, id)]
	if !ok {
		return AgentApp{}, ErrBindingNotFound
	}
	return v, nil
}
func (r *MemoryRegistry) Binding(ctx context.Context, channel, externalAppID string) (ChannelBinding, error) {
	if err := ctx.Err(); err != nil {
		return ChannelBinding{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.bindings[registryKey(channel, externalAppID)]
	if !ok {
		return ChannelBinding{}, ErrBindingNotFound
	}
	return v, nil
}

func RequireTenant(ctx context.Context, resourceTenantID string) (TenantContext, error) {
	tc, ok := FromContext(ctx)
	if !ok {
		return TenantContext{}, errors.New("tenant context is missing")
	}
	if err := tc.Validate(); err != nil {
		return TenantContext{}, err
	}
	if !tc.AllowsTenant(resourceTenantID) {
		return TenantContext{}, ErrTenantMismatch
	}
	return tc, nil
}
