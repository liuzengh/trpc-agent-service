package controlplane

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryRepository is an in-process control plane for tutorials and tests.
type MemoryRepository struct {
	mu         sync.RWMutex
	closed     bool
	tenants    map[string]Tenant
	apps       map[string]AgentApp
	revisions  map[string]AgentRevision
	channels   map[string]ChannelBinding
	channelIDs map[string]ChannelBinding
	backends   []BackendBinding
}

// NewMemoryRepository builds a validated in-process snapshot.
func NewMemoryRepository(data BootstrapData) *MemoryRepository {
	repository := &MemoryRepository{
		tenants:    make(map[string]Tenant),
		apps:       make(map[string]AgentApp),
		revisions:  make(map[string]AgentRevision),
		channels:   make(map[string]ChannelBinding),
		channelIDs: make(map[string]ChannelBinding),
		backends:   append([]BackendBinding(nil), data.BackendBindings...),
	}
	for _, tenant := range data.Tenants {
		repository.tenants[tenant.ID] = cloneTenant(tenant)
	}
	for _, app := range data.Apps {
		repository.apps[scopedKey(app.TenantID, app.ID)] = cloneAgentApp(app)
	}
	for _, revision := range data.Revisions {
		repository.revisions[scopedKey(revision.TenantID, revision.ID)] = cloneRevision(revision)
	}
	for _, binding := range data.ChannelBindings {
		repository.channels[binding.CallbackKey] = cloneChannelBinding(binding)
		repository.channelIDs[scopedKey(binding.TenantID, binding.ID)] = cloneChannelBinding(binding)
	}
	return repository
}

func (r *MemoryRepository) GetChannelBinding(
	ctx context.Context,
	tenantID string,
	bindingID string,
) (ChannelBinding, error) {
	if err := r.check(ctx); err != nil {
		return ChannelBinding{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	binding, ok := r.channelIDs[scopedKey(tenantID, bindingID)]
	if !ok {
		return ChannelBinding{}, ErrNotFound
	}
	return cloneChannelBinding(binding), nil
}

func (r *MemoryRepository) GetTenant(ctx context.Context, tenantID string) (Tenant, error) {
	if err := r.check(ctx); err != nil {
		return Tenant{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	tenant, ok := r.tenants[tenantID]
	if !ok {
		return Tenant{}, ErrNotFound
	}
	return cloneTenant(tenant), nil
}

func (r *MemoryRepository) GetAgentApp(
	ctx context.Context,
	tenantID string,
	appID string,
) (AgentApp, error) {
	if err := r.check(ctx); err != nil {
		return AgentApp{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	app, ok := r.apps[scopedKey(tenantID, appID)]
	if !ok {
		return AgentApp{}, ErrNotFound
	}
	return cloneAgentApp(app), nil
}

func (r *MemoryRepository) GetRevision(
	ctx context.Context,
	tenantID string,
	revisionID string,
) (AgentRevision, error) {
	if err := r.check(ctx); err != nil {
		return AgentRevision{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	revision, ok := r.revisions[scopedKey(tenantID, revisionID)]
	if !ok {
		return AgentRevision{}, ErrNotFound
	}
	return cloneRevision(revision), nil
}

func (r *MemoryRepository) GetStableRevision(
	ctx context.Context,
	tenantID string,
	appID string,
) (AgentRevision, error) {
	app, err := r.GetAgentApp(ctx, tenantID, appID)
	if err != nil {
		return AgentRevision{}, err
	}
	if app.StableRevisionID == "" {
		return AgentRevision{}, ErrNotFound
	}
	return r.GetRevision(ctx, tenantID, app.StableRevisionID)
}

func (r *MemoryRepository) GetChannelBindingByCallbackKey(
	ctx context.Context,
	callbackKey string,
) (ChannelBinding, error) {
	if err := r.check(ctx); err != nil {
		return ChannelBinding{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	binding, ok := r.channels[callbackKey]
	if !ok {
		return ChannelBinding{}, ErrNotFound
	}
	return cloneChannelBinding(binding), nil
}

func (r *MemoryRepository) ListBackendBindings(
	ctx context.Context,
	tenantID string,
	appID string,
) ([]BackendBinding, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]BackendBinding, 0)
	for _, binding := range r.backends {
		if binding.TenantID != tenantID || (binding.AppID != "" && binding.AppID != appID) {
			continue
		}
		result = append(result, cloneBackendBinding(binding))
	}
	return result, nil
}

func (r *MemoryRepository) Ready(ctx context.Context) error {
	return r.check(ctx)
}

func (r *MemoryRepository) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

func (r *MemoryRepository) CreateTenant(_ context.Context, tenant Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	if _, exists := r.tenants[tenant.ID]; exists {
		return ErrConflict
	}
	r.tenants[tenant.ID] = cloneTenant(tenant)
	return nil
}

func (r *MemoryRepository) CreateAgentApp(_ context.Context, app AgentApp) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tenants[app.TenantID]; !exists {
		return ErrNotFound
	}
	key := scopedKey(app.TenantID, app.ID)
	if _, exists := r.apps[key]; exists {
		return ErrConflict
	}
	r.apps[key] = cloneAgentApp(app)
	return nil
}

func (r *MemoryRepository) CreateRevision(_ context.Context, revision AgentRevision) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.apps[scopedKey(revision.TenantID, revision.AppID)]; !exists {
		return ErrNotFound
	}
	key := scopedKey(revision.TenantID, revision.ID)
	if _, exists := r.revisions[key]; exists {
		return ErrConflict
	}
	r.revisions[key] = cloneRevision(revision)
	return nil
}

func (r *MemoryRepository) PublishRevision(
	_ context.Context,
	tenantID string,
	appID string,
	revisionID string,
	expectedVersion int64,
) (AgentApp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := scopedKey(tenantID, appID)
	app, exists := r.apps[key]
	if !exists {
		return AgentApp{}, ErrNotFound
	}
	revision, exists := r.revisions[scopedKey(tenantID, revisionID)]
	if !exists || revision.AppID != appID {
		return AgentApp{}, ErrNotFound
	}
	if app.Version != expectedVersion {
		return AgentApp{}, ErrConflict
	}
	app.StableRevisionID = revisionID
	app.Version++
	app.UpdatedAt = time.Now().UTC()
	r.apps[key] = app
	return cloneAgentApp(app), nil
}

func (r *MemoryRepository) CreateChannelBinding(
	_ context.Context,
	binding ChannelBinding,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.apps[scopedKey(binding.TenantID, binding.AppID)]; !exists {
		return ErrNotFound
	}
	if _, exists := r.channels[binding.CallbackKey]; exists {
		return ErrConflict
	}
	if _, exists := r.channelIDs[scopedKey(binding.TenantID, binding.ID)]; exists {
		return ErrConflict
	}
	r.channels[binding.CallbackKey] = cloneChannelBinding(binding)
	r.channelIDs[scopedKey(binding.TenantID, binding.ID)] = cloneChannelBinding(binding)
	return nil
}

func (r *MemoryRepository) CreateBackendBinding(
	_ context.Context,
	binding BackendBinding,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if binding.AppID != "" {
		if _, exists := r.apps[scopedKey(binding.TenantID, binding.AppID)]; !exists {
			return ErrNotFound
		}
	}
	for _, existing := range r.backends {
		if existing.ID == binding.ID ||
			(existing.TenantID == binding.TenantID && existing.AppID == binding.AppID &&
				existing.ResourceType == binding.ResourceType) {
			return ErrConflict
		}
	}
	r.backends = append(r.backends, cloneBackendBinding(binding))
	return nil
}

func (r *MemoryRepository) check(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	return nil
}

var ErrRepositoryClosed = errors.New("control-plane repository is closed")

func scopedKey(first string, second string) string { return first + "\x00" + second }

func cloneJSON(value []byte) []byte { return append([]byte(nil), value...) }

func cloneTenant(value Tenant) Tenant {
	value.QuotaConfig = cloneJSON(value.QuotaConfig)
	value.AuditPolicy = cloneJSON(value.AuditPolicy)
	return value
}

func cloneAgentApp(value AgentApp) AgentApp {
	value.RolloutPolicy = cloneJSON(value.RolloutPolicy)
	return value
}

func cloneRevision(value AgentRevision) AgentRevision {
	value.AgentConfig = cloneJSON(value.AgentConfig)
	value.ModelConfig = cloneJSON(value.ModelConfig)
	value.ToolPolicy = cloneJSON(value.ToolPolicy)
	value.KnowledgeConfig = cloneJSON(value.KnowledgeConfig)
	value.MemoryConfig = cloneJSON(value.MemoryConfig)
	value.GuardrailConfig = cloneJSON(value.GuardrailConfig)
	return value
}

func cloneChannelBinding(value ChannelBinding) ChannelBinding {
	value.Config = cloneJSON(value.Config)
	return value
}

func cloneBackendBinding(value BackendBinding) BackendBinding {
	value.Config = cloneJSON(value.Config)
	return value
}

var _ Repository = (*MemoryRepository)(nil)
var _ MutableRepository = (*MemoryRepository)(nil)
