package tenant

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore is an in-process ConfigStore for tests and live-path wiring.
type MemoryStore struct {
	mu       sync.Mutex
	tenants  map[string]Tenant
	apps     map[string][]AgentApp
	bindings map[string]ChannelBinding
}

// NewMemoryStore constructs an empty control-plane store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tenants:  make(map[string]Tenant),
		apps:     make(map[string][]AgentApp),
		bindings: make(map[string]ChannelBinding),
	}
}

// UpsertTenant implements ConfigStore.
func (m *MemoryStore) UpsertTenant(_ context.Context, value Tenant) error {
	if err := value.Validate(); err != nil {
		return fmt.Errorf("validate tenant: %w", err)
	}
	m.mu.Lock()
	m.tenants[value.ID] = value
	m.mu.Unlock()
	return nil
}

// GetTenant implements ConfigStore.
func (m *MemoryStore) GetTenant(_ context.Context, id string) (Tenant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.tenants[id]
	if !ok {
		return Tenant{}, ErrNotFound
	}
	return value, nil
}

// ListTenants implements ConfigStore.
func (m *MemoryStore) ListTenants(context.Context) ([]Tenant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]Tenant, 0, len(m.tenants))
	for _, value := range m.tenants {
		result = append(result, value)
	}
	return result, nil
}

// DeactivateTenant implements ConfigStore.
func (m *MemoryStore) DeactivateTenant(ctx context.Context, id string) error {
	value, err := m.GetTenant(ctx, id)
	if err != nil {
		return err
	}
	value.IsActive = false
	return m.UpsertTenant(ctx, value)
}

// UpsertApp implements ConfigStore.
func (m *MemoryStore) UpsertApp(_ context.Context, value AgentApp) (int, error) {
	if err := value.Validate(); err != nil {
		return 0, fmt.Errorf("validate app: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value.Version = len(m.apps[value.ID]) + 1
	value.IsCurrent = value.Version == 1
	m.apps[value.ID] = append(m.apps[value.ID], value)
	return value.Version, nil
}

// GetCurrentApp implements ConfigStore.
func (m *MemoryStore) GetCurrentApp(_ context.Context, id string) (AgentApp, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, value := range m.apps[id] {
		if value.IsCurrent {
			return value, nil
		}
	}
	return AgentApp{}, ErrNotFound
}

// ListApps implements ConfigStore.
func (m *MemoryStore) ListApps(_ context.Context, tenantID string) ([]AgentApp, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []AgentApp
	for _, versions := range m.apps {
		for _, value := range versions {
			if value.TenantID == tenantID && value.IsCurrent {
				result = append(result, value)
			}
		}
	}
	return result, nil
}

// BindAppVersion implements ConfigStore.
func (m *MemoryStore) BindAppVersion(_ context.Context, id string, version int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := m.apps[id]
	found := false
	for index := range values {
		values[index].IsCurrent = values[index].Version == version
		found = found || values[index].IsCurrent
	}
	if !found {
		return ErrNotFound
	}
	m.apps[id] = values
	return nil
}

// UpsertBinding implements ConfigStore.
func (m *MemoryStore) UpsertBinding(_ context.Context, value ChannelBinding) error {
	if err := value.Validate(); err != nil {
		return fmt.Errorf("validate binding: %w", err)
	}
	m.mu.Lock()
	m.bindings[value.ID] = value
	m.mu.Unlock()
	return nil
}

// GetBindingByRoute implements ConfigStore.
func (m *MemoryStore) GetBindingByRoute(_ context.Context, channel, routeKey string) (ChannelBinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, value := range m.bindings {
		if value.Channel == channel && value.RouteKey == routeKey {
			return value, nil
		}
	}
	return ChannelBinding{}, ErrNotFound
}

// ListBindings implements ConfigStore.
func (m *MemoryStore) ListBindings(_ context.Context, appID string) ([]ChannelBinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []ChannelBinding
	for _, value := range m.bindings {
		if value.AppID == appID {
			result = append(result, value)
		}
	}
	return result, nil
}
