// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Status values for a tenant.
const (
	StatusActive   = "active"
	StatusDisabled = "disabled"
)

// Data-backend domain keys and backend values stored in Tenant.DataBackend.
// Values align with storage.Backend* string constants by contract.
const (
	DomainSession = "session"
	DomainMemory  = "memory"
)

// Backend values selectable per domain (kept as plain strings to avoid an
// import cycle with the storage package).
const (
	BackendInMemory = "inmemory"
	BackendMySQL    = "mysql"
	BackendRedis    = "redis"
)

// Tenant is the first isolation boundary of the platform. DataBackend maps a
// data domain (session/memory/...) to a backend id; unset domains fall back to
// the platform default.
type Tenant struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Status      string            `json:"status"`
	DataBackend map[string]string `json:"data_backend,omitempty"`
}

// Validate checks that the tenant fields are well-formed.
func (t *Tenant) Validate() error {
	if t.ID == "" {
		return errors.New("tenant: id is required")
	}
	if t.Name == "" {
		return errors.New("tenant: name is required")
	}
	switch t.Status {
	case StatusActive, StatusDisabled:
		return nil
	default:
		return fmt.Errorf("tenant: invalid status %q", t.Status)
	}
}

// ErrNotFound is returned when a tenant does not exist.
var ErrNotFound = errors.New("tenant: not found")

// store is the persistence contract behind Manager. The in-memory store keeps
// the service runnable without MySQL; the MySQL store (tenant_mysql.go) is
// the production path.
type store interface {
	Create(ctx context.Context, t *Tenant) error
	Get(ctx context.Context, id string) (*Tenant, error)
	List(ctx context.Context) ([]*Tenant, error)
	Update(ctx context.Context, t *Tenant) error
	Delete(ctx context.Context, id string) error
}

// Manager owns tenants behind a swappable store.
type Manager struct {
	store store
}

// NewManager returns an in-memory tenant manager.
func NewManager() *Manager {
	return &Manager{store: newMemStore()}
}

// Create inserts a new tenant, failing on duplicate IDs.
func (m *Manager) Create(ctx context.Context, t *Tenant) error {
	if err := t.Validate(); err != nil {
		return err
	}
	return m.store.Create(ctx, t)
}

// Get returns a copy of the tenant, or ErrNotFound.
func (m *Manager) Get(ctx context.Context, id string) (*Tenant, error) {
	return m.store.Get(ctx, id)
}

// List returns all tenants.
func (m *Manager) List(ctx context.Context) ([]*Tenant, error) {
	return m.store.List(ctx)
}

// Update replaces an existing tenant.
func (m *Manager) Update(ctx context.Context, t *Tenant) error {
	if err := t.Validate(); err != nil {
		return err
	}
	return m.store.Update(ctx, t)
}

// Delete removes a tenant.
func (m *Manager) Delete(ctx context.Context, id string) error {
	return m.store.Delete(ctx, id)
}

// memStore keeps tenants in a map; the zero-dependency dev/test backend.
type memStore struct {
	mu    sync.RWMutex
	items map[string]*Tenant
}

func newMemStore() *memStore {
	return &memStore{items: make(map[string]*Tenant)}
}

// cloneTenant deep-copies the DataBackend map so callers can never mutate
// stored state through a returned tenant.
func cloneTenant(t *Tenant) *Tenant {
	cp := *t
	if t.DataBackend != nil {
		cp.DataBackend = make(map[string]string, len(t.DataBackend))
		for k, v := range t.DataBackend {
			cp.DataBackend[k] = v
		}
	}
	return &cp
}

func (s *memStore) Create(_ context.Context, t *Tenant) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[t.ID]; ok {
		return fmt.Errorf("tenant: %q already exists", t.ID)
	}
	s.items[t.ID] = cloneTenant(t)
	return nil
}

func (s *memStore) Get(_ context.Context, id string) (*Tenant, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.items[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneTenant(t), nil
}

func (s *memStore) List(_ context.Context) ([]*Tenant, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Tenant, 0, len(s.items))
	for _, t := range s.items {
		out = append(out, cloneTenant(t))
	}
	return out, nil
}

func (s *memStore) Update(_ context.Context, t *Tenant) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[t.ID]; !ok {
		return ErrNotFound
	}
	s.items[t.ID] = cloneTenant(t)
	return nil
}

func (s *memStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[id]; !ok {
		return ErrNotFound
	}
	delete(s.items, id)
	return nil
}
