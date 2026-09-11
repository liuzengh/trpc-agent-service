// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Status values for a tenant.
const (
	StatusActive   = "active"
	StatusDisabled = "disabled"
)

// Data-backend domain keys and backend values stored in Tenant.DataBackend.
// Values align with storage.Backend* string constants by contract. Summary is
// deliberately absent: it has no standalone backend and follows the session
// backend (see docs/存储与数据访问设计.md §3.2).
const (
	DomainSession  = "session"
	DomainMemory   = "memory"
	DomainVector   = "vector"
	DomainArtifact = "artifact"
	DomainAudit    = "audit"
)

// Backend values selectable per domain (kept as plain strings to avoid an
// import cycle with the storage package).
const (
	BackendInMemory = "inmemory"
	BackendMySQL    = "mysql"
	BackendRedis    = "redis"
)

// Tenant is the first isolation boundary of the platform. DataBackend maps a
// data domain to a backend id; domains with a second implementation today are
// session, memory, vector, artifact and audit. Summary has no entry: summaries
// live inside the session backend (framework session storage), so they follow
// the tenant's session choice. An unset domain falls back to the platform
// default. Quota and AuditPolicy carry the tenant-level governance strategies;
// nil means the safe default.
type Tenant struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Status      string            `json:"status"`
	DataBackend map[string]string `json:"data_backend,omitempty"`
	Quota       *Quota            `json:"quota,omitempty"`
	AuditPolicy *AuditPolicy      `json:"audit_policy,omitempty"`
}

// Quota caps the tenant's resource consumption. TokenQuota 0 (or a nil
// Quota) means unlimited — budgets apply only when explicitly configured.
type Quota struct {
	TokenQuota int64 `json:"token_quota,omitempty"` // per-tenant token budget; 0 = unlimited
}

// AuditPolicy holds the tenant-level governance strategies that modulate how
// its agents run: sensitive-data redaction (default on), the IM user
// allow-list (empty = everyone allowed), the static-tool whitelist (empty =
// unrestricted; knowledge_search tools are never restricted) and the
// tenant-mandated approval tool list (union-ed with the agent-level list).
type AuditPolicy struct {
	Redact             *bool    `json:"redact,omitempty"`               // nil = enabled (default on)
	IMAllowUsers       []string `json:"im_allow_users,omitempty"`       // empty = allow all IM users
	ToolWhitelist      []string `json:"tool_whitelist,omitempty"`       // tool ids allowed; empty = unrestricted
	ForceApprovalTools []string `json:"force_approval_tools,omitempty"` // tool ids always requiring human approval
}

// RedactEnabled reports whether sensitive-data redaction is on. A nil policy
// or an unset Redact both mean enabled (default-on, safe by default).
func (p *AuditPolicy) RedactEnabled() bool {
	if p == nil || p.Redact == nil {
		return true
	}
	return *p.Redact
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

// ErrConfigVersionNotFound is returned when a tenant config version does not
// exist (rollback to an unknown history point).
var ErrConfigVersionNotFound = errors.New("tenant: config version not found")

// ConfigVersion is one recorded configuration state of a tenant. Each
// successful tenant update snapshots the post-update tenant with a
// per-tenant monotonic version; restoring a version re-applies its Config
// (which records a fresh version on the current head).
type ConfigVersion struct {
	Version   int       `json:"version"`
	Config    *Tenant   `json:"config"`
	CreatedAt time.Time `json:"created_at"`
}

// Store is the persistence contract behind Manager. The in-memory store keeps
// the service runnable without MySQL; MySQL and other backend stores are
// implemented in the infra/storage package against this interface.
type Store interface {
	Create(ctx context.Context, t *Tenant) error
	Get(ctx context.Context, id string) (*Tenant, error)
	List(ctx context.Context) ([]*Tenant, error)
	Update(ctx context.Context, t *Tenant) error
	Delete(ctx context.Context, id string) error

	// SnapshotConfig records t as a new configuration version and returns its
	// version number. Implementations assign the version atomically.
	SnapshotConfig(ctx context.Context, t *Tenant) (int, error)
	// ListConfigVersions returns the tenant's configuration history, newest
	// first.
	ListConfigVersions(ctx context.Context, tenantID string) ([]*ConfigVersion, error)
	// GetConfigVersion returns one recorded configuration, or
	// ErrConfigVersionNotFound.
	GetConfigVersion(ctx context.Context, tenantID string, version int) (*ConfigVersion, error)
}

// Manager owns tenants behind a swappable store.
type Manager struct {
	store Store
}

// NewManager returns an in-memory tenant manager.
func NewManager() *Manager {
	return &Manager{store: newMemStore()}
}

// NewManagerWithStore returns a manager over the given store implementation,
// used by infra/storage to back a Manager with MySQL.
func NewManagerWithStore(s Store) *Manager {
	return &Manager{store: s}
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

// Update replaces an existing tenant and records a configuration version
// snapshot of the new state. When the primary update succeeds but the history
// write fails, the error surfaces so the caller knows history is incomplete
// (the live tenant is already updated).
func (m *Manager) Update(ctx context.Context, t *Tenant) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if err := m.store.Update(ctx, t); err != nil {
		return err
	}
	if _, err := m.store.SnapshotConfig(ctx, cloneTenant(t)); err != nil {
		return fmt.Errorf("tenant: updated %q but config history write failed: %w", t.ID, err)
	}
	return nil
}

// ConfigVersions returns the tenant's configuration history (newest first).
func (m *Manager) ConfigVersions(ctx context.Context, tenantID string) ([]*ConfigVersion, error) {
	return m.store.ListConfigVersions(ctx, tenantID)
}

// RollbackConfig restores a tenant's configuration to a recorded version.
// Restoring is itself an update, so it records a new version at the head and
// returns the restored tenant. The tenant row must exist; the version must be
// recorded (ErrConfigVersionNotFound otherwise).
func (m *Manager) RollbackConfig(ctx context.Context, tenantID string, version int) (*Tenant, error) {
	cv, err := m.store.GetConfigVersion(ctx, tenantID, version)
	if err != nil {
		return nil, err
	}
	if cv == nil || cv.Config == nil {
		return nil, ErrConfigVersionNotFound
	}
	restored := cloneTenant(cv.Config)
	restored.ID = tenantID
	if err := m.Update(ctx, restored); err != nil {
		return nil, err
	}
	return m.Get(ctx, tenantID)
}

// Delete removes a tenant.
func (m *Manager) Delete(ctx context.Context, id string) error {
	return m.store.Delete(ctx, id)
}

// memStore keeps tenants in a map; the zero-dependency dev/test backend.
type memStore struct {
	mu       sync.RWMutex
	items    map[string]*Tenant
	versions map[string][]*ConfigVersion // tenantID -> history, oldest first
}

func newMemStore() *memStore {
	return &memStore{items: make(map[string]*Tenant), versions: make(map[string][]*ConfigVersion)}
}

// cloneTenant deep-copies the DataBackend map and governance slices so
// callers can never mutate stored state through a returned tenant.
func cloneTenant(t *Tenant) *Tenant {
	cp := *t
	if t.DataBackend != nil {
		cp.DataBackend = make(map[string]string, len(t.DataBackend))
		for k, v := range t.DataBackend {
			cp.DataBackend[k] = v
		}
	}
	if t.Quota != nil {
		q := *t.Quota
		cp.Quota = &q
	}
	if t.AuditPolicy != nil {
		ap := *t.AuditPolicy
		cp.AuditPolicy = &ap
		cp.AuditPolicy.IMAllowUsers = append([]string(nil), t.AuditPolicy.IMAllowUsers...)
		cp.AuditPolicy.ToolWhitelist = append([]string(nil), t.AuditPolicy.ToolWhitelist...)
		cp.AuditPolicy.ForceApprovalTools = append([]string(nil), t.AuditPolicy.ForceApprovalTools...)
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

func (s *memStore) SnapshotConfig(_ context.Context, t *Tenant) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[t.ID]; !ok {
		return 0, ErrNotFound
	}
	hist := s.versions[t.ID]
	v := len(hist) + 1
	s.versions[t.ID] = append(hist, &ConfigVersion{
		Version:   v,
		Config:    cloneTenant(t),
		CreatedAt: time.Now().UTC(),
	})
	return v, nil
}

func (s *memStore) ListConfigVersions(_ context.Context, tenantID string) ([]*ConfigVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hist := s.versions[tenantID]
	out := make([]*ConfigVersion, 0, len(hist))
	// Newest first for display.
	for i := len(hist) - 1; i >= 0; i-- {
		cp := *hist[i]
		cp.Config = cloneTenant(hist[i].Config)
		out = append(out, &cp)
	}
	return out, nil
}

func (s *memStore) GetConfigVersion(_ context.Context, tenantID string, version int) (*ConfigVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hist := s.versions[tenantID]
	idx := version - 1 // versions are dense 1..N in memory
	if idx < 0 || idx >= len(hist) || hist[idx].Version != version {
		return nil, ErrConfigVersionNotFound
	}
	cp := *hist[idx]
	cp.Config = cloneTenant(hist[idx].Config)
	return &cp, nil
}
