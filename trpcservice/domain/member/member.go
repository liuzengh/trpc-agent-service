package member

import (
	"context"
	"fmt"
	"time"
)

// Member represents a tenant member with RBAC role.
type Member struct {
	TenantID  string    `json:"tenant_id"`
	UserID    string    `json:"user_id"`
	Role      string    `json:"role"` // owner | admin | member
	Password  string    `json:"-"`    // bcrypt hash; never serialized
	CreatedAt time.Time `json:"created_at"`
}

const (
	RoleOwner  = "owner"
	RoleAdmin  = "admin"
	RoleMember = "member"
)

func (m *Member) Validate() error {
	if m.TenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if m.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	switch m.Role {
	case RoleOwner, RoleAdmin, RoleMember:
	default:
		return fmt.Errorf("invalid role %q: must be owner/admin/member", m.Role)
	}
	return nil
}

func (m *Member) IsAdmin() bool { return m.Role == RoleOwner || m.Role == RoleAdmin }

// Store persists member data.
type Store interface {
	Create(ctx context.Context, m *Member) error
	Get(ctx context.Context, tenantID, userID string) (*Member, error)
	GetByUserID(ctx context.Context, userID string) (*Member, error)
	List(ctx context.Context, tenantID string) ([]*Member, error)
	UpdatePassword(ctx context.Context, tenantID, userID, passwordHash string) error
	UpdateRole(ctx context.Context, tenantID, userID, role string) error
	Delete(ctx context.Context, tenantID, userID string) error
}

// Manager provides member management operations.
type Manager struct {
	store Store
}

func NewManager() *Manager {
	return &Manager{store: NewMemStore()}
}

func NewManagerWithStore(s Store) *Manager {
	return &Manager{store: s}
}

func (mgr *Manager) Create(ctx context.Context, m *Member) error {
	return mgr.store.Create(ctx, m)
}

func (mgr *Manager) Get(ctx context.Context, tenantID, userID string) (*Member, error) {
	return mgr.store.Get(ctx, tenantID, userID)
}

func (mgr *Manager) GetByUserID(ctx context.Context, userID string) (*Member, error) {
	return mgr.store.GetByUserID(ctx, userID)
}

func (mgr *Manager) List(ctx context.Context, tenantID string) ([]*Member, error) {
	return mgr.store.List(ctx, tenantID)
}

func (mgr *Manager) UpdatePassword(ctx context.Context, tenantID, userID, passwordHash string) error {
	return mgr.store.UpdatePassword(ctx, tenantID, userID, passwordHash)
}

func (mgr *Manager) UpdateRole(ctx context.Context, tenantID, userID, role string) error {
	return mgr.store.UpdateRole(ctx, tenantID, userID, role)
}

func (mgr *Manager) Delete(ctx context.Context, tenantID, userID string) error {
	return mgr.store.Delete(ctx, tenantID, userID)
}
