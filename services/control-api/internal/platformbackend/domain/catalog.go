// Package domain defines the non-secret tenant-visible platform backend catalog.
// Physical connection targets and credentials deliberately do not belong here.
package domain

import (
	"errors"
	"regexp"
	"sort"
)

type Kind string
type Role string

const (
	PostgreSQL Kind = "postgresql"
	Redis      Kind = "redis"
	Qdrant     Kind = "qdrant"
	S3         Kind = "s3"
	Session    Role = "session"
	Memory     Role = "memory"
	Knowledge  Role = "knowledge"
	Artifact   Role = "artifact"
)

var (
	ErrInvalid = errors.New("BACKEND_CONFIG_INVALID")
	// Missing and unauthorized selections share an external error to prevent enumeration.
	ErrNotAvailable     = errors.New("BACKEND_NOT_AVAILABLE")
	ErrUnavailable      = errors.New("BACKEND_UNAVAILABLE")
	ErrRevisionConflict = errors.New("BACKEND_REVISION_CONFLICT")
	ErrCapability       = errors.New("CAPABILITY_MISMATCH")
)
var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$`)

// Entry is platform-controlled metadata, never a tenant-submitted registration.
// Roles are explicit; support by an implementation does not enable a role by default.
type Entry struct {
	ID        string
	Revision  uint64
	Label     string
	Kind      Kind
	Roles     []Role
	Enabled   bool
	TenantIDs []string
}

// View is the only catalog shape intended for public serialization.
// No target, credentials or tenant allowlist can leak through this projection.
type View struct {
	ID        string `json:"id"`
	Revision  uint64 `json:"revision"`
	Label     string `json:"label"`
	Kind      Kind   `json:"kind"`
	Roles     []Role `json:"roles"`
	Available bool   `json:"available"`
}

type Selection struct {
	BackendID string `json:"backend_id"`
	Revision  uint64 `json:"revision"`
	Role      Role   `json:"role"`
}

// Catalog is an immutable validated value; all returned slices are owned by callers.
type Catalog struct{ entries map[string]Entry }

func NewCatalog(entries []Entry) (*Catalog, error) {
	c := &Catalog{entries: make(map[string]Entry, len(entries))}
	for _, e := range entries {
		if !identifier.MatchString(e.ID) || e.Revision == 0 || e.Revision > 9007199254740991 || len(e.Label) == 0 || len(e.Label) > 256 || len(e.Roles) == 0 || len(e.TenantIDs) == 0 {
			return nil, ErrInvalid
		}
		if _, exists := c.entries[e.ID]; exists {
			return nil, ErrInvalid
		}
		roles := make(map[Role]bool)
		for _, role := range e.Roles {
			if roles[role] || !supports(e.Kind, role) {
				return nil, ErrInvalid
			}
			roles[role] = true
		}
		tenants := make(map[string]bool)
		for _, t := range e.TenantIDs {
			if !identifier.MatchString(t) || tenants[t] {
				return nil, ErrInvalid
			}
			tenants[t] = true
		}
		e.Roles = append([]Role(nil), e.Roles...)
		e.TenantIDs = append([]string(nil), e.TenantIDs...)
		sort.Slice(e.Roles, func(i, j int) bool { return e.Roles[i] < e.Roles[j] })
		c.entries[e.ID] = e
	}
	return c, nil
}

func supports(k Kind, r Role) bool {
	switch k {
	case PostgreSQL:
		return r == Session || r == Memory
	case Redis:
		return r == Session || r == Memory
	case Qdrant:
		return r == Knowledge
	case S3:
		return r == Artifact
	default:
		return false
	}
}
func allowed(e Entry, tenant string) bool {
	for _, t := range e.TenantIDs {
		if t == tenant {
			return true
		}
	}
	return false
}
func view(e Entry) View {
	return View{ID: e.ID, Revision: e.Revision, Label: e.Label, Kind: e.Kind, Roles: append([]Role(nil), e.Roles...), Available: e.Enabled}
}

// List includes disabled authorized entries so clients can explain stale selections
// rather than silently replacing them. Tenant identity must come from authentication.
func (c *Catalog) List(tenant string) ([]View, error) {
	if c == nil || !identifier.MatchString(tenant) {
		return nil, ErrNotAvailable
	}
	out := make([]View, 0)
	for _, e := range c.entries {
		if allowed(e, tenant) {
			out = append(out, view(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Resolve validates one explicit role and fixed revision; it never chooses a fallback.
// Callers still need membership authorization and physical adapter compatibility checks.
func (c *Catalog) Resolve(tenant string, s Selection) (View, error) {
	if c == nil || !identifier.MatchString(tenant) {
		return View{}, ErrNotAvailable
	}
	e, ok := c.entries[s.BackendID]
	if !ok || !allowed(e, tenant) {
		return View{}, ErrNotAvailable
	}
	if !e.Enabled {
		return View{}, ErrUnavailable
	}
	if s.Revision == 0 || s.Revision != e.Revision {
		return View{}, ErrRevisionConflict
	}
	for _, r := range e.Roles {
		if r == s.Role {
			return view(e), nil
		}
	}
	return View{}, ErrCapability
}
