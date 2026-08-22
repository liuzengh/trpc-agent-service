package memory

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Scope string

const (
	ScopeSession Scope = "session"
	ScopeUser    Scope = "user"
	ScopeTenant  Scope = "tenant"
)

type Memory struct {
	TenantID  string    `json:"tenant_id"`
	ID        string    `json:"memory_id"`
	Scope     Scope     `json:"scope"`
	ScopeID   string    `json:"scope_id"`
	SessionID string    `json:"session_id,omitempty"`
	Kind      string    `json:"kind"`
	Content   string    `json:"content"`
	VectorRef string    `json:"vector_ref,omitempty"`
	Version   int64     `json:"version"`
	SourceSeq int64     `json:"source_seq"`
	Deleted   bool      `json:"deleted"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

var ErrInvalidMemory = errors.New("invalid memory")

func (m Memory) Validate() error {
	for name, value := range map[string]string{"tenant_id": m.TenantID, "memory_id": m.ID, "scope_id": m.ScopeID, "kind": m.Kind} {
		if strings.TrimSpace(value) == "" || len(value) > 256 {
			return fmt.Errorf("%w: %s is required and bounded", ErrInvalidMemory, name)
		}
	}
	switch m.Scope {
	case ScopeSession:
		if strings.TrimSpace(m.SessionID) == "" || m.ScopeID != m.SessionID {
			return fmt.Errorf("%w: session scope requires matching session_id", ErrInvalidMemory)
		}
	case ScopeUser, ScopeTenant:
	default:
		return fmt.Errorf("%w: invalid scope %q", ErrInvalidMemory, m.Scope)
	}
	if m.Version < 1 || m.SourceSeq < 0 {
		return fmt.Errorf("%w: invalid version or source sequence", ErrInvalidMemory)
	}
	if strings.TrimSpace(m.Content) == "" && !m.Deleted {
		return fmt.Errorf("%w: content is required unless deleted", ErrInvalidMemory)
	}
	if !m.CreatedAt.IsZero() && !m.UpdatedAt.IsZero() && m.UpdatedAt.Before(m.CreatedAt) {
		return fmt.Errorf("%w: updated_at precedes created_at", ErrInvalidMemory)
	}
	return nil
}
