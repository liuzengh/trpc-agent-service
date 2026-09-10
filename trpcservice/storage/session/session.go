// Package sessionstorage contains the small persistence contract shared by
// the Agent session adapter and runtime storage implementations. It owns data
// shapes that describe session recovery, not execution scheduling.
package sessionstorage

import (
	"context"
	"errors"
	"time"

	storageerrors "github.com/XnLemon/trpc-agent-service/trpcservice/storage/errors"
)

var (
	// ErrNotFound reports a missing tenant-scoped session record.
	ErrNotFound = errors.New("runtime record not found")
	// ErrDuplicate reports an existing session record with the same identity.
	ErrDuplicate = errors.New("runtime record already exists")
	// ErrInvalid reports malformed session persistence input.
	ErrInvalid = errors.New("invalid runtime record")
	// ErrStorage is the stable, redacted storage failure category shared by
	// session and runtime storage adapters.
	ErrStorage = storageerrors.ErrPostgres
)

// Session is the durable tenant-scoped conversation state used by an Agent
// session adapter.
type Session struct {
	TenantID  string
	SessionID string
	Status    string
	Version   int64
	State     map[string]any
	CreatedAt time.Time
	UpdatedAt time.Time
}

// EventPayload is one immutable upstream Runner event retained for durable
// session recovery.
type EventPayload struct {
	TenantID   string
	SessionID  string
	EventID    string
	Payload    []byte
	HistorySeq int64
	CreatedAt  time.Time
}

// SessionStateStore is the session-state persistence capability.
type SessionStateStore interface {
	GetSession(context.Context, string, string) (Session, error)
	CreateSession(context.Context, string, string, map[string]any) (Session, error)
	UpdateSessionState(context.Context, string, string, int64, map[string]any) (Session, error)
	DeleteSession(context.Context, string, string) error
}

// EventHistoryStore is the immutable event-history persistence capability.
type EventHistoryStore interface {
	AppendEventPayload(context.Context, EventPayload) (EventPayload, error)
	ListEventPayloads(context.Context, string, string) ([]EventPayload, error)
}

// ValidateTenant checks the required tenant identity.
func ValidateTenant(tenantID string) error {
	if tenantID == "" {
		return ErrInvalid
	}
	return nil
}

// ValidateSession checks a tenant and session identity pair.
func ValidateSession(tenantID, sessionID string) error {
	if ValidateTenant(tenantID) != nil || sessionID == "" {
		return ErrInvalid
	}
	return nil
}
