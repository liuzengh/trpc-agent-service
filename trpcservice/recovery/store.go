// Package recovery provides the tenant-scoped operator workflow for durable
// Inbox and Outbox failures. It intentionally exposes metadata only; message
// payloads and provider errors stay inside the runtime database.
package recovery

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("recovery: message not found")
	ErrConflict = errors.New("recovery: message state conflict")
	ErrInvalid  = errors.New("recovery: invalid operation")
)

type Kind string

const (
	KindInbox  Kind = "inbox"
	KindOutbox Kind = "outbox"
)

type Status string

const (
	StatusDLQ       Status = "dlq"
	StatusUncertain Status = "uncertain"
	StatusRetry     Status = "retry"
	StatusSent      Status = "sent"
)

// Item is the deliberately limited Admin API projection. It never contains a
// message payload, external identity, recipient, session, or last-error text.
type Item struct {
	TenantID string    `json:"tenant_id"`
	Kind     Kind      `json:"kind"`
	ID       string    `json:"id"`
	AppID    string    `json:"app_id"`
	Binding  string    `json:"binding_id"`
	Status   Status    `json:"status"`
	Attempts int       `json:"attempts"`
	TraceID  string    `json:"trace_id,omitempty"`
	Created  time.Time `json:"created_at"`
}

// Store performs state transitions with a tenant/id/expected-status CAS.
type Store interface {
	List(context.Context, string, Kind, []Status, int) ([]Item, error)
	Redrive(context.Context, string, Kind, string, Status) (Item, error)
	ResolveOutbox(context.Context, string, string, Status, Status) (Item, error)
}
