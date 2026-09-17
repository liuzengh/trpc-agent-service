// Package coordination defines session ownership and fencing contracts.
package coordination

import (
	"context"
	"time"
)

type SessionKey struct{ TenantID, AgentAppID, SessionID string }
type Lease struct {
	Session   SessionKey
	WorkerID  string
	LeaseID   string
	Fence     uint64
	ExpiresAt time.Time
}

type LeaseManager interface {
	Acquire(context.Context, SessionKey, string, time.Duration) (Lease, error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
	EnsureFenceAtLeast(context.Context, SessionKey, uint64) error
}
