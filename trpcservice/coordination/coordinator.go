// Package coordination serializes complete Agent turns for one Session.
package coordination

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrCoordinatorClosed means the coordinator no longer accepts leases.
	ErrCoordinatorClosed = errors.New("session coordinator is closed")
	// ErrLeaseLost means the caller can no longer safely continue its turn.
	ErrLeaseLost = errors.New("session lease lost")
)

// Key identifies the complete Session scope protected by one lease.
type Key struct {
	AppName   string
	UserID    string
	SessionID string
}

// Validate rejects incomplete coordination keys.
func (k Key) Validate() error {
	if strings.TrimSpace(k.AppName) == "" {
		return fmt.Errorf("coordination app name is required")
	}
	if strings.TrimSpace(k.UserID) == "" {
		return fmt.Errorf("coordination user ID is required")
	}
	if strings.TrimSpace(k.SessionID) == "" {
		return fmt.Errorf("coordination session ID is required")
	}
	return nil
}

// Coordinator grants exclusive execution rights for a Session.
type Coordinator interface {
	Acquire(ctx context.Context, key Key) (Lease, error)
	Ready(ctx context.Context) error
	Close() error
}

// Lease remains valid while Context is active. A distributed coordinator
// cancels Context when it can no longer prove ownership of the lease.
type Lease interface {
	Context() context.Context
	FencingToken() int64
	Release(ctx context.Context) error
}

type fencingTokenContextKey struct{}

// ContextWithFencingToken makes the acquired token available to downstream
// storage wrappers, plugins and tools. SessionRouter persists the highest token
// observed at its synchronized storage boundary and rejects older mutations.
func ContextWithFencingToken(ctx context.Context, token int64) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, fencingTokenContextKey{}, token)
}

// FencingTokenFromContext returns the current Session lease token.
func FencingTokenFromContext(ctx context.Context) (int64, bool) {
	if ctx == nil {
		return 0, false
	}
	token, ok := ctx.Value(fencingTokenContextKey{}).(int64)
	return token, ok && token > 0
}
