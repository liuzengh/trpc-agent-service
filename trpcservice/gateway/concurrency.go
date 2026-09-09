// Package gateway provides the platform-independent inbound routing pipeline.
package gateway

import (
	"context"
	"errors"
)

var (
	// ErrDuplicate means an inbound idempotency key is already claimed or done.
	ErrDuplicate = errors.New("message is duplicate")
	// ErrHeld means another Worker currently owns the session lock.
	ErrHeld = errors.New("session lock is held")
	// ErrLeaseLost means this Worker no longer owns a previously acquired lock.
	ErrLeaseLost = errors.New("session lock lease was lost")
)

// Deduper implements Claim/Mark/Release two-phase inbound idempotency.
type Deduper interface {
	Claim(ctx context.Context, key string) (token string, err error)
	Mark(ctx context.Context, key, token string) error
	Release(ctx context.Context, key, token string) error
}

// SessionLock serializes execution and session writes per session.
type SessionLock interface {
	Acquire(ctx context.Context, sessionID string) (Lease, error)
}

// Lease is an owned session lock.
type Lease interface {
	Release(ctx context.Context) error
	// Lost reports lease loss. The returned channel yields the loss reason
	// exactly once and may be nil when the implementation cannot detect loss.
	Lost() <-chan error
}

// Debouncer coalesces multiple schedules for one session.
type Debouncer interface {
	Schedule(key string, flush func())
	FlushAll()
}
