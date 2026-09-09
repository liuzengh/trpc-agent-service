//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// DefaultLockTTL is the lease lifetime for a session lock. It is deliberately
	// short: a long-running operation keeps the lock alive by renewal rather than
	// by holding a TTL long enough to cover the worst-case model latency. A long
	// TTL would delay takeover for that same duration after a worker crash.
	DefaultLockTTL = 30 * time.Second

	// renewalDivisor sets the renewal interval to TTL/3, giving two renewal
	// attempts before a lease would otherwise expire.
	renewalDivisor = 3

	// leaseOperationTimeout bounds one Redis round trip. A client-side timeout
	// is required even when the caller supplied a context without a deadline:
	// a half-open Redis connection must not pin a renewal goroutine forever.
	leaseOperationTimeout = 5 * time.Second
)

// ErrStaleWriter indicates the caller's invocation lease was lost. The Worker
// cancels/rejects that invocation; durable queue writes use independent,
// monotonic PostgreSQL fencing versions.
var ErrStaleWriter = errors.New("session invocation lease no longer held")

// Lease is a session lock that renews itself in the background for as long as
// the caller holds it, so an operation slower than the lock TTL does not lose
// the lock mid-flight.
type Lease struct {
	lock    *SessionLock
	manager *SessionLockManager

	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	lostErr error
}

// AcquireLease acquires a session lock and starts renewing it every ttl/3 until
// Release is called. A renewal that finds the token no longer present marks the
// lease lost, and subsequent guarded writes fail with ErrStaleWriter.
func (m *SessionLockManager) AcquireLease(ctx context.Context, key string, ttl time.Duration) (*Lease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ttl <= 0 {
		ttl = DefaultLockTTL
	}

	lock, err := m.AcquireLock(ctx, key, ttl)
	if err != nil {
		return nil, err
	}

	// The renewal loop must outlive the request context that started it only
	// until Release; it is cancelled explicitly rather than tied to ctx so a
	// cancelled request does not silently stop renewing a still-held lock.
	renewCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	l := &Lease{
		lock:    lock,
		manager: m,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	go l.renewLoop(renewCtx, ttl)
	return l, nil
}

// renewLoop extends the lease until cancelled or lost.
func (l *Lease) renewLoop(ctx context.Context, ttl time.Duration) {
	defer close(l.done)

	interval := ttl / renewalDivisor
	if interval <= 0 {
		interval = time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			opCtx, cancel := context.WithTimeout(ctx, leaseOperationTimeout)
			err := l.manager.ExtendLock(opCtx, l.lock, ttl)
			cancel()
			if err != nil {
				// Losing the lease is terminal: another worker now owns the
				// session, and re-acquiring here would defeat fencing.
				l.markLost(fmt.Errorf("%w: %v", ErrStaleWriter, err))
				return
			}
		}
	}
}

// markLost records the first error that invalidated the lease.
func (l *Lease) markLost(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lostErr == nil {
		l.lostErr = err
	}
}

// Err reports why the lease was lost, or nil while it is still held.
func (l *Lease) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lostErr
}

// Done closes when renewal stops because the lease was released or ownership
// was lost. Callers can cancel long-running model/tool work on lease loss.
func (l *Lease) Done() <-chan struct{} {
	if l == nil {
		return nil
	}
	return l.done
}

// Token returns the opaque ownership token of the underlying lock.
func (l *Lease) Token() string {
	if l == nil || l.lock == nil {
		return ""
	}
	return l.lock.Token
}

// Release stops renewal and releases the lock if this lease still owns it.
func (l *Lease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	l.cancel()
	// Release is a lifecycle operation and should still try to clean up after
	// the request context is cancelled, but it must never wait indefinitely for
	// a renewal call that is stuck in a broken network path.
	waitCtx, cancelWait := context.WithTimeout(context.WithoutCancel(ctx), leaseOperationTimeout)
	defer cancelWait()
	select {
	case <-l.done:
	case <-waitCtx.Done():
		return waitCtx.Err()
	}

	// A lost lease no longer owns the key; releasing would be a no-op at best
	// and, without the token check inside ReleaseLock, could free another
	// worker's lock.
	if l.Err() != nil {
		return l.Err()
	}
	releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), leaseOperationTimeout)
	defer cancelRelease()
	return l.manager.ReleaseLock(releaseCtx, l.lock)
}
