// Package sessioncoord coordinates ordered, fenced session writes.
package sessioncoord

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

var (
	// ErrLeaseHeld indicates that another owner still has a live lease.
	ErrLeaseHeld = errors.New("sessioncoord: lease held")
	// ErrStaleFence indicates that an expired owner attempted a write.
	ErrStaleFence = errors.New("sessioncoord: stale fence")
)

// Lease grants temporary ownership and a monotonically increasing fencing token.
type Lease struct {
	Key       gateway.SessionKey
	Owner     string
	Token     uint64
	ExpiresAt time.Time
}

// FenceAdvancer publishes a new token before a Worker begins execution.
type FenceAdvancer interface {
	AdvanceFence(context.Context, gateway.SessionKey, uint64) error
}

// FenceReader lets a volatile lease backend seed its counter from the
// persistent session fence after restart or failover.
type FenceReader interface {
	CurrentFence(context.Context, gateway.SessionKey) (uint64, error)
}

// LeaseCoordinator is implemented by the local reference and Redis coordinator.
type LeaseCoordinator interface {
	Acquire(context.Context, gateway.SessionKey, string, time.Duration) (Lease, error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Release(Lease)
}

type leaseState struct {
	owner     string
	token     uint64
	expiresAt time.Time
}

// Coordinator is a concurrency-safe single-process lease reference.
// A Redis implementation must use equivalent atomic INCR + owner/TTL Lua semantics.
type Coordinator struct {
	mu     sync.Mutex
	leases map[gateway.SessionKey]leaseState
	fencer FenceAdvancer
	now    func() time.Time
}

// NewCoordinator creates a coordinator that advances the write fence on acquire.
func NewCoordinator(fencer FenceAdvancer) (*Coordinator, error) {
	if fencer == nil {
		return nil, errors.New("sessioncoord: nil fence store")
	}
	return &Coordinator{leases: make(map[gateway.SessionKey]leaseState), fencer: fencer, now: time.Now}, nil
}

// Acquire grants a new monotonically fenced lease when no live owner exists.
func (coordinator *Coordinator) Acquire(ctx context.Context, key gateway.SessionKey, owner string, ttl time.Duration) (Lease, error) {
	if owner == "" || ttl <= 0 {
		return Lease{}, errors.New("sessioncoord: owner and positive ttl are required")
	}
	coordinator.mu.Lock()
	current := coordinator.leases[key]
	now := coordinator.now().UTC()
	if current.owner != "" && now.Before(current.expiresAt) {
		coordinator.mu.Unlock()
		return Lease{}, ErrLeaseHeld
	}
	current.token++
	current.owner = owner
	current.expiresAt = now.Add(ttl)
	coordinator.leases[key] = current
	coordinator.mu.Unlock()
	if err := coordinator.fencer.AdvanceFence(ctx, key, current.token); err != nil {
		coordinator.mu.Lock()
		failed := coordinator.leases[key]
		failed.owner = ""
		failed.expiresAt = time.Time{}
		coordinator.leases[key] = failed
		coordinator.mu.Unlock()
		return Lease{}, err
	}
	return Lease{Key: key, Owner: owner, Token: current.token, ExpiresAt: current.expiresAt}, nil
}

// Renew extends an unexpired lease only for the exact owner and token.
func (coordinator *Coordinator) Renew(_ context.Context, lease Lease, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, errors.New("sessioncoord: positive ttl is required")
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	current := coordinator.leases[lease.Key]
	now := coordinator.now().UTC()
	if current.owner != lease.Owner || current.token != lease.Token || !now.Before(current.expiresAt) {
		return Lease{}, ErrStaleFence
	}
	current.expiresAt = now.Add(ttl)
	coordinator.leases[lease.Key] = current
	lease.ExpiresAt = current.expiresAt
	return lease, nil
}

// Release relinquishes ownership without reducing the fencing token.
func (coordinator *Coordinator) Release(lease Lease) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	current := coordinator.leases[lease.Key]
	if current.owner == lease.Owner && current.token == lease.Token {
		current.owner = ""
		current.expiresAt = time.Time{}
		coordinator.leases[lease.Key] = current
	}
}

// CurrentFence returns the latest token for tests and diagnostics.
func (coordinator *Coordinator) CurrentFence(key gateway.SessionKey) uint64 {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.leases[key].token
}
