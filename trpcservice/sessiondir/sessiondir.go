// Package sessiondir pins every conversation to the agent revision that served
// its first run, so publishing or rolling back a revision never rewrites the
// behaviour of a session that is already in flight.
package sessiondir

import (
	"context"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Key identifies one conversation. Epoch is reserved for a future explicit
// unpin, which will start a new conversation under the same session id; this
// slice only ever writes epoch 0.
type Key struct {
	TenantID    string
	AppID       string
	PrincipalID string
	SessionID   string
	Epoch       uint32
}

// Validate rejects a key that cannot address exactly one conversation.
func (k Key) Validate() error {
	if err := tenant.ValidateResourceID("tenant id", k.TenantID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("app id", k.AppID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("principal id", k.PrincipalID); err != nil {
		return err
	}
	return tenant.ValidateResourceID("session id", k.SessionID)
}

// Directory stores the revision pin of every session. EnsurePin must be atomic:
// concurrent first runs of one session have to agree on a single winner.
type Directory interface {
	GetPin(ctx context.Context, key Key) (string, bool, error)
	EnsurePin(ctx context.Context, key Key, candidateRevisionID string) (string, error)
}

// MemoryDirectory is the process-local reference implementation. One mutex is
// enough because every operation is a single map access; callers must not hold
// a Directory call across a Runtime build or an agent run.
type MemoryDirectory struct {
	mu   sync.Mutex
	pins map[Key]string
}

func NewMemoryDirectory() *MemoryDirectory {
	return &MemoryDirectory{pins: make(map[Key]string)}
}

// GetPin reports the revision an existing session is pinned to.
func (d *MemoryDirectory) GetPin(ctx context.Context, key Key) (string, bool, error) {
	if err := d.validateCall(ctx); err != nil {
		return "", false, err
	}
	if err := key.Validate(); err != nil {
		return "", false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	revisionID, ok := d.pins[key]
	return revisionID, ok, nil
}

// EnsurePin returns the revision this session is pinned to, storing
// candidateRevisionID only when the session has no pin yet. The first write
// under the mutex is the linearization point: every concurrent caller observes
// the same winner, and a caller whose candidate lost must discard it.
func (d *MemoryDirectory) EnsurePin(
	ctx context.Context,
	key Key,
	candidateRevisionID string,
) (string, error) {
	if err := d.validateCall(ctx); err != nil {
		return "", err
	}
	if err := key.Validate(); err != nil {
		return "", err
	}
	if err := tenant.ValidateResourceID("revision id", candidateRevisionID); err != nil {
		return "", err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if pinned, ok := d.pins[key]; ok {
		return pinned, nil
	}
	d.pins[key] = candidateRevisionID
	return candidateRevisionID, nil
}

// Size reports how many sessions are pinned. Until per-tenant quotas exist, a
// caller holding one valid credential can grow this map without bound.
func (d *MemoryDirectory) Size() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pins)
}

func (d *MemoryDirectory) validateCall(ctx context.Context) error {
	if d == nil || d.pins == nil {
		return fmt.Errorf("%w: session directory is not initialised", tenant.ErrInvalidArgument)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is required", tenant.ErrInvalidArgument)
	}
	return ctx.Err()
}
