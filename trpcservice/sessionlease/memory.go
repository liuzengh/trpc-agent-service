package sessionlease

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// MemoryStore is the process-local state a group of coordinators coordinate
// through. Production uses exactly one store and one coordinator per process;
// several coordinators over one store exist so the reference implementation can
// be held to the same contract as a shared backend, where "a Worker stopped
// renewing" and "a Worker closed" are distinct events.
//
// A store is safe for concurrent use. Its zero value is not usable; call
// [NewMemoryStore].
type MemoryStore struct {
	mu     sync.Mutex
	locks  map[sessiondir.Key]memoryLock
	fences map[sessiondir.Key]uint64
}

type memoryLock struct {
	owner   string
	expires time.Time
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		locks:  make(map[sessiondir.Key]memoryLock),
		fences: make(map[sessiondir.Key]uint64),
	}
}

// acquire takes the lock for key unless a live one is already held. It mirrors
// the Redis script: the fence advances only on a successful acquisition, and
// the fence counter is never removed, because monotonicity has to outlive every
// individual lock. Fence entries therefore accumulate for the lifetime of the
// store, which is the same known growth the Redis backend documents.
func (s *MemoryStore) acquire(key sessiondir.Key, owner string, ttl time.Duration) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if lock, held := s.locks[key]; held && now.Before(lock.expires) {
		return 0, false
	}
	fence := s.fences[key] + 1
	s.fences[key] = fence
	s.locks[key] = memoryLock{owner: owner, expires: now.Add(ttl)}
	return fence, true
}

// renew extends the lock only for its current owner.
func (s *MemoryStore) renew(key sessiondir.Key, owner string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	lock, held := s.locks[key]
	if !held || lock.owner != owner || !now.Before(lock.expires) {
		return false
	}
	s.locks[key] = memoryLock{owner: owner, expires: now.Add(ttl)}
	return true
}

// release deletes the lock only for its current owner, so a release that
// arrives after a takeover leaves the new owner alone.
func (s *MemoryStore) release(key sessiondir.Key, owner string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lock, held := s.locks[key]; held && lock.owner == owner {
		delete(s.locks, key)
	}
}

// MemoryCoordinator is the in-process reference implementation of
// [Coordinator]: the local default, and the baseline the conformance suite
// pins every other backend to.
//
// It coordinates the Workers inside one process and nothing else. That makes it
// the right choice for a single-Worker deployment on any store, persistent or
// not, and the wrong one the moment a second Worker is started against the same
// Session store — nothing here would see that Worker. What the process
// configuration can check, it checks: it refuses Redis coordination over
// in-process sessions, because a shared lock over unshared state protects
// nothing. It cannot check the reverse. "One process is the only writer" is an
// operator's promise, not a validated configuration.
//
// It has no unreachable-backend failure mode, so it never returns
// [ErrUnavailable].
type MemoryCoordinator struct {
	store *MemoryStore
	life  *Lifetime
}

var _ Coordinator = (*MemoryCoordinator)(nil)

// NewMemoryCoordinator returns a coordinator over store. Passing a store
// explicitly keeps "which coordinators can see each other" visible at the call
// site; a process that needs only one writes
// NewMemoryCoordinator(NewMemoryStore(), cfg).
func NewMemoryCoordinator(store *MemoryStore, cfg Config) (*MemoryCoordinator, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil store", ErrInvalidConfig)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &MemoryCoordinator{store: store, life: NewLifetime(cfg)}, nil
}

// Acquire implements [Coordinator].
func (c *MemoryCoordinator) Acquire(ctx context.Context, key sessiondir.Key) (Lease, error) {
	if c == nil || c.store == nil {
		return nil, fmt.Errorf("%w: nil coordinator", tenant.ErrInvalidArgument)
	}
	if err := ValidateAcquire(ctx, key); err != nil {
		return nil, err
	}
	if err := c.life.Begin(); err != nil {
		return nil, err
	}
	defer c.life.End()

	// There is no Lifetime.Call here, and nothing to cut short: a map guarded
	// by a mutex answers or blocks for the length of another map operation. The
	// Redis backend, whose acquire is a network call, does need one.
	owner := uuid.NewString()
	ttl := c.life.Config().TTL
	// Read before the lock is taken, never after: the store dates the lock from
	// the moment it takes it, and this must not be later than that.
	acquiredAt := time.Now()
	fence, ok := c.store.acquire(key, owner, ttl)
	if !ok {
		return nil, ErrSessionBusy
	}
	holder := &memoryHolder{store: c.store, key: key, owner: owner, ttl: ttl}
	return c.life.HandOut(ctx, fence, acquiredAt, holder)
}

// Close implements [Coordinator]. Locks this coordinator holds are left to
// expire by TTL rather than deleted, so a run that is still winding down keeps
// the window it was given.
func (c *MemoryCoordinator) Close() error {
	if c == nil {
		return nil
	}
	return c.life.Close()
}

// memoryHolder is one acquisition's view of the store.
type memoryHolder struct {
	store *MemoryStore
	key   sessiondir.Key
	owner string
	ttl   time.Duration
}

var _ Holder = (*memoryHolder)(nil)

// Renew implements [Holder]. A process-local map cannot fail to answer, so this
// never reports the unknown outcome.
func (h *memoryHolder) Renew(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return h.store.renew(h.key, h.owner, h.ttl), nil
}

// Release implements [Holder].
func (h *memoryHolder) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.store.release(h.key, h.owner)
	return nil
}
