package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ClaimState int

const (
	Claimed ClaimState = iota
	AlreadyProcessing
	AlreadyCompleted
)

var (
	ErrClaimOwnershipLost     = errors.New("coordination: claim ownership lost")
	ErrLockOwnershipLost      = errors.New("coordination: lock ownership lost")
	ErrClaimInProgress        = errors.New("coordination: message claim is still processing")
	ErrRateLimiterUnavailable = errors.New("coordination: shared rate limiter unavailable")
)

// ClaimLease identifies the worker that owns a processing claim. The token is
// deliberately required by Complete and ReleaseClaim so a delayed worker
// cannot overwrite or delete a newer worker's claim after the first lease
// expires.
type ClaimLease struct {
	State ClaimState
	Token string
}

// LockLease exposes lease loss to the caller. Release is idempotent. A nil
// value is never returned on a successful Lock call.
type LockLease struct {
	Release func()
	Lost    <-chan error
}

type Coordinator interface {
	Claim(ctx context.Context, key string, ttl time.Duration) (ClaimLease, error)
	Complete(ctx context.Context, key, ownerToken string, ttl time.Duration) error
	ReleaseClaim(ctx context.Context, key, ownerToken string) error
	SaveResult(ctx context.Context, key, ownerToken string, value any, ttl time.Duration) error
	LoadResult(ctx context.Context, key string, value any) (bool, error)
	DeleteResult(ctx context.Context, key string) error
	Lock(ctx context.Context, key string, ttl time.Duration) (*LockLease, error)
	Close() error
}

// RateLimiter is the shared tenant request limiter. Implementations must make
// the increment and limit check one atomic operation. A false result means the
// tenant has reached its configured fixed-window limit; retryAfter is a best
// effort delay until the next window.
type RateLimiter interface {
	Allow(ctx context.Context, tenantID string, limit int) (allowed bool, retryAfter time.Duration, err error)
}

// TenantFreezeReader is an optional distributed maintenance fence. Workers
// that receive a coordinator implementing it reject new tenant work while a
// backend migration drains already-running turns.
type TenantFreezeReader interface {
	TenantFreeze(ctx context.Context, tenantID, appName string) (migrationID string, frozenAt time.Time, frozen bool, err error)
}

type claim struct {
	state   ClaimState
	owner   string
	expires time.Time
}

type lockEntry struct {
	sem  chan struct{}
	refs int
}

type InMemory struct {
	mu        sync.Mutex
	claims    map[string]claim
	results   map[string]storedResult
	locks     map[string]*lockEntry
	rate      map[string]rateBucket
	nextSweep time.Time
}

type storedResult struct {
	value   []byte
	expires time.Time
}

type rateBucket struct {
	window int64
	count  int
}

func NewInMemory() *InMemory {
	return &InMemory{
		claims: make(map[string]claim), results: make(map[string]storedResult),
		locks: make(map[string]*lockEntry), rate: make(map[string]rateBucket),
	}
}

func (m *InMemory) Ping(context.Context) error { return nil }

// Allow implements the same fixed UTC-minute semantics as the Redis limiter.
// It exists for unit tests and explicitly non-production in-memory runs.
func (m *InMemory) Allow(_ context.Context, tenantID string, limit int) (bool, time.Duration, error) {
	if tenantID == "" {
		return false, 0, errors.New("coordination: empty rate limiter tenant")
	}
	now := time.Now()
	window := now.Unix() / 60
	m.mu.Lock()
	bucket := m.rate[tenantID]
	if bucket.window != window {
		bucket = rateBucket{window: window}
	}
	if limit <= 0 || bucket.count >= limit {
		m.rate[tenantID] = bucket
		m.mu.Unlock()
		return false, nextMinute(now), nil
	}
	bucket.count++
	m.rate[tenantID] = bucket
	m.mu.Unlock()
	return true, 0, nil
}

func nextMinute(now time.Time) time.Duration {
	retryAfter := now.Truncate(time.Minute).Add(time.Minute).Sub(now)
	if retryAfter < time.Second {
		retryAfter = time.Second
	}
	return retryAfter
}

func (m *InMemory) Claim(_ context.Context, key string, ttl time.Duration) (ClaimLease, error) {
	if key == "" {
		return ClaimLease{}, errors.New("coordination: empty claim key")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	m.sweepExpired(now)
	if existing, ok := m.claims[key]; ok && now.Before(existing.expires) {
		return ClaimLease{State: existing.state}, nil
	}
	owner := newOwnerToken()
	m.claims[key] = claim{state: AlreadyProcessing, owner: owner, expires: now.Add(ttl)}
	return ClaimLease{State: Claimed, Token: owner}, nil
}

func (m *InMemory) sweepExpired(now time.Time) {
	if !m.nextSweep.IsZero() && now.Before(m.nextSweep) {
		return
	}
	for key, item := range m.claims {
		if !now.Before(item.expires) {
			delete(m.claims, key)
		}
	}
	for key, item := range m.results {
		if !now.Before(item.expires) {
			delete(m.results, key)
		}
	}
	m.nextSweep = now.Add(time.Minute)
}

func (m *InMemory) Complete(_ context.Context, key, ownerToken string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, ok := m.claims[key]
	now := time.Now()
	if !ok || existing.state != AlreadyProcessing || existing.owner != ownerToken || now.After(existing.expires) {
		return ErrClaimOwnershipLost
	}
	expires := now.Add(ttl)
	m.claims[key] = claim{state: AlreadyCompleted, expires: expires}
	if result, ok := m.results[key]; ok {
		result.expires = expires
		m.results[key] = result
	}
	return nil
}

func (m *InMemory) ReleaseClaim(_ context.Context, key, ownerToken string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.claims[key]; ok && existing.state == AlreadyProcessing && existing.owner == ownerToken {
		delete(m.claims, key)
	}
	return nil
}

func (m *InMemory) SaveResult(_ context.Context, key, ownerToken string, value any, ttl time.Duration) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode pending result: %w", err)
	}
	m.mu.Lock()
	claimValue, ok := m.claims[key]
	if !ok || claimValue.state != AlreadyProcessing || claimValue.owner != ownerToken || time.Now().After(claimValue.expires) {
		m.mu.Unlock()
		return ErrClaimOwnershipLost
	}
	m.results[key] = storedResult{value: encoded, expires: time.Now().Add(ttl)}
	m.mu.Unlock()
	return nil
}

func (m *InMemory) LoadResult(_ context.Context, key string, value any) (bool, error) {
	m.mu.Lock()
	item, ok := m.results[key]
	if ok && time.Now().After(item.expires) {
		delete(m.results, key)
		ok = false
	}
	m.mu.Unlock()
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(item.value, value); err != nil {
		return false, fmt.Errorf("decode pending result: %w", err)
	}
	return true, nil
}

func (m *InMemory) DeleteResult(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.results, key)
	m.mu.Unlock()
	return nil
}

func (m *InMemory) Lock(ctx context.Context, key string, _ time.Duration) (*LockLease, error) {
	if key == "" {
		return nil, errors.New("coordination: empty lock key")
	}
	m.mu.Lock()
	entry := m.locks[key]
	if entry == nil {
		entry = &lockEntry{sem: make(chan struct{}, 1)}
		entry.sem <- struct{}{}
		m.locks[key] = entry
	}
	entry.refs++
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		m.releaseRef(key, entry)
		return nil, fmt.Errorf("acquire session lock: %w", ctx.Err())
	case <-entry.sem:
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			entry.sem <- struct{}{}
			m.releaseRef(key, entry)
		})
	}
	return &LockLease{Release: release, Lost: make(chan error)}, nil
}

func (m *InMemory) releaseRef(key string, entry *lockEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry.refs--
	if entry.refs == 0 && m.locks[key] == entry {
		delete(m.locks, key)
	}
}

func (*InMemory) Close() error { return nil }

func newOwnerToken() string { return uuid.NewString() }
