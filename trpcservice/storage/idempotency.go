// Package storage defines durable state contracts used by the execution plane.
package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

var (
	// ErrLeaseLost indicates a caller tried to finish or release a lease it no
	// longer owns, usually because its TTL elapsed or another worker recovered it.
	ErrLeaseLost = errors.New("idempotency lease lost")
)

// LeaseState describes whether a message may enter execution.
type LeaseState string

const (
	// LeaseAcquired permits the caller that owns Lease to execute the message.
	LeaseAcquired LeaseState = "acquired"
	// LeaseInProgress indicates another worker owns the active message lease.
	LeaseInProgress LeaseState = "in_progress"
	// LeaseAlreadyCompleted indicates the message's business effect was already
	// committed and the caller must not execute it again.
	LeaseAlreadyCompleted LeaseState = "already_completed"
)

// Lease is an opaque ownership token for one idempotency key.
type Lease struct {
	Key   string
	token string
}

// AcquireResult contains the decision made atomically by an idempotency store.
type AcquireResult struct {
	State LeaseState
	Lease Lease
}

// IdempotencyStore provides atomic, shared message execution leases. A durable
// implementation is required in production so retries across processes remain
// safe after a worker restart.
type IdempotencyStore interface {
	Acquire(ctx context.Context, key string, processingTTL time.Duration) (AcquireResult, error)
	Renew(ctx context.Context, lease Lease, processingTTL time.Duration) error
	Complete(ctx context.Context, lease Lease, completedTTL time.Duration) error
	Release(ctx context.Context, lease Lease) error
}

// BuildIdempotencyKey creates a tenant-scoped key. Equal channel message IDs
// are safe across tenants but must collide within one tenant and channel.
func BuildIdempotencyKey(tenantID string, channel channels.Channel, bindingID, messageID string) (string, error) {
	if strings.TrimSpace(tenantID) == "" {
		return "", fmt.Errorf("idempotency tenant ID is required")
	}
	if !channel.Supported() {
		return "", fmt.Errorf("idempotency channel %q is unsupported", channel)
	}
	if strings.TrimSpace(bindingID) == "" {
		return "", fmt.Errorf("idempotency binding ID is required")
	}
	if strings.TrimSpace(messageID) == "" {
		return "", fmt.Errorf("idempotency message ID is required")
	}
	return "idempotency:" + encodeKeyPart(tenantID) + encodeKeyPart(string(channel)) + encodeKeyPart(bindingID) + encodeKeyPart(messageID), nil
}

func encodeKeyPart(value string) string {
	return fmt.Sprintf("%d:%s", len(value), value)
}

// MemoryIdempotencyStore is a per-instance deterministic test/development
// implementation. It is not a replacement for a shared durable store.
type MemoryIdempotencyStore struct {
	mu      sync.Mutex
	entries map[string]memoryLeaseEntry
	now     func() time.Time
}

type memoryLeaseEntry struct {
	token     string
	completed bool
	expiresAt time.Time
}

// NewMemoryIdempotencyStore constructs an isolated in-memory store.
func NewMemoryIdempotencyStore() *MemoryIdempotencyStore {
	return &MemoryIdempotencyStore{
		entries: make(map[string]memoryLeaseEntry),
		now:     time.Now,
	}
}

// Acquire atomically grants one processing lease, observes an active owner, or
// reports an already completed message.
func (s *MemoryIdempotencyStore) Acquire(ctx context.Context, key string, processingTTL time.Duration) (AcquireResult, error) {
	if err := ctx.Err(); err != nil {
		return AcquireResult{}, err
	}
	if strings.TrimSpace(key) == "" {
		return AcquireResult{}, fmt.Errorf("idempotency key is required")
	}
	if processingTTL <= 0 {
		return AcquireResult{}, fmt.Errorf("processing lease TTL must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, exists := s.entries[key]; exists {
		if entry.expiresAt.After(s.now()) {
			if entry.completed {
				return AcquireResult{State: LeaseAlreadyCompleted}, nil
			}
			return AcquireResult{State: LeaseInProgress}, nil
		}
		delete(s.entries, key)
	}

	token, err := newLeaseToken()
	if err != nil {
		return AcquireResult{}, err
	}
	s.entries[key] = memoryLeaseEntry{token: token, expiresAt: s.now().Add(processingTTL)}
	return AcquireResult{
		State: LeaseAcquired,
		Lease: Lease{Key: key, token: token},
	}, nil
}

// Renew extends an owned processing lease without changing its ownership token.
func (s *MemoryIdempotencyStore) Renew(ctx context.Context, lease Lease, processingTTL time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if processingTTL <= 0 {
		return fmt.Errorf("processing lease TTL must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.entries[lease.Key]
	if !exists || entry.completed || entry.token != lease.token || !entry.expiresAt.After(s.now()) {
		return ErrLeaseLost
	}
	entry.expiresAt = s.now().Add(processingTTL)
	s.entries[lease.Key] = entry
	return nil
}

// Complete marks a lease as finished for the completed-message retention TTL.
func (s *MemoryIdempotencyStore) Complete(ctx context.Context, lease Lease, completedTTL time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if completedTTL <= 0 {
		return fmt.Errorf("completed lease TTL must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.entries[lease.Key]
	if !exists || entry.completed || entry.token != lease.token || !entry.expiresAt.After(s.now()) {
		return ErrLeaseLost
	}
	entry.completed = true
	entry.expiresAt = s.now().Add(completedTTL)
	s.entries[lease.Key] = entry
	return nil
}

// Release removes an owned processing lease after a retryable failure.
func (s *MemoryIdempotencyStore) Release(ctx context.Context, lease Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.entries[lease.Key]
	if !exists || entry.completed || entry.token != lease.token {
		return ErrLeaseLost
	}
	delete(s.entries, lease.Key)
	return nil
}

// RedisIdempotencyStore is the production-capable shared implementation. Its
// Lua scripts make acquire, complete, and release compare-and-set operations
// atomic across all Gateway and Worker processes.
type RedisIdempotencyStore struct {
	client redis.UniversalClient
}

// NewRedisIdempotencyStore constructs a store around an injected, configured
// Redis client. The composition root owns connection and timeout settings.
func NewRedisIdempotencyStore(client redis.UniversalClient) (*RedisIdempotencyStore, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis client is required")
	}
	return &RedisIdempotencyStore{client: client}, nil
}

// Acquire obtains a shared Redis processing lease.
func (s *RedisIdempotencyStore) Acquire(ctx context.Context, key string, processingTTL time.Duration) (AcquireResult, error) {
	if err := ctx.Err(); err != nil {
		return AcquireResult{}, err
	}
	if strings.TrimSpace(key) == "" {
		return AcquireResult{}, fmt.Errorf("idempotency key is required")
	}
	if processingTTL <= 0 {
		return AcquireResult{}, fmt.Errorf("processing lease TTL must be positive")
	}
	token, err := newLeaseToken()
	if err != nil {
		return AcquireResult{}, err
	}

	result, err := redisAcquireScript.Run(ctx, s.client, []string{key}, token, processingTTL.Milliseconds()).Int64()
	if err != nil {
		return AcquireResult{}, fmt.Errorf("acquire Redis idempotency lease: %w", err)
	}
	switch result {
	case 1:
		return AcquireResult{State: LeaseAcquired, Lease: Lease{Key: key, token: token}}, nil
	case 2:
		return AcquireResult{State: LeaseAlreadyCompleted}, nil
	case 3:
		return AcquireResult{State: LeaseInProgress}, nil
	default:
		return AcquireResult{}, fmt.Errorf("unexpected Redis lease state %d", result)
	}
}

// Renew atomically extends an owned Redis processing lease.
func (s *RedisIdempotencyStore) Renew(ctx context.Context, lease Lease, processingTTL time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if processingTTL <= 0 {
		return fmt.Errorf("processing lease TTL must be positive")
	}
	result, err := redisRenewScript.Run(ctx, s.client, []string{lease.Key}, lease.token, processingTTL.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("renew Redis idempotency lease: %w", err)
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

// Complete atomically converts an owned processing lease into a completed mark.
func (s *RedisIdempotencyStore) Complete(ctx context.Context, lease Lease, completedTTL time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if completedTTL <= 0 {
		return fmt.Errorf("completed lease TTL must be positive")
	}
	result, err := redisCompleteScript.Run(ctx, s.client, []string{lease.Key}, lease.token, completedTTL.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("complete Redis idempotency lease: %w", err)
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

// Release atomically removes a still-owned processing lease.
func (s *RedisIdempotencyStore) Release(ctx context.Context, lease Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := redisReleaseScript.Run(ctx, s.client, []string{lease.Key}, lease.token).Int64()
	if err != nil {
		return fmt.Errorf("release Redis idempotency lease: %w", err)
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

var redisAcquireScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if not current then
    redis.call("SET", KEYS[1], "p:" .. ARGV[1], "PX", ARGV[2], "NX")
    return 1
end
if current == "d" then
    return 2
end
return 3
`)

var redisCompleteScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == "p:" .. ARGV[1] then
    redis.call("SET", KEYS[1], "d", "PX", ARGV[2])
    return 1
end
return 0
`)

var redisRenewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == "p:" .. ARGV[1] then
    redis.call("PEXPIRE", KEYS[1], ARGV[2])
    return 1
end
return 0
`)

var redisReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == "p:" .. ARGV[1] then
    redis.call("DEL", KEYS[1])
    return 1
end
return 0
`)

func newLeaseToken() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("generate idempotency lease token: %w", err)
	}
	return id.String(), nil
}
