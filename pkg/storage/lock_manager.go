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
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

var (
	// ErrLockAcquisitionFailed indicates the lock could not be acquired.
	ErrLockAcquisitionFailed = errors.New("failed to acquire lock")
	// ErrLockNotHeld indicates the lock is not held by the caller.
	ErrLockNotHeld = errors.New("lock not held")
	// ErrLockManagerUnavailable indicates that the manager was not composed
	// with a usable Redis client. Public methods fail closed instead of
	// panicking on a nil dependency.
	ErrLockManagerUnavailable = errors.New("lock manager is unavailable")
	// ErrInvalidLockRequest protects the Redis key and TTL contract at the
	// public seam. Callers must not be able to create unbounded/control-bearing
	// lock keys or an immediately-expiring lease.
	ErrInvalidLockRequest = errors.New("invalid lock request")
)

const maxSessionLockKeyBytes = 512

// Redis PEXPIRE accepts millisecond precision. Rejecting shorter TTLs at the
// public seam prevents a positive duration from being truncated to zero and
// deleting a live lock during renewal.
const minimumSessionLockTTL = time.Millisecond

// SessionLock represents a distributed lock on a session.
//
// A lock may be renewed by a background lease goroutine while other goroutines
// validate it, so the expiry is guarded rather than exported directly.
type SessionLock struct {
	Key        string
	Token      string // Opaque ownership token used by compare-and-delete/renew.
	AcquiredAt time.Time

	mu        sync.Mutex
	expiresAt time.Time
}

// ExpiresAt returns the current local expiry of the lock.
func (l *SessionLock) ExpiresAt() time.Time {
	if l == nil {
		return time.Time{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expiresAt
}

// setExpiresAt records a new local expiry after a successful renewal.
func (l *SessionLock) setExpiresAt(t time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expiresAt = t
}

// SessionLockManager manages distributed locks for session operations.
type SessionLockManager struct {
	redis *redis.Client
}

// NewSessionLockManager creates a new session lock manager.
func NewSessionLockManager(redisClient *redis.Client) *SessionLockManager {
	return &SessionLockManager{
		redis: redisClient,
	}
}

// AcquireLock acquires a distributed lock with the given key and TTL.
// Returns a lock with a unique ownership token used for renewal and release.
func (m *SessionLockManager) AcquireLock(ctx context.Context, key string, ttl time.Duration) (*SessionLock, error) {
	if m == nil || m.redis == nil {
		return nil, ErrLockManagerUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if key == "" || len(key) > maxSessionLockKeyBytes ||
		strings.ContainsAny(key, "\x00\r\n") {
		return nil, ErrInvalidLockRequest
	}
	if ttl < minimumSessionLockTTL {
		return nil, ErrInvalidLockRequest
	}
	// Generate a unique ownership token. This is not a monotonic database
	// fencing token and must not be represented as one.
	token := uuid.New().String()

	// Try to set the lock with NX (only if not exists) and PX (TTL in milliseconds)
	lockKey := fmt.Sprintf("lock:%s", key)

	// Retry with exponential backoff
	maxRetries := 3
	baseDelay := 50 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		success, err := m.redis.SetNX(ctx, lockKey, token, ttl).Result()
		if err != nil {
			return nil, fmt.Errorf("redis error acquiring lock: %w", err)
		}

		if success {
			// Lock acquired
			return &SessionLock{
				Key:        key,
				Token:      token,
				AcquiredAt: time.Now(),
				expiresAt:  time.Now().Add(ttl),
			}, nil
		}

		// Lock already held, wait and retry
		if i < maxRetries-1 {
			delay := baseDelay * time.Duration(1<<uint(i))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// Continue to next retry
			}
		}
	}

	return nil, ErrLockAcquisitionFailed
}

// ReleaseLock releases a distributed lock.
// Uses Lua script to ensure atomic check-and-delete based on token.
func (m *SessionLockManager) ReleaseLock(ctx context.Context, lock *SessionLock) error {
	if m == nil || m.redis == nil {
		return ErrLockManagerUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if lock == nil {
		return nil
	}
	if err := validateLock(lock); err != nil {
		return err
	}

	lockKey := fmt.Sprintf("lock:%s", lock.Key)

	// Lua script to atomically check token and delete
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`

	result, err := m.redis.Eval(ctx, script, []string{lockKey}, lock.Token).Result()
	if err != nil {
		return fmt.Errorf("redis error releasing lock: %w", err)
	}

	if result == int64(0) {
		return ErrLockNotHeld
	}

	return nil
}

// ValidateLock checks if the lock is still held with the correct token (fencing check).
func (m *SessionLockManager) ValidateLock(ctx context.Context, lock *SessionLock) bool {
	if m == nil || m.redis == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if lock == nil {
		return false
	}
	if validateLock(lock) != nil {
		return false
	}

	// Check if lock has expired locally
	if time.Now().After(lock.ExpiresAt()) {
		return false
	}

	lockKey := fmt.Sprintf("lock:%s", lock.Key)

	// Check if token still matches in Redis
	currentToken, err := m.redis.Get(ctx, lockKey).Result()
	if err != nil {
		return false
	}

	return currentToken == lock.Token
}

// ExtendLock extends the TTL of a held lock (for long-running operations).
func (m *SessionLockManager) ExtendLock(ctx context.Context, lock *SessionLock, additionalTTL time.Duration) error {
	if m == nil || m.redis == nil {
		return ErrLockManagerUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if lock == nil {
		return ErrLockNotHeld
	}
	if validateLock(lock) != nil || additionalTTL < minimumSessionLockTTL {
		return ErrInvalidLockRequest
	}

	lockKey := fmt.Sprintf("lock:%s", lock.Key)

	// Lua script to atomically check token and extend TTL
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	result, err := m.redis.Eval(ctx, script, []string{lockKey}, lock.Token, additionalTTL.Milliseconds()).Result()
	if err != nil {
		return fmt.Errorf("redis error extending lock: %w", err)
	}

	if result == int64(0) {
		return ErrLockNotHeld
	}

	// Update local expiration time
	lock.setExpiresAt(time.Now().Add(additionalTTL))

	return nil
}

func validateLock(lock *SessionLock) error {
	if lock == nil || lock.Key == "" || len(lock.Key) > maxSessionLockKeyBytes ||
		strings.ContainsAny(lock.Key, "\x00\r\n") || lock.Token == "" ||
		strings.ContainsAny(lock.Token, "\x00\r\n") {
		return ErrInvalidLockRequest
	}
	return nil
}
