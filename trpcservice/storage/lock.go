package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lock is a session-scoped distributed lock (key
// lock:sess:{app_id}:{session_id}). It serializes concurrent processing of
// the same session across worker replicas. The app dimension matches the
// session store's (app_id, session_key) identity: two tenants' users can
// carry the same channel:user session key, and one tenant's long run must
// never block the other's. The (session_id, event_seq) unique
// constraint remains the last-resort backstop if the lock is ever lost.
type Lock struct {
	rdb *redis.Client
}

// NewLock creates a Lock on an established Redis client.
func NewLock(rdb *redis.Client) *Lock {
	return &Lock{rdb: rdb}
}

func lockKey(appID, sessionID string) string {
	return "lock:sess:" + appID + ":" + sessionID
}

// TryAcquire attempts SET NX EX once; ok=false means another worker holds it.
func (l *Lock) TryAcquire(ctx context.Context, appID, sessionID, owner string, ttl time.Duration) (bool, error) {
	ok, err := l.rdb.SetNX(ctx, lockKey(appID, sessionID), owner, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("lock acquire %s: %w", sessionID, err)
	}
	return ok, nil
}

// releaseScript deletes the key only when the value still belongs to us, so a
// lock that expired and was re-acquired by someone else is never deleted.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end`)

// releaseKey deletes the key only when the value still belongs to us, so a
// lock that expired and was re-acquired by someone else is never deleted.
func releaseKey(ctx context.Context, rdb *redis.Client, key, owner string) error {
	if err := releaseScript.Run(ctx, rdb, []string{key}, owner).Err(); err != nil {
		return fmt.Errorf("lock release %s: %w", key, err)
	}
	return nil
}

// Release frees the lock if and only if we still own it.
func (l *Lock) Release(ctx context.Context, appID, sessionID, owner string) error {
	return releaseKey(ctx, l.rdb, lockKey(appID, sessionID), owner)
}

// extendScript refreshes the TTL only when we still own the lock.
var extendScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end`)

// extendKey refreshes the TTL only when we still own the lock; ok=false means
// it is no longer ours (expired or taken over).
func extendKey(ctx context.Context, rdb *redis.Client, key, owner string, ttl time.Duration) (bool, error) {
	n, err := extendScript.Run(ctx, rdb, []string{key}, owner, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("lock extend %s: %w", key, err)
	}
	return n == 1, nil
}

// Extend renews the TTL; ok=false means the lock is no longer ours (expired
// or taken over) and the caller should consider the session unprotected.
func (l *Lock) Extend(ctx context.Context, appID, sessionID, owner string, ttl time.Duration) (bool, error) {
	return extendKey(ctx, l.rdb, lockKey(appID, sessionID), owner, ttl)
}
