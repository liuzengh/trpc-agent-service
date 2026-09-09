package sessioncoord

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeRedis struct {
	mu       sync.Mutex
	values   map[string]string
	counters map[string]uint64
}

func (redis *fakeRedis) Eval(_ context.Context, script string, keys []string, args ...any) (any, error) {
	redis.mu.Lock()
	defer redis.mu.Unlock()
	if redis.values == nil {
		redis.values = make(map[string]string)
		redis.counters = make(map[string]uint64)
	}
	switch {
	case strings.Contains(script, "INCR"):
		if live := redis.values[keys[0]]; live != "" {
			return []any{int64(0), live}, nil
		}
		redis.counters[keys[1]]++
		value := args[0].(string) + "|" + strconv.FormatUint(redis.counters[keys[1]], 10)
		redis.values[keys[0]] = value
		return []any{int64(redis.counters[keys[1]]), value}, nil
	case strings.Contains(script, "PEXPIRE"):
		if redis.values[keys[0]] != args[0].(string) {
			return int64(0), nil
		}
		return int64(1), nil
	default:
		if redis.values[keys[0]] != args[0].(string) {
			return int64(0), nil
		}
		delete(redis.values, keys[0])
		return int64(1), nil
	}
}

func TestRedisCoordinatorMonotonicFenceAndCompareRelease(t *testing.T) {
	store := NewMemoryWriteStore()
	backend := &fakeRedis{}
	coordinator := &RedisCoordinator{Redis: backend, Fencer: store, Prefix: "test"}
	key := sessionKey()
	first, err := coordinator.Acquire(context.Background(), key, "worker-a", time.Minute)
	if err != nil || first.Token != 1 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if _, err := coordinator.Acquire(context.Background(), key, "worker-b", time.Minute); err != ErrLeaseHeld {
		t.Fatalf("held err=%v", err)
	}
	renewed, err := coordinator.Renew(context.Background(), first, time.Minute)
	if err != nil || renewed.Token != 1 {
		t.Fatalf("renew=%+v err=%v", renewed, err)
	}
	coordinator.Release(first)
	second, err := coordinator.Acquire(context.Background(), key, "worker-b", time.Minute)
	if err != nil || second.Token != 2 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	coordinator.Release(first) // stale release must not remove worker-b.
	if _, err := coordinator.Acquire(context.Background(), key, "worker-c", time.Minute); err != ErrLeaseHeld {
		t.Fatalf("stale release removed current lease: %v", err)
	}
	if err := store.ValidateFence(context.Background(), key, 1); err != ErrStaleFence {
		t.Fatalf("old fence err=%v", err)
	}
}
