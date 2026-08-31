package coordination

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func TestRedisCoordinatorSerializesAcrossInstances(t *testing.T) {
	server := miniredis.RunT(t)
	firstCoordinator := newTestRedisCoordinator(t, server, "serialize")
	secondCoordinator := newTestRedisCoordinator(t, server, "serialize")
	key := testKey("shared")

	first, err := firstCoordinator.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := secondCoordinator.Acquire(waitCtx, key); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("competing acquire error = %v, want deadline exceeded", err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatalf("release first lease: %v", err)
	}

	second, err := secondCoordinator.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire second lease: %v", err)
	}
	if second.FencingToken() <= first.FencingToken() {
		t.Fatalf("fencing token did not increase: first=%d second=%d", first.FencingToken(), second.FencingToken())
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("release second lease: %v", err)
	}
}

func TestRedisCoordinatorRenewsLease(t *testing.T) {
	server := miniredis.RunT(t)
	coordinator := newRedisCoordinatorWithTiming(
		t,
		server,
		"renew",
		120*time.Millisecond,
		25*time.Millisecond,
	)
	key := testKey("renewed")
	lease, err := coordinator.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	lockKey, _ := coordinator.redisKeys(key)

	server.FastForward(80 * time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	remaining := server.TTL(lockKey)
	if remaining < 80*time.Millisecond {
		t.Fatalf("lease TTL was not renewed, remaining = %s", remaining)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("release lease: %v", err)
	}
}

func TestRedisCoordinatorDetectsLostLeaseAndDoesNotDeleteNewOwner(t *testing.T) {
	server := miniredis.RunT(t)
	coordinator := newTestRedisCoordinator(t, server, "lost")
	key := testKey("lost")
	lease, err := coordinator.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	lockKey, _ := coordinator.redisKeys(key)
	if err := server.Set(lockKey, "replacement-owner"); err != nil {
		t.Fatalf("replace lease owner: %v", err)
	}

	select {
	case <-lease.Context().Done():
		if !errors.Is(context.Cause(lease.Context()), ErrLeaseLost) {
			t.Fatalf("lease cause = %v", context.Cause(lease.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("lease loss was not detected")
	}
	if err := lease.Release(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("release lost lease error = %v", err)
	}
	if value, err := server.Get(lockKey); err != nil || value != "replacement-owner" {
		t.Fatalf("replacement lease was modified: value=%q err=%v", value, err)
	}
}

func TestRedisCoordinatorLeaseExpiresWhenRenewalStops(t *testing.T) {
	server := miniredis.RunT(t)
	firstCoordinator := newTestRedisCoordinator(t, server, "expiry")
	secondCoordinator := newTestRedisCoordinator(t, server, "expiry")
	key := testKey("expiry")
	lease, err := firstCoordinator.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	redisLease, ok := lease.(*redisLease)
	if !ok {
		t.Fatalf("lease type = %T", lease)
	}
	redisLease.cancel(context.Canceled)
	<-redisLease.renewDone

	server.FastForward(600 * time.Millisecond)
	second, err := secondCoordinator.Acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire after expired lease: %v", err)
	}
	if second.FencingToken() <= lease.FencingToken() {
		t.Fatalf("fencing token did not increase after expiry")
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("release second lease: %v", err)
	}
}

func TestRedisCoordinatorReadyFailsWhenRedisStops(t *testing.T) {
	server := miniredis.RunT(t)
	coordinator := newTestRedisCoordinator(t, server, "ready")
	server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := coordinator.Ready(ctx); err == nil {
		t.Fatal("expected readiness error")
	}
}

func TestRedisCoordinatorReleaseIsIdempotent(t *testing.T) {
	server := miniredis.RunT(t)
	coordinator := newTestRedisCoordinator(t, server, "idempotent")
	lease, err := coordinator.Acquire(context.Background(), testKey("idempotent"))
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("second release: %v", err)
	}
}

func newTestRedisCoordinator(
	t *testing.T,
	server *miniredis.Miniredis,
	prefix string,
) *RedisCoordinator {
	t.Helper()
	return newRedisCoordinatorWithTiming(
		t,
		server,
		prefix,
		500*time.Millisecond,
		50*time.Millisecond,
	)
}

func newRedisCoordinatorWithTiming(
	t *testing.T,
	server *miniredis.Miniredis,
	prefix string,
	ttl time.Duration,
	renew time.Duration,
) *RedisCoordinator {
	t.Helper()
	coordinator, err := NewRedisCoordinator(RedisOptions{
		URL:           "redis://" + server.Addr() + "/0",
		KeyPrefix:     prefix,
		LeaseTTL:      ttl,
		RenewInterval: renew,
		RetryInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("create Redis coordinator: %v", err)
	}
	t.Cleanup(func() {
		if err := coordinator.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
			t.Fatalf("close Redis coordinator: %v", err)
		}
	})
	if err := coordinator.Ready(context.Background()); err != nil {
		t.Fatalf("coordinator readiness: %v", err)
	}
	return coordinator
}
