package idempotency

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func TestRedisStoreDeduplicatesAcrossInstances(t *testing.T) {
	server := miniredis.RunT(t)
	ownerStore := newTestRedisStore(t, server, "shared")
	waiterStore := newTestRedisStore(t, server, "shared")
	key := testKey("distributed")

	begin, err := ownerStore.Begin(context.Background(), key, "fingerprint")
	if err != nil || begin.Status != BeginStarted {
		t.Fatalf("owner begin = %+v, err = %v", begin, err)
	}
	duplicate, err := waiterStore.Begin(context.Background(), key, "fingerprint")
	if err != nil || duplicate.Status != BeginProcessing {
		t.Fatalf("duplicate begin = %+v, err = %v", duplicate, err)
	}

	waitResult := make(chan Result, 1)
	waitErr := make(chan error, 1)
	go func() {
		result, waitError := waiterStore.Wait(context.Background(), key, "fingerprint")
		waitResult <- result
		waitErr <- waitError
	}()
	expected := Result{Reply: "cached", RequestID: "request-1", EventCount: 7}
	if err := begin.Attempt.Complete(context.Background(), expected); err != nil {
		t.Fatalf("complete owner attempt: %v", err)
	}
	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("wait duplicate: %v", err)
		}
		if got := <-waitResult; got != expected {
			t.Fatalf("wait result = %+v, want %+v", got, expected)
		}
	case <-time.After(time.Second):
		t.Fatal("duplicate did not observe completion")
	}

	replay, err := waiterStore.Begin(context.Background(), key, "fingerprint")
	if err != nil || replay.Status != BeginCompleted || replay.Result != expected {
		t.Fatalf("replay = %+v, err = %v", replay, err)
	}
}

func TestRedisStoreFailureAndExpiryAllowRetry(t *testing.T) {
	server := miniredis.RunT(t)
	store := newTestRedisStore(t, server, "retry")
	key := testKey("failure")
	begin, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if err := begin.Attempt.Fail(context.Background()); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	retry, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil || retry.Status != BeginStarted {
		t.Fatalf("retry after failure = %+v, err = %v", retry, err)
	}

	redisAttempt := retry.Attempt.(*redisAttempt)
	redisAttempt.cancel(context.Canceled)
	<-redisAttempt.renewDone
	server.FastForward(600 * time.Millisecond)
	afterExpiry, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil || afterExpiry.Status != BeginStarted {
		t.Fatalf("retry after expiry = %+v, err = %v", afterExpiry, err)
	}
	if err := afterExpiry.Attempt.Fail(context.Background()); err != nil {
		t.Fatalf("clean attempt: %v", err)
	}
}

func TestRedisStoreRenewsProcessingTTL(t *testing.T) {
	server := miniredis.RunT(t)
	store := newRedisStoreWithTiming(
		t,
		server,
		"renew",
		120*time.Millisecond,
		25*time.Millisecond,
	)
	key := testKey("renew")
	begin, err := store.Begin(context.Background(), key, "fingerprint")
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	redisKey := store.redisKey(key)
	server.FastForward(80 * time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if remaining := server.TTL(redisKey); remaining < 80*time.Millisecond {
		t.Fatalf("processing TTL was not renewed: %s", remaining)
	}
	if err := begin.Attempt.Fail(context.Background()); err != nil {
		t.Fatalf("clean attempt: %v", err)
	}
}

func TestRedisStoreRejectsConflictAndLostAttempt(t *testing.T) {
	server := miniredis.RunT(t)
	store := newTestRedisStore(t, server, "lost")
	key := testKey("conflict")
	begin, err := store.Begin(context.Background(), key, "first")
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	if _, err := store.Begin(context.Background(), key, "second"); !errors.Is(err, ErrKeyConflict) {
		t.Fatalf("conflict error = %v", err)
	}
	if err := server.Set(store.redisKey(key), `{"status":"processing","fingerprint":"first","owner":"replacement"}`); err != nil {
		t.Fatalf("replace record: %v", err)
	}
	select {
	case <-begin.Attempt.Context().Done():
		if !errors.Is(context.Cause(begin.Attempt.Context()), ErrAttemptLost) {
			t.Fatalf("attempt cause = %v", context.Cause(begin.Attempt.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("lost attempt was not cancelled")
	}
	if err := begin.Attempt.Complete(context.Background(), Result{Reply: "old"}); !errors.Is(err, ErrAttemptLost) {
		t.Fatalf("complete lost attempt error = %v", err)
	}
	value, err := server.Get(store.redisKey(key))
	if err != nil || value != `{"status":"processing","fingerprint":"first","owner":"replacement"}` {
		t.Fatalf("replacement record changed: value=%q err=%v", value, err)
	}
}

func TestRedisStoreReadyFailsWhenRedisStops(t *testing.T) {
	server := miniredis.RunT(t)
	store := newTestRedisStore(t, server, "ready")
	server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := store.Ready(ctx); err == nil {
		t.Fatal("expected readiness failure")
	}
}

func newTestRedisStore(t *testing.T, server *miniredis.Miniredis, prefix string) *RedisStore {
	t.Helper()
	return newRedisStoreWithTiming(
		t,
		server,
		prefix,
		500*time.Millisecond,
		50*time.Millisecond,
	)
}

func newRedisStoreWithTiming(
	t *testing.T,
	server *miniredis.Miniredis,
	prefix string,
	processingTTL time.Duration,
	renewInterval time.Duration,
) *RedisStore {
	t.Helper()
	store, err := NewRedisStore(RedisOptions{
		URL:           "redis://" + server.Addr() + "/0",
		KeyPrefix:     prefix,
		ProcessingTTL: processingTTL,
		CompletedTTL:  time.Hour,
		RenewInterval: renewInterval,
		PollInterval:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("create Redis store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
			t.Fatalf("close Redis store: %v", err)
		}
	})
	if err := store.Ready(context.Background()); err != nil {
		t.Fatalf("store readiness: %v", err)
	}
	return store
}
