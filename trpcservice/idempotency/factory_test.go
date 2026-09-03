package idempotency

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestNewStoreLocal(t *testing.T) {
	store, err := New(context.Background(), config.IdempotencyConfig{
		Backend: config.IdempotencyBackendLocal,
	})
	if err != nil {
		t.Fatalf("create local store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
}

func TestNewStoreRedis(t *testing.T) {
	server := miniredis.RunT(t)
	store, err := New(context.Background(), config.IdempotencyConfig{
		Backend:       config.IdempotencyBackendRedis,
		RedisURL:      "redis://" + server.Addr() + "/0",
		RedisPrefix:   "factory",
		ProcessingTTL: time.Second,
		CompletedTTL:  time.Hour,
		RenewInterval: 250 * time.Millisecond,
		PollInterval:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("create Redis store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
}

func TestNewStoreRejectsUnsupportedBackend(t *testing.T) {
	_, err := New(context.Background(), config.IdempotencyConfig{Backend: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error = %v", err)
	}
}
