package coordination

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestNewCoordinatorLocal(t *testing.T) {
	coordinator, err := New(context.Background(), config.CoordinatorConfig{
		Backend: config.CoordinatorBackendLocal,
	})
	if err != nil {
		t.Fatalf("create local coordinator: %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
}

func TestNewCoordinatorRedis(t *testing.T) {
	server := miniredis.RunT(t)
	coordinator, err := New(context.Background(), config.CoordinatorConfig{
		Backend:       config.CoordinatorBackendRedis,
		RedisURL:      "redis://" + server.Addr() + "/0",
		RedisPrefix:   "factory",
		LeaseTTL:      time.Second,
		RenewInterval: 250 * time.Millisecond,
		RetryInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("create Redis coordinator: %v", err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
}

func TestNewCoordinatorRejectsUnsupportedBackend(t *testing.T) {
	_, err := New(context.Background(), config.CoordinatorConfig{Backend: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("error = %v", err)
	}
}
