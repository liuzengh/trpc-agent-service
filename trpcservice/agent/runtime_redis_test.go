package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestRedisSessionSurvivesRuntimeRestart(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := redisSessionTestConfig(server, "restart-test")

	firstRuntime := newRedisTestRuntime(t, cfg)
	if _, err := firstRuntime.Chat(
		context.Background(),
		"alice",
		"restart-session",
		"我叫小明。",
	); err != nil {
		t.Fatalf("first chat turn: %v", err)
	}
	if err := firstRuntime.Close(); err != nil {
		t.Fatalf("close first runtime: %v", err)
	}

	secondRuntime := newRedisTestRuntime(t, cfg)
	t.Cleanup(func() {
		if err := secondRuntime.Close(); err != nil {
			t.Fatalf("close second runtime: %v", err)
		}
	})
	result, err := secondRuntime.Chat(
		context.Background(),
		"alice",
		"restart-session",
		"我叫什么？",
	)
	if err != nil {
		t.Fatalf("second chat turn: %v", err)
	}
	if !strings.Contains(result.Reply, "你叫小明") {
		t.Fatalf("reply %q did not restore Redis session history", result.Reply)
	}
}

func TestRedisSessionSharedAcrossRuntimeInstances(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := redisSessionTestConfig(server, "multi-instance-test")

	runtimeA := newRedisTestRuntime(t, cfg)
	runtimeB := newRedisTestRuntime(t, cfg)
	t.Cleanup(func() {
		if err := runtimeA.Close(); err != nil {
			t.Fatalf("close runtime A: %v", err)
		}
		if err := runtimeB.Close(); err != nil {
			t.Fatalf("close runtime B: %v", err)
		}
	})

	if _, err := runtimeA.Chat(
		context.Background(),
		"alice",
		"shared-session",
		"我叫小明。",
	); err != nil {
		t.Fatalf("runtime A chat: %v", err)
	}
	result, err := runtimeB.Chat(
		context.Background(),
		"alice",
		"shared-session",
		"我叫什么？",
	)
	if err != nil {
		t.Fatalf("runtime B chat: %v", err)
	}
	if !strings.Contains(result.Reply, "你叫小明") {
		t.Fatalf("reply %q did not use the session written by runtime A", result.Reply)
	}
}

func newRedisTestRuntime(t *testing.T, cfg config.SessionConfig) *Runtime {
	t.Helper()
	service, err := platformstorage.NewSessionService(context.Background(), cfg)
	if err != nil {
		t.Fatalf("create Redis session service: %v", err)
	}
	runtime, err := NewRuntimeWithSession(NewTutorialModel(), service, false)
	if err != nil {
		_ = service.Close()
		t.Fatalf("create runtime: %v", err)
	}
	return runtime
}

func redisSessionTestConfig(
	server *miniredis.Miniredis,
	prefix string,
) config.SessionConfig {
	return config.SessionConfig{
		Backend:        config.SessionBackendRedis,
		RedisURL:       "redis://" + server.Addr() + "/0",
		RedisKeyPrefix: prefix,
	}
}
