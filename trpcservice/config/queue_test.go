package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadQueueConfigDefaults(t *testing.T) {
	clearQueueEnvironment(t)
	cfg, err := LoadQueueConfigFromEnv()
	if err != nil {
		t.Fatalf("load queue config: %v", err)
	}
	if cfg.Backend != QueueBackendMemory || cfg.Stream != "agent-runs" ||
		cfg.Group != "agent-workers" || cfg.BlockTimeout != time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadQueueConfigRedis(t *testing.T) {
	clearQueueEnvironment(t)
	t.Setenv("TRPC_AGENT_QUEUE_BACKEND", "redis")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("TRPC_AGENT_QUEUE_STREAM", "tasks")
	t.Setenv("TRPC_AGENT_QUEUE_GROUP", "workers")
	t.Setenv("TRPC_AGENT_QUEUE_BLOCK_TIMEOUT", "2s")
	t.Setenv("TRPC_AGENT_QUEUE_CLAIM_MIN_IDLE", "45s")
	cfg, err := LoadQueueConfigFromEnv()
	if err != nil {
		t.Fatalf("load queue config: %v", err)
	}
	if cfg.Backend != QueueBackendRedis || cfg.Stream != "tasks" ||
		cfg.Group != "workers" || cfg.ClaimMinIdle != 45*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadQueueConfigRejectsInvalid(t *testing.T) {
	tests := []struct {
		name      string
		backend   string
		url       string
		block     string
		wantError string
	}{
		{name: "unknown", backend: "kafka", wantError: "unsupported"},
		{name: "missing Redis", backend: "redis", wantError: "REDIS_URL"},
		{name: "invalid block", block: "0s", wantError: "must be positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearQueueEnvironment(t)
			t.Setenv("TRPC_AGENT_QUEUE_BACKEND", test.backend)
			t.Setenv("REDIS_URL", test.url)
			t.Setenv("TRPC_AGENT_QUEUE_BLOCK_TIMEOUT", test.block)
			_, err := LoadQueueConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func clearQueueEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"TRPC_AGENT_QUEUE_BACKEND", "REDIS_URL", "REDIS_KEY_PREFIX",
		"TRPC_AGENT_QUEUE_STREAM", "TRPC_AGENT_QUEUE_GROUP",
		"TRPC_AGENT_QUEUE_CONSUMER", "TRPC_AGENT_QUEUE_BLOCK_TIMEOUT",
		"TRPC_AGENT_QUEUE_CLAIM_MIN_IDLE", "TRPC_AGENT_QUEUE_MAX_LEN",
	} {
		t.Setenv(key, "")
	}
}
