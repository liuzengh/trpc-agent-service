package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadIdempotencyConfigDefaultsToLocal(t *testing.T) {
	clearIdempotencyEnvironment(t)
	cfg, err := LoadIdempotencyConfigFromEnv()
	if err != nil {
		t.Fatalf("load idempotency config: %v", err)
	}
	if cfg.Backend != IdempotencyBackendLocal {
		t.Fatalf("backend = %q", cfg.Backend)
	}
	if cfg.ProcessingTTL != 2*time.Minute || cfg.CompletedTTL != 24*time.Hour {
		t.Fatalf("unexpected TTL defaults: %+v", cfg)
	}
}

func TestLoadIdempotencyConfigRedis(t *testing.T) {
	clearIdempotencyEnvironment(t)
	t.Setenv("TRPC_AGENT_IDEMPOTENCY_BACKEND", "redis")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/4")
	t.Setenv("REDIS_KEY_PREFIX", "idempotency-test")
	t.Setenv("TRPC_AGENT_IDEMPOTENCY_PROCESSING_TTL", "30s")
	t.Setenv("TRPC_AGENT_IDEMPOTENCY_COMPLETED_TTL", "2h")
	t.Setenv("TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL", "10s")
	t.Setenv("TRPC_AGENT_IDEMPOTENCY_POLL_INTERVAL", "25ms")

	cfg, err := LoadIdempotencyConfigFromEnv()
	if err != nil {
		t.Fatalf("load idempotency config: %v", err)
	}
	if cfg.Backend != IdempotencyBackendRedis || cfg.RedisPrefix != "idempotency-test" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.ProcessingTTL != 30*time.Second || cfg.CompletedTTL != 2*time.Hour ||
		cfg.RenewInterval != 10*time.Second || cfg.PollInterval != 25*time.Millisecond {
		t.Fatalf("unexpected durations: %+v", cfg)
	}
}

func TestLoadIdempotencyConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name       string
		backend    string
		redisURL   string
		processing string
		completed  string
		renew      string
		poll       string
		wantError  string
	}{
		{name: "unknown backend", backend: "sql", wantError: "unsupported"},
		{name: "missing Redis URL", backend: "redis", wantError: "REDIS_URL"},
		{name: "invalid processing TTL", processing: "later", wantError: "PROCESSING_TTL"},
		{name: "invalid completed TTL", completed: "0s", wantError: "must be positive"},
		{name: "invalid poll interval", poll: "-1s", wantError: "must be positive"},
		{name: "renew too slow", processing: "10s", renew: "6s", wantError: "at most half"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearIdempotencyEnvironment(t)
			t.Setenv("TRPC_AGENT_IDEMPOTENCY_BACKEND", test.backend)
			t.Setenv("REDIS_URL", test.redisURL)
			t.Setenv("TRPC_AGENT_IDEMPOTENCY_PROCESSING_TTL", test.processing)
			t.Setenv("TRPC_AGENT_IDEMPOTENCY_COMPLETED_TTL", test.completed)
			t.Setenv("TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL", test.renew)
			t.Setenv("TRPC_AGENT_IDEMPOTENCY_POLL_INTERVAL", test.poll)

			_, err := LoadIdempotencyConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func clearIdempotencyEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"TRPC_AGENT_IDEMPOTENCY_BACKEND",
		"TRPC_AGENT_IDEMPOTENCY_PROCESSING_TTL",
		"TRPC_AGENT_IDEMPOTENCY_COMPLETED_TTL",
		"TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL",
		"TRPC_AGENT_IDEMPOTENCY_POLL_INTERVAL",
		"REDIS_URL",
		"REDIS_KEY_PREFIX",
	} {
		t.Setenv(key, "")
	}
}
