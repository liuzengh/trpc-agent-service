package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadCoordinatorConfigDefaultsToLocal(t *testing.T) {
	clearCoordinatorEnvironment(t)

	cfg, err := LoadCoordinatorConfigFromEnv()
	if err != nil {
		t.Fatalf("load coordinator config: %v", err)
	}
	if cfg.Backend != CoordinatorBackendLocal {
		t.Fatalf("backend = %q", cfg.Backend)
	}
	if cfg.LeaseTTL != 30*time.Second || cfg.RenewInterval != 10*time.Second {
		t.Fatalf("unexpected lease defaults: %+v", cfg)
	}
}

func TestLoadCoordinatorConfigRedis(t *testing.T) {
	clearCoordinatorEnvironment(t)
	t.Setenv("TRPC_AGENT_COORDINATOR_BACKEND", "redis")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/3")
	t.Setenv("REDIS_KEY_PREFIX", "coordinator-test")
	t.Setenv("TRPC_AGENT_COORDINATOR_LEASE_TTL", "12s")
	t.Setenv("TRPC_AGENT_COORDINATOR_RENEW_INTERVAL", "4s")
	t.Setenv("TRPC_AGENT_COORDINATOR_RETRY_INTERVAL", "25ms")

	cfg, err := LoadCoordinatorConfigFromEnv()
	if err != nil {
		t.Fatalf("load coordinator config: %v", err)
	}
	if cfg.Backend != CoordinatorBackendRedis || cfg.RedisPrefix != "coordinator-test" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.LeaseTTL != 12*time.Second || cfg.RenewInterval != 4*time.Second ||
		cfg.RetryInterval != 25*time.Millisecond {
		t.Fatalf("unexpected timing config: %+v", cfg)
	}
}

func TestLoadCoordinatorConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name      string
		backend   string
		redisURL  string
		ttl       string
		renew     string
		retry     string
		wantError string
	}{
		{name: "unknown backend", backend: "mysql", wantError: "unsupported"},
		{name: "missing Redis URL", backend: "redis", wantError: "REDIS_URL"},
		{name: "invalid lease TTL", ttl: "later", wantError: "LEASE_TTL"},
		{name: "zero renew interval", renew: "0s", wantError: "must be positive"},
		{name: "negative retry interval", retry: "-1s", wantError: "must be positive"},
		{name: "renew too slow", ttl: "10s", renew: "6s", wantError: "at most half"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearCoordinatorEnvironment(t)
			t.Setenv("TRPC_AGENT_COORDINATOR_BACKEND", test.backend)
			t.Setenv("REDIS_URL", test.redisURL)
			t.Setenv("TRPC_AGENT_COORDINATOR_LEASE_TTL", test.ttl)
			t.Setenv("TRPC_AGENT_COORDINATOR_RENEW_INTERVAL", test.renew)
			t.Setenv("TRPC_AGENT_COORDINATOR_RETRY_INTERVAL", test.retry)

			_, err := LoadCoordinatorConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func clearCoordinatorEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"TRPC_AGENT_COORDINATOR_BACKEND",
		"TRPC_AGENT_COORDINATOR_LEASE_TTL",
		"TRPC_AGENT_COORDINATOR_RENEW_INTERVAL",
		"TRPC_AGENT_COORDINATOR_RETRY_INTERVAL",
		"REDIS_URL",
		"REDIS_KEY_PREFIX",
	} {
		t.Setenv(key, "")
	}
}
