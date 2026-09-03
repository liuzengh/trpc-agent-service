package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadSessionConfigDefaultsToInMemory(t *testing.T) {
	clearSessionEnvironment(t)

	config, err := LoadSessionConfigFromEnv()
	if err != nil {
		t.Fatalf("load session config: %v", err)
	}
	if config.Backend != SessionBackendInMemory {
		t.Fatalf("backend = %q", config.Backend)
	}
	if config.RedisKeyPrefix != defaultRedisKeyPrefix {
		t.Fatalf("key prefix = %q", config.RedisKeyPrefix)
	}
}

func TestLoadSessionConfigRedis(t *testing.T) {
	clearSessionEnvironment(t)
	t.Setenv("TRPC_AGENT_SESSION_BACKEND", "redis")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/2")
	t.Setenv("REDIS_KEY_PREFIX", "test-prefix")
	t.Setenv("TRPC_AGENT_SESSION_TTL", "24h")

	config, err := LoadSessionConfigFromEnv()
	if err != nil {
		t.Fatalf("load session config: %v", err)
	}
	if config.Backend != SessionBackendRedis || config.RedisURL != "redis://127.0.0.1:6379/2" {
		t.Fatalf("unexpected config: %+v", config)
	}
	if config.RedisKeyPrefix != "test-prefix" || config.TTL != 24*time.Hour {
		t.Fatalf("unexpected Redis options: %+v", config)
	}
}

func TestLoadSessionConfigPostgres(t *testing.T) {
	clearSessionEnvironment(t)
	t.Setenv("TRPC_AGENT_SESSION_BACKEND", "postgres")
	t.Setenv("TRPC_AGENT_POSTGRES_URL", "postgres://user:pass@localhost/agent?sslmode=disable")
	t.Setenv("TRPC_AGENT_SESSION_POSTGRES_PREFIX", "runtime")
	config, err := LoadSessionConfigFromEnv()
	if err != nil {
		t.Fatalf("load session config: %v", err)
	}
	if config.Backend != SessionBackendPostgres || config.PostgresPrefix != "runtime" {
		t.Fatalf("config=%+v", config)
	}
}

func TestLoadSessionConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name      string
		backend   string
		redisURL  string
		ttl       string
		wantError string
	}{
		{name: "unknown backend", backend: "sql", wantError: "unsupported"},
		{name: "missing Redis URL", backend: "redis", wantError: "REDIS_URL"},
		{name: "missing PostgreSQL URL", backend: "postgres", wantError: "POSTGRES_URL"},
		{name: "invalid Redis scheme", backend: "redis", redisURL: "http://localhost:6379", wantError: "redis or rediss"},
		{name: "invalid TTL", ttl: "tomorrow", wantError: "TRPC_AGENT_SESSION_TTL"},
		{name: "negative TTL", ttl: "-1s", wantError: "must not be negative"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearSessionEnvironment(t)
			t.Setenv("TRPC_AGENT_SESSION_BACKEND", test.backend)
			t.Setenv("REDIS_URL", test.redisURL)
			t.Setenv("TRPC_AGENT_SESSION_TTL", test.ttl)

			_, err := LoadSessionConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func clearSessionEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"TRPC_AGENT_SESSION_BACKEND",
		"TRPC_AGENT_SESSION_TTL",
		"REDIS_URL",
		"REDIS_KEY_PREFIX",
		"TRPC_AGENT_POSTGRES_URL",
		"TRPC_AGENT_SESSION_POSTGRES_PREFIX",
	} {
		t.Setenv(key, "")
	}
}
