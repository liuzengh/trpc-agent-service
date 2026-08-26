package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	// SessionBackendInMemory keeps sessions inside one service process.
	SessionBackendInMemory = "inmemory"
	// SessionBackendRedis stores sessions in Redis for restart persistence and
	// cross-instance sharing.
	SessionBackendRedis = "redis"

	defaultRedisKeyPrefix = "trpc-agent-service"
)

// SessionConfig contains the settings needed to create a Session service.
type SessionConfig struct {
	Backend        string
	RedisURL       string
	RedisKeyPrefix string
	TTL            time.Duration
}

// LoadSessionConfigFromEnv reads Session backend settings from environment
// variables and validates them before the server starts.
func LoadSessionConfigFromEnv() (SessionConfig, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("TRPC_AGENT_SESSION_BACKEND")))
	if backend == "" {
		backend = SessionBackendInMemory
	}

	config := SessionConfig{
		Backend:        backend,
		RedisURL:       strings.TrimSpace(os.Getenv("REDIS_URL")),
		RedisKeyPrefix: strings.TrimSpace(os.Getenv("REDIS_KEY_PREFIX")),
	}
	if config.RedisKeyPrefix == "" {
		config.RedisKeyPrefix = defaultRedisKeyPrefix
	}

	if rawTTL := strings.TrimSpace(os.Getenv("TRPC_AGENT_SESSION_TTL")); rawTTL != "" {
		ttl, err := time.ParseDuration(rawTTL)
		if err != nil {
			return SessionConfig{}, fmt.Errorf("parse TRPC_AGENT_SESSION_TTL: %w", err)
		}
		if ttl < 0 {
			return SessionConfig{}, fmt.Errorf("TRPC_AGENT_SESSION_TTL must not be negative")
		}
		config.TTL = ttl
	}

	switch config.Backend {
	case SessionBackendInMemory:
		return config, nil
	case SessionBackendRedis:
		if config.RedisURL == "" {
			return SessionConfig{}, fmt.Errorf(
				"REDIS_URL is required when session backend is redis",
			)
		}
		if err := validateRedisURL(config.RedisURL); err != nil {
			return SessionConfig{}, err
		}
		return config, nil
	default:
		return SessionConfig{}, fmt.Errorf(
			"unsupported TRPC_AGENT_SESSION_BACKEND %q: use inmemory or redis",
			config.Backend,
		)
	}
}

func validateRedisURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse REDIS_URL: %w", err)
	}
	if parsed.Scheme != "redis" && parsed.Scheme != "rediss" {
		return fmt.Errorf("REDIS_URL must use redis or rediss")
	}
	if parsed.Host == "" {
		return fmt.Errorf("REDIS_URL must include a host")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("REDIS_URL must not include query parameters or fragments")
	}
	return nil
}
