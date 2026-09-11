package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	// CoordinatorBackendLocal serializes a Session only inside one process.
	CoordinatorBackendLocal = "local"
	// CoordinatorBackendRedis coordinates the same Session across processes.
	CoordinatorBackendRedis = "redis"

	defaultCoordinatorLeaseTTL      = 30 * time.Second
	defaultCoordinatorRenewInterval = 10 * time.Second
	defaultCoordinatorRetryInterval = 50 * time.Millisecond
)

// CoordinatorConfig controls how one complete Agent turn is serialized.
type CoordinatorConfig struct {
	Backend       string
	RedisURL      string
	RedisPrefix   string
	LeaseTTL      time.Duration
	RenewInterval time.Duration
	RetryInterval time.Duration
}

// LoadCoordinatorConfigFromEnv reads and validates Session coordination
// settings. The Redis coordinator intentionally shares REDIS_URL with the
// Redis Session backend, while using a separate key namespace.
func LoadCoordinatorConfigFromEnv() (CoordinatorConfig, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv(
		"TRPC_AGENT_COORDINATOR_BACKEND",
	)))
	if backend == "" {
		backend = CoordinatorBackendLocal
	}

	cfg := CoordinatorConfig{
		Backend:       backend,
		RedisURL:      strings.TrimSpace(os.Getenv("REDIS_URL")),
		RedisPrefix:   strings.TrimSpace(os.Getenv("REDIS_KEY_PREFIX")),
		LeaseTTL:      defaultCoordinatorLeaseTTL,
		RenewInterval: defaultCoordinatorRenewInterval,
		RetryInterval: defaultCoordinatorRetryInterval,
	}
	if cfg.RedisPrefix == "" {
		cfg.RedisPrefix = defaultRedisKeyPrefix
	}

	var err error
	if cfg.LeaseTTL, err = parsePositiveDurationEnv(
		"TRPC_AGENT_COORDINATOR_LEASE_TTL",
		cfg.LeaseTTL,
	); err != nil {
		return CoordinatorConfig{}, err
	}
	if cfg.RenewInterval, err = parsePositiveDurationEnv(
		"TRPC_AGENT_COORDINATOR_RENEW_INTERVAL",
		cfg.RenewInterval,
	); err != nil {
		return CoordinatorConfig{}, err
	}
	if cfg.RetryInterval, err = parsePositiveDurationEnv(
		"TRPC_AGENT_COORDINATOR_RETRY_INTERVAL",
		cfg.RetryInterval,
	); err != nil {
		return CoordinatorConfig{}, err
	}

	if cfg.RenewInterval > cfg.LeaseTTL/2 {
		return CoordinatorConfig{}, fmt.Errorf(
			"TRPC_AGENT_COORDINATOR_RENEW_INTERVAL must be at most half of lease TTL",
		)
	}

	switch cfg.Backend {
	case CoordinatorBackendLocal:
		return cfg, nil
	case CoordinatorBackendRedis:
		if cfg.RedisURL == "" {
			return CoordinatorConfig{}, fmt.Errorf(
				"REDIS_URL is required when coordinator backend is redis",
			)
		}
		if err := validateRedisURL(cfg.RedisURL); err != nil {
			return CoordinatorConfig{}, err
		}
		return cfg, nil
	default:
		return CoordinatorConfig{}, fmt.Errorf(
			"unsupported TRPC_AGENT_COORDINATOR_BACKEND %q: use local or redis",
			cfg.Backend,
		)
	}
}

func parsePositiveDurationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return value, nil
}
