package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	// IdempotencyBackendLocal deduplicates requests inside one process.
	IdempotencyBackendLocal = "local"
	// IdempotencyBackendRedis deduplicates requests across service processes.
	IdempotencyBackendRedis = "redis"

	defaultIdempotencyProcessingTTL = 2 * time.Minute
	defaultIdempotencyCompletedTTL  = 24 * time.Hour
	defaultIdempotencyRenewInterval = 30 * time.Second
	defaultIdempotencyPollInterval  = 50 * time.Millisecond
)

// IdempotencyConfig controls duplicate message detection and result caching.
type IdempotencyConfig struct {
	Backend       string
	RedisURL      string
	RedisPrefix   string
	ProcessingTTL time.Duration
	CompletedTTL  time.Duration
	RenewInterval time.Duration
	PollInterval  time.Duration
}

// LoadIdempotencyConfigFromEnv reads and validates message idempotency
// settings. Redis mode reuses the platform Redis connection settings while
// keeping its own key namespace.
func LoadIdempotencyConfigFromEnv() (IdempotencyConfig, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv(
		"TRPC_AGENT_IDEMPOTENCY_BACKEND",
	)))
	if backend == "" {
		backend = IdempotencyBackendLocal
	}

	cfg := IdempotencyConfig{
		Backend:       backend,
		RedisURL:      strings.TrimSpace(os.Getenv("REDIS_URL")),
		RedisPrefix:   strings.TrimSpace(os.Getenv("REDIS_KEY_PREFIX")),
		ProcessingTTL: defaultIdempotencyProcessingTTL,
		CompletedTTL:  defaultIdempotencyCompletedTTL,
		RenewInterval: defaultIdempotencyRenewInterval,
		PollInterval:  defaultIdempotencyPollInterval,
	}
	if cfg.RedisPrefix == "" {
		cfg.RedisPrefix = defaultRedisKeyPrefix
	}

	var err error
	if cfg.ProcessingTTL, err = parsePositiveDurationEnv(
		"TRPC_AGENT_IDEMPOTENCY_PROCESSING_TTL",
		cfg.ProcessingTTL,
	); err != nil {
		return IdempotencyConfig{}, err
	}
	if cfg.CompletedTTL, err = parsePositiveDurationEnv(
		"TRPC_AGENT_IDEMPOTENCY_COMPLETED_TTL",
		cfg.CompletedTTL,
	); err != nil {
		return IdempotencyConfig{}, err
	}
	if cfg.RenewInterval, err = parsePositiveDurationEnv(
		"TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL",
		cfg.RenewInterval,
	); err != nil {
		return IdempotencyConfig{}, err
	}
	if cfg.PollInterval, err = parsePositiveDurationEnv(
		"TRPC_AGENT_IDEMPOTENCY_POLL_INTERVAL",
		cfg.PollInterval,
	); err != nil {
		return IdempotencyConfig{}, err
	}
	if cfg.RenewInterval > cfg.ProcessingTTL/2 {
		return IdempotencyConfig{}, fmt.Errorf(
			"TRPC_AGENT_IDEMPOTENCY_RENEW_INTERVAL must be at most half of processing TTL",
		)
	}

	switch cfg.Backend {
	case IdempotencyBackendLocal:
		return cfg, nil
	case IdempotencyBackendRedis:
		if cfg.RedisURL == "" {
			return IdempotencyConfig{}, fmt.Errorf(
				"REDIS_URL is required when idempotency backend is redis",
			)
		}
		if err := validateRedisURL(cfg.RedisURL); err != nil {
			return IdempotencyConfig{}, err
		}
		return cfg, nil
	default:
		return IdempotencyConfig{}, fmt.Errorf(
			"unsupported TRPC_AGENT_IDEMPOTENCY_BACKEND %q: use local or redis",
			cfg.Backend,
		)
	}
}
