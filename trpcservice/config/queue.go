package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	QueueBackendMemory = "memory"
	QueueBackendRedis  = "redis"
)

// QueueConfig configures the Agent task queue.
type QueueConfig struct {
	WorkerConcurrency int
	Backend           string
	RedisURL          string
	RedisPrefix       string
	Stream            string
	Group             string
	Consumer          string
	BlockTimeout      time.Duration
	ClaimMinIdle      time.Duration
	MaxLen            int64
}

func LoadQueueConfigFromEnv() (QueueConfig, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("TRPC_AGENT_QUEUE_BACKEND")))
	if backend == "" {
		backend = QueueBackendMemory
	}
	cfg := QueueConfig{
		WorkerConcurrency: 4,
		Backend:           backend,
		RedisURL:          strings.TrimSpace(os.Getenv("REDIS_URL")),
		RedisPrefix:       strings.TrimSpace(os.Getenv("REDIS_KEY_PREFIX")),
		Stream:            strings.TrimSpace(os.Getenv("TRPC_AGENT_QUEUE_STREAM")),
		Group:             strings.TrimSpace(os.Getenv("TRPC_AGENT_QUEUE_GROUP")),
		Consumer:          strings.TrimSpace(os.Getenv("TRPC_AGENT_QUEUE_CONSUMER")),
		BlockTimeout:      time.Second,
		ClaimMinIdle:      30 * time.Second,
		MaxLen:            100000,
	}
	if cfg.RedisPrefix == "" {
		cfg.RedisPrefix = defaultRedisKeyPrefix
	}
	if cfg.Stream == "" {
		cfg.Stream = "agent-runs"
	}
	if cfg.Group == "" {
		cfg.Group = "agent-workers"
	}
	var err error
	if cfg.WorkerConcurrency, err = parsePositiveIntEnv("TRPC_AGENT_WORKER_CONCURRENCY", cfg.WorkerConcurrency); err != nil {
		return QueueConfig{}, err
	}
	if cfg.WorkerConcurrency > 64 {
		return QueueConfig{}, fmt.Errorf("TRPC_AGENT_WORKER_CONCURRENCY must not exceed 64")
	}
	if cfg.BlockTimeout, err = parsePositiveDurationEnv(
		"TRPC_AGENT_QUEUE_BLOCK_TIMEOUT", cfg.BlockTimeout,
	); err != nil {
		return QueueConfig{}, err
	}
	if cfg.ClaimMinIdle, err = parsePositiveDurationEnv(
		"TRPC_AGENT_QUEUE_CLAIM_MIN_IDLE", cfg.ClaimMinIdle,
	); err != nil {
		return QueueConfig{}, err
	}
	maxLen, err := parsePositiveIntEnv("TRPC_AGENT_QUEUE_MAX_LEN", int(cfg.MaxLen))
	if err != nil {
		return QueueConfig{}, err
	}
	cfg.MaxLen = int64(maxLen)

	switch cfg.Backend {
	case QueueBackendMemory:
		return cfg, nil
	case QueueBackendRedis:
		if cfg.RedisURL == "" {
			return QueueConfig{}, fmt.Errorf("REDIS_URL is required when queue backend is redis")
		}
		if err := validateRedisURL(cfg.RedisURL); err != nil {
			return QueueConfig{}, err
		}
		return cfg, nil
	default:
		return QueueConfig{}, fmt.Errorf(
			"unsupported TRPC_AGENT_QUEUE_BACKEND %q: use memory or redis",
			cfg.Backend,
		)
	}
}
