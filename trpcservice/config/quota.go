package config

import (
	"fmt"
	"os"
	"strings"
)

const (
	QuotaBackendLocal = "local"
	QuotaBackendRedis = "redis"
)

type QuotaConfig struct {
	Backend   string
	RedisURL  string
	KeyPrefix string
}

func LoadQuotaConfigFromEnv() (QuotaConfig, error) {
	config := QuotaConfig{
		Backend:   strings.ToLower(strings.TrimSpace(os.Getenv("TRPC_AGENT_QUOTA_BACKEND"))),
		RedisURL:  strings.TrimSpace(os.Getenv("REDIS_URL")),
		KeyPrefix: strings.TrimSpace(os.Getenv("REDIS_KEY_PREFIX")),
	}
	if config.Backend == "" {
		config.Backend = QuotaBackendLocal
	}
	if config.KeyPrefix == "" {
		config.KeyPrefix = "trpc-agent-service"
	}
	switch config.Backend {
	case QuotaBackendLocal:
		return config, nil
	case QuotaBackendRedis:
		if config.RedisURL == "" {
			return QuotaConfig{}, fmt.Errorf("REDIS_URL is required when quota backend is redis")
		}
		if err := validateRedisURL(config.RedisURL); err != nil {
			return QuotaConfig{}, err
		}
		return config, nil
	default:
		return QuotaConfig{}, fmt.Errorf("unsupported TRPC_AGENT_QUOTA_BACKEND %q", config.Backend)
	}
}
