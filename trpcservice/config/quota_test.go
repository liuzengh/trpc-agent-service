package config

import "testing"

func TestLoadQuotaConfig(t *testing.T) {
	t.Setenv("TRPC_AGENT_QUOTA_BACKEND", "redis")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/0")
	config, err := LoadQuotaConfigFromEnv()
	if err != nil || config.Backend != QuotaBackendRedis {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}
