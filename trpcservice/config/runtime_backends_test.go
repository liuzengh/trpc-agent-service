package config

import "testing"

func TestUnusedRoleBackendsDoNotReadRemoteSettings(t *testing.T) {
	for _, key := range []string{"TRPC_AGENT_SESSION_BACKEND", "TRPC_AGENT_COORDINATOR_BACKEND", "TRPC_AGENT_IDEMPOTENCY_BACKEND", "TRPC_AGENT_QUEUE_BACKEND", "TRPC_AGENT_QUOTA_BACKEND"} {
		t.Setenv(key, "redis")
	}
	t.Setenv("REDIS_URL", "")
	for _, name := range []string{RoleSender, RoleAdmin} {
		roles, _ := ParseRole(name)
		cfg, err := LoadRuntimeBackends(roles, false)
		if err != nil || cfg.Session.Backend != SessionBackendInMemory || cfg.Queue.Backend != QueueBackendMemory {
			t.Fatal("unused role required remote credentials")
		}
	}
	t.Setenv("REDIS_URL", "redis://127.0.0.1:6379/0")
	t.Setenv("REDIS_KEY_PREFIX", "test-platform")
	roles, _ := ParseRole(RoleGateway)
	cfg, err := LoadRuntimeBackends(roles, true)
	if err != nil || cfg.PollCoordinator.RedisPrefix != "test-platform:channel-poll" || cfg.Coordinator.Backend != CoordinatorBackendLocal || cfg.Quota.Backend != QuotaBackendRedis || cfg.Queue.Backend != QueueBackendMemory {
		t.Fatal("gateway backend scope is incorrect")
	}
	roles, _ = ParseRole(RoleAll)
	cfg, err = LoadRuntimeBackends(roles, true)
	if err != nil || cfg.Session.Backend != SessionBackendRedis || cfg.Queue.Backend != QueueBackendRedis || cfg.Coordinator.RedisPrefix == cfg.PollCoordinator.RedisPrefix {
		t.Fatal("all role lost shared backends or poll namespace")
	}
}
