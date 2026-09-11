package config

import "testing"

func TestSandboxConfigurationIsExplicit(t *testing.T) {
	for _, key := range []string{"TRPC_AGENT_SKILLS_ROOT", "TRPC_AGENT_SKILL_GRANTS_JSON", "TRPC_AGENT_SANDBOX_ENABLED", "TRPC_AGENT_SANDBOX_IMAGE", "TRPC_AGENT_SANDBOX_SOCKET"} {
		t.Setenv(key, "")
	}
	cfg, err := LoadSkillsConfigFromEnv()
	if err != nil || cfg.SandboxEnabled || cfg.Root != "" {
		t.Fatal("sandbox enabled by default", err)
	}
	t.Setenv("TRPC_AGENT_SANDBOX_ENABLED", "true")
	if _, err := LoadSkillsConfigFromEnv(); err == nil {
		t.Fatal("execution enabled without an explicit image")
	}
	t.Setenv("TRPC_AGENT_SKILLS_ROOT", "./skills")
	t.Setenv("TRPC_AGENT_SANDBOX_IMAGE", "alpine:3.22")
	if _, err := LoadSkillsConfigFromEnv(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRPC_AGENT_SKILLS_ROOT", "")
	if _, err := LoadSkillsConfigFromEnv(); err != nil {
		t.Fatal("database uploads must not require a deployment skill directory", err)
	}
}
