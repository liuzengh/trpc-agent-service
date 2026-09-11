package config

import (
	"errors"
	"os"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/workspace"
)

// SkillsConfig enables deployment-owned skills and optional Docker execution.
type SkillsConfig struct {
	Root, GrantsJSON string
	SandboxEnabled   bool
	Sandbox          workspace.Config
}

func LoadSkillsConfigFromEnv() (SkillsConfig, error) {
	enabled, err := parseBoolEnv("TRPC_AGENT_SANDBOX_ENABLED", false)
	if err != nil {
		return SkillsConfig{}, err
	}
	cfg := SkillsConfig{Root: os.Getenv("TRPC_AGENT_SKILLS_ROOT"), GrantsJSON: os.Getenv("TRPC_AGENT_SKILL_GRANTS_JSON"), SandboxEnabled: enabled,
		Sandbox: workspace.Config{Image: os.Getenv("TRPC_AGENT_SANDBOX_IMAGE"), Socket: os.Getenv("TRPC_AGENT_SANDBOX_SOCKET"), Timeout: 10 * time.Second}}
	if enabled && cfg.Sandbox.Image == "" {
		return cfg, errors.New("sandbox requires an explicitly configured local image")
	}
	return cfg, nil
}
