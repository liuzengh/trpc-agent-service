package config

import (
	"fmt"
	"os"
	"strings"
)

type AdminConfig struct {
	Enabled bool
	Token   string
}

func LoadAdminConfigFromEnv() (AdminConfig, error) {
	enabled, err := parseBoolEnv("TRPC_AGENT_ADMIN_ENABLED", false)
	if err != nil {
		return AdminConfig{}, err
	}
	config := AdminConfig{
		Enabled: enabled,
		Token:   strings.TrimSpace(os.Getenv("TRPC_AGENT_ADMIN_TOKEN")),
	}
	if config.Enabled && len(config.Token) < 24 {
		return AdminConfig{}, fmt.Errorf("TRPC_AGENT_ADMIN_TOKEN must contain at least 24 characters")
	}
	return config, nil
}
