package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type AdminConfig struct {
	Enabled    bool
	Token      string
	Principals []AdminPrincipalConfig
}

type AdminPrincipalConfig struct {
	Name      string   `json:"name"`
	Token     string   `json:"token"`
	Role      string   `json:"role"`
	TenantIDs []string `json:"tenant_ids"`
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
	if raw := strings.TrimSpace(os.Getenv("TRPC_AGENT_ADMIN_PRINCIPALS_JSON")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &config.Principals); err != nil {
			return AdminConfig{}, fmt.Errorf("parse TRPC_AGENT_ADMIN_PRINCIPALS_JSON: %w", err)
		}
	}
	if config.Token != "" {
		if len(config.Token) < 24 {
			return AdminConfig{}, fmt.Errorf("TRPC_AGENT_ADMIN_TOKEN must contain at least 24 characters")
		}
		config.Principals = append(config.Principals, AdminPrincipalConfig{
			Name: "legacy-superadmin", Token: config.Token, Role: "superadmin",
		})
	}
	if config.Enabled && len(config.Principals) == 0 {
		return AdminConfig{}, fmt.Errorf("admin requires TRPC_AGENT_ADMIN_TOKEN or TRPC_AGENT_ADMIN_PRINCIPALS_JSON")
	}
	for _, principal := range config.Principals {
		if strings.TrimSpace(principal.Name) == "" || len(principal.Token) < 24 {
			return AdminConfig{}, fmt.Errorf("admin principal name and token are invalid")
		}
	}
	return config, nil
}
