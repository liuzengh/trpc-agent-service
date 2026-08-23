// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import (
	"errors"
	"fmt"
	"strings"
)

// Tenant is the immutable runtime profile resolved before every Agent run.
// SecretRef values point to a provider; secret material never belongs here.
type Tenant struct {
	ID       string           `json:"id" yaml:"id"`
	Name     string           `json:"name" yaml:"name"`
	Enabled  bool             `json:"enabled" yaml:"enabled"`
	Agent    AgentProfile     `json:"agent" yaml:"agent"`
	Model    ModelProfile     `json:"model" yaml:"model"`
	Backend  BackendProfile   `json:"backend" yaml:"backend"`
	Channels []ChannelBinding `json:"channels" yaml:"channels"`
}

type AgentProfile struct {
	ID            string   `json:"id" yaml:"id"`
	Version       string   `json:"version" yaml:"version"`
	Instruction   string   `json:"instruction" yaml:"instruction"`
	ToolAllowlist []string `json:"tool_allowlist" yaml:"tool_allowlist"`
}

type ModelProfile struct {
	Provider  string `json:"provider" yaml:"provider"`
	BaseURL   string `json:"base_url" yaml:"base_url"`
	Model     string `json:"model" yaml:"model"`
	APIKeyRef string `json:"api_key_ref" yaml:"api_key_ref"`
	Timeout   string `json:"timeout" yaml:"timeout"`
}

type BackendProfile struct {
	Session   string `json:"session" yaml:"session"`
	Memory    string `json:"memory" yaml:"memory"`
	Knowledge string `json:"knowledge" yaml:"knowledge"`
	Artifact  string `json:"artifact" yaml:"artifact"`
	Namespace string `json:"namespace" yaml:"namespace"`
}

type ChannelBinding struct {
	ID            string `json:"id" yaml:"id"`
	Type          string `json:"type" yaml:"type"`
	Enabled       bool   `json:"enabled" yaml:"enabled"`
	CredentialRef string `json:"credential_ref" yaml:"credential_ref"`
}

func (t Tenant) Validate() error {
	if strings.TrimSpace(t.ID) == "" {
		return errors.New("tenant id is required")
	}
	if strings.ContainsAny(t.ID, "|/\\") {
		return fmt.Errorf("tenant id %q contains reserved characters", t.ID)
	}
	if t.Agent.ID == "" || t.Agent.Version == "" {
		return errors.New("agent id and version are required")
	}
	seen := make(map[string]struct{}, len(t.Channels))
	for _, binding := range t.Channels {
		if binding.ID == "" || binding.Type == "" {
			return errors.New("channel binding id and type are required")
		}
		if _, ok := seen[binding.ID]; ok {
			return fmt.Errorf("duplicate channel binding %q", binding.ID)
		}
		seen[binding.ID] = struct{}{}
	}
	return nil
}

func (t Tenant) AllowsTool(name string) bool {
	for _, allowed := range t.Agent.ToolAllowlist {
		if allowed == name {
			return true
		}
	}
	return false
}
