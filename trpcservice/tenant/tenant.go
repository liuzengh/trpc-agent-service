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
	ID              string           `json:"id" yaml:"id"`
	Name            string           `json:"name" yaml:"name"`
	Enabled         bool             `json:"enabled" yaml:"enabled"`
	Agent           AgentProfile     `json:"agent" yaml:"agent"`
	Model           ModelProfile     `json:"model" yaml:"model"`
	Backend         BackendProfile   `json:"backend" yaml:"backend"`
	Budget          BudgetPolicy     `json:"budget" yaml:"budget"`
	Channels        []ChannelBinding `json:"channels" yaml:"channels"`
	RuntimeRevision int64            `json:"-" yaml:"-"`
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

// BudgetPolicy is enforced before tRPC-Agent-Go invokes the model. Cost
// accounting rates are explicit configuration rather than hard-coded provider
// prices, so an operator can update them without rebuilding the platform.
type BudgetPolicy struct {
	MaxConcurrent           int     `json:"max_concurrent" yaml:"max_concurrent"`
	MaxInputTokens          int     `json:"max_input_tokens" yaml:"max_input_tokens"`
	MaxOutputTokens         int     `json:"max_output_tokens" yaml:"max_output_tokens"`
	DailyCostUSD            float64 `json:"daily_cost_usd" yaml:"daily_cost_usd"`
	InputCostPerMillionUSD  float64 `json:"input_cost_per_million_usd" yaml:"input_cost_per_million_usd"`
	OutputCostPerMillionUSD float64 `json:"output_cost_per_million_usd" yaml:"output_cost_per_million_usd"`
}

// RuntimeProfile is the immutable, published configuration consumed by a
// worker. Revision changes whenever a version is published or rolled back.
type RuntimeProfile struct {
	TenantID         string         `json:"tenant_id"`
	Agent            AgentProfile   `json:"agent"`
	Model            ModelProfile   `json:"model"`
	Backend          BackendProfile `json:"backend"`
	Budget           BudgetPolicy   `json:"budget"`
	PublishedVersion string         `json:"published_version"`
	Revision         int64          `json:"revision"`
}

func RuntimeProfileFromTenant(t Tenant) RuntimeProfile {
	return RuntimeProfile{
		TenantID: t.ID, Agent: t.Agent, Model: t.Model, Backend: t.Backend,
		Budget: t.Budget, PublishedVersion: t.Agent.Version,
	}
}

// Apply overlays a published runtime profile on the tenant identity and
// channel bindings supplied by the control-plane tenant record.
func (p RuntimeProfile) Apply(base Tenant) Tenant {
	base.Agent = p.Agent
	base.Model = p.Model
	base.Backend = p.Backend
	base.Budget = p.Budget
	if p.PublishedVersion != "" {
		base.Agent.Version = p.PublishedVersion
	}
	base.RuntimeRevision = p.Revision
	return base
}

func (p RuntimeProfile) Validate() error {
	if strings.TrimSpace(p.TenantID) == "" {
		return errors.New("runtime tenant id is required")
	}
	probe := Tenant{ID: p.TenantID, Agent: p.Agent, Model: p.Model, Backend: p.Backend, Budget: p.Budget}
	return probe.Validate()
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
	if t.Budget.MaxConcurrent < 0 || t.Budget.MaxInputTokens < 0 || t.Budget.MaxOutputTokens < 0 ||
		t.Budget.DailyCostUSD < 0 || t.Budget.InputCostPerMillionUSD < 0 || t.Budget.OutputCostPerMillionUSD < 0 {
		return errors.New("budget limits and accounting rates cannot be negative")
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
