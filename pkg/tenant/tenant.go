//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package tenant

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

const (
	// DefaultMaxLLMCalls bounds legacy Agent configurations which predate the
	// explicit field. It preserves availability without retaining an unbounded
	// model/tool loop.
	DefaultMaxLLMCalls = 8
	// MaxConfiguredLLMCalls limits the blast radius of a tenant-selected value.
	MaxConfiguredLLMCalls = 32
)

// TenantStatus represents the tenant lifecycle status.
type TenantStatus string

const (
	// TenantStatusActive indicates the tenant is active and operational.
	TenantStatusActive TenantStatus = "active"
	// TenantStatusSuspended indicates the tenant is temporarily suspended.
	TenantStatusSuspended TenantStatus = "suspended"
	// TenantStatusDeleted indicates the tenant is marked for deletion.
	TenantStatusDeleted TenantStatus = "deleted"
)

// Tenant represents a multi-tenant configuration entity.
type Tenant struct {
	ID            string       `json:"id"`
	Name          string       `json:"name"`
	CreatedAt     time.Time    `json:"createdAt"`
	UpdatedAt     time.Time    `json:"updatedAt"`
	Status        TenantStatus `json:"status"`
	ConfigVersion int64        `json:"configVersion"`

	// Agent Configuration
	Agents []AgentConfig `json:"agents"`

	// Model Configuration
	Models []ModelConfig `json:"models"`

	// Tool Permissions
	ToolPolicy ToolPolicy `json:"toolPolicy"`

	// IM Channel Bindings
	Channels []ChannelBinding `json:"channels"`

	// Storage Configuration
	Storage StorageConfig `json:"storage"`

	// Governance Policies
	Governance GovernancePolicy `json:"governance"`

	// Budget and Limits
	Budget BudgetConfig `json:"budget"`
}

// AgentConfig defines an agent configuration for a tenant.
type AgentConfig struct {
	Name         string              `json:"name"`
	Type         string              `json:"type"` // llm, chain, graph, parallel, cycle
	SystemPrompt string              `json:"systemPrompt"`
	DefaultModel string              `json:"defaultModel"`
	MaxLLMCalls  int                 `json:"maxLLMCalls,omitempty"`
	Tools        []string            `json:"tools"` // Tool names allowed for this agent
	Metadata     map[string]string   `json:"metadata,omitempty"`
	Runtime      *AgentRuntimeConfig `json:"runtime,omitempty"`
}

// AgentRuntimeConfig is the immutable topology for a composite runtime. Every
// node uses the version's bound model while keeping its own prompt, tool subset
// and call limit. Edges are used only by graph runtimes; array order defines
// chain, parallel and cycle node order.
type AgentRuntimeConfig struct {
	Nodes         []AgentRuntimeNode `json:"nodes"`
	Edges         []AgentRuntimeEdge `json:"edges,omitempty"`
	Entry         string             `json:"entry,omitempty"`
	MaxIterations int                `json:"maxIterations,omitempty"`
}

// AgentRuntimeNode configures one concrete LLMAgent node inside a composite.
type AgentRuntimeNode struct {
	Name         string   `json:"name"`
	SystemPrompt string   `json:"systemPrompt,omitempty"`
	MaxLLMCalls  int      `json:"maxLLMCalls,omitempty"`
	Tools        []string `json:"tools,omitempty"`
}

// EffectiveMaxLLMCalls returns the node-local provider-call limit. Composite
// nodes default to one call so the parent can enforce an exact aggregate cap.
func (n AgentRuntimeNode) EffectiveMaxLLMCalls() int {
	if n.MaxLLMCalls > 0 {
		return n.MaxLLMCalls
	}
	return 1
}

// AgentRuntimeEdge connects two named nodes in a graph runtime.
type AgentRuntimeEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Canonical agent runtime type names. A type being syntactically supported
// does not mean its implementation is installed in a process; composition
// must still provide a matching runtime factory.
const (
	AgentTypeLLM      = "llm"
	AgentTypeChain    = "chain"
	AgentTypeGraph    = "graph"
	AgentTypeParallel = "parallel"
	AgentTypeCycle    = "cycle"
)

// EffectiveMaxLLMCalls returns the bounded execution limit used by Worker.
func (a AgentConfig) EffectiveMaxLLMCalls() int {
	if a.MaxLLMCalls > 0 {
		return a.MaxLLMCalls
	}
	return DefaultMaxLLMCalls
}

// ModelConfig defines a model provider configuration.
type ModelConfig struct {
	Provider    string            `json:"provider"` // openai, anthropic, gemini, ollama, bedrock
	ModelName   string            `json:"modelName"`
	APIKey      string            `json:"apiKey"`              // Encrypted at rest
	APIKeyRef   string            `json:"apiKeyRef,omitempty"` // Operator-owned SecretRef; resolved only at runtime.
	Endpoint    string            `json:"endpoint,omitempty"`
	MaxTokens   int               `json:"maxTokens,omitempty"`
	Temperature float64           `json:"temperature,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ToolPolicy defines tool access control rules.
type ToolPolicy struct {
	Mode                string   `json:"mode"` // whitelist, blacklist
	Allowed             []string `json:"allowed,omitempty"`
	Denied              []string `json:"denied,omitempty"`
	RequireConfirmation []string `json:"requireConfirmation,omitempty"`
}

// IsAllowed checks if a tool is allowed under the policy.
func (p *ToolPolicy) IsAllowed(toolName string) bool {
	if p.Mode == "whitelist" {
		return contains(p.Allowed, toolName)
	}
	// Blacklist mode
	return !contains(p.Denied, toolName)
}

// RequiresConfirmation checks if a tool requires user confirmation.
func (p *ToolPolicy) RequiresConfirmation(toolName string) bool {
	return contains(p.RequireConfirmation, toolName)
}

// ChannelBinding represents an IM channel binding.
type ChannelBinding struct {
	AccountID         string              `json:"accountId"`  // Stable, non-secret channel account identifier.
	WebhookKey        string              `json:"webhookKey"` // Non-secret lookup key used only in the callback URL.
	AgentApp          string              `json:"agentApp"`   // Agent app bound to this channel account.
	Type              string              `json:"type"`       // wework, telegram, wechat_service, wechat_mp
	WebhookURL        string              `json:"webhookUrl"`
	Token             string              `json:"token"`                       // For signature verification
	TokenRef          string              `json:"tokenRef,omitempty"`          // Operator-owned SecretRef for the token.
	Secret            string              `json:"secret"`                      // Encrypted at rest
	SecretRef         string              `json:"secretRef,omitempty"`         // Operator-owned SecretRef for the secret.
	EncodingAESKey    string              `json:"encodingAESKey,omitempty"`    // Encrypted at rest.
	EncodingAESKeyRef string              `json:"encodingAESKeyRef,omitempty"` // Operator-owned SecretRef for the AES key.
	AppID             string              `json:"appId,omitempty"`
	Config            map[string]string   `json:"config,omitempty"`
	AccessPolicy      ChannelAccessPolicy `json:"accessPolicy"`
}

// ChannelAccessPolicy is an exact provider-identity allowlist. A zero value
// denies every message. Group messages require both an allowed sender and an
// allowed conversation so membership in one trusted group does not authorize
// the same user in every group.
type ChannelAccessPolicy struct {
	AllowDirectMessages bool     `json:"allowDirectMessages"`
	AllowGroupMessages  bool     `json:"allowGroupMessages"`
	AllowedUsers        []string `json:"allowedUsers"`
	AllowedGroups       []string `json:"allowedGroups,omitempty"`
}

// EffectiveWebhookKey returns only the non-secret routing identifier. It does
// not fall back to Token: putting a provider credential in a webhook URL would
// leak it through ingress, proxy and access logs for legacy records.
func (b *ChannelBinding) EffectiveWebhookKey() string {
	return b.WebhookKey
}

// EnsureAccountID initializes a stable, non-secret identifier used in
// idempotency keys and session namespaces. It deliberately never exposes the
// token itself.
func (b *ChannelBinding) EnsureAccountID() string {
	if b.AccountID != "" {
		return b.AccountID
	}
	if configured := b.Config["account_id"]; configured != "" {
		b.AccountID = configured
		return b.AccountID
	}
	if b.AppID != "" {
		b.AccountID = b.AppID
		return b.AccountID
	}
	digest := sha256.Sum256([]byte(b.Type + "\x00" + b.EffectiveWebhookKey()))
	b.AccountID = "acct_" + hex.EncodeToString(digest[:8])
	return b.AccountID
}

// StorageConfig defines backend storage configuration.
type StorageConfig struct {
	SessionBackend   string            `json:"sessionBackend"` // installed: inmemory, redis, postgres
	SessionProfile   string            `json:"sessionProfile,omitempty"`
	SessionConfig    map[string]string `json:"sessionConfig,omitempty"`
	MemoryBackend    string            `json:"memoryBackend"`
	MemoryProfile    string            `json:"memoryProfile,omitempty"`
	MemoryConfig     map[string]string `json:"memoryConfig,omitempty"`
	KnowledgeBackend string            `json:"knowledgeBackend,omitempty"` // installed: qdrant
	KnowledgeProfile string            `json:"knowledgeProfile,omitempty"`
	ArtifactBackend  string            `json:"artifactBackend,omitempty"` // installed: s3
	ArtifactProfile  string            `json:"artifactProfile,omitempty"`
}

// GovernancePolicy defines security and compliance policies.
type GovernancePolicy struct {
	DataMasking    []MaskingRule   `json:"dataMasking,omitempty"`
	AuditLevel     string          `json:"auditLevel"` // basic: outcomes/tool decisions; detailed also persists pre-Runner admission; empty is basic
	ContentFilters []ContentFilter `json:"contentFilters,omitempty"`
}

// MaskingRule defines a data masking rule.
type MaskingRule struct {
	Type    string `json:"type"`              // credit_card, email, phone, ssn, api_key, custom
	Pattern string `json:"pattern,omitempty"` // Regex pattern
	Replace string `json:"replace"`           // Replacement template
}

// ContentFilter defines content filtering rules.
type ContentFilter struct {
	Name     string   `json:"name"`
	Type     string   `json:"type"` // keyword, regex, ml_model
	Patterns []string `json:"patterns,omitempty"`
	Action   string   `json:"action"` // block, warn, log
}

// BudgetConfig defines resource limits and budgets.
type BudgetConfig struct {
	MaxTokensPerDay       int64     `json:"maxTokensPerDay,omitempty"`
	MaxTokensPerRequest   int64     `json:"maxTokensPerRequest,omitempty"`
	MaxCostPerDay         float64   `json:"maxCostPerDay,omitempty"` // USD
	MaxConcurrentSessions int       `json:"maxConcurrentSessions,omitempty"`
	AlertThresholds       []float64 `json:"alertThresholds,omitempty"`
}
