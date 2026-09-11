// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import "time"

// ModelConfig selects the LLM backend of one tenant. Secrets are loaded from
// the gitignored config file (or MODEL_* environment overrides) and must
// never be written to logs, traces, or error reports.
type ModelConfig struct {
	Name                string `yaml:"name"`               // model name, e.g. "deepseek-v4-flash"
	APIKey              string `yaml:"api_key"`            // secret; KMS-encrypted in production
	BaseURL             string `yaml:"base_url,omitempty"` // optional OpenAI-compatible endpoint
	PromptCostPer1K     uint32 `yaml:"prompt_cost_per_1k,omitempty"`
	CompletionCostPer1K uint32 `yaml:"completion_cost_per_1k,omitempty"`
}

// WeComBinding is one WeCom (企业微信) self-built app credential set. All
// fields are secrets or identifiers filled by the tenant admin; they must
// never be written to logs, traces, or error reports.
//
// Tagged for json as well as yaml: in the reliable path a binding lives in
// channel_bindings.config, and a struct without json tags serializes with Go
// field names — so the same credential would have two spellings depending on
// which mode read it. That trap already caught Guardrails; this is the same
// fix applied before it bites.
type WeComBinding struct {
	CorpID         string `yaml:"corp_id" json:"corp_id"`
	CorpSecret     string `yaml:"corp_secret" json:"corp_secret"`
	AgentID        int    `yaml:"agent_id" json:"agent_id"`
	Token          string `yaml:"token" json:"token"`
	EncodingAESKey string `yaml:"encoding_aes_key" json:"encoding_aes_key"`
}

// WeChatKfBinding is one WeChat customer service (微信客服) credential set.
// Unlike WeCom there is no agent_id: replies target the open_kfid carried by
// each callback event. All fields are secrets or identifiers filled by the
// tenant admin; they must never be written to logs, traces, or error reports.
// Tagged for json for the same reason as WeComBinding.
type WeChatKfBinding struct {
	CorpID         string `yaml:"corp_id" json:"corp_id"`
	Secret         string `yaml:"secret" json:"secret"` // 微信客服 secret, distinct from the WeCom app secret
	Token          string `yaml:"token" json:"token"`
	EncodingAESKey string `yaml:"encoding_aes_key" json:"encoding_aes_key"`
}

// Channels holds the IM channel bindings of one tenant.
type Channels struct {
	WeCom    *WeComBinding    `yaml:"wecom,omitempty"`
	WeChatKf *WeChatKfBinding `yaml:"wechat_kf,omitempty"`
}

// Guardrails is the per-tenant governance policy enforced by the Gateway
// (proposal doc 3.5): input checks run before the model call, output checks
// run as a streaming tripwire over reply chunks. Zero value means no policy.
//
// The json tags match the yaml tags on purpose. This struct is serialized
// two ways: into config.yaml the same way everything else on a tenant is,
// and into a JSON column of a published revision. Without explicit json
// tags, the second path silently uses Go field names instead, so the same
// policy would have two spellings depending on which table it was read from
// — measured while wiring the execution loop, not anticipated.
type Guardrails struct {
	// MaxInputBytes rejects longer inputs; 0 means unlimited.
	MaxInputBytes int `yaml:"max_input_bytes,omitempty" json:"max_input_bytes,omitempty"`
	// BlockedKeywords are matched case-insensitively against user input.
	BlockedKeywords []string `yaml:"blocked_keywords,omitempty" json:"blocked_keywords,omitempty"`
	// OutputBlockedKeywords trip the streaming output checker.
	OutputBlockedKeywords []string `yaml:"output_blocked_keywords,omitempty" json:"output_blocked_keywords,omitempty"`
	// Budget limits. Zero means unlimited (no budget enforcement).
	// MaxPromptTokens caps prompt tokens per reset window.
	MaxPromptTokens uint64 `yaml:"max_prompt_tokens,omitempty" json:"max_prompt_tokens,omitempty"`
	// MaxCompletionTokens caps completion tokens per reset window.
	MaxCompletionTokens uint64 `yaml:"max_completion_tokens,omitempty" json:"max_completion_tokens,omitempty"`
	// MaxCostCents caps model cost in cents per reset window.
	MaxCostCents uint64 `yaml:"max_cost_cents,omitempty" json:"max_cost_cents,omitempty"`
	// MaxCalls caps the number of model calls per reset window.
	MaxCalls uint64 `yaml:"max_calls,omitempty" json:"max_calls,omitempty"`
	// BudgetResetInterval is the duration after which counters reset
	// (e.g. "24h" or "720h"). Zero means no reset (lifetime budget).
	BudgetResetInterval time.Duration `yaml:"-" json:"-"`
}

// Tools is the tenant-scoped capability policy. Allowed controls which built-in
// tools are visible to its agent; ApprovalRequired keeps a visible tool from
// executing until a host-side approval workflow supplies an allow decision.
//
// Tagged for both yaml and json for the same reason as Guardrails.
type Tools struct {
	Allowed          []string `yaml:"allowed,omitempty" json:"allowed,omitempty"`
	ApprovalRequired []string `yaml:"approval_required,omitempty" json:"approval_required,omitempty"`
}

// Context is the per-tenant configuration carried across the platform.
// Routing, execution, storage, and telemetry all key off ID (tenant_id).
// The yaml tags double as the persistence and Admin API wire shape.
type Context struct {
	ID         string      `yaml:"id"`
	Name       string      `yaml:"name,omitempty"`
	Model      ModelConfig `yaml:"model"`
	Channels   Channels    `yaml:"channels,omitempty"`
	Guardrails Guardrails  `yaml:"guardrails,omitempty"`
	Tools      Tools       `yaml:"tools,omitempty"`
	// Instruction is the agent's system prompt. Empty means "use the built-in
	// default", which is what every config written before this field existed
	// has; a published revision always sets it explicitly, since the revision
	// spec is the only place a prompt is allowed to be defined once an
	// app is under version control.
	Instruction string `yaml:"instruction,omitempty"`
}
