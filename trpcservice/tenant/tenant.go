// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

// ModelConfig selects the LLM backend of one tenant. Secrets are loaded from
// the gitignored config file (or MODEL_* environment overrides) and must
// never be written to logs, traces, or error reports.
type ModelConfig struct {
	Name    string `yaml:"name"`               // model name, e.g. "deepseek-v4-flash"
	APIKey  string `yaml:"api_key"`            // secret; KMS-encrypted in production deployments
	BaseURL string `yaml:"base_url,omitempty"` // optional OpenAI-compatible endpoint
}

// WeComBinding is one WeCom (企业微信) self-built app credential set. All
// fields are secrets or identifiers filled by the tenant admin; they must
// never be written to logs, traces, or error reports.
type WeComBinding struct {
	CorpID         string `yaml:"corp_id"`
	CorpSecret     string `yaml:"corp_secret"`
	AgentID        int    `yaml:"agent_id"`
	Token          string `yaml:"token"`
	EncodingAESKey string `yaml:"encoding_aes_key"`
}

// WeChatKfBinding is one WeChat customer service (微信客服) credential set.
// Unlike WeCom there is no agent_id: replies target the open_kfid carried by
// each callback event. All fields are secrets or identifiers filled by the
// tenant admin; they must never be written to logs, traces, or error reports.
type WeChatKfBinding struct {
	CorpID         string `yaml:"corp_id"`
	Secret         string `yaml:"secret"` // 微信客服 secret, distinct from the WeCom app secret
	Token          string `yaml:"token"`
	EncodingAESKey string `yaml:"encoding_aes_key"`
}

// Channels holds the IM channel bindings of one tenant.
type Channels struct {
	WeCom    *WeComBinding    `yaml:"wecom,omitempty"`
	WeChatKf *WeChatKfBinding `yaml:"wechat_kf,omitempty"`
}

// Guardrails is the per-tenant governance policy enforced by the Gateway
// (proposal doc 3.5): input checks run before the model call, output checks
// run as a streaming tripwire over reply chunks. Zero value means no policy.
type Guardrails struct {
	// MaxInputBytes rejects longer inputs; 0 means unlimited.
	MaxInputBytes int `yaml:"max_input_bytes,omitempty"`
	// BlockedKeywords are matched case-insensitively against user input.
	BlockedKeywords []string `yaml:"blocked_keywords,omitempty"`
	// OutputBlockedKeywords trip the streaming output checker.
	OutputBlockedKeywords []string `yaml:"output_blocked_keywords,omitempty"`
}

// Tools is the tenant-scoped capability policy. Allowed controls which built-in
// tools are visible to its agent; ApprovalRequired keeps a visible tool from
// executing until a host-side approval workflow supplies an allow decision.
type Tools struct {
	Allowed          []string `yaml:"allowed,omitempty"`
	ApprovalRequired []string `yaml:"approval_required,omitempty"`
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
}
