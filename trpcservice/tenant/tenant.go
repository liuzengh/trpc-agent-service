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

// Channels holds the IM channel bindings of one tenant.
type Channels struct {
	WeCom *WeComBinding `yaml:"wecom,omitempty"`
}

// Context is the per-tenant configuration carried across the platform.
// Routing, execution, storage, and telemetry all key off ID (tenant_id).
// The yaml tags double as the persistence and Admin API wire shape.
type Context struct {
	ID       string      `yaml:"id"`
	Name     string      `yaml:"name,omitempty"`
	Model    ModelConfig `yaml:"model"`
	Channels Channels    `yaml:"channels,omitempty"`
}
