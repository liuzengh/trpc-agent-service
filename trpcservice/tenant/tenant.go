// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

// ModelConfig selects the LLM backend of one tenant. Secrets are loaded from
// the gitignored config file (or MODEL_* environment overrides) and must
// never be written to logs, traces, or error reports.
type ModelConfig struct {
	Name    string // model name, e.g. "deepseek-v4-flash"
	APIKey  string // secret; KMS-encrypted in production deployments
	BaseURL string // optional OpenAI-compatible endpoint
}

// Context is the per-tenant configuration carried across the platform.
// Routing, execution, storage, and telemetry all key off ID (tenant_id).
type Context struct {
	ID    string
	Name  string
	Model ModelConfig
}
