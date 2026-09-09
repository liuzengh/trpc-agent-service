package tenant

import "fmt"

// ConfigVersion identifies an immutable published tenant configuration.
type ConfigVersion uint64

// SecretProvider identifies the system that owns a secret value.
type SecretProvider string

const (
	// SecretProviderEnv resolves secrets from process environment variables.
	SecretProviderEnv SecretProvider = "env"
	// SecretProviderFile resolves secrets from mounted secret files.
	SecretProviderFile SecretProvider = "file"
	// SecretProviderVault resolves secrets from a Vault-compatible service.
	SecretProviderVault SecretProvider = "vault"
	// SecretProviderKMS resolves encrypted secrets through a KMS provider.
	SecretProviderKMS SecretProvider = "kms"
)

// SecretRef identifies a secret without containing its value.
type SecretRef struct {
	Provider SecretProvider `json:"provider" yaml:"provider"`
	Key      string         `json:"key" yaml:"key"`
}

// IsZero reports whether no secret reference is configured.
func (ref SecretRef) IsZero() bool {
	return ref.Provider == "" && ref.Key == ""
}

// String intentionally omits the provider key so formatted errors and logs do
// not reveal secret lookup metadata.
func (ref SecretRef) String() string {
	if ref.IsZero() {
		return "<secret-ref:none>"
	}
	return fmt.Sprintf("<secret-ref:%s>", ref.Provider)
}

// Tenant is the top-level isolation boundary.
type Tenant struct {
	ID            string        `json:"tenant_id" yaml:"tenant_id"`
	Name          string        `json:"name" yaml:"name"`
	Enabled       bool          `json:"enabled" yaml:"enabled"`
	ConfigVersion ConfigVersion `json:"config_version" yaml:"config_version"`
	Audit         AuditPolicy   `json:"audit" yaml:"audit"`
	Runtime       RuntimePolicy `json:"runtime,omitempty" yaml:"runtime,omitempty"`
	Apps          []AgentApp    `json:"apps" yaml:"apps"`
}

// RuntimePolicy bounds shared execution resources for one tenant. Zero uses
// the documented conservative default so existing published versions remain
// valid while a later version can change the quota without restarting nodes.
type RuntimePolicy struct {
	MaxConcurrentRuns int `json:"max_concurrent_runs,omitempty" yaml:"max_concurrent_runs,omitempty"`
}

// ConcurrentRunLimit returns the effective cross-node Runner quota.
func (policy RuntimePolicy) ConcurrentRunLimit() int {
	if policy.MaxConcurrentRuns > 0 {
		return policy.MaxConcurrentRuns
	}
	return 8
}

// AgentApp contains one tenant-owned Agent application's runtime policy.
type AgentApp struct {
	ID            string             `json:"app_id" yaml:"app_id"`
	Name          string             `json:"name" yaml:"name"`
	Enabled       bool               `json:"enabled" yaml:"enabled"`
	Config        AppConfig          `json:"config" yaml:"config"`
	Workflow      WorkflowConfig     `json:"workflow,omitempty" yaml:"workflow,omitempty"`
	Model         ModelProfile       `json:"model" yaml:"model"`
	Tools         ToolPolicy         `json:"tools" yaml:"tools"`
	MCPServers    []MCPServer        `json:"mcp_servers,omitempty" yaml:"mcp_servers,omitempty"`
	BusinessTools []HTTPBusinessTool `json:"business_tools,omitempty" yaml:"business_tools,omitempty"`
	Channels      []ChannelBinding   `json:"channels" yaml:"channels"`
	Storage       StorageProfile     `json:"storage" yaml:"storage"`
	Knowledge     KnowledgePolicy    `json:"knowledge,omitempty" yaml:"knowledge,omitempty"`
}

type WorkflowType string

const (
	WorkflowLLM      WorkflowType = "llm"
	WorkflowChain    WorkflowType = "chain"
	WorkflowParallel WorkflowType = "parallel"
	WorkflowCycle    WorkflowType = "cycle"
	WorkflowGraph    WorkflowType = "graph"
)

// WorkflowConfig declares a bounded tRPC-Agent-Go composition. An empty type
// preserves the historical single LLMAgent runtime.
type WorkflowConfig struct {
	Type           WorkflowType   `json:"type,omitempty" yaml:"type,omitempty"`
	Nodes          []WorkflowNode `json:"nodes,omitempty" yaml:"nodes,omitempty"`
	Edges          []WorkflowEdge `json:"edges,omitempty" yaml:"edges,omitempty"`
	Entry          string         `json:"entry,omitempty" yaml:"entry,omitempty"`
	Finish         string         `json:"finish,omitempty" yaml:"finish,omitempty"`
	Aggregator     *WorkflowNode  `json:"aggregator,omitempty" yaml:"aggregator,omitempty"`
	MaxIterations  int            `json:"max_iterations,omitempty" yaml:"max_iterations,omitempty"`
	MaxConcurrency int            `json:"max_concurrency,omitempty" yaml:"max_concurrency,omitempty"`
}

type WorkflowNode struct {
	ID          string `json:"id" yaml:"id"`
	Instruction string `json:"instruction" yaml:"instruction"`
}

type WorkflowEdge struct {
	From string `json:"from" yaml:"from"`
	To   string `json:"to" yaml:"to"`
}

// KnowledgePolicy configures the optional tenant-scoped RAG tool. The API key
// remains a SecretRef and is resolved only while constructing an immutable
// Runtime Bundle.
type KnowledgePolicy struct {
	Enabled    bool             `json:"enabled" yaml:"enabled"`
	Embedding  EmbeddingProfile `json:"embedding" yaml:"embedding"`
	MaxResults int              `json:"max_results,omitempty" yaml:"max_results,omitempty"`
	MinScore   float64          `json:"min_score,omitempty" yaml:"min_score,omitempty"`
}

// EmbeddingProfile selects an OpenAI-compatible embeddings endpoint.
type EmbeddingProfile struct {
	Provider   string    `json:"provider" yaml:"provider"`
	Model      string    `json:"model" yaml:"model"`
	BaseURL    string    `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	APIKey     SecretRef `json:"api_key" yaml:"api_key"`
	Dimensions int       `json:"dimensions" yaml:"dimensions"`
}

// AppConfig contains model-independent Agent behavior.
type AppConfig struct {
	Instruction string `json:"instruction" yaml:"instruction"`
}

// ModelProfile selects and configures one model provider.
type ModelProfile struct {
	Provider    string       `json:"provider" yaml:"provider"`
	Name        string       `json:"name" yaml:"name"`
	BaseURL     string       `json:"base_url,omitempty" yaml:"base_url,omitempty"`
	APIKey      SecretRef    `json:"api_key,omitempty" yaml:"api_key,omitempty"`
	Temperature *float64     `json:"temperature,omitempty" yaml:"temperature,omitempty"`
	MaxTokens   int          `json:"max_tokens,omitempty" yaml:"max_tokens,omitempty"`
	Multimodal  bool         `json:"multimodal,omitempty" yaml:"multimodal,omitempty"`
	Pricing     ModelPricing `json:"pricing,omitempty" yaml:"pricing,omitempty"`
}

// ModelPricing is pinned in the tenant configuration version used by a run.
// Rates are expressed in micros per one million tokens so accounting remains
// integer-only and can distinguish prompt and completion prices.
type ModelPricing struct {
	Version                string `json:"version,omitempty" yaml:"version,omitempty"`
	InputMicrosPerMillion  int64  `json:"input_micros_per_million,omitempty" yaml:"input_micros_per_million,omitempty"`
	OutputMicrosPerMillion int64  `json:"output_micros_per_million,omitempty" yaml:"output_micros_per_million,omitempty"`
}

// ToolPolicy controls tenant-visible and tenant-executable tools.
type ToolPolicy struct {
	Allow                  []string `json:"allow,omitempty" yaml:"allow,omitempty"`
	Deny                   []string `json:"deny,omitempty" yaml:"deny,omitempty"`
	RequireApproval        []string `json:"require_approval,omitempty" yaml:"require_approval,omitempty"`
	RequestTokenBudget     int64    `json:"request_token_budget,omitempty" yaml:"request_token_budget,omitempty"`
	MonthlyCostBudgetCents int64    `json:"monthly_cost_budget_cents,omitempty" yaml:"monthly_cost_budget_cents,omitempty"`
}

// MCPServer is one administrator-published, tenant-scoped remote MCP server.
// Production accepts only named Streamable HTTP endpoints; models cannot
// supply ad-hoc URLs or start local stdio processes.
type MCPServer struct {
	ID               string    `json:"server_id" yaml:"server_id"`
	Endpoint         string    `json:"endpoint" yaml:"endpoint"`
	Credential       SecretRef `json:"credential,omitempty" yaml:"credential,omitempty"`
	CredentialHeader string    `json:"credential_header,omitempty" yaml:"credential_header,omitempty"`
	CredentialScheme string    `json:"credential_scheme,omitempty" yaml:"credential_scheme,omitempty"`
	AllowedTools     []string  `json:"allowed_tools" yaml:"allowed_tools"`
	Idempotent       bool      `json:"idempotent" yaml:"idempotent"`
	TimeoutSeconds   int       `json:"timeout_seconds,omitempty" yaml:"timeout_seconds,omitempty"`
	Enabled          bool      `json:"enabled" yaml:"enabled"`
}

// HTTPBusinessTool exposes one fixed HTTPS JSON endpoint as a function tool.
// The endpoint and credential are immutable configuration, never model input.
type HTTPBusinessTool struct {
	Name           string    `json:"name" yaml:"name"`
	Description    string    `json:"description" yaml:"description"`
	Endpoint       string    `json:"endpoint" yaml:"endpoint"`
	Credential     SecretRef `json:"credential,omitempty" yaml:"credential,omitempty"`
	TimeoutSeconds int       `json:"timeout_seconds,omitempty" yaml:"timeout_seconds,omitempty"`
	Enabled        bool      `json:"enabled" yaml:"enabled"`
}

// ChannelType identifies a supported inbound and outbound surface.
type ChannelType string

const (
	// ChannelTypeHTTP is the development and automation gateway.
	ChannelTypeHTTP ChannelType = "http"
	// ChannelTypeWeCom is the WeCom channel.
	ChannelTypeWeCom ChannelType = "wecom"
	// ChannelTypeFeishu is the Feishu (Lark) channel.
	ChannelTypeFeishu ChannelType = "feishu"
)

// ChannelBinding binds one provider account to a tenant application.
type ChannelBinding struct {
	ID                string      `json:"binding_id" yaml:"binding_id"`
	Type              ChannelType `json:"type" yaml:"type"`
	ProviderAccountID string      `json:"provider_account_id" yaml:"provider_account_id"`
	ProviderAppID     string      `json:"provider_app_id,omitempty" yaml:"provider_app_id,omitempty"`
	WebhookURL        string      `json:"webhook_url,omitempty" yaml:"webhook_url,omitempty"`
	Token             SecretRef   `json:"token,omitempty" yaml:"token,omitempty"`
	Secret            SecretRef   `json:"secret,omitempty" yaml:"secret,omitempty"`
	EncryptionKey     SecretRef   `json:"encryption_key,omitempty" yaml:"encryption_key,omitempty"`
	ReplyFormat       string      `json:"reply_format,omitempty" yaml:"reply_format,omitempty"`
	AllowedUsers      []string    `json:"allowed_users,omitempty" yaml:"allowed_users,omitempty"`
	AllowedChats      []string    `json:"allowed_chats,omitempty" yaml:"allowed_chats,omitempty"`
	Enabled           bool        `json:"enabled" yaml:"enabled"`
}

// AllowsIdentity applies the binding-local IM access list to provider-owned
// user and conversation identifiers. Empty lists preserve existing bindings.
// A chat-only policy intentionally denies direct messages.
func (binding ChannelBinding) AllowsIdentity(externalUserID, conversationID string) bool {
	hasUsers, hasChats := len(binding.AllowedUsers) > 0, len(binding.AllowedChats) > 0
	if !hasUsers && !hasChats {
		return true
	}
	userAllowed := !hasUsers || containsString(binding.AllowedUsers, externalUserID)
	if conversationID == "" {
		return hasUsers && userAllowed
	}
	return userAllowed && (!hasChats || containsString(binding.AllowedChats, conversationID))
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// BackendType identifies a storage implementation.
type BackendType string

const (
	BackendInMemory      BackendType = "inmemory"
	BackendRedis         BackendType = "redis"
	BackendMySQL         BackendType = "mysql"
	BackendPostgres      BackendType = "postgres"
	BackendSQLite        BackendType = "sqlite"
	BackendMongoDB       BackendType = "mongodb"
	BackendExternal      BackendType = "external"
	BackendLocal         BackendType = "local"
	BackendS3            BackendType = "s3"
	BackendCOS           BackendType = "cos"
	BackendQdrant        BackendType = "qdrant"
	BackendMilvus        BackendType = "milvus"
	BackendElasticsearch BackendType = "elasticsearch"
)

// BackendConfig selects one backend without embedding credentials.
type BackendConfig struct {
	Type            BackendType    `json:"type" yaml:"type"`
	Endpoint        string         `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	Credential      SecretRef      `json:"credential,omitempty" yaml:"credential,omitempty"`
	Namespace       string         `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	MigrationTarget *BackendConfig `json:"migration_target,omitempty" yaml:"migration_target,omitempty"`
}

// StorageProfile routes each data domain independently.
type StorageProfile struct {
	Session   BackendConfig `json:"session" yaml:"session"`
	Memory    BackendConfig `json:"memory" yaml:"memory"`
	Summary   BackendConfig `json:"summary" yaml:"summary"`
	Artifact  BackendConfig `json:"artifact" yaml:"artifact"`
	Knowledge BackendConfig `json:"knowledge" yaml:"knowledge"`
	Audit     BackendConfig `json:"audit" yaml:"audit"`
}

// AuditPolicy controls tenant audit retention and content handling.
type AuditPolicy struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
	// FailClosed rejects completion when an enabled audit sink cannot persist
	// the record. Keep false for non-regulated tenants that prefer availability.
	FailClosed    bool     `json:"fail_closed" yaml:"fail_closed"`
	RetentionDays int      `json:"retention_days" yaml:"retention_days"`
	StoreContent  bool     `json:"store_content" yaml:"store_content"`
	RedactFields  []string `json:"redact_fields,omitempty" yaml:"redact_fields,omitempty"`
}

// Clone returns a deep copy safe for mutation by the caller.
func (value Tenant) Clone() Tenant {
	cloned := value
	cloned.Audit = value.Audit.Clone()
	cloned.Apps = make([]AgentApp, len(value.Apps))
	for i := range value.Apps {
		cloned.Apps[i] = value.Apps[i].Clone()
	}
	return cloned
}

// Clone returns a deep copy safe for mutation by the caller.
func (value AgentApp) Clone() AgentApp {
	cloned := value
	cloned.Workflow.Nodes = append([]WorkflowNode(nil), value.Workflow.Nodes...)
	cloned.Workflow.Edges = append([]WorkflowEdge(nil), value.Workflow.Edges...)
	if value.Workflow.Aggregator != nil {
		aggregator := *value.Workflow.Aggregator
		cloned.Workflow.Aggregator = &aggregator
	}
	if value.Model.Temperature != nil {
		temperature := *value.Model.Temperature
		cloned.Model.Temperature = &temperature
	}
	cloned.Tools.Allow = append([]string(nil), value.Tools.Allow...)
	cloned.Tools.Deny = append([]string(nil), value.Tools.Deny...)
	cloned.Tools.RequireApproval = append(
		[]string(nil), value.Tools.RequireApproval...,
	)
	cloned.MCPServers = make([]MCPServer, len(value.MCPServers))
	for i := range value.MCPServers {
		cloned.MCPServers[i] = value.MCPServers[i]
		cloned.MCPServers[i].AllowedTools = append([]string(nil), value.MCPServers[i].AllowedTools...)
	}
	cloned.BusinessTools = append([]HTTPBusinessTool(nil), value.BusinessTools...)
	cloned.Channels = make([]ChannelBinding, len(value.Channels))
	for i := range value.Channels {
		cloned.Channels[i] = value.Channels[i]
		cloned.Channels[i].AllowedUsers = append([]string(nil), value.Channels[i].AllowedUsers...)
		cloned.Channels[i].AllowedChats = append([]string(nil), value.Channels[i].AllowedChats...)
	}
	cloned.Storage = value.Storage.Clone()
	return cloned
}

// Clone returns a deep copy of all independently routed storage domains.
func (value StorageProfile) Clone() StorageProfile {
	cloned := value
	cloned.Session = value.Session.Clone()
	cloned.Memory = value.Memory.Clone()
	cloned.Summary = value.Summary.Clone()
	cloned.Artifact = value.Artifact.Clone()
	cloned.Knowledge = value.Knowledge.Clone()
	cloned.Audit = value.Audit.Clone()
	return cloned
}

// Clone returns a deep copy of a route and its one-level migration target.
func (value BackendConfig) Clone() BackendConfig {
	cloned := value
	if value.MigrationTarget != nil {
		target := value.MigrationTarget.Clone()
		cloned.MigrationTarget = &target
	}
	return cloned
}

// Clone returns a deep copy safe for mutation by the caller.
func (value AuditPolicy) Clone() AuditPolicy {
	cloned := value
	cloned.RedactFields = append([]string(nil), value.RedactFields...)
	return cloned
}
