package config

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

var (
	safeID          = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,62}$`)
	safeRevision    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	envReference    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	secretReference = regexp.MustCompile(`^secret://[a-z][a-z0-9_-]{0,31}/[A-Za-z0-9._:/-]{1,240}(#[A-Za-z0-9._-]{1,64})?$`)
	numericID       = regexp.MustCompile(`^[1-9][0-9]*$`)
)

type Config struct {
	DeploymentMode string              `yaml:"deployment_mode" json:"deployment_mode,omitempty"`
	Server         ServerConfig        `yaml:"server" json:"server"`
	Coordination   CoordinationConfig  `yaml:"coordination" json:"coordination"`
	Queue          QueueConfig         `yaml:"queue" json:"queue"`
	ControlPlane   ControlPlaneConfig  `yaml:"control_plane" json:"control_plane"`
	Admin          AdminConfig         `yaml:"admin" json:"admin"`
	Secrets        SecretConfig        `yaml:"secrets" json:"secrets"`
	Health         HealthConfig        `yaml:"health" json:"health"`
	ContentSafety  ContentSafetyConfig `yaml:"content_safety" json:"content_safety"`
	Telemetry      TelemetryConfig     `yaml:"telemetry" json:"telemetry"`
	Tenants        []TenantConfig      `yaml:"tenants" json:"tenants"`
}

type ServerConfig struct {
	Address       string `yaml:"address" json:"address"`
	QueueSize     int    `yaml:"queue_size" json:"queue_size"`
	WorkerCount   int    `yaml:"worker_count" json:"worker_count"`
	AdminTokenEnv string `yaml:"admin_token_env" json:"admin_token_env,omitempty"`
}

// AdminConfig controls management-plane authentication. local_token is kept
// for local/demo runs; production deployments must use oidc.
type AdminConfig struct {
	Mode         string        `yaml:"mode" json:"mode"`
	Issuer       string        `yaml:"issuer" json:"issuer,omitempty"`
	Audience     string        `yaml:"audience" json:"audience,omitempty"`
	JWKSURL      string        `yaml:"jwks_url" json:"jwks_url,omitempty"`
	RolesClaim   string        `yaml:"roles_claim" json:"roles_claim,omitempty"`
	TenantsClaim string        `yaml:"tenants_claim" json:"tenants_claim,omitempty"`
	ClockSkew    time.Duration `yaml:"clock_skew" json:"clock_skew"`
	JWKSCacheTTL time.Duration `yaml:"jwks_cache_ttl" json:"jwks_cache_ttl"`
}

// SecretConfig selects the reference resolver. Secret values never belong in
// Config and are never serialized to revisions or admin responses.
type SecretConfig struct {
	Provider   string        `yaml:"provider" json:"provider"`
	CacheTTL   time.Duration `yaml:"cache_ttl" json:"cache_ttl"`
	FailClosed bool          `yaml:"fail_closed" json:"fail_closed"`
}

type HealthConfig struct {
	ProbeInterval time.Duration `yaml:"probe_interval" json:"probe_interval"`
	ProbeTimeout  time.Duration `yaml:"probe_timeout" json:"probe_timeout"`
}

type ContentSafetyConfig struct {
	Enabled       bool          `yaml:"enabled" json:"enabled"`
	Backend       string        `yaml:"backend" json:"backend"`
	FailClosed    bool          `yaml:"fail_closed" json:"fail_closed"`
	PolicyVersion string        `yaml:"policy_version" json:"policy_version"`
	Timeout       time.Duration `yaml:"timeout" json:"timeout"`
	LeaseTTL      time.Duration `yaml:"lease_ttl" json:"lease_ttl"`
}

type CoordinationConfig struct {
	Backend     string        `yaml:"backend" json:"backend"`
	RedisURLEnv string        `yaml:"redis_url_env" json:"redis_url_env,omitempty"`
	KeyPrefix   string        `yaml:"key_prefix" json:"key_prefix"`
	LockTTL     time.Duration `yaml:"lock_ttl" json:"lock_ttl"`
	DedupTTL    time.Duration `yaml:"dedup_ttl" json:"dedup_ttl"`
}

type TelemetryConfig struct {
	ServiceName  string `yaml:"service_name" json:"service_name"`
	OTLPEndpoint string `yaml:"otlp_endpoint" json:"otlp_endpoint,omitempty"`
	OTLPProtocol string `yaml:"otlp_protocol" json:"otlp_protocol,omitempty"`
}

// QueueConfig selects the message pipeline backend. "inmemory" keeps the
// legacy in-process queue; "postgres" enables the durable Inbox/Outbox where
// callbacks are ACKed only after persistence and replies survive restarts.
type QueueConfig struct {
	Backend      string        `yaml:"backend" json:"backend"`
	DSNEnv       string        `yaml:"dsn_env" json:"dsn_env,omitempty"`
	PollInterval time.Duration `yaml:"poll_interval" json:"poll_interval"`
	LeaseTTL     time.Duration `yaml:"lease_ttl" json:"lease_ttl"`
	// BatchSize is retained as the deployment-facing name, but the durable
	// dispatcher interprets it as the maximum concurrent Inbox leases per
	// process. Each relay worker leases exactly one immediately runnable row.
	BatchSize         int           `yaml:"batch_size" json:"batch_size"`
	InboxMaxAttempts  int           `yaml:"inbox_max_attempts" json:"inbox_max_attempts"`
	OutboxMaxAttempts int           `yaml:"outbox_max_attempts" json:"outbox_max_attempts"`
	RetryBase         time.Duration `yaml:"retry_base" json:"retry_base"`
	RetryMax          time.Duration `yaml:"retry_max" json:"retry_max"`
}

// ControlPlaneConfig selects the source of truth for tenant revisions and
// release state. PostgreSQL mode deliberately shares QueueConfig.DSNEnv so a
// service cannot accidentally use a second, independently migrated database.
type ControlPlaneConfig struct {
	Backend           string        `yaml:"backend" json:"backend"`
	RefreshInterval   time.Duration `yaml:"refresh_interval" json:"refresh_interval"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" json:"heartbeat_interval"`
	HeartbeatTTL      time.Duration `yaml:"heartbeat_ttl" json:"heartbeat_ttl"`
	AckTimeout        time.Duration `yaml:"ack_timeout" json:"ack_timeout"`
	NodeIDEnv         string        `yaml:"node_id_env" json:"node_id_env"`
}

type TenantConfig struct {
	TenantID string          `yaml:"tenant_id" json:"tenant_id"`
	Version  string          `yaml:"version" json:"version"`
	Enabled  bool            `yaml:"enabled" json:"enabled"`
	App      AppConfig       `yaml:"app" json:"app"`
	Model    ModelConfig     `yaml:"model" json:"model"`
	Tools    ToolPolicy      `yaml:"tools" json:"tools"`
	Skills   SkillPolicy     `yaml:"skills,omitempty" json:"skills,omitzero"`
	Channels []ChannelConfig `yaml:"channels" json:"channels"`
	Data     DataConfig      `yaml:"data" json:"data"`
	Audit    AuditPolicy     `yaml:"audit" json:"audit"`
	Budget   BudgetPolicy    `yaml:"budget" json:"budget"`
	Privacy  PrivacyPolicy   `yaml:"privacy,omitempty" json:"privacy,omitzero"`
}

// PrivacyPolicy controls text sent to models and replies sent to IM. Empty/off
// retains old revision behavior. Redact replaces matches; block rejects them.
type PrivacyPolicy struct {
	Input  string `yaml:"input" json:"input"`
	Output string `yaml:"output" json:"output"`
}

type AppConfig struct {
	Name        string `yaml:"name" json:"name"`
	AgentName   string `yaml:"agent_name" json:"agent_name"`
	Description string `yaml:"description" json:"description"`
	Instruction string `yaml:"instruction" json:"instruction"`
}

type ModelConfig struct {
	Provider         string  `yaml:"provider" json:"provider"`
	Name             string  `yaml:"name" json:"name"`
	Variant          string  `yaml:"variant" json:"variant,omitempty"`
	BaseURL          string  `yaml:"base_url" json:"base_url,omitempty"`
	APIKeyEnv        string  `yaml:"api_key_env" json:"api_key_env,omitempty"`
	FallbackProvider string  `yaml:"fallback_provider" json:"fallback_provider,omitempty"`
	MaxTokens        int     `yaml:"max_tokens" json:"max_tokens"`
	Temperature      float64 `yaml:"temperature" json:"temperature"`
	Streaming        bool    `yaml:"streaming" json:"streaming"`
	InputPrice       float64 `yaml:"input_price_per_million" json:"input_price_per_million,omitempty"`
	OutputPrice      float64 `yaml:"output_price_per_million" json:"output_price_per_million,omitempty"`
}

type ToolPolicy struct {
	Allow          []string `yaml:"allow" json:"allow"`
	Deny           []string `yaml:"deny" json:"deny"`
	RequireConfirm []string `yaml:"require_confirmation" json:"require_confirmation"`
	// SideEffects identifies tools whose successful execution may mutate an
	// external system. Worker routes these calls through the durable operation
	// ledger; arguments are represented only by a canonical SHA-256 digest.
	SideEffects []string `yaml:"side_effects" json:"side_effects"`
}

// SkillPolicy grants access to exact skill names in the operator-managed
// repository. An omitted or empty allowlist exposes no skill resources.
type SkillPolicy struct {
	Allow []string `yaml:"allow" json:"allow"`
}

type ChannelConfig struct {
	Type             string   `yaml:"type" json:"type"`
	BindingID        string   `yaml:"binding_id" json:"binding_id"`
	Enabled          bool     `yaml:"enabled" json:"enabled"`
	BotIDEnv         string   `yaml:"bot_id_env" json:"bot_id_env,omitempty"`
	BotSecretEnv     string   `yaml:"bot_secret_env" json:"bot_secret_env,omitempty"`
	TokenEnv         string   `yaml:"token_env" json:"token_env,omitempty"`
	SigningSecretEnv string   `yaml:"signing_secret_env" json:"signing_secret_env,omitempty"`
	EncryptionKeyEnv string   `yaml:"encryption_key_env" json:"encryption_key_env,omitempty"`
	APIBaseURL       string   `yaml:"api_base_url" json:"api_base_url,omitempty"`
	WebSocketURL     string   `yaml:"websocket_url" json:"websocket_url,omitempty"`
	AllowedUsers     []string `yaml:"allowed_users" json:"allowed_users,omitempty"`
	MaxMessageLength int      `yaml:"max_message_length" json:"max_message_length,omitempty"`
	WorkspaceID      string   `yaml:"workspace_id" json:"workspace_id,omitempty"`
	ApplicationID    string   `yaml:"application_id" json:"application_id,omitempty"`
}

type DataConfig struct {
	Session   BackendConfig `yaml:"session" json:"session"`
	Memory    BackendConfig `yaml:"memory" json:"memory"`
	Summary   BackendConfig `yaml:"summary" json:"summary"`
	Artifact  BackendConfig `yaml:"artifact" json:"artifact"`
	Knowledge BackendConfig `yaml:"knowledge" json:"knowledge"`
	AuditLog  BackendConfig `yaml:"audit_log" json:"audit_log"`
}

type BackendConfig struct {
	Type     string `yaml:"type" json:"type"`
	Provider string `yaml:"provider" json:"provider,omitempty"`
	DSNEnv   string `yaml:"dsn_env" json:"dsn_env,omitempty"`
	// MigrationDSNEnv is mounted only into the one-shot backend migrator. The
	// long-running service never resolves it.
	MigrationDSNEnv string `yaml:"migration_dsn_env" json:"migration_dsn_env,omitempty"`
	Namespace       string `yaml:"namespace" json:"namespace,omitempty"`
	Bucket          string `yaml:"bucket" json:"bucket,omitempty"`

	// Endpoint is non-secret connection metadata. Credentials are always
	// indirect environment references so admin/config projections cannot leak
	// their values.
	Endpoint        string `yaml:"endpoint" json:"endpoint,omitempty"`
	Region          string `yaml:"region" json:"region,omitempty"`
	APIKeyEnv       string `yaml:"api_key_env" json:"api_key_env,omitempty"`
	AccessKeyEnv    string `yaml:"access_key_env" json:"access_key_env,omitempty"`
	SecretKeyEnv    string `yaml:"secret_key_env" json:"secret_key_env,omitempty"`
	SessionTokenEnv string `yaml:"session_token_env" json:"session_token_env,omitempty"`
	PathStyle       bool   `yaml:"path_style" json:"path_style,omitempty"`

	// Vector knowledge uses an OpenAI-compatible embedding endpoint. Namespace
	// is the pre-migrated PostgreSQL table name.
	EmbeddingModel     string `yaml:"embedding_model" json:"embedding_model,omitempty"`
	EmbeddingDimension int    `yaml:"embedding_dimension" json:"embedding_dimension,omitempty"`
	EmbeddingBaseURL   string `yaml:"embedding_base_url" json:"embedding_base_url,omitempty"`

	// Mode selects provider protocol variants, currently cloud or self_hosted
	// for external Mem0. Async controls transcript ingestion delivery.
	Mode  string `yaml:"mode" json:"mode,omitempty"`
	Async *bool  `yaml:"async" json:"async,omitempty"`
}

type AuditPolicy struct {
	Enabled        bool     `yaml:"enabled" json:"enabled"`
	Sink           string   `yaml:"sink" json:"sink"`
	Path           string   `yaml:"path" json:"path,omitempty"`
	SpoolPath      string   `yaml:"spool_path" json:"spool_path,omitempty"`
	RedactPatterns []string `yaml:"redact_patterns" json:"redact_patterns,omitempty"`
	LogContent     bool     `yaml:"log_content" json:"log_content"`
}

type BudgetPolicy struct {
	RequestsPerMinute int     `yaml:"requests_per_minute" json:"requests_per_minute"`
	MaxInputChars     int     `yaml:"max_input_chars" json:"max_input_chars"`
	MonthlyCostUSD    float64 `yaml:"monthly_cost_usd" json:"monthly_cost_usd"`
}

func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()
	return Decode(f)
}

// LoadStatic decodes the process/static portion of the YAML. Tenant entries
// are intentionally not required to be present or authoritative once the
// persistent control plane has been bootstrapped.
func LoadStatic(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()
	return DecodeStatic(f)
}

func Decode(r io.Reader) (*Config, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	applyDefaults(&cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func DecodeStatic(r io.Reader) (*Config, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	applyDefaults(&cfg)
	if err := cfg.ValidateStatic(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(c *Config) {
	if c.DeploymentMode == "" {
		c.DeploymentMode = "local"
	}
	if c.Server.Address == "" {
		c.Server.Address = ":8080"
	}
	if c.Server.QueueSize <= 0 {
		c.Server.QueueSize = 256
	}
	if c.Server.WorkerCount <= 0 {
		c.Server.WorkerCount = 4
	}
	if c.Coordination.Backend == "" {
		c.Coordination.Backend = "inmemory"
	}
	if c.Coordination.KeyPrefix == "" {
		c.Coordination.KeyPrefix = "trpc-agent"
	}
	if c.Coordination.LockTTL <= 0 {
		c.Coordination.LockTTL = 2 * time.Minute
	}
	if c.Coordination.DedupTTL <= 0 {
		c.Coordination.DedupTTL = 24 * time.Hour
	}
	if c.Queue.Backend == "" {
		c.Queue.Backend = "inmemory"
	}
	if c.Queue.PollInterval <= 0 {
		c.Queue.PollInterval = 250 * time.Millisecond
	}
	if c.Queue.LeaseTTL <= 0 {
		c.Queue.LeaseTTL = 2 * time.Minute
	}
	if c.Queue.BatchSize <= 0 {
		c.Queue.BatchSize = 16
	}
	if c.Queue.InboxMaxAttempts <= 0 {
		c.Queue.InboxMaxAttempts = 5
	}
	if c.Queue.OutboxMaxAttempts <= 0 {
		c.Queue.OutboxMaxAttempts = 8
	}
	if c.Queue.RetryBase <= 0 {
		c.Queue.RetryBase = 500 * time.Millisecond
	}
	if c.Queue.RetryMax <= 0 {
		c.Queue.RetryMax = time.Minute
	}
	if c.ControlPlane.Backend == "" {
		if c.Queue.Backend == "postgres" {
			c.ControlPlane.Backend = "postgres"
		} else {
			c.ControlPlane.Backend = "inmemory"
		}
	}
	if c.ControlPlane.RefreshInterval <= 0 {
		c.ControlPlane.RefreshInterval = 5 * time.Second
	}
	if c.ControlPlane.HeartbeatInterval <= 0 {
		c.ControlPlane.HeartbeatInterval = 5 * time.Second
	}
	if c.ControlPlane.HeartbeatTTL <= 0 {
		c.ControlPlane.HeartbeatTTL = 30 * time.Second
	}
	if c.ControlPlane.AckTimeout <= 0 {
		c.ControlPlane.AckTimeout = 30 * time.Second
	}
	if c.ControlPlane.NodeIDEnv == "" {
		c.ControlPlane.NodeIDEnv = "TRPC_CONFIG_NODE_ID"
	}
	if c.Admin.Mode == "" {
		c.Admin.Mode = "local_token"
	}
	if c.Admin.RolesClaim == "" {
		c.Admin.RolesClaim = "roles"
	}
	if c.Admin.TenantsClaim == "" {
		c.Admin.TenantsClaim = "tenants"
	}
	if c.Admin.ClockSkew <= 0 {
		c.Admin.ClockSkew = time.Minute
	}
	if c.Admin.JWKSCacheTTL <= 0 {
		c.Admin.JWKSCacheTTL = 5 * time.Minute
	}
	if c.Secrets.Provider == "" {
		c.Secrets.Provider = "env"
	}
	if c.Secrets.CacheTTL <= 0 {
		c.Secrets.CacheTTL = 5 * time.Minute
	}
	if c.DeploymentMode == "production" {
		c.Secrets.FailClosed = true
	}
	if c.Health.ProbeInterval <= 0 {
		c.Health.ProbeInterval = 5 * time.Second
	}
	if c.Health.ProbeTimeout <= 0 {
		c.Health.ProbeTimeout = 2 * time.Second
	}
	if c.ContentSafety.Backend == "" {
		c.ContentSafety.Backend = "postgres"
	}
	if c.ContentSafety.PolicyVersion == "" {
		c.ContentSafety.PolicyVersion = "default-v1"
	}
	if c.ContentSafety.Timeout <= 0 {
		c.ContentSafety.Timeout = 2 * time.Second
	}
	if c.ContentSafety.LeaseTTL <= 0 {
		c.ContentSafety.LeaseTTL = 30 * time.Second
	}
	if c.DeploymentMode == "production" {
		c.ContentSafety.Enabled = true
	}
	if c.Telemetry.ServiceName == "" {
		c.Telemetry.ServiceName = "trpc-agent-service"
	}
	for i := range c.Tenants {
		t := &c.Tenants[i]
		if t.Version == "" {
			t.Version = "1"
		}
		if t.Model.MaxTokens <= 0 {
			t.Model.MaxTokens = 2048
		}
		if t.Budget.MaxInputChars <= 0 {
			t.Budget.MaxInputChars = 12000
		}
		if t.Budget.RequestsPerMinute <= 0 {
			t.Budget.RequestsPerMinute = 60
		}
		for j := range t.Channels {
			if t.Channels[j].MaxMessageLength <= 0 {
				switch t.Channels[j].Type {
				case "telegram":
					t.Channels[j].MaxMessageLength = 4096
				case "slack":
					t.Channels[j].MaxMessageLength = 40000
				case "wecom":
					// WeCom limits text content to 2048 UTF-8 bytes.
					t.Channels[j].MaxMessageLength = 2048
				case "wecom-aibot":
					// Intelligent bot stream/markdown replies support 20 KiB.
					t.Channels[j].MaxMessageLength = 20480
				default:
					t.Channels[j].MaxMessageLength = 4000
				}
			}
		}
	}
}

func (c *Config) Validate() error {
	if err := c.ValidateStatic(); err != nil {
		return err
	}
	if len(c.Tenants) == 0 {
		return errors.New("config: at least one tenant is required")
	}
	tenantIDs := map[string]struct{}{}
	bindings := map[string]string{}
	for i := range c.Tenants {
		t := &c.Tenants[i]
		if !safeID.MatchString(t.TenantID) {
			return fmt.Errorf("config: invalid tenant_id %q", t.TenantID)
		}
		if !safeRevision.MatchString(t.Version) {
			return fmt.Errorf("config: tenant %s has invalid version", t.TenantID)
		}
		if _, exists := tenantIDs[t.TenantID]; exists {
			return fmt.Errorf("config: duplicate tenant_id %q", t.TenantID)
		}
		tenantIDs[t.TenantID] = struct{}{}
		if !safeID.MatchString(t.App.Name) || !safeID.MatchString(t.App.AgentName) {
			return fmt.Errorf("config: tenant %s has invalid app or agent name", t.TenantID)
		}
		if t.Model.Provider != "mock" && t.Model.Provider != "openai" {
			return fmt.Errorf("config: tenant %s has unsupported model provider %q", t.TenantID, t.Model.Provider)
		}
		if t.Model.Provider == "openai" && (t.Model.Name == "" || t.Model.APIKeyEnv == "") {
			return fmt.Errorf("config: tenant %s openai model requires name and api_key_env", t.TenantID)
		}
		if t.Model.FallbackProvider != "" {
			if t.Model.Provider != "openai" || t.Model.FallbackProvider != "mock" {
				return fmt.Errorf("config: tenant %s model fallback_provider supports only openai primary with mock fallback", t.TenantID)
			}
			if t.Model.Streaming {
				return fmt.Errorf("config: tenant %s model streaming must be false when fallback_provider is configured", t.TenantID)
			}
		}
		if err := validateOptionalEnvReference("tenant model api_key_env", t.Model.APIKeyEnv); err != nil {
			return err
		}
		if err := validateTools(t.TenantID, t.Tools); err != nil {
			return err
		}
		seenSkills := make(map[string]bool, len(t.Skills.Allow))
		for _, name := range t.Skills.Allow {
			if !validToolName(name) || strings.ContainsAny(name, "/\\") || name == "." || name == ".." || seenSkills[name] {
				return fmt.Errorf("config: tenant %s skills.allow requires unique exact skill names", t.TenantID)
			}
			seenSkills[name] = true
		}
		for _, mode := range []string{t.Privacy.Input, t.Privacy.Output} {
			if mode != "" && mode != "off" && mode != "redact" && mode != "block" {
				return fmt.Errorf("config: tenant %s privacy mode must be off, redact or block", t.TenantID)
			}
		}
		if err := validateData(t); err != nil {
			return err
		}
		if c.Queue.Backend == "postgres" && t.Data.Session.Type == "sql" &&
			t.Data.Session.DSNEnv != c.Queue.DSNEnv {
			return fmt.Errorf(
				"config: tenant %s sql session dsn_env must match queue.dsn_env for atomic turn commit",
				t.TenantID,
			)
		}
		if t.Audit.Sink != "" && t.Audit.Sink != "stdout" && t.Audit.Sink != "file" && t.Audit.Sink != "sql" && t.Audit.Sink != "disabled" {
			return fmt.Errorf("config: tenant %s has unsupported audit sink %q", t.TenantID, t.Audit.Sink)
		}
		if t.Audit.Sink == "file" && t.Audit.Path == "" {
			return fmt.Errorf("config: tenant %s file audit sink requires path", t.TenantID)
		}
		if t.Audit.Sink == "sql" && t.Data.AuditLog.Type != "sql" {
			return fmt.Errorf("config: tenant %s sql audit sink requires data.audit_log.type=sql", t.TenantID)
		}
		if t.Audit.Sink == "sql" && c.Queue.Backend != "postgres" {
			return fmt.Errorf("config: tenant %s sql audit sink requires queue.backend=postgres", t.TenantID)
		}
		if c.Queue.Backend == "postgres" && t.Audit.Sink == "sql" &&
			t.Data.AuditLog.DSNEnv != c.Queue.DSNEnv {
			return fmt.Errorf("config: tenant %s sql audit dsn_env must match queue.dsn_env", t.TenantID)
		}
		if t.Audit.LogContent {
			return fmt.Errorf("config: tenant %s raw audit content logging is not supported", t.TenantID)
		}
		for _, pattern := range t.Audit.RedactPatterns {
			if _, err := regexp.Compile(pattern); err != nil {
				return fmt.Errorf("config: tenant %s has invalid audit redact pattern: %w", t.TenantID, err)
			}
		}
		for _, ch := range t.Channels {
			if ch.Type != "telegram" && ch.Type != "slack" && ch.Type != "wecom" && ch.Type != "wecom-aibot" {
				return fmt.Errorf("config: tenant %s has unsupported channel %q", t.TenantID, ch.Type)
			}
			if !safeID.MatchString(ch.BindingID) {
				return fmt.Errorf("config: tenant %s has invalid binding_id %q", t.TenantID, ch.BindingID)
			}
			if ch.Type != "wecom-aibot" && (ch.SigningSecretEnv == "" || ch.TokenEnv == "") {
				return fmt.Errorf("config: tenant %s channel %s requires token_env and signing_secret_env", t.TenantID, ch.BindingID)
			}
			if ch.Type == "wecom-aibot" && (ch.BotIDEnv == "" || ch.BotSecretEnv == "") {
				return fmt.Errorf("config: tenant %s WeCom intelligent bot channel %s requires bot_id_env and bot_secret_env", t.TenantID, ch.BindingID)
			}
			if ch.Type == "slack" && (!safeRevision.MatchString(ch.WorkspaceID) || !safeRevision.MatchString(ch.ApplicationID)) {
				return fmt.Errorf("config: tenant %s Slack channel %s requires valid workspace_id and application_id", t.TenantID, ch.BindingID)
			}
			if ch.Type == "wecom" {
				// workspace_id is the corpid and application_id is the numeric
				// agentid; both bind the encrypted callback to one app.
				if !safeRevision.MatchString(ch.WorkspaceID) || !numericID.MatchString(ch.ApplicationID) {
					return fmt.Errorf("config: tenant %s WeCom channel %s requires valid workspace_id (corpid) and application_id (agentid)", t.TenantID, ch.BindingID)
				}
				if ch.EncryptionKeyEnv == "" {
					return fmt.Errorf("config: tenant %s WeCom channel %s requires encryption_key_env", t.TenantID, ch.BindingID)
				}
				if err := validateOptionalEnvReference("channel encryption_key_env", ch.EncryptionKeyEnv); err != nil {
					return err
				}
			}
			if err := validateOptionalEnvReference("channel token_env", ch.TokenEnv); err != nil {
				return err
			}
			if err := validateOptionalEnvReference("channel bot_id_env", ch.BotIDEnv); err != nil {
				return err
			}
			if err := validateOptionalEnvReference("channel bot_secret_env", ch.BotSecretEnv); err != nil {
				return err
			}
			if err := validateOptionalEnvReference("channel signing_secret_env", ch.SigningSecretEnv); err != nil {
				return err
			}
			key := ch.Type + "/" + ch.BindingID
			if owner, exists := bindings[key]; exists {
				return fmt.Errorf("config: binding %s is shared by tenants %s and %s", key, owner, t.TenantID)
			}
			bindings[key] = t.TenantID
		}
	}
	return nil
}

// ValidateStatic checks only configuration needed to start the process. It is
// used after a persistent control plane exists so a stale or partial tenant
// list in YAML cannot replace the database authority.
func (c *Config) ValidateStatic() error {
	deploymentMode := c.DeploymentMode
	if deploymentMode == "" {
		deploymentMode = "local"
	}
	if deploymentMode != "local" && deploymentMode != "production" && deploymentMode != "test" {
		return fmt.Errorf("config: unsupported deployment_mode %q", c.DeploymentMode)
	}
	if c.Coordination.Backend != "inmemory" && c.Coordination.Backend != "redis" {
		return fmt.Errorf("config: unsupported coordination backend %q", c.Coordination.Backend)
	}
	switch c.Queue.Backend {
	case "", "inmemory", "memory":
	case "postgres":
		if c.Queue.DSNEnv == "" {
			return errors.New("config: queue backend postgres requires dsn_env")
		}
	default:
		return fmt.Errorf("config: unsupported queue backend %q", c.Queue.Backend)
	}
	if err := validateOptionalEnvReference("queue.dsn_env", c.Queue.DSNEnv); err != nil {
		return err
	}
	if err := validateOptionalEnvReference("server.admin_token_env", c.Server.AdminTokenEnv); err != nil {
		return err
	}
	adminMode := c.Admin.Mode
	if adminMode == "" {
		adminMode = "local_token"
	}
	if adminMode != "local_token" && adminMode != "oidc" {
		return fmt.Errorf("config: unsupported admin.mode %q", c.Admin.Mode)
	}
	if adminMode == "oidc" {
		if c.Admin.Issuer == "" || c.Admin.Audience == "" || c.Admin.JWKSURL == "" {
			return errors.New("config: admin oidc requires issuer, audience and jwks_url")
		}
		if err := validatePublicEndpoint("admin.jwks_url", c.Admin.JWKSURL); err != nil {
			return err
		}
	}
	secretProvider := c.Secrets.Provider
	if secretProvider == "" {
		secretProvider = "env"
	}
	if secretProvider != "env" && secretProvider != "external" && secretProvider != "file" {
		return fmt.Errorf("config: unsupported secrets.provider %q", c.Secrets.Provider)
	}
	if c.Health.ProbeInterval < 0 || c.Health.ProbeTimeout < 0 {
		return errors.New("config: health probe intervals must be positive")
	}
	safetyBackend := c.ContentSafety.Backend
	if safetyBackend == "" {
		safetyBackend = "disabled"
	}
	if safetyBackend != "postgres" && safetyBackend != "memory" && safetyBackend != "disabled" {
		return fmt.Errorf("config: unsupported content_safety.backend %q", c.ContentSafety.Backend)
	}
	if c.ContentSafety.Enabled && safetyBackend == "postgres" && c.Queue.Backend != "postgres" {
		return errors.New("config: postgres content safety requires postgres queue")
	}
	if deploymentMode == "production" {
		if c.Queue.Backend != "postgres" || c.ControlPlane.Backend != "postgres" || c.Coordination.Backend != "redis" {
			return errors.New("config: production requires postgres queue/control_plane and redis coordination")
		}
		if adminMode != "oidc" {
			return errors.New("config: production requires admin oidc authentication")
		}
		if secretProvider == "env" || !c.Secrets.FailClosed {
			return errors.New("config: production requires fail-closed external secret references")
		}
		if !c.ContentSafety.Enabled || safetyBackend != "postgres" {
			return errors.New("config: production requires postgres content safety")
		}
	}
	if c.Coordination.Backend == "redis" && c.Coordination.RedisURLEnv == "" {
		return errors.New("config: coordination.redis_url_env is required for redis")
	}
	if err := validateOptionalEnvReference("coordination.redis_url_env", c.Coordination.RedisURLEnv); err != nil {
		return err
	}
	if c.Coordination.LockTTL > 0 && c.Coordination.LockTTL < 3*time.Second {
		return errors.New("config: coordination.lock_ttl must be at least 3s")
	}
	if c.Telemetry.OTLPProtocol != "" && c.Telemetry.OTLPProtocol != "http" && c.Telemetry.OTLPProtocol != "grpc" {
		return fmt.Errorf("config: unsupported telemetry.otlp_protocol %q", c.Telemetry.OTLPProtocol)
	}
	switch c.ControlPlane.Backend {
	case "", "inmemory", "memory":
	case "postgres":
		if c.Queue.Backend != "postgres" {
			return errors.New("config: control_plane backend postgres requires queue backend postgres")
		}
		if c.Queue.DSNEnv == "" {
			return errors.New("config: control_plane backend postgres requires queue.dsn_env")
		}
	default:
		return fmt.Errorf("config: unsupported control_plane backend %q", c.ControlPlane.Backend)
	}
	if err := validateOptionalEnvReference("control_plane.node_id_env", c.ControlPlane.NodeIDEnv); err != nil {
		return err
	}
	if c.ControlPlane.Backend != "" && (c.ControlPlane.RefreshInterval <= 0 || c.ControlPlane.HeartbeatInterval <= 0 || c.ControlPlane.HeartbeatTTL <= 0 || c.ControlPlane.AckTimeout <= 0) {
		return errors.New("config: control_plane intervals must be positive")
	}
	if c.ControlPlane.HeartbeatTTL < c.ControlPlane.HeartbeatInterval {
		return errors.New("config: control_plane heartbeat_ttl must be at least heartbeat_interval")
	}
	if c.ControlPlane.Backend == "postgres" && c.ControlPlane.NodeIDEnv == "" {
		return errors.New("config: control_plane postgres requires node_id_env")
	}
	return nil
}

// ValidateTenantConfig validates one persisted revision independently of the
// process-level YAML. It is used by revision imports and by every node before
// acknowledging a prepared release.
func ValidateTenantConfig(tenant TenantConfig) error {
	cfg := &Config{Tenants: []TenantConfig{tenant}}
	applyDefaults(cfg)
	return cfg.Validate()
}

func validateTools(tenantID string, p ToolPolicy) error {
	for field, names := range map[string][]string{
		"allow": p.Allow, "deny": p.Deny,
		"require_confirmation": p.RequireConfirm, "side_effects": p.SideEffects,
	} {
		seen := make(map[string]struct{}, len(names))
		for _, name := range names {
			if !validToolName(name) {
				return fmt.Errorf("config: tenant %s tools.%s contains an invalid tool name", tenantID, field)
			}
			if _, exists := seen[name]; exists {
				return fmt.Errorf("config: tenant %s tools.%s contains duplicate tool %q", tenantID, field, name)
			}
			seen[name] = struct{}{}
		}
	}
	denied := make(map[string]struct{}, len(p.Deny))
	for _, name := range p.Deny {
		denied[name] = struct{}{}
	}
	for _, name := range p.Allow {
		if _, exists := denied[name]; exists {
			return fmt.Errorf("config: tenant %s tool %q is both allowed and denied", tenantID, name)
		}
	}
	allowed := make(map[string]struct{}, len(p.Allow))
	for _, name := range p.Allow {
		allowed[name] = struct{}{}
	}
	for _, name := range p.RequireConfirm {
		if _, exists := allowed[name]; !exists {
			return fmt.Errorf("config: tenant %s confirmation tool %q must be allowed", tenantID, name)
		}
	}
	sideEffects := make(map[string]struct{}, len(p.SideEffects))
	for _, name := range p.SideEffects {
		if _, exists := allowed[name]; !exists {
			return fmt.Errorf("config: tenant %s side-effect tool %q must be allowed", tenantID, name)
		}
		if _, exists := denied[name]; exists {
			return fmt.Errorf("config: tenant %s side-effect tool %q cannot be denied", tenantID, name)
		}
		sideEffects[name] = struct{}{}
	}
	for _, name := range p.RequireConfirm {
		if _, exists := sideEffects[name]; !exists {
			return fmt.Errorf("config: tenant %s confirmation tool %q must be declared in side_effects", tenantID, name)
		}
	}
	return nil
}

func validToolName(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		switch r {
		case '-', '_', '.', ':', '/':
			continue
		default:
			return false
		}
	}
	return true
}

func validateData(t *TenantConfig) error {
	backends := []struct {
		name string
		cfg  BackendConfig
	}{
		{"session", t.Data.Session}, {"memory", t.Data.Memory},
		{"summary", t.Data.Summary}, {"artifact", t.Data.Artifact},
		{"knowledge", t.Data.Knowledge}, {"audit_log", t.Data.AuditLog},
	}
	allowed := map[string]bool{
		"disabled": true, "inmemory": true, "redis": true, "sql": true,
		"vector": true, "object": true, "external": true, "stdout": true, "file": true,
	}
	for _, item := range backends {
		if item.cfg.Type == "" {
			return fmt.Errorf("config: tenant %s data.%s.type is required", t.TenantID, item.name)
		}
		if !allowed[item.cfg.Type] {
			return fmt.Errorf("config: tenant %s data.%s has unsupported type %q", t.TenantID, item.name, item.cfg.Type)
		}
		if (item.cfg.Type == "redis" || item.cfg.Type == "sql" || item.cfg.Type == "vector") && item.cfg.DSNEnv == "" {
			return fmt.Errorf("config: tenant %s data.%s requires dsn_env", t.TenantID, item.name)
		}
		if err := validateOptionalEnvReference("data backend dsn_env", item.cfg.DSNEnv); err != nil {
			return err
		}
		for field, value := range map[string]string{
			"migration_dsn_env": item.cfg.MigrationDSNEnv, "api_key_env": item.cfg.APIKeyEnv, "access_key_env": item.cfg.AccessKeyEnv,
			"secret_key_env": item.cfg.SecretKeyEnv, "session_token_env": item.cfg.SessionTokenEnv,
		} {
			if err := validateOptionalEnvReference("data backend "+field, value); err != nil {
				return err
			}
		}
		if err := validatePublicEndpoint("data backend endpoint", item.cfg.Endpoint); err != nil {
			return err
		}
		if err := validatePublicEndpoint("data backend embedding_base_url", item.cfg.EmbeddingBaseURL); err != nil {
			return err
		}
	}
	if t.Data.Session.Type != "inmemory" && t.Data.Session.Type != "redis" && t.Data.Session.Type != "sql" {
		return fmt.Errorf("config: tenant %s runnable session backend must be inmemory, redis or sql", t.TenantID)
	}
	if t.Data.Memory.Type != "disabled" && t.Data.Memory.Type != "inmemory" && t.Data.Memory.Type != "redis" && t.Data.Memory.Type != "external" {
		return fmt.Errorf("config: tenant %s runnable memory backend must be disabled, inmemory, redis or external", t.TenantID)
	}
	if t.Data.Memory.Type == "external" {
		if t.Data.Memory.Provider != "mem0" {
			return fmt.Errorf("config: tenant %s external memory provider must be mem0", t.TenantID)
		}
		if t.Data.Memory.Endpoint == "" {
			return fmt.Errorf("config: tenant %s external memory requires endpoint", t.TenantID)
		}
		if t.Data.Memory.Mode != "cloud" && t.Data.Memory.Mode != "self_hosted" {
			return fmt.Errorf("config: tenant %s external memory mode must be cloud or self_hosted", t.TenantID)
		}
		if t.Data.Memory.Mode == "cloud" && t.Data.Memory.APIKeyEnv == "" {
			return fmt.Errorf("config: tenant %s cloud external memory requires api_key_env", t.TenantID)
		}
		for _, name := range []string{"memory_add", "memory_update", "memory_delete", "memory_clear"} {
			if stringInSlice(t.Tools.Allow, name) {
				return fmt.Errorf("config: tenant %s external mem0 memory exposes read-only tools; %s cannot be allowed", t.TenantID, name)
			}
		}
	}
	// Summary is part of the framework Session Service in the current runtime;
	// it is not an independently constructed backend. Require the declaration
	// to match the authoritative Session backend instead of silently ignoring a
	// different SQL/Redis/namespace selection.
	if t.Data.Summary.Type != "disabled" && t.Data.Summary.Type != t.Data.Session.Type {
		return fmt.Errorf("config: tenant %s runnable summary backend must be disabled or match session backend %q", t.TenantID, t.Data.Session.Type)
	}
	if t.Data.Summary.Type != "disabled" && t.Data.Summary.Namespace != "" && t.Data.Summary.Namespace != t.Data.Session.Namespace {
		return fmt.Errorf("config: tenant %s summary namespace must match session namespace", t.TenantID)
	}
	if t.Data.Summary.Type == "redis" {
		if t.Data.Summary.DSNEnv != t.Data.Session.DSNEnv {
			return fmt.Errorf("config: tenant %s summary redis dsn_env must match session dsn_env", t.TenantID)
		}
	}
	if t.Data.Summary.Type == "sql" && t.Data.Session.Type != "sql" {
		return fmt.Errorf("config: tenant %s sql summary requires sql session backend", t.TenantID)
	}
	if t.Data.Summary.Type == "sql" && t.Data.Summary.DSNEnv != t.Data.Session.DSNEnv {
		return fmt.Errorf("config: tenant %s sql summary dsn_env must match session dsn_env", t.TenantID)
	}
	if t.Data.Artifact.Type != "inmemory" && t.Data.Artifact.Type != "object" {
		return fmt.Errorf("config: tenant %s runnable artifact backend must be inmemory or object", t.TenantID)
	}
	if t.Data.Artifact.Type == "object" {
		if t.Data.Artifact.Provider != "s3" || t.Data.Artifact.Bucket == "" {
			return fmt.Errorf("config: tenant %s object artifact requires provider=s3 and bucket", t.TenantID)
		}
		if t.Data.Artifact.DSNEnv == "" || t.Data.Artifact.MigrationDSNEnv == "" {
			return fmt.Errorf("config: tenant %s object artifact requires dsn_env and migration_dsn_env for its metadata ledger", t.TenantID)
		}
		if (t.Data.Artifact.AccessKeyEnv == "") != (t.Data.Artifact.SecretKeyEnv == "") {
			return fmt.Errorf("config: tenant %s object artifact static credentials require both access_key_env and secret_key_env", t.TenantID)
		}
		if t.Data.Artifact.SessionTokenEnv != "" && t.Data.Artifact.AccessKeyEnv == "" {
			return fmt.Errorf("config: tenant %s object artifact session_token_env requires static credentials", t.TenantID)
		}
	}
	if t.Data.Knowledge.Type != "disabled" && t.Data.Knowledge.Type != "vector" {
		return fmt.Errorf("config: tenant %s runnable knowledge backend must be disabled or vector", t.TenantID)
	}
	if t.Data.Knowledge.Type == "vector" {
		if t.Data.Knowledge.Provider != "pgvector" {
			return fmt.Errorf("config: tenant %s vector knowledge provider must be pgvector", t.TenantID)
		}
		if !safeSQLIdentifier(t.Data.Knowledge.Namespace) {
			return fmt.Errorf("config: tenant %s vector knowledge namespace must be a safe PostgreSQL table name", t.TenantID)
		}
		if t.Data.Knowledge.EmbeddingModel == "" || t.Data.Knowledge.EmbeddingDimension <= 0 {
			return fmt.Errorf("config: tenant %s vector knowledge requires embedding_model and positive embedding_dimension", t.TenantID)
		}
		if t.Data.Knowledge.APIKeyEnv == "" {
			return fmt.Errorf("config: tenant %s vector knowledge requires api_key_env", t.TenantID)
		}
		if t.Data.Knowledge.MigrationDSNEnv == "" {
			return fmt.Errorf("config: tenant %s vector knowledge requires migration_dsn_env", t.TenantID)
		}
	}
	effectiveAuditSink := t.Audit.Sink
	if effectiveAuditSink == "" {
		effectiveAuditSink = "stdout"
	}
	if !t.Audit.Enabled || effectiveAuditSink == "disabled" {
		effectiveAuditSink = "disabled"
	}
	if t.Data.AuditLog.Type != effectiveAuditSink {
		return fmt.Errorf("config: tenant %s data.audit_log type must match effective audit sink %q", t.TenantID, effectiveAuditSink)
	}
	return nil
}

func stringInSlice(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func safeSQLIdentifier(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for i, r := range value {
		if (r >= 'a' && r <= 'z') || r == '_' || (i > 0 && r >= '0' && r <= '9') {
			continue
		}
		return false
	}
	return true
}

// Secret resolves a secret by environment-variable name. Configuration stores
// references only, which keeps credentials out of YAML, logs and admin output.
// Runtime secrets registered via RegisterSecret (dynamic tenants) take
// precedence over the process environment.
func Secret(envName string) (string, error) {
	resolverMu.RLock()
	resolver := secretResolver
	resolverMu.RUnlock()
	if resolver == nil {
		return "", errors.New("secret resolver is unavailable")
	}
	return resolver.Resolve(envName)
}

func validateOptionalEnvReference(field, value string) error {
	if value != "" && !envReference.MatchString(value) && !secretReference.MatchString(value) {
		return fmt.Errorf("config: %s must be an environment variable name or secret reference", field)
	}
	return nil
}

func validatePublicEndpoint(field, value string) error {
	if value == "" {
		return nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.User != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("config: %s must be an http(s) URL without embedded credentials", field)
	}
	return nil
}

// SecretEnvNames returns the configured secret references for redaction and
// startup diagnostics; it never returns secret values.
func (c *Config) SecretEnvNames() []string {
	set := map[string]struct{}{}
	add := func(s string) {
		if strings.TrimSpace(s) != "" {
			set[s] = struct{}{}
		}
	}
	add(c.Server.AdminTokenEnv)
	add(c.Coordination.RedisURLEnv)
	add(c.Queue.DSNEnv)
	for _, t := range c.Tenants {
		for _, name := range t.SecretEnvNames() {
			add(name)
		}
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// SecretEnvNames returns only this tenant revision's secret references. It is
// used to keep audit redaction aligned with an in-flight immutable snapshot.
func (t TenantConfig) SecretEnvNames() []string {
	set := map[string]struct{}{}
	add := func(value string) {
		if strings.TrimSpace(value) != "" {
			set[value] = struct{}{}
		}
	}
	add(t.Model.APIKeyEnv)
	for _, ch := range t.Channels {
		add(ch.BotIDEnv)
		add(ch.BotSecretEnv)
		add(ch.TokenEnv)
		add(ch.SigningSecretEnv)
		add(ch.EncryptionKeyEnv)
	}
	for _, backend := range []BackendConfig{t.Data.Session, t.Data.Memory, t.Data.Summary, t.Data.Artifact, t.Data.Knowledge, t.Data.AuditLog} {
		add(backend.DSNEnv)
		add(backend.MigrationDSNEnv)
		add(backend.APIKeyEnv)
		add(backend.AccessKeyEnv)
		add(backend.SecretKeyEnv)
		add(backend.SessionTokenEnv)
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
