// Package config loads tenant, model, channel, and storage backend settings.
package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// DefaultPath is the config file looked up in the working directory.
// The real file is gitignored; commit only config.example.yaml.
const DefaultPath = "config.yaml"

// Environment variables overriding the default tenant's model settings, kept
// for the pre-config-file development workflow.
const (
	envAPIKey     = "MODEL_API_KEY"
	envModel      = "MODEL_NAME"
	envBaseURL    = "MODEL_BASE_URL"
	envAdminToken = "ADMIN_TOKEN"
)

// Session storage backend names and defaults (proposal doc 3.3). The
// STORAGE_SESSION_* variables mirror the MODEL_* convenience for container
// deployments that inject the backend without rewriting the config file.
const (
	BackendMemory = "memory"
	BackendRedis  = "redis"

	// DefaultKeyPrefix namespaces every session key; tenant isolation rides
	// on the {tenant}:{channel}:{user} session id inside the prefix.
	DefaultKeyPrefix = "trpc-agent-service:"

	envStorageBackend  = "STORAGE_SESSION_BACKEND"
	envStorageRedisURL = "STORAGE_SESSION_REDIS_URL"
)

// Agent runtime envelope defaults (proposal doc 2.3). The AGENT_* variables
// let a container or K8s deployment retune the timeout and the per-tenant
// concurrency quota without rewriting the mounted config file.
const (
	// DefaultMessageTimeout caps one inbound message end to end: the model
	// call plus draining the reply stream. It is the only backstop once the
	// dispatch context is detached from the IM request deadline.
	DefaultMessageTimeout = 2 * time.Minute

	// DefaultMaxLLMCalls bounds how many model calls one inbound message may
	// cause. A tool-free question needs exactly one, so the rest is headroom
	// for tool loops. The framework reads a non-positive cap as "no limit",
	// and an upstream that answers 200 with an empty stream never satisfies
	// the flow's exit condition: measured at ~8.3k upstream calls per second
	// for as long as the message budget lasts (spec-deployment-fault-drill
	// §4.5). This cap is what turns that into a bounded, reported error.
	DefaultMaxLLMCalls = 8

	envAgentMessageTimeout = "AGENT_MESSAGE_TIMEOUT"
	envAgentMaxConcurrency = "AGENT_MAX_CONCURRENCY_PER_TENANT"
	envAgentMaxLLMCalls    = "AGENT_MAX_LLM_CALLS"
)

// Observability defaults (proposal doc 3.5). Exporters are off unless the
// config asks for stdout (local debugging) or otlp (collector deployment).
const (
	ExporterOff    = "off"
	ExporterStdout = "stdout"
	ExporterOTLP   = "otlp"

	DefaultLogLevel       = "info"
	DefaultMetricInterval = 15 * time.Second
)

// fileYAML is the on-disk shape; tenant.Context carries the yaml tags so
// Load and Save round-trip through the same schema.
type fileYAML struct {
	DefaultTenant string            `yaml:"default_tenant,omitempty"`
	ControlPlane  *controlPlaneYAML `yaml:"control_plane,omitempty"`
	Knowledge     *knowledgeYAML    `yaml:"knowledge,omitempty"`
	Storage       *storageYAML      `yaml:"storage,omitempty"`
	Agent         *agentYAML        `yaml:"agent,omitempty"`
	Log           *logYAML          `yaml:"log,omitempty"`
	Audit         *auditYAML        `yaml:"audit,omitempty"`
	Admin         *adminYAML        `yaml:"admin,omitempty"`
	Telemetry     *telemetryYAML    `yaml:"telemetry,omitempty"`
	Tenants       []tenant.Context  `yaml:"tenants"`
}

// Control-plane mode selects where the platform's configuration actually
// lives (approved plan, "MySQL 是权威事实源"). legacy keeps the pre-second-
// batch behaviour: a YAML file, with the Redis runtime store as a hot-update
// copy. mysql makes the control plane authoritative and turns the YAML file
// into bootstrap-only input.
const (
	ControlPlaneLegacy = "legacy"
	ControlPlaneMySQL  = "mysql"

	envControlPlaneMode = "CONTROLPLANE_MODE"
	envControlPlaneDSN  = "CONTROLPLANE_MYSQL_DSN"
)

type controlPlaneYAML struct {
	Mode  string `yaml:"mode,omitempty"`
	MySQL string `yaml:"mysql_dsn,omitempty"`
}

// ControlPlane is the validated configuration-source selection.
type ControlPlane struct {
	Mode     string // ControlPlaneLegacy (default) or ControlPlaneMySQL
	MySQLDSN string // required for ControlPlaneMySQL
}

type storageYAML struct {
	Session sessionStorageYAML `yaml:"session,omitempty"`
}

type sessionStorageYAML struct {
	Backend    string `yaml:"backend,omitempty"`
	RedisURL   string `yaml:"redis_url,omitempty"`
	KeyPrefix  string `yaml:"key_prefix,omitempty"`
	SessionTTL string `yaml:"session_ttl,omitempty"`
}

// SessionStorage is the validated platform-level session backend selection.
type SessionStorage struct {
	Backend    string        // BackendMemory (default) or BackendRedis
	RedisURL   string        // required for BackendRedis, redis:// or rediss://
	KeyPrefix  string        // defaults to DefaultKeyPrefix
	SessionTTL time.Duration // 0 means no expiration
}

// Storage groups platform-level shared backends; per-tenant backend override
// is second-phase Storage Adapter work and intentionally absent here.
type Storage struct {
	Session SessionStorage
}

// AgentConfig is the validated runtime envelope the gateway applies to every
// inbound message (proposal doc 2.3): how long one message may take, and how
// many messages of one tenant may be in flight at once.
type AgentConfig struct {
	// MessageTimeout caps one message end to end; defaults to
	// DefaultMessageTimeout and must stay positive.
	MessageTimeout time.Duration
	// MaxConcurrencyPerTenant caps in-flight messages per tenant. 0 means
	// unlimited (the default), so a config without an agent section behaves
	// exactly like the platform did before quotas existed.
	MaxConcurrencyPerTenant int
	// MaxLLMCalls caps the model calls one message may cause. Unlike the
	// quota above, 0 does not mean unlimited: it selects DefaultMaxLLMCalls,
	// because "unlimited" is exactly the hole this field closes.
	MaxLLMCalls int
}

// LogConfig selects the structured log level and encoding.
type LogConfig struct {
	Level string // debug|info|warn|error, default DefaultLogLevel
	JSON  bool   // JSON encoding for container deployments
}

// AuditConfig points the append-only JSONL audit trail (proposal doc 3.5);
// an empty File keeps audit records in the structured log only.
type AuditConfig struct {
	File string
}

// AdminConfig protects the runtime configuration API. Prefer ADMIN_TOKEN in
// deployment environments so the credential never lives in a ConfigMap.
type AdminConfig struct {
	Token string
}

// ExporterConfig selects a trace/metric exporter: off, stdout, or otlp
// (OTLP/HTTP base endpoint, e.g. http://127.0.0.1:4318).
type ExporterConfig struct {
	Exporter string
	Endpoint string
}

// MetricsExporterConfig adds the push interval of the metric exporter.
type MetricsExporterConfig struct {
	Exporter string
	Endpoint string
	Interval time.Duration
}

// TelemetryConfig drives the OpenTelemetry setup: traces carry the
// end-to-end spans, metrics carry the tenant-labelled counters.
type TelemetryConfig struct {
	Traces  ExporterConfig
	Metrics MetricsExporterConfig
}

// Config is the loaded and validated platform configuration.
type Config struct {
	DefaultTenant string
	ControlPlane  ControlPlane
	Knowledge     Knowledge
	Storage       Storage
	Agent         AgentConfig
	Log           LogConfig
	Audit         AuditConfig
	Admin         AdminConfig
	Telemetry     TelemetryConfig
	Tenants       map[string]*tenant.Context
}

// Load reads the YAML config at path. If the file does not exist it falls
// back to a single "default" tenant built from MODEL_* environment
// variables, preserving the export-based development workflow.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return fromEnv(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	cfg, err := LoadBytes(data)
	if err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	applyEnvOverride(cfg.Tenants[cfg.DefaultTenant])
	applyAdminEnv(&cfg.Admin)
	return cfg, nil
}

// LoadBytes parses a persisted config snapshot. Runtime stores use this form;
// environment overrides deliberately remain at the process boundary in Load.
func LoadBytes(data []byte) (*Config, error) {
	var err error
	var f fileYAML
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	cfg := &Config{Tenants: make(map[string]*tenant.Context, len(f.Tenants))}
	for i := range f.Tenants {
		t := f.Tenants[i]
		if t.ID == "" {
			return nil, fmt.Errorf("every tenant needs an id")
		}
		if _, dup := cfg.Tenants[t.ID]; dup {
			return nil, fmt.Errorf("duplicate tenant %q", t.ID)
		}
		cfg.Tenants[t.ID] = &t
	}
	cfg.DefaultTenant = f.DefaultTenant
	if cfg.DefaultTenant == "" && len(f.Tenants) > 0 {
		cfg.DefaultTenant = f.Tenants[0].ID
	}
	if cfg.ControlPlane, err = parseControlPlane(f.ControlPlane); err != nil {
		return nil, err
	}
	if cfg.Knowledge, err = parseKnowledge(f.Knowledge); err != nil {
		return nil, err
	}
	if cfg.Storage, err = parseStorage(f.Storage); err != nil {
		return nil, err
	}
	if cfg.Agent, err = parseAgent(f.Agent); err != nil {
		return nil, err
	}
	if cfg.Log, cfg.Audit, cfg.Telemetry, err = parseObservability(f.Log, f.Audit, f.Telemetry); err != nil {
		return nil, err
	}
	cfg.Admin = parseAdmin(f.Admin)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate checks the invariants shared by Load, Save, and the Admin API:
// at least one tenant, complete model settings, complete channel bindings,
// a usable session storage backend, and an existing default tenant.
func (c *Config) Validate() error {
	if len(c.Tenants) == 0 {
		return fmt.Errorf("at least one tenant is required")
	}
	for id, t := range c.Tenants {
		if t.Model.APIKey == "" {
			return fmt.Errorf("tenant %q: model.api_key is required", id)
		}
		if t.Model.Name == "" {
			return fmt.Errorf("tenant %q: model.name is required", id)
		}
		if err := validateWeCom(id, t.Channels.WeCom); err != nil {
			return err
		}
		if err := validateWeChatKf(id, t.Channels.WeChatKf); err != nil {
			return err
		}
		if err := validateGuardrails(id, t.Guardrails); err != nil {
			return err
		}
		if err := validateTools(id, t.Tools); err != nil {
			return err
		}
	}
	if err := c.validateStorage(); err != nil {
		return err
	}
	if err := c.validateControlPlane(); err != nil {
		return err
	}
	if err := c.validateAgent(); err != nil {
		return err
	}
	if err := c.validateObservability(); err != nil {
		return err
	}
	if _, ok := c.Tenants[c.DefaultTenant]; !ok {
		return fmt.Errorf("default_tenant %q is not defined", c.DefaultTenant)
	}
	return nil
}

// Save atomically persists cfg to path (tmp file + rename), so a partial
// write can never leave an unloadable config behind. Tenants are written in
// sorted order for stable diffs.
func Save(path string, cfg *Config) error {
	data, err := Marshal(cfg)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}

// Marshal serializes a validated config for the local file and shared runtime
// store. Tenants are sorted for stable snapshots.
func Marshal(cfg *Config) ([]byte, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(cfg.Tenants))
	for id := range cfg.Tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	f := fileYAML{
		DefaultTenant: cfg.DefaultTenant,
		ControlPlane:  controlPlaneToYAML(cfg.ControlPlane),
		Knowledge:     knowledgeToYAML(cfg.Knowledge),
		Storage:       storageToYAML(cfg.Storage),
		Agent:         agentToYAML(cfg.Agent),
		Log:           logToYAML(cfg.Log),
		Audit:         auditToYAML(cfg.Audit),
		Admin:         adminToYAML(cfg.Admin),
		Telemetry:     telemetryToYAML(cfg.Telemetry),
	}
	for _, id := range ids {
		f.Tenants = append(f.Tenants, *cfg.Tenants[id])
	}
	data, err := yaml.Marshal(f)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return data, nil
}

// parseControlPlane normalizes the control_plane section: an absent section
// means legacy, and the CONTROLPLANE_* variables override it the same way
// STORAGE_SESSION_* overrides storage.
func parseControlPlane(y *controlPlaneYAML) (ControlPlane, error) {
	cp := ControlPlane{Mode: ControlPlaneLegacy}
	if y != nil {
		if y.Mode != "" {
			cp.Mode = strings.ToLower(y.Mode)
		}
		cp.MySQLDSN = y.MySQL
	}
	applyControlPlaneEnv(&cp)
	return cp, nil
}

func applyControlPlaneEnv(cp *ControlPlane) {
	if v := os.Getenv(envControlPlaneMode); v != "" {
		cp.Mode = strings.ToLower(v)
	}
	if v := os.Getenv(envControlPlaneDSN); v != "" {
		cp.MySQLDSN = v
	}
}

// validateControlPlane rejects an unknown mode, and a mysql mode without a
// DSN, at load time rather than at first use. An empty mode is legal here
// and means legacy: a Config assembled by hand, without going through
// parseControlPlane, leaves the field zero, and treating that as invalid
// would make every literal construction site a latent validation failure.
// This was measured — it is exactly how a cloneConfig that forgot the new
// field would have surfaced, and the same trap caught a Telemetry field once
// before (docs/spec-governance-observability.md §6).
func (c *Config) validateControlPlane() error {
	switch c.ControlPlane.Mode {
	case "", ControlPlaneLegacy:
		return nil
	case ControlPlaneMySQL:
		if c.ControlPlane.MySQLDSN == "" {
			return fmt.Errorf("control_plane.mysql_dsn is required when mode is %s", ControlPlaneMySQL)
		}
		return nil
	default:
		return fmt.Errorf("control_plane.mode %q must be %s or %s",
			c.ControlPlane.Mode, ControlPlaneLegacy, ControlPlaneMySQL)
	}
}

// controlPlaneToYAML omits the section entirely when it is the default, so
// an existing config file round-trips unchanged if it never mentions it.
func controlPlaneToYAML(cp ControlPlane) *controlPlaneYAML {
	if cp.Mode == "" || cp.Mode == ControlPlaneLegacy {
		return nil
	}
	return &controlPlaneYAML{Mode: cp.Mode, MySQL: cp.MySQLDSN}
}

// parseStorage normalizes the on-disk storage section: it fills defaults,
// parses session_ttl, and applies the STORAGE_SESSION_* overrides so the
// result is ready for validateStorage.
func parseStorage(y *storageYAML) (Storage, error) {
	s := Storage{Session: SessionStorage{Backend: BackendMemory, KeyPrefix: DefaultKeyPrefix}}
	if y != nil {
		if y.Session.Backend != "" {
			s.Session.Backend = y.Session.Backend
		}
		s.Session.RedisURL = y.Session.RedisURL
		if y.Session.KeyPrefix != "" {
			s.Session.KeyPrefix = y.Session.KeyPrefix
		}
		if y.Session.SessionTTL != "" {
			d, err := time.ParseDuration(y.Session.SessionTTL)
			if err != nil {
				return Storage{}, fmt.Errorf("storage.session.session_ttl %q: %w", y.Session.SessionTTL, err)
			}
			if d < 0 {
				return Storage{}, fmt.Errorf("storage.session.session_ttl must not be negative")
			}
			s.Session.SessionTTL = d
		}
	}
	applyStorageEnv(&s.Session)
	return s, nil
}

func applyStorageEnv(ss *SessionStorage) {
	if v := os.Getenv(envStorageBackend); v != "" {
		ss.Backend = v
	}
	if v := os.Getenv(envStorageRedisURL); v != "" {
		ss.RedisURL = v
	}
}

// validateStorage rejects unknown backends and half-filled redis settings at
// load time, so a typo surfaces at startup instead of on the first message.
func (c *Config) validateStorage() error {
	ss := c.Storage.Session
	switch ss.Backend {
	case "", BackendMemory:
		return nil
	case BackendRedis:
		if ss.RedisURL == "" {
			return fmt.Errorf("storage.session.redis_url is required when backend is %s", BackendRedis)
		}
		if !strings.HasPrefix(ss.RedisURL, "redis://") && !strings.HasPrefix(ss.RedisURL, "rediss://") {
			return fmt.Errorf("storage.session.redis_url %q must start with redis:// or rediss://", ss.RedisURL)
		}
		return nil
	default:
		return fmt.Errorf("storage.session.backend %q must be %s or %s", ss.Backend, BackendMemory, BackendRedis)
	}
}

// storageToYAML serializes the storage section, omitting it entirely when it
// is the pure default so Save output stays diff-clean for existing configs.
func storageToYAML(s Storage) *storageYAML {
	ss := s.Session
	if ss.Backend == "" || (ss.Backend == BackendMemory && ss.RedisURL == "" &&
		(ss.KeyPrefix == "" || ss.KeyPrefix == DefaultKeyPrefix) && ss.SessionTTL == 0) {
		return nil
	}
	y := sessionStorageYAML{Backend: ss.Backend, RedisURL: ss.RedisURL}
	if ss.KeyPrefix != "" && ss.KeyPrefix != DefaultKeyPrefix {
		y.KeyPrefix = ss.KeyPrefix
	}
	if ss.SessionTTL > 0 {
		y.SessionTTL = ss.SessionTTL.String()
	}
	return &storageYAML{Session: y}
}

// agentYAML is the on-disk agent section; everything optional so an absent
// section keeps the pure defaults.
type agentYAML struct {
	MessageTimeout          string `yaml:"message_timeout,omitempty"`
	MaxConcurrencyPerTenant int    `yaml:"max_concurrency_per_tenant,omitempty"`
	MaxLLMCalls             int    `yaml:"max_llm_calls,omitempty"`
}

// parseAgent normalizes the agent section: it fills defaults, parses
// message_timeout, and applies the AGENT_* overrides so the result is ready
// for validateAgent. Environment values are parsed here rather than in
// Validate so a bad AGENT_MESSAGE_TIMEOUT is reported with its variable name.
func parseAgent(y *agentYAML) (AgentConfig, error) {
	a := AgentConfig{MessageTimeout: DefaultMessageTimeout, MaxLLMCalls: DefaultMaxLLMCalls}
	if y != nil {
		if y.MessageTimeout != "" {
			d, err := time.ParseDuration(y.MessageTimeout)
			if err != nil {
				return AgentConfig{}, fmt.Errorf("agent.message_timeout %q: %w", y.MessageTimeout, err)
			}
			a.MessageTimeout = d
		}
		a.MaxConcurrencyPerTenant = y.MaxConcurrencyPerTenant
		// 0 keeps the default rather than meaning "no cap"; a negative value
		// is passed through for validateAgent to name.
		if y.MaxLLMCalls != 0 {
			a.MaxLLMCalls = y.MaxLLMCalls
		}
	}
	if err := applyAgentEnv(&a); err != nil {
		return AgentConfig{}, err
	}
	return a, nil
}

func applyAgentEnv(a *AgentConfig) error {
	if v := os.Getenv(envAgentMessageTimeout); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s %q: %w", envAgentMessageTimeout, v, err)
		}
		a.MessageTimeout = d
	}
	if v := os.Getenv(envAgentMaxConcurrency); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s %q: %w", envAgentMaxConcurrency, v, err)
		}
		a.MaxConcurrencyPerTenant = n
	}
	if v := os.Getenv(envAgentMaxLLMCalls); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s %q: %w", envAgentMaxLLMCalls, v, err)
		}
		if n == 0 {
			n = DefaultMaxLLMCalls
		}
		a.MaxLLMCalls = n
	}
	return nil
}

// validateAgent rejects an envelope the gateway could not honour: a
// non-positive timeout would cancel every message immediately, and a
// negative quota would reject everything instead of limiting it.
func (c *Config) validateAgent() error {
	if c.Agent.MessageTimeout <= 0 {
		return fmt.Errorf("agent.message_timeout must be positive")
	}
	if c.Agent.MaxConcurrencyPerTenant < 0 {
		return fmt.Errorf("agent.max_concurrency_per_tenant must not be negative (0 means unlimited)")
	}
	// Reached only by a hand-built Config, since both parse paths substitute
	// the default for 0 — which makes this the guard that names a regression
	// where a copied config loses the field and silently reopens the
	// unbounded-call hole.
	if c.Agent.MaxLLMCalls <= 0 {
		return fmt.Errorf("agent.max_llm_calls must be positive")
	}
	return nil
}

// agentToYAML serializes the agent section, omitting it entirely when it is
// the pure default so Save output stays diff-clean, mirroring storageToYAML.
func agentToYAML(a AgentConfig) *agentYAML {
	timeoutDefault := a.MessageTimeout <= 0 || a.MessageTimeout == DefaultMessageTimeout
	callsDefault := a.MaxLLMCalls <= 0 || a.MaxLLMCalls == DefaultMaxLLMCalls
	if timeoutDefault && a.MaxConcurrencyPerTenant == 0 && callsDefault {
		return nil
	}
	y := &agentYAML{}
	if !timeoutDefault {
		y.MessageTimeout = a.MessageTimeout.String()
	}
	if a.MaxConcurrencyPerTenant != 0 {
		y.MaxConcurrencyPerTenant = a.MaxConcurrencyPerTenant
	}
	if !callsDefault {
		y.MaxLLMCalls = a.MaxLLMCalls
	}
	return y
}

// logYAML / auditYAML / telemetryYAML are the on-disk observability shape;
// everything optional so absent sections keep the pure defaults.
type logYAML struct {
	Level string `yaml:"level,omitempty"`
	JSON  bool   `yaml:"json,omitempty"`
}

type auditYAML struct {
	File string `yaml:"file,omitempty"`
}

type adminYAML struct {
	Token string `yaml:"token,omitempty"`
}

type telemetryYAML struct {
	Traces  exporterYAML        `yaml:"traces,omitempty"`
	Metrics metricsExporterYAML `yaml:"metrics,omitempty"`
}

type exporterYAML struct {
	Exporter string `yaml:"exporter,omitempty"`
	Endpoint string `yaml:"endpoint,omitempty"`
}

type metricsExporterYAML struct {
	Exporter string `yaml:"exporter,omitempty"`
	Endpoint string `yaml:"endpoint,omitempty"`
	Interval string `yaml:"interval,omitempty"`
}

// parseObservability normalizes the log/audit/telemetry sections, filling
// defaults so the rest of the platform can read them without nil checks.
func parseObservability(l *logYAML, a *auditYAML, t *telemetryYAML) (LogConfig, AuditConfig, TelemetryConfig, error) {
	lc := LogConfig{Level: DefaultLogLevel}
	if l != nil {
		if l.Level != "" {
			lc.Level = l.Level
		}
		lc.JSON = l.JSON
	}
	ac := AuditConfig{}
	if a != nil {
		ac.File = a.File
	}
	tc := TelemetryConfig{
		Traces:  ExporterConfig{Exporter: ExporterOff},
		Metrics: MetricsExporterConfig{Exporter: ExporterOff, Interval: DefaultMetricInterval},
	}
	if t != nil {
		if t.Traces.Exporter != "" {
			tc.Traces.Exporter = t.Traces.Exporter
		}
		tc.Traces.Endpoint = t.Traces.Endpoint
		if t.Metrics.Exporter != "" {
			tc.Metrics.Exporter = t.Metrics.Exporter
		}
		tc.Metrics.Endpoint = t.Metrics.Endpoint
		if t.Metrics.Interval != "" {
			d, err := time.ParseDuration(t.Metrics.Interval)
			if err != nil {
				return lc, ac, tc, fmt.Errorf("telemetry.metrics.interval %q: %w", t.Metrics.Interval, err)
			}
			if d <= 0 {
				return lc, ac, tc, fmt.Errorf("telemetry.metrics.interval must be positive")
			}
			tc.Metrics.Interval = d
		}
	}
	return lc, ac, tc, nil
}

func parseAdmin(a *adminYAML) AdminConfig {
	if a == nil {
		return AdminConfig{}
	}
	return AdminConfig{Token: a.Token}
}

func applyAdminEnv(a *AdminConfig) {
	if token := os.Getenv(envAdminToken); token != "" {
		a.Token = token
	}
}

// validateObservability rejects unknown levels/exporters and half-filled
// otlp settings at load time, like validateStorage does for redis.
func (c *Config) validateObservability() error {
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log.level %q must be debug, info, warn, or error", c.Log.Level)
	}
	if err := validateExporter("telemetry.traces", c.Telemetry.Traces.Exporter, c.Telemetry.Traces.Endpoint); err != nil {
		return err
	}
	return validateExporter("telemetry.metrics", c.Telemetry.Metrics.Exporter, c.Telemetry.Metrics.Endpoint)
}

func validateExporter(section, exporter, endpoint string) error {
	switch exporter {
	case "", ExporterOff, ExporterStdout:
		return nil
	case ExporterOTLP:
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			return fmt.Errorf("%s.endpoint %q must start with http:// or https:// when exporter is %s", section, endpoint, ExporterOTLP)
		}
		return nil
	default:
		return fmt.Errorf("%s.exporter %q must be %s, %s, or %s", section, exporter, ExporterOff, ExporterStdout, ExporterOTLP)
	}
}

// validateGuardrails fails fast on unusable policies: negative limits or
// empty keywords would silently disable or mis-fire the checks.
func validateGuardrails(tenantID string, g tenant.Guardrails) error {
	if g.MaxInputBytes < 0 {
		return fmt.Errorf("tenant %q: guardrails.max_input_bytes must not be negative", tenantID)
	}
	for _, kw := range g.BlockedKeywords {
		if strings.TrimSpace(kw) == "" {
			return fmt.Errorf("tenant %q: guardrails.blocked_keywords must not contain empty entries", tenantID)
		}
	}
	for _, kw := range g.OutputBlockedKeywords {
		if strings.TrimSpace(kw) == "" {
			return fmt.Errorf("tenant %q: guardrails.output_blocked_keywords must not contain empty entries", tenantID)
		}
	}
	return nil
}

func validateTools(tenantID string, tools tenant.Tools) error {
	seen := make(map[string]struct{}, len(tools.Allowed))
	for _, name := range tools.Allowed {
		if name == "" {
			return fmt.Errorf("tenant %q: tools.allowed must not contain empty entries", tenantID)
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("tenant %q: tools.allowed contains duplicate %q", tenantID, name)
		}
		seen[name] = struct{}{}
	}
	for _, name := range tools.ApprovalRequired {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("tenant %q: tools.approval_required %q is not allowed", tenantID, name)
		}
	}
	return nil
}

// logToYAML / auditToYAML / telemetryToYAML omit pure-default sections so
// Save output stays diff-clean, mirroring storageToYAML.
func logToYAML(l LogConfig) *logYAML {
	if l.Level == DefaultLogLevel && !l.JSON {
		return nil
	}
	y := logYAML{Level: l.Level, JSON: l.JSON}
	if l.Level == DefaultLogLevel {
		y.Level = ""
	}
	return &y
}

func auditToYAML(a AuditConfig) *auditYAML {
	if a.File == "" {
		return nil
	}
	return &auditYAML{File: a.File}
}

func adminToYAML(a AdminConfig) *adminYAML {
	if a.Token == "" {
		return nil
	}
	return &adminYAML{Token: a.Token}
}

func telemetryToYAML(t TelemetryConfig) *telemetryYAML {
	tracesDefault := t.Traces.Exporter == ExporterOff && t.Traces.Endpoint == ""
	metricsDefault := t.Metrics.Exporter == ExporterOff && t.Metrics.Endpoint == "" &&
		(t.Metrics.Interval == 0 || t.Metrics.Interval == DefaultMetricInterval)
	if tracesDefault && metricsDefault {
		return nil
	}
	y := telemetryYAML{
		Traces:  exporterYAML{Exporter: t.Traces.Exporter, Endpoint: t.Traces.Endpoint},
		Metrics: metricsExporterYAML{Exporter: t.Metrics.Exporter, Endpoint: t.Metrics.Endpoint},
	}
	if t.Metrics.Interval > 0 && t.Metrics.Interval != DefaultMetricInterval {
		y.Metrics.Interval = t.Metrics.Interval.String()
	}
	return &y
}

// validateWeCom fails fast on half-filled channel bindings: a declared
// wecom block must be complete, otherwise the callback would 403 at runtime
// with a much less obvious error.
func validateWeCom(tenantID string, w *tenant.WeComBinding) error {
	if w == nil {
		return nil
	}
	if w.CorpID == "" || w.CorpSecret == "" || w.AgentID <= 0 || w.Token == "" || w.EncodingAESKey == "" {
		return fmt.Errorf("tenant %q: channels.wecom is incomplete (corp_id, corp_secret, agent_id, token, encoding_aes_key are all required)", tenantID)
	}
	if len(w.EncodingAESKey) != 43 {
		return fmt.Errorf("tenant %q: channels.wecom.encoding_aes_key must be 43 characters", tenantID)
	}
	return nil
}

// WeComBinding returns the WeCom app binding of tenantID. Tenants without a
// binding reject wecom callbacks with a clear error.
func (c *Config) WeComBinding(tenantID string) (*tenant.WeComBinding, bool) {
	if t, ok := c.Tenants[tenantID]; ok && t.Channels.WeCom != nil {
		return t.Channels.WeCom, true
	}
	return nil, false
}

// validateWeChatKf applies the same fail-fast rule as validateWeCom: a
// declared wechat_kf block must be complete.
func validateWeChatKf(tenantID string, k *tenant.WeChatKfBinding) error {
	if k == nil {
		return nil
	}
	if k.CorpID == "" || k.Secret == "" || k.Token == "" || k.EncodingAESKey == "" {
		return fmt.Errorf("tenant %q: channels.wechat_kf is incomplete (corp_id, secret, token, encoding_aes_key are all required)", tenantID)
	}
	if len(k.EncodingAESKey) != 43 {
		return fmt.Errorf("tenant %q: channels.wechat_kf.encoding_aes_key must be 43 characters", tenantID)
	}
	return nil
}

// WeChatKfBinding returns the WeChat customer service binding of tenantID.
// Tenants without one reject wechat_kf callbacks with a clear error.
func (c *Config) WeChatKfBinding(tenantID string) (*tenant.WeChatKfBinding, bool) {
	if t, ok := c.Tenants[tenantID]; ok && t.Channels.WeChatKf != nil {
		return t.Channels.WeChatKf, true
	}
	return nil, false
}

// fromEnv builds the legacy single-tenant config from environment variables.
func fromEnv(path string) (*Config, error) {
	apiKey := os.Getenv(envAPIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("no config at %s and %s unset; copy config.example.yaml to %s or export %s",
			path, envAPIKey, path, envAPIKey)
	}
	name := os.Getenv(envModel)
	if name == "" {
		name = "gpt-4o-mini"
	}
	t := &tenant.Context{
		ID:   "default",
		Name: "default",
		Model: tenant.ModelConfig{
			Name:    name,
			APIKey:  apiKey,
			BaseURL: os.Getenv(envBaseURL),
		},
	}
	st, err := parseStorage(nil)
	if err != nil {
		return nil, err
	}
	ag, err := parseAgent(nil)
	if err != nil {
		return nil, err
	}
	lc, ac, tc, err := parseObservability(nil, nil, nil)
	if err != nil {
		return nil, err
	}
	cp, err := parseControlPlane(nil)
	if err != nil {
		return nil, err
	}
	return &Config{
		DefaultTenant: t.ID,
		ControlPlane:  cp,
		Storage:       st,
		Agent:         ag,
		Log:           lc,
		Audit:         ac,
		Admin:         AdminConfig{Token: os.Getenv(envAdminToken)},
		Telemetry:     tc,
		Tenants:       map[string]*tenant.Context{t.ID: t},
	}, nil
}

// applyEnvOverride lets MODEL_* variables override the default tenant's
// model settings for local development convenience.
func applyEnvOverride(t *tenant.Context) {
	if v := os.Getenv(envAPIKey); v != "" {
		t.Model.APIKey = v
	}
	if v := os.Getenv(envModel); v != "" {
		t.Model.Name = v
	}
	if v := os.Getenv(envBaseURL); v != "" {
		t.Model.BaseURL = v
	}
}
