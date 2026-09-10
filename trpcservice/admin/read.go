package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const maxAdminListLimit = 1000

// ListOptions is the common exact-scope filter for control-plane reads.
// TenantID is optional only for the system-wide tenant list.
type ListOptions struct {
	TenantID string
	AppID    string
	Limit    int
}

func (o ListOptions) validate(requireApp bool) error {
	if strings.TrimSpace(o.TenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if requireApp && strings.TrimSpace(o.AppID) == "" {
		return errors.New("app_id is required")
	}
	if o.Limit < 0 || o.Limit > maxAdminListLimit {
		return errors.New("list limit is invalid")
	}
	return nil
}

// AppConfigView is a safe immutable configuration view. Opaque provider
// options and model parameters are omitted because their values may contain
// credentials even when their keys do not identify them as secrets. External
// SecretRef metadata is retained so the view can be edited and republished;
// secret values are resolved outside the configuration payload.
type AppConfigView struct {
	Config    tenant.AppConfig `json:"config"`
	Status    string           `json:"status"`
	Active    bool             `json:"active"`
	CreatedAt time.Time        `json:"created_at"`
}

// MarshalJSON applies the same safe projection at the wire boundary while
// keeping Config as a typed value for in-process repository consumers.
func (v AppConfigView) MarshalJSON() ([]byte, error) {
	safeConfig, err := json.Marshal(sanitizeAppConfig(v.Config))
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Config    json.RawMessage `json:"config"`
		Status    string          `json:"status"`
		Active    bool            `json:"active"`
		CreatedAt time.Time       `json:"created_at"`
	}{Config: safeConfig, Status: v.Status, Active: v.Active, CreatedAt: v.CreatedAt})
}

// TenantView contains the metadata and bounded health summary needed by the
// Admin UI. It never contains credentials or runtime payloads.
type TenantView struct {
	ID            string             `json:"tenant_id"`
	Name          string             `json:"name"`
	Status        tenant.Status      `json:"status"`
	Audit         tenant.AuditPolicy `json:"audit"`
	Quota         tenant.QuotaPolicy `json:"quota"`
	AgentAppCount int                `json:"agent_app_count"`
	AnomalyStatus string             `json:"anomaly_status"`
	UpdatedAt     time.Time          `json:"updated_at"`
}

// BackendSummary is a safe app-level backend reference. Opaque endpoint
// options are intentionally omitted.
type BackendSummary struct {
	Kind     tenant.BackendKind `json:"kind"`
	Provider string             `json:"provider"`
	Name     string             `json:"name"`
	Status   string             `json:"status"`
	// SecretRef remains an empty compatibility field for in-process callers;
	// it is never serialized by the Admin API.
	SecretRef tenant.SecretRef `json:"-"`
}

// ChannelSummary is a safe app-level channel binding summary.
type ChannelSummary struct {
	Provider         string `json:"provider"`
	BindingID        string `json:"binding_id"`
	Status           string `json:"status"`
	ConnectionStatus string `json:"connection_status"`
}

// ChannelBindingView is the Admin read representation. Provider credentials
// are write-only references and never cross this response boundary.
type ChannelBindingView struct {
	TenantID         string                    `json:"tenant_id"`
	AppID            string                    `json:"app_id"`
	BindingID        string                    `json:"binding_id"`
	Channel          channels.Channel          `json:"channel"`
	ExternalAccount  string                    `json:"external_account"`
	PublicRouteID    string                    `json:"public_route_id,omitempty"`
	BindingRevision  int64                     `json:"binding_revision"`
	Status           channels.BindingStatus    `json:"status"`
	ConnectionStatus channels.ConnectionStatus `json:"connection_status"`
	LastConnectedAt  *time.Time                `json:"last_connected_at,omitempty"`
	LastError        string                    `json:"last_error,omitempty"`
}

func safeErrorCategory(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	if lower == "" {
		return ""
	}
	switch {
	case strings.Contains(lower, "timeout"), strings.Contains(lower, "deadline"):
		return "timeout"
	case strings.Contains(lower, "auth"), strings.Contains(lower, "credential"), strings.Contains(lower, "secret"):
		return "authentication"
	case strings.Contains(lower, "lease"):
		return "lease_lost"
	case strings.Contains(lower, "quota"):
		return "quota_exceeded"
	case strings.Contains(lower, "policy"), strings.Contains(lower, "permission"):
		return "policy_denied"
	case strings.Contains(lower, "connect"), strings.Contains(lower, "database"), strings.Contains(lower, "redis"), strings.Contains(lower, "backend"):
		return "backend_unavailable"
	default:
		return "execution_failed"
	}
}

func sanitizeBinding(value channels.Binding) ChannelBindingView {
	return ChannelBindingView{
		TenantID: value.TenantID, AppID: value.AppID, BindingID: value.BindingID,
		Channel: value.Channel, ExternalAccount: value.ExternalAccount,
		PublicRouteID: value.PublicRouteID, BindingRevision: value.BindingRevision,
		Status: value.Status, ConnectionStatus: value.ConnectionStatus,
		LastConnectedAt: value.LastConnectedAt, LastError: safeErrorCategory(value.LastError),
	}
}

// AgentAppView contains release state plus bounded backend/channel summaries.
type AgentAppView struct {
	TenantID            string              `json:"tenant_id"`
	AppID               string              `json:"app_id"`
	Name                string              `json:"name"`
	ActiveConfigVersion string              `json:"active_config_version"`
	CanaryConfigVersion string              `json:"canary_config_version,omitempty"`
	CanaryPercentage    int                 `json:"canary_percentage"`
	CanaryStatus        tenant.CanaryStatus `json:"canary_status"`
	Status              tenant.Status       `json:"status"`
	Backends            []BackendSummary    `json:"backend_summary"`
	Channels            []ChannelSummary    `json:"channel_summary"`
	UpdatedAt           time.Time           `json:"updated_at"`
}

// ExecutionView omits the durable command payload and lease run token.
type ExecutionView struct {
	TenantID           string            `json:"tenant_id"`
	AppID              string            `json:"app_id"`
	RequestID          string            `json:"request_id"`
	SessionPrincipalID string            `json:"session_principal_id"`
	SessionID          string            `json:"session_id"`
	UserID             string            `json:"user_id"`
	TurnSeq            int64             `json:"turn_seq"`
	ConfigVersion      string            `json:"config_version"`
	Status             string            `json:"status"`
	Attempt            int               `json:"attempt"`
	NextAttemptAt      time.Time         `json:"next_attempt_at"`
	LeaseOwner         string            `json:"lease_owner,omitempty"`
	LeaseUntil         *time.Time        `json:"lease_until,omitempty"`
	LastError          string            `json:"last_error,omitempty"`
	TraceID            string            `json:"trace_id"`
	StartedAt          *time.Time        `json:"started_at,omitempty"`
	FinishedAt         *time.Time        `json:"finished_at,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
	UpdatedAt          time.Time         `json:"updated_at"`
	DurationMS         int64             `json:"duration_ms"`
	ErrorType          string            `json:"error_type,omitempty"`
	ToolCalls          []ToolCallSummary `json:"tool_calls,omitempty"`
}

// ToolCallSummary contains only audit metadata, never arguments or results.
type ToolCallSummary struct {
	ToolName  string `json:"tool_name"`
	EventType string `json:"event_type"`
	Decision  string `json:"decision"`
	Count     int    `json:"count"`
}

type tenantListReader interface {
	ListTenants(context.Context, ListOptions) ([]TenantView, error)
}

type appListReader interface {
	ListAgentApps(context.Context, ListOptions) ([]AgentAppView, error)
}

type appConfigListReader interface {
	ListAppConfigs(context.Context, ListOptions) ([]AppConfigView, error)
}

type bindingListReader interface {
	ListChannelBindings(context.Context, ListOptions) ([]channels.Binding, error)
}

type executionListReader interface {
	ListExecutions(context.Context, ListOptions) ([]ExecutionView, error)
}

type migrationListReader interface {
	ListDataMigrations(context.Context, ListOptions) ([]migration.Record, error)
}

// ListTenantsForPrincipal lists tenants visible to a system administrator.
// Operators and auditors use an explicit tenant filter so the query cannot
// accidentally become an unbounded cross-tenant read.
func (a API) ListTenantsForPrincipal(
	ctx context.Context,
	principal AdminPrincipal,
	options ListOptions,
) ([]TenantView, error) {
	if options.Limit < 0 || options.Limit > maxAdminListLimit {
		return nil, invalidInput(errors.New("list limit is invalid"))
	}
	if principal.Role != RoleSystemAdmin {
		if strings.TrimSpace(options.TenantID) == "" || !principal.AllowsTenant(options.TenantID) {
			return nil, ErrForbidden
		}
	}
	reader, ok := a.Repository.(tenantListReader)
	if !ok {
		return nil, errors.New("admin repository does not support tenant listing")
	}
	values, err := reader.ListTenants(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	return values, nil
}

func (a API) ListAgentAppsForPrincipal(
	ctx context.Context,
	principal AdminPrincipal,
	options ListOptions,
) ([]AgentAppView, error) {
	if err := options.validate(false); err != nil {
		return nil, invalidInput(err)
	}
	if !principal.AllowsTenant(options.TenantID) {
		return nil, ErrForbidden
	}
	reader, ok := a.Repository.(appListReader)
	if !ok {
		return nil, errors.New("admin repository does not support application listing")
	}
	values, err := reader.ListAgentApps(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("list agent apps: %w", err)
	}
	return values, nil
}

func (a API) ListAppConfigsForPrincipal(
	ctx context.Context,
	principal AdminPrincipal,
	options ListOptions,
) ([]AppConfigView, error) {
	if err := options.validate(true); err != nil {
		return nil, invalidInput(err)
	}
	if !principal.AllowsTenant(options.TenantID) {
		return nil, ErrForbidden
	}
	reader, ok := a.Repository.(appConfigListReader)
	if !ok {
		return nil, errors.New("admin repository does not support app config listing")
	}
	values, err := reader.ListAppConfigs(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("list app configs: %w", err)
	}
	for i := range values {
		values[i].Config = sanitizeAppConfig(values[i].Config)
	}
	return values, nil
}

func sanitizeAppConfig(value tenant.AppConfig) tenant.AppConfig {
	value = value.Clone()
	value.Model.Parameters = sanitizeModelParameters(value.Model.Parameters)
	value.BackendConfig.Session.Options = sanitizeOptions(value.BackendConfig.Session.Options, "schema")
	value.BackendConfig.Memory.Options = nil
	value.BackendConfig.Knowledge.Options = sanitizeOptions(value.BackendConfig.Knowledge.Options,
		"embedding_model", "embedding_dimensions", "embedding_profile", "index_generation")
	value.BackendConfig.Artifact.Options = nil
	return value
}

func sanitizeStringMap(values map[string]string) map[string]string {
	return sanitizeOptions(values)
}

func sanitizeModelParameters(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string)
	if baseURL := values["base_url"]; baseURL != "" && safeBaseURL(baseURL) {
		result["base_url"] = baseURL
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func sanitizeOptions(values map[string]string, allowed ...string) map[string]string {
	if len(values) == 0 || len(allowed) == 0 {
		return nil
	}
	allow := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allow[key] = struct{}{}
	}
	result := make(map[string]string)
	for key, value := range values {
		if _, ok := allow[key]; ok && !looksSensitiveOption(key, value) {
			result[key] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func looksSensitiveOption(key, value string) bool {
	lowerKey := strings.ToLower(key)
	for _, marker := range []string{"secret", "token", "password", "passwd", "credential", "authorization", "api_key", "apikey", "dsn"} {
		if strings.Contains(lowerKey, marker) {
			return true
		}
	}
	return strings.ContainsAny(value, "\r\n\x00")
}

func safeBaseURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil &&
		(strings.EqualFold(parsed.Scheme, "http") || strings.EqualFold(parsed.Scheme, "https")) &&
		parsed.Hostname() != "" &&
		parsed.User == nil &&
		parsed.RawQuery == "" &&
		parsed.Fragment == ""
}

func (a API) ListChannelBindingsForPrincipal(
	ctx context.Context,
	principal AdminPrincipal,
	options ListOptions,
) ([]ChannelBindingView, error) {
	if err := options.validate(false); err != nil {
		return nil, invalidInput(err)
	}
	if !principal.AllowsTenant(options.TenantID) {
		return nil, ErrForbidden
	}
	reader, ok := a.Repository.(bindingListReader)
	if !ok {
		return nil, errors.New("admin repository does not support channel binding listing")
	}
	values, err := reader.ListChannelBindings(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("list channel bindings: %w", err)
	}
	result := make([]ChannelBindingView, 0, len(values))
	for _, value := range values {
		result = append(result, sanitizeBinding(value))
	}
	return result, nil
}

func (a API) ListExecutionsForPrincipal(
	ctx context.Context,
	principal AdminPrincipal,
	options ListOptions,
) ([]ExecutionView, error) {
	if err := options.validate(false); err != nil {
		return nil, invalidInput(err)
	}
	if !principal.AllowsTenant(options.TenantID) {
		return nil, ErrForbidden
	}
	reader, ok := a.Repository.(executionListReader)
	if !ok {
		return nil, errors.New("admin repository does not support execution listing")
	}
	values, err := reader.ListExecutions(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	for i := range values {
		values[i].LastError = safeErrorCategory(values[i].LastError)
	}
	return values, nil
}

func (a API) ListDataMigrationsForPrincipal(
	ctx context.Context,
	principal AdminPrincipal,
	options ListOptions,
) ([]migration.Record, error) {
	if err := options.validate(false); err != nil {
		return nil, invalidInput(err)
	}
	if !principal.AllowsTenant(options.TenantID) {
		return nil, ErrForbidden
	}
	reader, ok := a.Repository.(migrationListReader)
	if !ok {
		return nil, errors.New("admin repository does not support data migration listing")
	}
	values, err := reader.ListDataMigrations(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("list data migrations: %w", err)
	}
	return values, nil
}
