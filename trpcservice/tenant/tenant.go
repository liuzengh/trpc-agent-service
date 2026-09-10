// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"slices"
	"strings"
)

// ErrBudgetExceeded is returned when an execution has consumed its pinned
// token budget. It is deliberately owned by the tenant policy package so the
// runtime callback and worker completion path share one stable error class.
var ErrBudgetExceeded = errors.New("execution token budget exceeded")

// ErrQuotaExceeded is returned when a tenant or application period quota is
// exhausted. It is intentionally separate from the per-execution budget.
var ErrQuotaExceeded = errors.New("period quota exceeded")

// Status is the lifecycle state of a tenant or agent application.
type Status string

const (
	// StatusActive allows new tenant traffic.
	StatusActive Status = "ACTIVE"
	// StatusSuspended rejects new tenant traffic while preserving stored data.
	StatusSuspended Status = "SUSPENDED"
)

// CanaryStatus controls the application-level configuration rollout. The
// stable version remains ActiveConfigVersion; canary selection is an
// admission concern and never changes an already admitted execution.
type CanaryStatus string

const (
	CanaryDisabled CanaryStatus = "DISABLED"
	CanaryEnabled  CanaryStatus = "ENABLED"
	CanaryPaused   CanaryStatus = "PAUSED"
)

// Tenant describes the top-level isolation boundary for platform data.
type Tenant struct {
	ID     string      `json:"tenant_id"`
	Name   string      `json:"name"`
	Status Status      `json:"status"`
	Audit  AuditPolicy `json:"audit"`
	Quota  QuotaPolicy `json:"quota"`
}

// Validate checks the persisted tenant configuration. The zero value is invalid.
func (t Tenant) Validate() error {
	if t.ID == "" {
		return errors.New("tenant_id is required")
	}
	if t.Name == "" {
		return errors.New("tenant name is required")
	}
	if !validStatus(t.Status) {
		return errors.New("tenant status is invalid")
	}
	if err := t.Audit.Validate(); err != nil {
		return fmt.Errorf("audit policy: %w", err)
	}
	if err := t.Quota.Validate(); err != nil {
		return fmt.Errorf("quota policy: %w", err)
	}
	return nil
}

// Scope returns the tenant and application scope for app-local resources.
func (t Tenant) Scope(appID string) Scope {
	return Scope{TenantID: t.ID, AppID: appID}
}

// AgentApp describes an agent application owned by a tenant.
type AgentApp struct {
	TenantID            string       `json:"tenant_id"`
	AppID               string       `json:"app_id"`
	Name                string       `json:"name"`
	ActiveConfigVersion string       `json:"active_config_version"`
	CanaryConfigVersion string       `json:"canary_config_version,omitempty"`
	CanaryPercentage    int          `json:"canary_percentage"`
	CanaryStatus        CanaryStatus `json:"canary_status"`
	Status              Status       `json:"status"`
}

// Validate checks the persisted agent application metadata. The zero value is invalid.
func (a AgentApp) Validate() error {
	if a.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if a.AppID == "" {
		return errors.New("app_id is required")
	}
	if a.Name == "" {
		return errors.New("app name is required")
	}
	if a.ActiveConfigVersion == "" {
		return errors.New("active_config_version is required")
	}
	if !validStatus(a.Status) {
		return errors.New("app status is invalid")
	}
	if err := a.validateCanary(); err != nil {
		return err
	}
	return nil
}

func (a AgentApp) validateCanary() error {
	status := a.CanaryStatus
	// Zero-value AgentApp literals are used by callers that predate Canary.
	// Treat that representation as the disabled state; persisted rows always
	// carry the explicit DISABLED default from the schema migration.
	if status == "" {
		if a.CanaryConfigVersion != "" || a.CanaryPercentage != 0 {
			return errors.New("canary status is required when canary config is set")
		}
		return nil
	}
	if status != CanaryDisabled && status != CanaryEnabled && status != CanaryPaused {
		return errors.New("canary status is invalid")
	}
	if a.CanaryPercentage < 0 || a.CanaryPercentage > 100 {
		return errors.New("canary percentage must be between 0 and 100")
	}
	switch status {
	case CanaryDisabled:
		if a.CanaryConfigVersion != "" || a.CanaryPercentage != 0 {
			return errors.New("disabled canary must not have a target or percentage")
		}
	case CanaryEnabled, CanaryPaused:
		if a.CanaryConfigVersion == "" {
			return errors.New("canary config version is required")
		}
		if a.CanaryPercentage <= 0 {
			return errors.New("enabled or paused canary percentage must be positive")
		}
	}
	return nil
}

// AppConfig is an immutable version of tenant application configuration.
type AppConfig struct {
	TenantID         string         `json:"tenant_id"`
	AppID            string         `json:"app_id"`
	Version          string         `json:"version"`
	Model            ModelConfig    `json:"model"`
	Tools            ToolPolicy     `json:"tools"`
	IMAccess         IMAccessPolicy `json:"im_access"`
	Budget           BudgetPolicy   `json:"budget"`
	BackendConfig    BackendConfig  `json:"backend_config"`
	Audit            AuditPolicy    `json:"audit"`
	SecretRefs       []SecretRef    `json:"secret_refs"`
	ChannelBinding   []string       `json:"channel_binding"`
	KnowledgeBaseIDs []string       `json:"knowledge_base_ids"`
}

// Clone returns a deep copy of caller-owned slices and maps in the config.
func (c AppConfig) Clone() AppConfig {
	cloned := c
	cloned.Model = c.Model.Clone()
	cloned.Tools = c.Tools.Clone()
	cloned.IMAccess = c.IMAccess.Clone()
	cloned.BackendConfig = c.BackendConfig.Clone()
	cloned.SecretRefs = cloneSecretRefs(c.SecretRefs)
	cloned.ChannelBinding = slices.Clone(c.ChannelBinding)
	cloned.KnowledgeBaseIDs = slices.Clone(c.KnowledgeBaseIDs)
	return cloned
}

// Validate checks one immutable tenant application config version.
//
// TenantID, AppID, Version, Model, and the session backend are required. Nil
// slices and maps are valid and mean no optional values are configured.
func (c AppConfig) Validate() error {
	if c.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if c.AppID == "" {
		return errors.New("app_id is required")
	}
	if c.Version == "" {
		return errors.New("config version is required")
	}
	if err := c.Model.Validate(); err != nil {
		return fmt.Errorf("model config: %w", err)
	}
	if err := c.Tools.Validate(); err != nil {
		return fmt.Errorf("tool policy: %w", err)
	}
	if err := c.IMAccess.Validate(); err != nil {
		return fmt.Errorf("im access policy: %w", err)
	}
	if err := c.Budget.Validate(); err != nil {
		return fmt.Errorf("budget policy: %w", err)
	}
	if err := c.BackendConfig.Validate(); err != nil {
		return fmt.Errorf("backend_config: %w", err)
	}
	if err := c.Audit.Validate(); err != nil {
		return fmt.Errorf("audit policy: %w", err)
	}
	if err := validateUniqueStrings(c.KnowledgeBaseIDs, "knowledge base"); err != nil {
		return err
	}
	for i, ref := range c.SecretRefs {
		if err := ref.Validate(); err != nil {
			return fmt.Errorf("secret_ref %d: %w", i, err)
		}
	}
	for i, bindingID := range c.ChannelBinding {
		if bindingID == "" {
			return fmt.Errorf("channel_binding %d is required", i)
		}
	}
	return nil
}

// ModelAttachmentCapabilities is the explicit allowlist for non-text model
// content. Zero values are deliberately deny-by-default because an
// OpenAI-compatible endpoint is not assumed to implement every content part.
type ModelAttachmentCapabilities struct {
	Image bool `json:"image"`
	Audio bool `json:"audio"`
	File  bool `json:"file"`
}

// ModelConfig identifies the model runtime and scoped API key selected by a
// tenant application.
type ModelConfig struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
	// APIKeyRef is the external secret selected for this model. It is required
	// because the production worker resolves the model credential only through
	// a scoped secret reference.
	APIKeyRef              SecretRef                   `json:"api_key_ref"`
	Parameters             map[string]string           `json:"parameters"`
	AttachmentCapabilities ModelAttachmentCapabilities `json:"attachment_capabilities"`
}

// ModelProviderOpenAI is the only model provider constructed by the
// production runtime.
const ModelProviderOpenAI = "openai"

// ValidateModelProvider checks that a configured provider is executable by
// the deployed runtime.
func ValidateModelProvider(provider string) error {
	switch strings.TrimSpace(provider) {
	case ModelProviderOpenAI:
		return nil
	case "":
		return errors.New("model provider is required")
	default:
		return fmt.Errorf("unsupported model provider %q", provider)
	}
}

// Clone returns a deep copy of the model config.
func (c ModelConfig) Clone() ModelConfig {
	cloned := c
	cloned.Parameters = cloneStringMap(c.Parameters)
	return cloned
}

// Validate checks model provider and model name fields. Parameters may be nil.
// The API key reference is required.
func (c ModelConfig) Validate() error {
	if err := ValidateModelProvider(c.Provider); err != nil {
		return err
	}
	if c.Model == "" {
		return errors.New("model is required")
	}
	if err := c.APIKeyRef.Validate(); err != nil {
		return fmt.Errorf("api key ref: %w", err)
	}
	if err := validateStringMap(c.Parameters, "model parameter"); err != nil {
		return err
	}
	return nil
}

// ToolPolicy declares the tool contract for a tenant application.
type ToolPolicy struct {
	VisibleTools        []string `json:"visible_tools"`
	ExecutableTools     []string `json:"executable_tools"`
	ReviewRequiredTools []string `json:"review_required_tools"`
}

// Clone returns a deep copy of the tool policy.
func (p ToolPolicy) Clone() ToolPolicy {
	return ToolPolicy{
		VisibleTools:        slices.Clone(p.VisibleTools),
		ExecutableTools:     slices.Clone(p.ExecutableTools),
		ReviewRequiredTools: slices.Clone(p.ReviewRequiredTools),
	}
}

// Validate checks that configured tool names are non-empty and unique.
// Nil tool slices are valid and mean no tools are configured.
func (p ToolPolicy) Validate() error {
	if err := validateUniqueStrings(p.VisibleTools, "visible tool"); err != nil {
		return err
	}
	if err := validateUniqueStrings(p.ExecutableTools, "executable tool"); err != nil {
		return err
	}
	if err := validateUniqueStrings(p.ReviewRequiredTools, "review-required tool"); err != nil {
		return err
	}
	for _, name := range p.ReviewRequiredTools {
		if !p.CanExecute(name) {
			return fmt.Errorf("review-required tool %q must be executable", name)
		}
	}
	return nil
}

// CanView reports whether a tool declaration may be exposed to the tenant app.
func (p ToolPolicy) CanView(name string) bool {
	return containsString(p.VisibleTools, name)
}

// CanExecute reports whether the tenant app may execute a tool.
func (p ToolPolicy) CanExecute(name string) bool {
	return containsString(p.ExecutableTools, name)
}

// RequiresReview reports whether an executable tool needs human approval.
func (p ToolPolicy) RequiresReview(name string) bool {
	return containsString(p.ReviewRequiredTools, name)
}

// IMAccessPolicy restricts verified channel messages using platform-owned
// identity and conversation IDs. Provider IDs must never be stored here.
type IMAccessPolicy struct {
	AllowedUsers         []string `json:"allowed_users"`
	AllowedConversations []string `json:"allowed_conversations"`
}

// Clone returns a deep copy of the access policy.
func (p IMAccessPolicy) Clone() IMAccessPolicy {
	return IMAccessPolicy{
		AllowedUsers:         slices.Clone(p.AllowedUsers),
		AllowedConversations: slices.Clone(p.AllowedConversations),
	}
}

// Validate checks platform identity references and their uniqueness.
func (p IMAccessPolicy) Validate() error {
	if err := validateUniqueStrings(p.AllowedUsers, "allowed user"); err != nil {
		return err
	}
	if err := validateUniqueStrings(p.AllowedConversations, "allowed conversation"); err != nil {
		return err
	}
	return nil
}

// Allows applies the documented allowlist semantics: an empty policy imposes
// no additional restriction; otherwise either the mapped user or conversation
// must be listed.
func (p IMAccessPolicy) Allows(userID, conversationID string) bool {
	if len(p.AllowedUsers) == 0 && len(p.AllowedConversations) == 0 {
		return true
	}
	return containsString(p.AllowedUsers, userID) ||
		containsString(p.AllowedConversations, conversationID)
}

// BudgetPolicy controls per-execution and application-period model budgets.
// Zero means unlimited. Period quotas are durably accounted by the control
// plane; Max*PerExecution also bounds the reservation at admission.
type BudgetPolicy struct {
	MaxTokensPerExecution int     `json:"max_tokens_per_execution"`
	MaxCostPerExecution   float64 `json:"max_cost_per_execution"`
	DailyTokenQuota       int64   `json:"daily_token_quota"`
	MonthlyTokenQuota     int64   `json:"monthly_token_quota"`
	DailyCostQuota        float64 `json:"daily_cost_quota"`
	MonthlyCostQuota      float64 `json:"monthly_cost_quota"`
}

// QuotaPolicy contains tenant-level daily and monthly token/cost limits.
// Zero disables a limit.
type QuotaPolicy struct {
	DailyTokenQuota   int64   `json:"daily_token_quota"`
	MonthlyTokenQuota int64   `json:"monthly_token_quota"`
	DailyCostQuota    float64 `json:"daily_cost_quota"`
	MonthlyCostQuota  float64 `json:"monthly_cost_quota"`
}

func (p QuotaPolicy) Validate() error {
	if p.DailyTokenQuota < 0 || p.MonthlyTokenQuota < 0 {
		return errors.New("token quotas must be non-negative")
	}
	if p.DailyCostQuota < 0 || p.MonthlyCostQuota < 0 ||
		math.IsNaN(p.DailyCostQuota) || math.IsNaN(p.MonthlyCostQuota) ||
		math.IsInf(p.DailyCostQuota, 0) || math.IsInf(p.MonthlyCostQuota, 0) {
		return errors.New("cost quotas must be finite and non-negative")
	}
	return nil
}

func (p QuotaPolicy) HasLimit() bool {
	return p.DailyTokenQuota > 0 || p.MonthlyTokenQuota > 0 ||
		p.DailyCostQuota > 0 || p.MonthlyCostQuota > 0
}

func (p BudgetPolicy) QuotaPolicy() QuotaPolicy {
	return QuotaPolicy{
		DailyTokenQuota: p.DailyTokenQuota, MonthlyTokenQuota: p.MonthlyTokenQuota,
		DailyCostQuota: p.DailyCostQuota, MonthlyCostQuota: p.MonthlyCostQuota,
	}
}

// Validate checks the per-execution limits and requires a finite reservation
// bound whenever an application period quota is configured.
func (p BudgetPolicy) Validate() error {
	if p.MaxTokensPerExecution < 0 {
		return errors.New("max_tokens_per_execution must be non-negative")
	}
	if p.MaxCostPerExecution < 0 || math.IsNaN(p.MaxCostPerExecution) || math.IsInf(p.MaxCostPerExecution, 0) {
		return errors.New("max_cost_per_execution must be finite and non-negative")
	}
	quota := p.QuotaPolicy()
	if err := quota.Validate(); err != nil {
		return err
	}
	if p.DailyTokenQuota > 0 && p.MaxTokensPerExecution == 0 ||
		p.MonthlyTokenQuota > 0 && p.MaxTokensPerExecution == 0 {
		return errors.New("token quota requires max_tokens_per_execution")
	}
	if p.DailyCostQuota > 0 && p.MaxCostPerExecution == 0 ||
		p.MonthlyCostQuota > 0 && p.MaxCostPerExecution == 0 {
		return errors.New("cost quota requires max_cost_per_execution")
	}
	return nil
}

// BackendKind identifies a storage backend family.
type BackendKind string

const (
	// BackendInMemory is only suitable for tests and local development.
	BackendInMemory BackendKind = "inmemory"
	// BackendSQL stores strongly consistent relational state.
	BackendSQL BackendKind = "sql"
	// BackendRedis stores cache, queue, or Redis-backed session state.
	BackendRedis BackendKind = "redis"
	// BackendVector stores derived retrieval indexes.
	BackendVector BackendKind = "vector"
	// BackendObject stores artifacts and knowledge objects.
	BackendObject BackendKind = "object"
	// BackendExternal stores data through an external managed service.
	BackendExternal BackendKind = "external"
)

// BackendRef references one concrete backend without exposing its secret.
type BackendRef struct {
	Kind     BackendKind `json:"kind"`
	Provider string      `json:"provider"`
	Name     string      `json:"name"`
	// SecretRef identifies credentials owned by the tenant application scope.
	SecretRef SecretRef         `json:"secret_ref,omitempty"`
	Options   map[string]string `json:"options"`
}

// Clone returns a deep copy of the backend reference.
func (r BackendRef) Clone() BackendRef {
	cloned := r
	cloned.Options = cloneStringMap(r.Options)
	return cloned
}

// IsZero reports whether the backend reference is not configured.
func (r BackendRef) IsZero() bool {
	return r.Kind == "" && r.Provider == "" && r.Name == "" &&
		r.SecretRef == (SecretRef{}) && len(r.Options) == 0
}

// Validate checks that the backend reference can be resolved later.
// The zero value is invalid when Validate is called directly.
func (r BackendRef) Validate() error {
	if !validBackendKind(r.Kind) {
		return errors.New("backend kind is invalid")
	}
	if r.Provider == "" {
		return errors.New("backend provider is required")
	}
	if r.Name == "" {
		return errors.New("backend name is required")
	}
	if r.SecretRef != (SecretRef{}) {
		if err := r.SecretRef.Validate(); err != nil {
			return fmt.Errorf("backend secret_ref: %w", err)
		}
	}
	if err := validateStringMap(r.Options, "backend option"); err != nil {
		return err
	}
	return nil
}

// BackendConfig groups the backends used by one app config version.
type BackendConfig struct {
	Name      string     `json:"name"`
	Session   BackendRef `json:"session"`
	Memory    BackendRef `json:"memory"`
	Knowledge BackendRef `json:"knowledge"`
	Artifact  BackendRef `json:"artifact"`
}

// Clone returns a deep copy of backend references in the config.
func (c BackendConfig) Clone() BackendConfig {
	return BackendConfig{
		Name:      c.Name,
		Session:   c.Session.Clone(),
		Memory:    c.Memory.Clone(),
		Knowledge: c.Knowledge.Clone(),
		Artifact:  c.Artifact.Clone(),
	}
}

// Validate checks the required session backend and any optional backend refs.
// The zero value is invalid because a stateless worker needs a session backend.
func (c BackendConfig) Validate() error {
	if c.Name == "" {
		return errors.New("backend_config name is required")
	}
	if err := c.Session.Validate(); err != nil {
		return fmt.Errorf("session backend: %w", err)
	}
	if err := validateOptionalBackendRef("memory backend", c.Memory); err != nil {
		return err
	}
	if err := validateOptionalBackendRef("knowledge backend", c.Knowledge); err != nil {
		return err
	}
	if err := validateOptionalBackendRef("artifact backend", c.Artifact); err != nil {
		return err
	}
	return nil
}

// AuditPolicy controls tenant audit behavior.
type AuditPolicy struct {
	Enabled             bool `json:"enabled"`
	RecordToolDecisions bool `json:"record_tool_decisions"`
	RecordExecutions    bool `json:"record_executions"`
	RetentionDays       int  `json:"retention_days"`
	RedactPII           bool `json:"redact_pii"`
}

// Validate checks audit retention values. RetentionDays zero means no local
// limit; application policy resolution may inherit the tenant limit.
func (p AuditPolicy) Validate() error {
	if p.RetentionDays < 0 {
		return errors.New("retention_days must be non-negative")
	}
	if !p.Enabled && (p.RecordToolDecisions || p.RecordExecutions) {
		return errors.New("disabled audit policy cannot record events")
	}
	return nil
}

// ValidateAppConfig checks that an application audit policy is no broader than
// the tenant policy. An application retention of zero inherits a finite tenant
// retention; a zero tenant retention leaves a finite application limit intact.
func (p AuditPolicy) ValidateAppConfig(app AuditPolicy) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("tenant audit policy: %w", err)
	}
	if err := app.Validate(); err != nil {
		return fmt.Errorf("app audit policy: %w", err)
	}
	if app.Enabled && !p.Enabled {
		return errors.New("app audit cannot be enabled when tenant audit is disabled")
	}
	if app.RecordToolDecisions && !p.RecordToolDecisions {
		return errors.New("app tool audit is broader than tenant audit policy")
	}
	if app.RecordExecutions && !p.RecordExecutions {
		return errors.New("app execution audit is broader than tenant audit policy")
	}
	if p.RetentionDays > 0 && app.RetentionDays > p.RetentionDays {
		return errors.New("app audit retention is broader than tenant audit policy")
	}
	if p.RedactPII && !app.RedactPII {
		return errors.New("app audit redaction cannot be weaker than tenant policy")
	}
	return nil
}

// EffectiveRetentionDays returns the cleanup limit for one application. Zero
// at application scope inherits the tenant limit, while zero at tenant scope
// means no limit.
func (p AuditPolicy) EffectiveRetentionDays(app AuditPolicy) int {
	if app.RetentionDays == 0 {
		return p.RetentionDays
	}
	return app.RetentionDays
}

// SecretRef points to a secret managed outside the database.
type SecretRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Validate checks that the secret reference has a stable name.
// The zero value is invalid when present in a config.
func (r SecretRef) Validate() error {
	if r.Name == "" {
		return errors.New("secret name is required")
	}
	return nil
}

// RuntimeContext carries trusted tenant routing metadata through one request.
type RuntimeContext struct {
	TenantID      string `json:"tenant_id"`
	AppID         string `json:"app_id"`
	ConfigVersion string `json:"config_version"`
	Channel       string `json:"channel"`
	BindingID     string `json:"binding_id"`
	// BindingRevision identifies the authorization snapshot for a channel
	// execution. It is zero for non-channel contexts.
	BindingRevision int64  `json:"binding_revision,omitempty"`
	SessionID       string `json:"session_id"`
	// SessionPrincipalID identifies the owner of the conversation session. It
	// equals UserID for a private conversation and identifies the group or
	// thread for a shared conversation.
	SessionPrincipalID string `json:"session_principal_id"`
	// UserID identifies the user who sent the current message.
	UserID string `json:"user_id"`
	// TraceID identifies the end-to-end trace for this request.
	TraceID string `json:"trace_id"`
	// TraceParent and TraceState carry the standard W3C context across the
	// durable execution boundary. TraceID remains for operator correlation.
	TraceParent string `json:"trace_parent,omitempty"`
	TraceState  string `json:"trace_state,omitempty"`
}

// Scope returns the tenant and application scope for persistence keys.
func (c RuntimeContext) Scope() Scope {
	return Scope{TenantID: c.TenantID, AppID: c.AppID}
}

// Validate checks the routing, sender, and tracing fields required for stateless workers.
func (c RuntimeContext) Validate() error {
	if c.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if c.AppID == "" {
		return errors.New("app_id is required")
	}
	if c.ConfigVersion == "" {
		return errors.New("config_version is required")
	}
	if c.SessionID == "" {
		return errors.New("session_id is required")
	}
	if c.SessionPrincipalID == "" {
		return errors.New("session_principal_id is required")
	}
	if c.UserID == "" {
		return errors.New("user_id is required")
	}
	if c.TraceID == "" {
		return errors.New("trace_id is required")
	}
	return nil
}

// Scope identifies the tenant and application prefix used by shared backends.
type Scope struct {
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id"`
}

// NewScope creates a validated tenant application scope.
func NewScope(tenantID, appID string) (Scope, error) {
	s := Scope{TenantID: tenantID, AppID: appID}
	if err := s.Validate(); err != nil {
		return Scope{}, err
	}
	return s, nil
}

// Validate checks that the scope can safely prefix shared backend keys.
func (s Scope) Validate() error {
	if s.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if s.AppID == "" {
		return errors.New("app_id is required")
	}
	return nil
}

// Key builds a tenant-scoped key for persistence, cache, object, or vector use.
func (s Scope) Key(namespace string, parts ...string) (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	if namespace == "" {
		return "", errors.New("namespace is required")
	}
	segments := []string{
		"tenant", s.TenantID,
		"app", s.AppID,
		namespace,
	}
	segments = append(segments, parts...)
	for i, segment := range segments {
		if segment == "" {
			return "", fmt.Errorf("key segment %d is required", i)
		}
		segments[i] = escapeKeySegment(segment)
	}
	return strings.Join(segments, ":"), nil
}

func escapeKeySegment(segment string) string {
	return strings.ReplaceAll(url.PathEscape(segment), ":", "%3A")
}

func validStatus(status Status) bool {
	return status == StatusActive || status == StatusSuspended
}

func validBackendKind(kind BackendKind) bool {
	switch kind {
	case BackendInMemory, BackendSQL, BackendRedis, BackendVector, BackendObject, BackendExternal:
		return true
	default:
		return false
	}
}

func validateOptionalBackendRef(label string, ref BackendRef) error {
	if ref.IsZero() {
		return nil
	}
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}

func validateUniqueStrings(values []string, label string) error {
	seen := make(map[string]struct{}, len(values))
	for i, value := range values {
		if value == "" {
			return fmt.Errorf("%s %d is required", label, i)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s %q is duplicated", label, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateStringMap(values map[string]string, label string) error {
	for key := range values {
		if key == "" {
			return fmt.Errorf("%s key is required", label)
		}
	}
	return nil
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for k, v := range values {
		cloned[k] = v
	}
	return cloned
}

func cloneSecretRefs(values []SecretRef) []SecretRef {
	if values == nil {
		return nil
	}
	cloned := make([]SecretRef, len(values))
	copy(cloned, values)
	return cloned
}

func containsString(values []string, target string) bool {
	if target == "" {
		return false
	}
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
