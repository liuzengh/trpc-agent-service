package config

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/model"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	ModelProviderOpenAI      = "openai"
	ModelProviderHunyuan     = "hunyuan"
	ModelProviderHuggingFace = "huggingface"
)

// ModelCandidate identifies one fallback model managed by the platform.
type ModelCandidate struct {
	ProviderID string `json:"provider_id"`
	Name       string `json:"name"`
}

// ModelConfig selects a platform-managed model provider and model name, with
// optional failover candidates and generation parameters. Tenant snapshots
// never contain provider endpoints or secret references.
type ModelConfig struct {
	ProviderID         string                  `json:"provider_id,omitempty"`
	Name               string                  `json:"name,omitempty"`
	FailoverCandidates []ModelCandidate        `json:"failover_candidates,omitempty"`
	Generation         *model.GenerationConfig `json:"generation,omitempty"`
}

// ModelProviderConfig is platform-owned startup configuration. It is never
// copied into a tenant snapshot or accepted by the tenant console API.
type ModelProviderConfig struct {
	ID           string               `json:"id"`
	Type         string               `json:"type,omitempty"`
	BaseURL      string               `json:"base_url,omitempty"`
	APIKeyRef    string               `json:"api_key_ref,omitempty"`
	SecretIDRef  string               `json:"secret_id_ref,omitempty"`
	SecretKeyRef string               `json:"secret_key_ref,omitempty"`
	Models       []ModelPricingConfig `json:"models,omitempty"`
}

func (p ModelProviderConfig) NormalizedType() string {
	providerType := strings.ToLower(strings.TrimSpace(p.Type))
	if providerType == "" {
		return ModelProviderOpenAI
	}
	return providerType
}

// ModelCapabilities describes generation controls that are actually supported
// by one managed model. The console renders advanced controls from this catalog
// instead of guessing capabilities from model names.
type ModelCapabilities struct {
	ReasoningEfforts []string                `json:"reasoning_efforts,omitempty"`
	ThinkingToggle   bool                    `json:"thinking_toggle,omitempty"`
	ThinkingBudget   bool                    `json:"thinking_budget,omitempty"`
	Input            *ModelInputCapabilities `json:"input,omitempty"`
}

// ModelInputCapabilities declares the non-text inputs a managed model accepts.
// Protocol compatibility alone never implies multimodal support.
type ModelInputCapabilities struct {
	Image bool `json:"image,omitempty"`
	Audio bool `json:"audio,omitempty"`
	File  bool `json:"file,omitempty"`
}

// ModelPricingConfig is the platform-owned catalog entry for one managed model.
type ModelPricingConfig struct {
	Name                             string `json:"name"`
	PromptCostMicrosPerMillionTokens int64  `json:"prompt_cost_micros_per_million_tokens"`
	// CachedPromptCostMicrosPerMillionTokens is optional because an omitted
	// cache price must preserve the ordinary prompt rate. A configured zero
	// explicitly models providers that do not charge for cache reads.
	CachedPromptCostMicrosPerMillionTokens *int64             `json:"cached_prompt_cost_micros_per_million_tokens,omitempty"`
	CompletionCostMicrosPerMillionTokens   int64              `json:"completion_cost_micros_per_million_tokens"`
	Capabilities                           *ModelCapabilities `json:"capabilities,omitempty"`
}

// ToolPolicy declares the only platform tool names a tenant may invoke.
type ToolPolicy struct {
	Allowed             []string            `json:"allowed,omitempty"`
	AllowedRoles        map[string][]string `json:"allowed_roles,omitempty"`
	RequireConfirmation []string            `json:"require_confirmation,omitempty"`
	HTTP                []HTTPToolConfig    `json:"http,omitempty"`
	MCP                 []MCPToolConfig     `json:"mcp,omitempty"`
}

// HTTPToolConfig exposes one fixed HTTPS JSON endpoint as a framework Function Tool.
// The endpoint and credential reference are configuration, never model arguments.
type HTTPToolConfig struct {
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	URL           string            `json:"url"`
	CredentialRef string            `json:"credential_ref,omitempty"`
	InputSchema   *agenttool.Schema `json:"input_schema,omitempty"`
}

// MCPToolConfig attaches one remote MCP server. Only HTTP transports are exposed;
// stdio is intentionally excluded from the product surface.
type MCPToolConfig struct {
	Name          string `json:"name"`
	Description   string `json:"description,omitempty"`
	Transport     string `json:"transport,omitempty"`
	URL           string `json:"url"`
	CredentialRef string `json:"credential_ref,omitempty"`
}

func (c MCPToolConfig) RemoteToolName(modelToolName string) (string, bool) {
	prefix := strings.TrimSpace(c.Name) + "_"
	if prefix == "_" || !strings.HasPrefix(modelToolName, prefix) {
		return "", false
	}
	remoteName := strings.TrimSpace(strings.TrimPrefix(modelToolName, prefix))
	return remoteName, remoteName != ""
}

// BackendConfig is the platform-internal physical backend descriptor used by
// providers and migration jobs. Tenant application snapshots never contain it.
type BackendConfig struct {
	Driver        string `json:"driver,omitempty"`
	ConnectionRef string `json:"connection_ref,omitempty"`
}

// BackendProfileRef selects one platform-owned Backend Profile without
// exposing its driver, endpoint, credentials, or connection reference.
type BackendProfileRef struct {
	ProfileID string `json:"profile_id,omitempty"`
}

// StoragePolicy selects authorized platform Backend Profiles per data domain.
type StoragePolicy struct {
	Session   BackendProfileRef `json:"session,omitempty"`
	Memory    BackendProfileRef `json:"memory,omitempty"`
	Knowledge BackendProfileRef `json:"knowledge,omitempty"`
	Artifact  BackendProfileRef `json:"artifact,omitempty"`
}

// GovernancePolicy carries deterministic per-invocation guardrails.
type GovernancePolicy struct {
	MaxToolCalls       int   `json:"max_tool_calls,omitempty"`
	BudgetUnits        int64 `json:"budget_units,omitempty"`
	RequestsPerMinute  int   `json:"requests_per_minute,omitempty"`
	MaxConcurrentRuns  int   `json:"max_concurrent_runs,omitempty"`
	TokenBudgetPerHour int64 `json:"token_budget_per_hour,omitempty"`
	TokenReservation   int64 `json:"token_reservation,omitempty"`
}

// AuditPolicy controls tenant retention without sensitive detail.
type AuditPolicy struct {
	RetentionDays int `json:"retention_days,omitempty"`
}

// PlatformPolicyValidator validates tenant-controlled snapshots against the
// platform-owned provider, secret and tool catalogs.
type PlatformPolicyValidator struct {
	catalog         *ModelCatalog
	allowedSecrets  []string
	channelSecrets  []string
	toolSecrets     []string
	allowedTools    map[string]struct{}
	artifactDrivers []string
}

func NewPlatformPolicyValidator(catalog *ModelCatalog, allowedSecrets, channelSecrets, toolSecrets, allowedTools, artifactDrivers []string) (*PlatformPolicyValidator, error) {
	if catalog == nil {
		return nil, fmt.Errorf("model catalog is required")
	}
	validator := &PlatformPolicyValidator{
		catalog:         catalog,
		allowedSecrets:  append([]string(nil), allowedSecrets...),
		channelSecrets:  append([]string(nil), channelSecrets...),
		toolSecrets:     append([]string(nil), toolSecrets...),
		allowedTools:    make(map[string]struct{}, len(allowedTools)),
		artifactDrivers: append([]string(nil), artifactDrivers...),
	}
	for _, name := range allowedTools {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("platform tool name is required")
		}
		validator.allowedTools[name] = struct{}{}
	}
	if err := validatePlatformCatalog(catalog.Providers(), allowedSecrets, channelSecrets, toolSecrets); err != nil {
		return nil, err
	}
	return validator, nil
}

func (v *PlatformPolicyValidator) Validate(tenantConfig TenantConfig) error {
	if v == nil {
		return fmt.Errorf("platform policy validator is required")
	}
	if err := ValidateTenantStructure(tenantConfig); err != nil {
		return err
	}
	providers, dynamicCopy := v.catalog.snapshot()
	allowedSecrets := make(map[string]struct{}, len(v.allowedSecrets))
	for _, reference := range v.allowedSecrets {
		allowedSecrets[reference] = struct{}{}
	}
	channelSecrets := make(map[string]struct{}, len(v.channelSecrets))
	toolSecrets := make(map[string]struct{}, len(v.toolSecrets))
	if err := tenantConfig.validatePlatformPolicyWithDynamic("application", providers, allowedSecrets, dynamicCopy, v.artifactDrivers); err != nil {
		return err
	}
	for _, reference := range v.channelSecrets {
		channelSecrets[reference] = struct{}{}
	}
	for _, reference := range v.toolSecrets {
		toolSecrets[reference] = struct{}{}
	}
	for index, binding := range tenantConfig.Channels {
		location := fmt.Sprintf("application.channels[%d]", index)
		if len(channelSecrets) > 0 && strings.TrimSpace(binding.CredentialRef) == "" {
			return fmt.Errorf("%s.credential_ref is required", location)
		}
		if binding.CredentialRef != "" {
			if _, ok := channelSecrets[binding.CredentialRef]; !ok {
				return fmt.Errorf("%s.credential_ref is not managed by the platform", location)
			}
		}
	}
	for _, name := range tenantConfig.Tools.Allowed {
		if _, ok := v.allowedTools[name]; !ok && !tenantConfig.Tools.definesTool(name) {
			return fmt.Errorf("tool %q is not registered by the platform", name)
		}
	}
	if err := validateToolNameCollisions(tenantConfig.Tools, v.allowedTools); err != nil {
		return err
	}
	if err := validateCustomToolCredentials(tenantConfig.Tools, toolSecrets); err != nil {
		return err
	}
	return nil
}

func validateToolNameCollisions(policy ToolPolicy, platformTools map[string]struct{}) error {
	owners := make(map[string]string)
	for _, cfg := range policy.HTTP {
		name := strings.TrimSpace(cfg.Name)
		if _, exists := platformTools[name]; exists {
			return fmt.Errorf("custom HTTP tool %q conflicts with a platform tool", name)
		}
		owners[name] = "http"
	}
	for _, allowed := range policy.Allowed {
		owner := ""
		if _, exists := platformTools[allowed]; exists {
			owner = "platform"
		}
		if _, exists := owners[allowed]; exists {
			if owner != "" {
				return fmt.Errorf("tool %q has multiple definitions", allowed)
			}
			owner = "http"
		}
		for _, server := range policy.MCP {
			if _, matches := server.RemoteToolName(allowed); !matches {
				continue
			}
			if owner != "" {
				return fmt.Errorf("tool %q has multiple definitions", allowed)
			}
			owner = "mcp"
		}
	}
	return nil
}

func validateCustomToolCredentials(policy ToolPolicy, allowed map[string]struct{}) error {
	for index, cfg := range policy.HTTP {
		if reference := strings.TrimSpace(cfg.CredentialRef); reference != "" {
			if _, ok := allowed[reference]; !ok {
				return fmt.Errorf("application.tools.http[%d].credential_ref is not managed for tools", index)
			}
		}
	}
	for index, cfg := range policy.MCP {
		if reference := strings.TrimSpace(cfg.CredentialRef); reference != "" {
			if _, ok := allowed[reference]; !ok {
				return fmt.Errorf("application.tools.mcp[%d].credential_ref is not managed for tools", index)
			}
		}
	}
	return nil
}

func (c TenantConfig) validatePlatformPolicyWithDynamic(location string, providers map[string]ModelProviderConfig, allowedSecrets map[string]struct{}, dynamicModels map[string]map[string]struct{}, artifactDrivers []string) error {
	values := []string{c.Model.ProviderID, c.Model.Name}
	populated := 0
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			populated++
		}
	}
	if populated != 0 && populated != len(values) {
		return fmt.Errorf("%s.model requires provider_id and name together", location)
	}
	if len(providers) > 0 && c.Status == AgentActive && populated == 0 {
		return fmt.Errorf("%s.model is required for an active application", location)
	}
	var selectedCapabilities *ModelCapabilities
	if c.Model.ProviderID != "" {
		provider, ok := providers[c.Model.ProviderID]
		if !ok {
			return fmt.Errorf("%s.model.provider_id %q is not managed by the platform", location, c.Model.ProviderID)
		}
		allowed := false
		for _, pricing := range provider.Models {
			if pricing.Name == c.Model.Name {
				allowed = true
				selectedCapabilities = pricing.Capabilities
				break
			}
		}
		if !allowed && dynamicModels != nil {
			if set, ok := dynamicModels[c.Model.ProviderID]; ok {
				if _, exists := set[c.Model.Name]; exists {
					allowed = true
				}
			}
		}
		if !allowed {
			return fmt.Errorf("%s.model.name %q is not managed by provider %q", location, c.Model.Name, c.Model.ProviderID)
		}
	}
	for index, candidate := range c.Model.FailoverCandidates {
		candidateLocation := fmt.Sprintf("%s.model.failover_candidates[%d]", location, index)
		if strings.TrimSpace(candidate.ProviderID) == "" || strings.TrimSpace(candidate.Name) == "" {
			return fmt.Errorf("%s requires provider_id and name together", candidateLocation)
		}
		candidateProvider, ok := providers[candidate.ProviderID]
		if !ok {
			return fmt.Errorf("%s.provider_id %q is not managed by the platform", candidateLocation, candidate.ProviderID)
		}
		candidateAllowed := false
		for _, pricing := range candidateProvider.Models {
			if pricing.Name == candidate.Name {
				candidateAllowed = true
				break
			}
		}
		if !candidateAllowed && dynamicModels != nil {
			if set, ok := dynamicModels[candidate.ProviderID]; ok {
				if _, exists := set[candidate.Name]; exists {
					candidateAllowed = true
				}
			}
		}
		if !candidateAllowed {
			return fmt.Errorf("%s.name %q is not managed by provider %q", candidateLocation, candidate.Name, candidate.ProviderID)
		}
	}
	if c.Model.Generation != nil {
		gen := c.Model.Generation
		if gen.Temperature != nil && (*gen.Temperature < 0.0 || *gen.Temperature > 2.0) {
			return fmt.Errorf("%s.model.generation.temperature must be between 0.0 and 2.0", location)
		}
		if gen.TopP != nil && (*gen.TopP < 0.0 || *gen.TopP > 1.0) {
			return fmt.Errorf("%s.model.generation.top_p must be between 0.0 and 1.0", location)
		}
		if gen.MaxTokens != nil && *gen.MaxTokens <= 0 {
			return fmt.Errorf("%s.model.generation.max_tokens must be positive", location)
		}
		if gen.ThinkingTokens != nil {
			if *gen.ThinkingTokens <= 0 {
				return fmt.Errorf("%s.model.generation.thinking_tokens must be positive", location)
			}
			if selectedCapabilities == nil || !selectedCapabilities.ThinkingBudget {
				return fmt.Errorf("%s.model.generation.thinking_tokens is not supported by model %q", location, c.Model.Name)
			}
		}
		if gen.ThinkingEnabled != nil && (selectedCapabilities == nil || !selectedCapabilities.ThinkingToggle) {
			return fmt.Errorf("%s.model.generation.thinking_enabled is not supported by model %q", location, c.Model.Name)
		}
		if gen.ReasoningEffort != nil {
			effort := strings.ToLower(strings.TrimSpace(*gen.ReasoningEffort))
			if !containsReasoningEffort(selectedCapabilities, effort) {
				return fmt.Errorf("%s.model.generation.reasoning_effort %q is not supported by model %q", location, effort, c.Model.Name)
			}
		}
	}
	seen := make(map[string]struct{}, len(c.Tools.Allowed))
	for _, name := range c.Tools.Allowed {
		name = strings.TrimSpace(name)
		if !validToolName(name) {
			return fmt.Errorf("%s.tools.allowed contains an invalid tool name", location)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("%s.tools.allowed contains duplicate tool %q", location, name)
		}
		seen[name] = struct{}{}
	}
	if err := validateCustomTools(location+".tools", c.Tools, allowedSecrets); err != nil {
		return err
	}
	for _, name := range c.Tools.RequireConfirmation {
		name = strings.TrimSpace(name)
		if !validToolName(name) {
			return fmt.Errorf("%s.tools.require_confirmation contains an invalid tool name", location)
		}
		if _, allowed := seen[name]; !allowed {
			return fmt.Errorf("%s.tools.require_confirmation references unallowed tool %q", location, name)
		}
	}
	for toolName, roles := range c.Tools.AllowedRoles {
		if _, allowed := seen[toolName]; !allowed {
			return fmt.Errorf("%s.tools.allowed_roles references unallowed tool %q", location, toolName)
		}
		if len(roles) == 0 {
			return fmt.Errorf("%s.tools.allowed_roles for tool %q must not be empty", location, toolName)
		}
		for _, role := range roles {
			switch role {
			case "member", "admin":
			default:
				return fmt.Errorf("%s.tools.allowed_roles has unsupported role %q", location, role)
			}
		}
	}
	for domain, profile := range map[string]BackendProfileRef{
		"session": c.Storage.Session, "memory": c.Storage.Memory,
		"knowledge": c.Storage.Knowledge, "artifact": c.Storage.Artifact,
	} {
		if err := validateBackendProfileRef(location+".storage."+domain, profile); err != nil {
			return err
		}
	}
	if c.Governance.MaxToolCalls < 0 || c.Governance.BudgetUnits < 0 || c.Governance.RequestsPerMinute < 0 ||
		c.Governance.MaxConcurrentRuns < 0 || c.Governance.TokenBudgetPerHour < 0 || c.Governance.TokenReservation < 0 {
		return fmt.Errorf("%s.governance values must not be negative", location)
	}
	if c.Governance.TokenBudgetPerHour > 0 {
		if c.Governance.TokenReservation <= 0 {
			return fmt.Errorf("%s.governance.token_reservation must be positive when token_budget_per_hour is enabled", location)
		}
		if c.Governance.TokenReservation > c.Governance.TokenBudgetPerHour {
			return fmt.Errorf("%s.governance.token_reservation must not exceed token_budget_per_hour", location)
		}
	}
	if c.Audit.RetentionDays < 0 {
		return fmt.Errorf("%s.audit.retention_days must not be negative", location)
	}
	return nil
}

func (p ToolPolicy) definesTool(name string) bool {
	for _, candidate := range p.HTTP {
		if strings.TrimSpace(candidate.Name) == name {
			return true
		}
	}
	for _, server := range p.MCP {
		if _, matches := server.RemoteToolName(name); matches {
			return true
		}
	}
	return false
}

func validateCustomTools(location string, policy ToolPolicy, allowedSecrets map[string]struct{}) error {
	defined := make(map[string]struct{}, len(policy.HTTP)+len(policy.MCP))
	for index, cfg := range policy.HTTP {
		item := fmt.Sprintf("%s.http[%d]", location, index)
		name := strings.TrimSpace(cfg.Name)
		if !validToolName(name) {
			return fmt.Errorf("%s.name is invalid", item)
		}
		if _, exists := defined[name]; exists {
			return fmt.Errorf("%s.name %q is duplicated", item, name)
		}
		defined[name] = struct{}{}
		if err := validateRemoteToolURL(item+".url", cfg.URL); err != nil {
			return err
		}
		if err := validateToolCredential(item+".credential_ref", cfg.CredentialRef, allowedSecrets); err != nil {
			return err
		}
		if cfg.InputSchema != nil && cfg.InputSchema.Type != "" && cfg.InputSchema.Type != "object" {
			return fmt.Errorf("%s.input_schema must describe an object", item)
		}
	}
	for index, cfg := range policy.MCP {
		item := fmt.Sprintf("%s.mcp[%d]", location, index)
		name := strings.TrimSpace(cfg.Name)
		if !validToolName(name) {
			return fmt.Errorf("%s.name is invalid", item)
		}
		if _, exists := defined[name]; exists {
			return fmt.Errorf("%s.name %q is duplicated", item, name)
		}
		defined[name] = struct{}{}
		transport := strings.ToLower(strings.TrimSpace(cfg.Transport))
		if transport == "" {
			transport = "streamable"
		}
		if transport != "streamable" && transport != "sse" {
			return fmt.Errorf("%s.transport must be streamable or sse", item)
		}
		if err := validateRemoteToolURL(item+".url", cfg.URL); err != nil {
			return err
		}
		if err := validateToolCredential(item+".credential_ref", cfg.CredentialRef, allowedSecrets); err != nil {
			return err
		}
	}
	return nil
}

func validToolName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for _, character := range name {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validateRemoteToolURL(location, raw string) error {
	endpoint, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" {
		return fmt.Errorf("%s must be an HTTPS URL without credentials or fragment", location)
	}
	return nil
}

func validateToolCredential(location, reference string, allowedSecrets map[string]struct{}) error {
	reference = strings.TrimSpace(reference)
	if reference == "" {
		return nil
	}
	if !strings.HasPrefix(reference, "env:") {
		return fmt.Errorf("%s must use an env: reference", location)
	}
	if _, ok := allowedSecrets[reference]; !ok {
		return fmt.Errorf("%s is not managed by the platform", location)
	}
	return nil
}

func validateBackendProfileRef(location string, profile BackendProfileRef) error {
	profileID := strings.TrimSpace(profile.ProfileID)
	if profileID == "" {
		return nil
	}
	if strings.ContainsAny(profileID, " /\\") {
		return fmt.Errorf("%s.profile_id contains invalid characters", location)
	}
	return nil
}

func containsReasoningEffort(capabilities *ModelCapabilities, effort string) bool {
	if capabilities == nil {
		return false
	}
	for _, candidate := range capabilities.ReasoningEfforts {
		if strings.EqualFold(strings.TrimSpace(candidate), effort) {
			return true
		}
	}
	return false
}

func validateModelCapabilities(location string, capabilities *ModelCapabilities) error {
	if capabilities == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(capabilities.ReasoningEfforts))
	for _, raw := range capabilities.ReasoningEfforts {
		effort := strings.ToLower(strings.TrimSpace(raw))
		switch effort {
		case "low", "medium", "high", "max", "xhigh":
		default:
			return fmt.Errorf("%s.reasoning_efforts contains unsupported value %q", location, raw)
		}
		if _, duplicate := seen[effort]; duplicate {
			return fmt.Errorf("%s.reasoning_efforts contains duplicate value %q", location, raw)
		}
		seen[effort] = struct{}{}
	}
	return nil
}

func validateModelProvider(location string, provider ModelProviderConfig, allowedSecrets map[string]struct{}) error {
	if err := validateNameSegment(location+".id", provider.ID); err != nil {
		return err
	}
	providerType := provider.NormalizedType()
	switch providerType {
	case ModelProviderOpenAI, ModelProviderHuggingFace:
		if err := validateModelProviderBaseURL(location+".base_url", provider.BaseURL, providerType == ModelProviderOpenAI); err != nil {
			return err
		}
		if _, ok := allowedSecrets[provider.APIKeyRef]; !ok {
			return fmt.Errorf("%s.api_key_ref is not in service.allowed_secret_refs", location)
		}
	case ModelProviderHunyuan:
		if err := validateModelProviderBaseURL(location+".base_url", provider.BaseURL, false); err != nil {
			return err
		}
		if _, ok := allowedSecrets[provider.SecretIDRef]; !ok {
			return fmt.Errorf("%s.secret_id_ref is not in service.allowed_secret_refs", location)
		}
		if _, ok := allowedSecrets[provider.SecretKeyRef]; !ok {
			return fmt.Errorf("%s.secret_key_ref is not in service.allowed_secret_refs", location)
		}
	default:
		return fmt.Errorf("%s.type %q is unsupported (allowed: %q, %q, %q)", location, provider.Type, ModelProviderOpenAI, ModelProviderHunyuan, ModelProviderHuggingFace)
	}
	seenModels := make(map[string]struct{}, len(provider.Models))
	for index, pricing := range provider.Models {
		pricingLocation := fmt.Sprintf("%s.models[%d]", location, index)
		// Model names are provider-owned opaque identifiers. Unlike our own
		// tenant/provider IDs they may legitimately contain separators, for
		// example HuggingFace IDs such as "meta-llama/Llama-3.1-8B-Instruct".
		modelID := strings.TrimSpace(pricing.Name)
		if modelID == "" {
			return fmt.Errorf("%s.name is required", pricingLocation)
		}
		if _, exists := seenModels[modelID]; exists {
			return fmt.Errorf("%s.name is duplicated", pricingLocation)
		}
		seenModels[modelID] = struct{}{}
		if pricing.PromptCostMicrosPerMillionTokens < 0 || pricing.CompletionCostMicrosPerMillionTokens < 0 ||
			(pricing.CachedPromptCostMicrosPerMillionTokens != nil && *pricing.CachedPromptCostMicrosPerMillionTokens < 0) {
			return fmt.Errorf("%s cost rates must not be negative", pricingLocation)
		}
		if err := validateModelCapabilities(pricingLocation+".capabilities", pricing.Capabilities); err != nil {
			return err
		}
	}
	return nil
}

func validateModelProviderBaseURL(location, raw string, required bool) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if required {
			return fmt.Errorf("%s must be an absolute URL", location)
		}
		return nil
	}
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Host == "" {
		return fmt.Errorf("%s must be an absolute URL", location)
	}
	hostname := endpoint.Hostname()
	loopback := strings.EqualFold(hostname, "localhost")
	if address := net.ParseIP(hostname); address != nil {
		loopback = address.IsLoopback()
	}
	if endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback) {
		return fmt.Errorf("%s must use HTTPS (HTTP is allowed only for loopback testing)", location)
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return fmt.Errorf("%s must not include credentials, query, or fragment", location)
	}
	return nil
}
