// Package config loads platform startup configuration and validates tenant snapshots.
package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

const (
	ChannelTelegram = "telegram"
	ChannelWeCom    = "wecom"
	ChannelFeishu   = "feishu"

	ChannelAccessMemberOnly = "member_only"
	ChannelAccessAllowlist  = "allowlist"
	ChannelAccessPublic     = "public"

	AgentDraft    = "draft"
	AgentActive   = "active"
	AgentDisabled = "disabled"
)

// Duration is a JSON duration encoded as a Go duration string, for example "15s".
type Duration struct {
	time.Duration
}

// UnmarshalJSON decodes a duration string and rejects non-string values.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("request_timeout must be a duration string: %w", err)
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("request_timeout: %w", err)
	}
	d.Duration = parsed
	return nil
}

// Config is the complete, immutable-at-runtime service configuration snapshot.
type Config struct {
	Service        ServiceConfig         `json:"service"`
	ModelProviders []ModelProviderConfig `json:"model_providers,omitempty"`
}

// ServiceConfig controls inbound HTTP request limits and service timeouts.
type ServiceConfig struct {
	ListenAddress          string          `json:"listen_address"`
	RequestTimeout         Duration        `json:"request_timeout"`
	HTTPReadHeaderTimeout  Duration        `json:"http_read_header_timeout,omitempty"`
	HTTPReadTimeout        Duration        `json:"http_read_timeout,omitempty"`
	HTTPIdleTimeout        Duration        `json:"http_idle_timeout,omitempty"`
	MaxInboundBytes        int64           `json:"max_inbound_bytes"`
	SessionIdleArchiveAge  Duration        `json:"session_idle_archive_age,omitempty"`
	OutboxRetentionAge     Duration        `json:"outbox_retention_age,omitempty"`
	DoclingEndpoint        string          `json:"docling_endpoint,omitempty"`
	DocumentExtractTimeout Duration        `json:"document_extract_timeout,omitempty"`
	Knowledge              KnowledgeConfig `json:"knowledge,omitempty"`
	AllowedSecretRefs      []string        `json:"allowed_secret_refs,omitempty"`
	ChannelCredentialRefs  []string        `json:"channel_credential_refs,omitempty"`
	ToolCredentialRefs     []string        `json:"tool_credential_refs,omitempty"`
}

// KnowledgeConfig selects the platform-owned framework RAG components.
type KnowledgeConfig struct {
	EmbeddingProviderID string         `json:"embedding_provider_id"`
	EmbeddingModel      string         `json:"embedding_model"`
	EmbeddingDimensions int            `json:"embedding_dimensions"`
	Reranker            RerankerConfig `json:"reranker,omitempty"`
	AllowedSourceHosts  []string       `json:"allowed_source_hosts,omitempty"`
	AllowedSourceRoots  []string       `json:"allowed_source_roots,omitempty"`
}

// RerankerConfig selects one framework reranker implementation.
type RerankerConfig struct {
	Type      string `json:"type,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
	Model     string `json:"model,omitempty"`
	APIKeyRef string `json:"api_key_ref,omitempty"`
	TopN      int    `json:"top_n,omitempty"`
}

// TenantConfig identifies one tenant application and its allowed IM bindings.
type TenantConfig struct {
	TenantID string `json:"tenant_id"`

	AppCode       string           `json:"app_code"`
	Status        string           `json:"status"`
	ConfigVersion uint64           `json:"config_version"`
	Channels      []ChannelBinding `json:"channels"`
	// Instruction is the optional system prompt / business context for the
	// tenant application's agent. Empty means the framework default.
	Instruction string           `json:"instruction,omitempty"`
	Model       ModelConfig      `json:"model,omitempty"`
	Tools       ToolPolicy       `json:"tools,omitempty"`
	Storage     StoragePolicy    `json:"storage,omitempty"`
	Governance  GovernancePolicy `json:"governance,omitempty"`
	Audit       AuditPolicy      `json:"audit,omitempty"`
}

// AppName returns the framework application namespace for this tenant application.
func (c TenantConfig) AppName() string {
	return c.TenantID + "/" + c.AppCode
}

// ChannelBinding connects an external channel account to a tenant application.
// BindingID is an external identifier, not a secret.
type ChannelBinding struct {
	Type                string   `json:"type"`
	BindingID           string   `json:"binding_id"`
	CredentialRef       string   `json:"credential_ref,omitempty"`
	TrustedEnterpriseID string   `json:"trusted_enterprise_id,omitempty"`
	AccessPolicy        string   `json:"access_policy,omitempty"`
	Allowlist           []string `json:"allowlist,omitempty"`
}

func (b ChannelBinding) EffectiveAccessPolicy() string {
	if strings.TrimSpace(b.AccessPolicy) == "" {
		return ChannelAccessPublic
	}
	return strings.TrimSpace(b.AccessPolicy)
}

// Load decodes exactly one strict JSON configuration document and validates it.
func Load(reader io.Reader) (Config, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode configuration: %w", err)
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("decode configuration: multiple JSON documents are not allowed")
		}
		return Config{}, fmt.Errorf("decode configuration: %w", err)
	}

	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Validate checks configuration invariants before dependencies are constructed.
func (c Config) Validate() error {
	if strings.TrimSpace(c.Service.ListenAddress) == "" {
		return fmt.Errorf("service.listen_address is required")
	}
	if c.Service.RequestTimeout.Duration <= 0 {
		return fmt.Errorf("service.request_timeout must be positive")
	}
	if c.Service.HTTPReadHeaderTimeout.Duration < 0 {
		return fmt.Errorf("service.http_read_header_timeout must be positive when set")
	}
	if c.Service.HTTPReadTimeout.Duration < 0 {
		return fmt.Errorf("service.http_read_timeout must be positive when set")
	}
	if c.Service.HTTPIdleTimeout.Duration < 0 {
		return fmt.Errorf("service.http_idle_timeout must be positive when set")
	}
	if c.Service.MaxInboundBytes <= 0 {
		return fmt.Errorf("service.max_inbound_bytes must be positive")
	}
	if c.Service.SessionIdleArchiveAge.Duration < 0 {
		return fmt.Errorf("service.session_idle_archive_age must be positive when set")
	}
	if c.Service.OutboxRetentionAge.Duration < 0 {
		return fmt.Errorf("service.outbox_retention_age must be positive when set")
	}
	if c.Service.DocumentExtractTimeout.Duration < 0 {
		return fmt.Errorf("service.document_extract_timeout must be positive when set")
	}
	if endpoint := strings.TrimSpace(c.Service.DoclingEndpoint); endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("service.docling_endpoint must be an http(s) URL")
		}
	}
	if err := validatePlatformCatalog(c.ModelProviders, c.Service.AllowedSecretRefs, c.Service.ChannelCredentialRefs, c.Service.ToolCredentialRefs); err != nil {
		return err
	}
	return validateKnowledgeConfig(c.Service.Knowledge, c.ModelProviders, c.Service.AllowedSecretRefs)
}

func validateKnowledgeConfig(knowledge KnowledgeConfig, providers []ModelProviderConfig, allowedSecretRefs []string) error {
	if strings.TrimSpace(knowledge.EmbeddingProviderID) == "" {
		if strings.TrimSpace(knowledge.EmbeddingModel) != "" || knowledge.EmbeddingDimensions != 0 ||
			strings.TrimSpace(knowledge.Reranker.Type) != "" || len(knowledge.AllowedSourceHosts) > 0 || len(knowledge.AllowedSourceRoots) > 0 {
			return fmt.Errorf("service.knowledge.embedding_provider_id is required")
		}
		return nil
	}
	if strings.TrimSpace(knowledge.EmbeddingModel) == "" {
		return fmt.Errorf("service.knowledge.embedding_model is required")
	}
	if knowledge.EmbeddingDimensions != 1536 {
		return fmt.Errorf("service.knowledge.embedding_dimensions must be 1536")
	}
	providerFound := false
	for _, provider := range providers {
		if provider.ID != knowledge.EmbeddingProviderID {
			continue
		}
		providerFound = true
		if provider.NormalizedType() != ModelProviderOpenAI {
			return fmt.Errorf("service.knowledge embedding provider %q must be OpenAI-compatible", provider.ID)
		}
		break
	}
	if !providerFound {
		return fmt.Errorf("service.knowledge embedding provider %q is not managed by the platform", knowledge.EmbeddingProviderID)
	}

	rerankerType := strings.ToLower(strings.TrimSpace(knowledge.Reranker.Type))
	if rerankerType == "" {
		rerankerType = "topk"
	}
	switch rerankerType {
	case "topk":
		if knowledge.Reranker.Endpoint != "" || knowledge.Reranker.APIKeyRef != "" {
			return fmt.Errorf("service.knowledge.reranker topk does not accept endpoint or api_key_ref")
		}
	case "cohere", "infinity":
		if strings.TrimSpace(knowledge.Reranker.Endpoint) == "" && rerankerType == "infinity" {
			return fmt.Errorf("service.knowledge.reranker.endpoint is required for infinity")
		}
		if endpoint := strings.TrimSpace(knowledge.Reranker.Endpoint); endpoint != "" {
			parsed, err := url.Parse(endpoint)
			if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.Fragment != "" {
				return fmt.Errorf("service.knowledge.reranker.endpoint must be an http(s) URL without credentials or fragment")
			}
		}
	default:
		return fmt.Errorf("service.knowledge.reranker.type %q is unsupported", knowledge.Reranker.Type)
	}
	if knowledge.Reranker.TopN < 0 {
		return fmt.Errorf("service.knowledge.reranker.top_n must not be negative")
	}
	if reference := strings.TrimSpace(knowledge.Reranker.APIKeyRef); reference != "" {
		allowed := false
		for _, candidate := range allowedSecretRefs {
			allowed = allowed || candidate == reference
		}
		if !allowed {
			return fmt.Errorf("service.knowledge.reranker.api_key_ref is not in service.allowed_secret_refs")
		}
	}
	for index, host := range knowledge.AllowedSourceHosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host == "" || strings.ContainsAny(host, "/:@") {
			return fmt.Errorf("service.knowledge.allowed_source_hosts[%d] must be a hostname", index)
		}
	}
	for index, root := range knowledge.AllowedSourceRoots {
		if !filepath.IsAbs(strings.TrimSpace(root)) {
			return fmt.Errorf("service.knowledge.allowed_source_roots[%d] must be absolute", index)
		}
	}
	return nil
}

func validatePlatformCatalog(providers []ModelProviderConfig, allowedSecretRefs, channelCredentialRefs, toolCredentialRefs []string) error {
	allowedSecrets := make(map[string]struct{}, len(allowedSecretRefs))
	for index, reference := range allowedSecretRefs {
		reference = strings.TrimSpace(reference)
		if !strings.HasPrefix(reference, "env:") || strings.TrimSpace(strings.TrimPrefix(reference, "env:")) == "" {
			return fmt.Errorf("service.allowed_secret_refs[%d] must be an env: reference", index)
		}
		if _, duplicate := allowedSecrets[reference]; duplicate {
			return fmt.Errorf("service.allowed_secret_refs contains duplicate %q", reference)
		}
		allowedSecrets[reference] = struct{}{}
	}
	channelSecrets := make(map[string]struct{}, len(channelCredentialRefs))
	for index, reference := range channelCredentialRefs {
		reference = strings.TrimSpace(reference)
		if _, ok := allowedSecrets[reference]; !ok {
			return fmt.Errorf("service.channel_credential_refs[%d] is not in allowed_secret_refs", index)
		}
		if _, duplicate := channelSecrets[reference]; duplicate {
			return fmt.Errorf("service.channel_credential_refs contains duplicate %q", reference)
		}
		channelSecrets[reference] = struct{}{}
	}
	toolSecrets := make(map[string]struct{}, len(toolCredentialRefs))
	for index, reference := range toolCredentialRefs {
		reference = strings.TrimSpace(reference)
		if _, ok := allowedSecrets[reference]; !ok {
			return fmt.Errorf("service.tool_credential_refs[%d] is not in allowed_secret_refs", index)
		}
		if _, duplicate := toolSecrets[reference]; duplicate {
			return fmt.Errorf("service.tool_credential_refs contains duplicate %q", reference)
		}
		toolSecrets[reference] = struct{}{}
	}
	providerIDs := make(map[string]struct{}, len(providers))
	for index, provider := range providers {
		location := fmt.Sprintf("model_providers[%d]", index)
		if err := validateModelProvider(location, provider, allowedSecrets); err != nil {
			return err
		}
		if _, duplicate := providerIDs[provider.ID]; duplicate {
			return fmt.Errorf("duplicate model provider %q", provider.ID)
		}
		providerIDs[provider.ID] = struct{}{}
	}
	return nil
}

func validateNameSegment(location, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", location)
	}
	if strings.ContainsAny(value, "/\\") {
		return fmt.Errorf("%s contains a reserved separator", location)
	}
	return nil
}

// ValidateTenantStructure checks persistence invariants without consulting a
// platform-owned provider/secret/tool catalog. Production publication must
// additionally call PlatformPolicyValidator.Validate.
func ValidateTenantStructure(tenant TenantConfig) error {
	if err := validateNameSegment("tenant_id", tenant.TenantID); err != nil {
		return err
	}
	if err := validateNameSegment("app_code", tenant.AppCode); err != nil {
		return err
	}
	if tenant.Status != AgentDraft && tenant.Status != AgentActive && tenant.Status != AgentDisabled {
		return fmt.Errorf("status must be %q, %q, or %q", AgentDraft, AgentActive, AgentDisabled)
	}
	if tenant.ConfigVersion == 0 {
		return fmt.Errorf("config_version must be positive")
	}
	modelParts := 0
	for _, value := range []string{tenant.Model.ProviderID, tenant.Model.Name} {
		if strings.TrimSpace(value) != "" {
			modelParts++
		}
	}
	if modelParts != 0 && modelParts != 2 {
		return fmt.Errorf("model requires provider_id and name together")
	}
	for index, candidate := range tenant.Model.FailoverCandidates {
		if strings.TrimSpace(candidate.ProviderID) == "" || strings.TrimSpace(candidate.Name) == "" {
			return fmt.Errorf("model.failover_candidates[%d] requires provider_id and name together", index)
		}
	}
	seen := make(map[string]struct{}, len(tenant.Channels))
	for index, binding := range tenant.Channels {
		if binding.Type != ChannelTelegram && binding.Type != ChannelWeCom && binding.Type != ChannelFeishu {
			return fmt.Errorf("channels[%d] has unsupported channel %q", index, binding.Type)
		}
		if strings.TrimSpace(binding.BindingID) == "" || strings.ContainsAny(binding.BindingID, "/\\") {
			return fmt.Errorf("channels[%d].binding_id is invalid", index)
		}
		if strings.TrimSpace(binding.TrustedEnterpriseID) != "" && binding.Type != ChannelWeCom {
			return fmt.Errorf("channels[%d].trusted_enterprise_id is only valid for wecom", index)
		}
		if binding.CredentialRef != "" && !strings.HasPrefix(binding.CredentialRef, "env:") {
			return fmt.Errorf("channels[%d].credential_ref must use an env: reference", index)
		}
		switch binding.EffectiveAccessPolicy() {
		case ChannelAccessMemberOnly, ChannelAccessAllowlist, ChannelAccessPublic:
		default:
			return fmt.Errorf("channels[%d].access_policy is invalid", index)
		}
		if binding.EffectiveAccessPolicy() != ChannelAccessAllowlist && len(binding.Allowlist) > 0 {
			return fmt.Errorf("channels[%d].allowlist requires allowlist access policy", index)
		}
		seenAllowed := make(map[string]struct{}, len(binding.Allowlist))
		for allowedIndex, allowed := range binding.Allowlist {
			allowed = strings.TrimSpace(allowed)
			if allowed == "" {
				return fmt.Errorf("channels[%d].allowlist[%d] is empty", index, allowedIndex)
			}
			if _, duplicate := seenAllowed[allowed]; duplicate {
				return fmt.Errorf("channels[%d].allowlist contains duplicate identity %q", index, allowed)
			}
			seenAllowed[allowed] = struct{}{}
		}
		key := binding.Type + "/" + binding.BindingID
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate channel binding %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}
