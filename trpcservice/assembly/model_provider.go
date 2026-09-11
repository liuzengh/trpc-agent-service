package assembly

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/failover"
	"trpc.group/trpc-go/trpc-agent-go/model/huggingface"
	"trpc.group/trpc-go/trpc-agent-go/model/hunyuan"
	openaimodel "trpc.group/trpc-go/trpc-agent-go/model/openai"
)

// ModelProvider returns the model bound to one immutable tenant configuration.
type ModelProvider interface {
	Model(context.Context, config.TenantConfig) (model.Model, error)
}

type ModelInputCapabilityProvider interface {
	GuaranteedInputCapabilities(config.ModelConfig) (config.ModelInputCapabilities, bool)
}

type ModelCallbackProvider interface {
	ModelCallbacks(config.TenantConfig) *model.Callbacks
}

// ManagedModelProvider resolves opaque secret references only when building
// a tenant Runner. The resolved keys are never stored in TenantConfig or errors.
type ManagedModelProvider struct {
	secrets credential.SecretResolver
	catalog *config.ModelCatalog
	audits  storage.AuditRecorder
	health  *backendhealth.Registry
}

type ManagedModelProviderOption func(*ManagedModelProvider)

func WithModelAuditRecorder(audits storage.AuditRecorder) ManagedModelProviderOption {
	return func(provider *ManagedModelProvider) {
		provider.audits = audits
	}
}

func WithModelBackendHealth(registry *backendhealth.Registry) ManagedModelProviderOption {
	return func(provider *ManagedModelProvider) {
		provider.health = registry
	}
}

func NewManagedModelProvider(secrets credential.SecretResolver, catalog *config.ModelCatalog, options ...ManagedModelProviderOption) (*ManagedModelProvider, error) {
	if secrets == nil {
		return nil, fmt.Errorf("secret resolver is required")
	}
	if catalog == nil {
		return nil, fmt.Errorf("model catalog is required")
	}
	if len(catalog.Providers()) == 0 {
		return nil, fmt.Errorf("at least one managed model provider is required")
	}
	provider := &ManagedModelProvider{secrets: secrets, catalog: catalog}
	for _, option := range options {
		if option != nil {
			option(provider)
		}
	}
	return provider, nil
}

// SyncProvider refreshes framework-discoverable model IDs and reports whether
// the provider credentials are currently resolvable. Secret values never leave
// this module.
func (p *ManagedModelProvider) SyncProvider(ctx context.Context, httpClient *http.Client, providerID string) (bool, error) {
	provider, ok := p.catalog.Provider(providerID)
	if !ok {
		return false, fmt.Errorf("model provider %q is not managed by the platform", providerID)
	}
	switch provider.NormalizedType() {
	case config.ModelProviderOpenAI:
		apiKey, err := p.resolveSecret(ctx, provider.APIKeyRef)
		if err != nil {
			return false, fmt.Errorf("resolve model key for provider %q: %w", provider.ID, err)
		}
		discovered, err := DiscoverOpenAIModels(ctx, httpClient, provider.BaseURL, apiKey)
		if err != nil {
			return true, err
		}
		p.catalog.ReplaceDiscoveredModels(provider.ID, discovered)
		return true, nil
	case config.ModelProviderHunyuan:
		if _, err := p.resolveSecret(ctx, provider.SecretIDRef); err != nil {
			return false, fmt.Errorf("resolve hunyuan secret id for provider %q: %w", provider.ID, err)
		}
		if _, err := p.resolveSecret(ctx, provider.SecretKeyRef); err != nil {
			return false, fmt.Errorf("resolve hunyuan secret key for provider %q: %w", provider.ID, err)
		}
		return true, nil
	case config.ModelProviderHuggingFace:
		if _, err := p.resolveSecret(ctx, provider.APIKeyRef); err != nil {
			return false, fmt.Errorf("resolve model key for provider %q: %w", provider.ID, err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("model provider %q has unsupported type %q", provider.ID, provider.Type)
	}
}

func (p *ManagedModelProvider) Model(ctx context.Context, tenantConfig config.TenantConfig) (model.Model, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	modelConfig := tenantConfig.Model
	if strings.TrimSpace(modelConfig.ProviderID) == "" || strings.TrimSpace(modelConfig.Name) == "" {
		return nil, fmt.Errorf("tenant %q model requires provider_id and name", tenantConfig.AppName())
	}

	primary, err := p.buildSingleModel(ctx, tenantConfig.AppName(), modelConfig.ProviderID, modelConfig.Name)
	if err != nil {
		return nil, err
	}
	if len(modelConfig.FailoverCandidates) == 0 {
		return newAuditedModel(primary, p.audits), nil
	}

	candidates := make([]model.Model, 0, 1+len(modelConfig.FailoverCandidates))
	candidates = append(candidates, primary)
	for index, candidate := range modelConfig.FailoverCandidates {
		fallback, err := p.buildSingleModel(ctx, tenantConfig.AppName(), candidate.ProviderID, candidate.Name)
		if err != nil {
			return nil, fmt.Errorf("build failover candidate %d for tenant %q: %w", index, tenantConfig.AppName(), err)
		}
		candidates = append(candidates, fallback)
	}

	failoverModel, err := failover.New(failover.WithCandidates(candidates...))
	if err != nil {
		return nil, fmt.Errorf("construct failover model for tenant %q: %w", tenantConfig.AppName(), err)
	}
	return newAuditedModel(failoverModel, p.audits), nil
}

func (p *ManagedModelProvider) ModelCallbacks(tenantConfig config.TenantConfig) *model.Callbacks {
	if p == nil {
		return nil
	}
	return newInputValidationCallbacks(p.catalog, tenantConfig.Model)
}

func (p *ManagedModelProvider) GuaranteedInputCapabilities(modelConfig config.ModelConfig) (config.ModelInputCapabilities, bool) {
	if p == nil || p.catalog == nil {
		return config.ModelInputCapabilities{}, false
	}
	return p.catalog.GuaranteedInputCapabilities(modelConfig)
}

func (p *ManagedModelProvider) buildSingleModel(ctx context.Context, tenantName, providerID, modelName string) (model.Model, error) {
	provider, ok := p.catalog.Provider(providerID)
	if !ok {
		return nil, fmt.Errorf("tenant %q model provider %q is not managed by the platform", tenantName, providerID)
	}
	if !p.catalog.AllowsModel(providerID, modelName) {
		return nil, fmt.Errorf("tenant %q model %q is not managed by provider %q", tenantName, modelName, providerID)
	}

	providerType := provider.NormalizedType()
	if providerType == config.ModelProviderOpenAI {
		apiKey, err := p.resolveSecret(ctx, provider.APIKeyRef)
		if err != nil {
			return nil, fmt.Errorf("resolve model key for tenant %q: %w", tenantName, err)
		}
		configured := observeModel(openaimodel.New(
			modelName,
			openaimodel.WithBaseURL(provider.BaseURL),
			openaimodel.WithAPIKey(apiKey),
			openaimodel.WithEnableTokenTailoring(true),
		), p.health, providerID, providerType)
		return observeActualModelUsage(configured, providerID, modelName), nil
	}

	if providerType == config.ModelProviderHunyuan {
		secretID, err := p.resolveSecret(ctx, provider.SecretIDRef)
		if err != nil {
			return nil, fmt.Errorf("resolve hunyuan secret id for tenant %q: %w", tenantName, err)
		}
		secretKey, err := p.resolveSecret(ctx, provider.SecretKeyRef)
		if err != nil {
			return nil, fmt.Errorf("resolve hunyuan secret key for tenant %q: %w", tenantName, err)
		}
		opts := []hunyuan.Option{
			hunyuan.WithSecretId(secretID),
			hunyuan.WithSecretKey(secretKey),
			hunyuan.WithEnableTokenTailoring(true),
		}
		if strings.TrimSpace(provider.BaseURL) != "" {
			opts = append(opts, hunyuan.WithBaseUrl(provider.BaseURL))
		}
		configured := observeModel(hunyuan.New(modelName, opts...), p.health, providerID, providerType)
		return observeActualModelUsage(configured, providerID, modelName), nil
	}
	if providerType == config.ModelProviderHuggingFace {
		apiKey, err := p.resolveSecret(ctx, provider.APIKeyRef)
		if err != nil {
			return nil, fmt.Errorf("resolve model key for tenant %q: %w", tenantName, err)
		}
		opts := []huggingface.Option{
			huggingface.WithAPIKey(apiKey),
			huggingface.WithEnableTokenTailoring(true),
		}
		if strings.TrimSpace(provider.BaseURL) != "" {
			opts = append(opts, huggingface.WithBaseURL(provider.BaseURL))
		}
		configured, err := huggingface.New(modelName, opts...)
		if err != nil {
			return nil, err
		}
		observed := observeModel(configured, p.health, providerID, providerType)
		return observeActualModelUsage(observed, providerID, modelName), nil
	}

	return nil, fmt.Errorf("tenant %q model provider %q has unsupported type %q", tenantName, providerID, provider.Type)
}

func (p *ManagedModelProvider) resolveSecret(ctx context.Context, reference string) (string, error) {
	value, err := p.secrets.Resolve(ctx, reference)
	if err != nil {
		return "", err
	}
	value = strings.TrimSpace(value)
	if value == "" || strings.HasPrefix(value, "replace-") {
		return "", fmt.Errorf("secret reference %q is not configured", reference)
	}
	return value, nil
}
