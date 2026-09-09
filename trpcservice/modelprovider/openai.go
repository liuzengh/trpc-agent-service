// Package modelprovider constructs real tenant-scoped model clients.
package modelprovider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
	openaimodel "trpc.group/trpc-go/trpc-agent-go/model/openai"
)

const (
	ProviderDeepSeek         = "deepseek"
	ProviderOpenAI           = "openai"
	ProviderOpenAICompatible = "openai-compatible"
)

// New resolves the profile's SecretRef once and constructs an OpenAI-compatible
// client. Returned errors never include the secret value or lookup key.
func New(profile tenant.ModelProfile) (model.Model, error) {
	return newOpenAICompatible(profile, nil)
}

// NewScoped constructs a production model only after enforcing the immutable
// tenant/application secret namespace.
func NewScoped(profile tenant.ModelProfile, tenantID, appID string) (model.Model, error) {
	return newOpenAICompatibleWithResolver(profile, nil, func(ctx context.Context, ref tenant.SecretRef) (string, error) {
		if err := secret.ValidateScope(tenantID, appID, ref); err != nil {
			return "", err
		}
		return secret.Resolve(ctx, ref)
	})
}

func newOpenAICompatible(profile tenant.ModelProfile, transport http.RoundTripper) (model.Model, error) {
	return newOpenAICompatibleWithResolver(profile, transport, nil)
}

func newOpenAICompatibleWithResolver(profile tenant.ModelProfile, transport http.RoundTripper, resolve func(context.Context, tenant.SecretRef) (string, error)) (model.Model, error) {
	provider := strings.ToLower(strings.TrimSpace(profile.Provider))
	if provider != ProviderDeepSeek && provider != ProviderOpenAI && provider != ProviderOpenAICompatible {
		return nil, fmt.Errorf("model provider: unsupported provider %q", profile.Provider)
	}
	if strings.TrimSpace(profile.Name) == "" {
		return nil, errors.New("model provider: model name is required")
	}
	if resolve == nil {
		resolve = secret.Resolve
	}
	apiKey, err := resolve(context.Background(), profile.APIKey)
	if err != nil {
		return nil, errors.New("model provider: resolve API credential failed")
	}
	options := []openaimodel.Option{openaimodel.WithAPIKey(apiKey)}
	if transport != nil {
		options = append(options, openaimodel.WithHTTPClientOptions(openaimodel.WithHTTPClientTransport(transport)))
	}
	if profile.BaseURL != "" {
		options = append(options, openaimodel.WithBaseURL(profile.BaseURL))
	}
	if provider == ProviderDeepSeek {
		options = append(options, openaimodel.WithVariant(openaimodel.VariantDeepSeek))
	}
	return openaimodel.New(profile.Name, options...), nil
}
