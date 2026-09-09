package worker

import (
	"context"
	"errors"
	"fmt"

	openaiopt "github.com/openai/openai-go/option"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentopenai "trpc.group/trpc-go/trpc-agent-go/model/openai"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

// OpenAIModelFactory builds OpenAI and OpenAI-compatible model clients.
type OpenAIModelFactory struct{}

// Model resolves the API key only while constructing the client.
func (OpenAIModelFactory) Model(
	_ context.Context,
	config tenant.ModelConfig,
) (model.Model, error) {
	switch config.Provider {
	case "openai", "openai-compatible":
	default:
		return nil, fmt.Errorf("unsupported model provider %q", config.Provider)
	}
	if config.Model == "" {
		return nil, errors.New("model name is required")
	}
	apiKey, err := tenant.ResolveSecret(config.APIKeyRef)
	if err != nil {
		return nil, fmt.Errorf("resolve model API key: %w", err)
	}
	requestOptions := []openaiopt.RequestOption{openaiopt.WithMaxRetries(1)}
	if config.Timeout > 0 {
		requestOptions = append(requestOptions, openaiopt.WithRequestTimeout(config.Timeout))
	}
	options := []agentopenai.Option{
		agentopenai.WithAPIKey(apiKey),
		agentopenai.WithOpenAIOptions(requestOptions...),
	}
	if config.BaseURL != "" {
		options = append(options, agentopenai.WithBaseURL(config.BaseURL))
	}
	return agentopenai.New(config.Model, options...), nil
}
