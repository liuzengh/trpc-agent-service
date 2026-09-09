//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package worker

import (
	"context"
	"fmt"
	"os"

	openaiopt "github.com/openai/openai-go/option"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/modelendpoint"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
)

// ModelFactory creates model instances from tenant configuration.
type ModelFactory struct {
	// Internal construction seam for provider protocol tests. Production
	// factories always use modelendpoint.New and its pinned DNS transport.
	endpointClient func(string) (openaiopt.HTTPClient, error)
}

// NewModelFactory creates a new model factory.
func NewModelFactory() *ModelFactory {
	return &ModelFactory{}
}

// NewModelForTenant constructs a provider model from an immutable version
// snapshot while resolving credentials only at runtime. The input snapshot is
// cloned and never receives plaintext secret material.
func NewModelForTenant(
	ctx context.Context,
	config tenant.ModelConfig,
	t *tenant.Tenant,
	resolver tenant.SecretResolver,
) (model.Model, error) {
	cloned := cloneModelConfig(config)
	resolved, err := resolveModelCredential(ctx, &cloned, t, resolver)
	if err != nil {
		return nil, err
	}
	mdl, err := NewModelFactory().CreateModel(resolved)
	if err != nil {
		return nil, fmt.Errorf("create tenant model: %w", err)
	}
	return mdl, nil
}

// CreateModel creates a model instance based on configuration.
// This runtime intentionally wires only OpenAI. Other providers require an
// explicitly registered runtime dependency rather than a silent fallback.
func (f *ModelFactory) CreateModel(config *tenant.ModelConfig) (model.Model, error) {
	if config == nil {
		return nil, fmt.Errorf("model configuration is nil")
	}

	switch config.Provider {
	case "openai":
		return f.createOpenAIModel(config)
	default:
		return nil, fmt.Errorf("unsupported model provider: %s (only openai is wired in this runtime)", config.Provider)
	}
}

// createOpenAIModel creates an OpenAI model.
func (f *ModelFactory) createOpenAIModel(config *tenant.ModelConfig) (model.Model, error) {
	if config.Endpoint != "" {
		return nil, fmt.Errorf("tenant model endpoint overrides are not permitted")
	}
	// Hidden SDK retries would turn one bounded LLM call into multiple provider
	// requests without additional budget reservation or invocation audit. Any
	// retry must happen above this adapter where idempotency and accounting are
	// explicit.
	clientOptions := []openaiopt.RequestOption{openaiopt.WithMaxRetries(0)}
	endpoint := "https://api.openai.com/v1"
	operatorEndpoint := os.Getenv("TRPC_OPENAI_BASE_URL")
	if operatorEndpoint != "" {
		buildClient := f.endpointClient
		if buildClient == nil {
			buildClient = func(value string) (openaiopt.HTTPClient, error) { return modelendpoint.New(value) }
		}
		client, err := buildClient(operatorEndpoint)
		if err != nil {
			return nil, modelendpoint.ErrConfiguration
		}
		endpoint = operatorEndpoint
		clientOptions = append(clientOptions, openaiopt.WithHTTPClient(client))
	}
	opts := []openai.Option{openai.WithOpenAIOptions(clientOptions...), openai.WithBaseURL(endpoint)}

	if config.APIKey != "" {
		opts = append(opts, openai.WithAPIKey(config.APIKey))
	}

	base := openai.New(config.ModelName, opts...)
	if operatorEndpoint != "" {
		return &operatorEndpointModel{base: base}, nil
	}
	return base, nil
}

// BuildGenerationConfig builds a GenerationConfig from tenant ModelConfig.
func BuildGenerationConfig(config *tenant.ModelConfig) model.GenerationConfig {
	genConfig := model.GenerationConfig{}

	if config.MaxTokens > 0 {
		maxTokens := config.MaxTokens
		genConfig.MaxTokens = &maxTokens
	}

	if config.Temperature > 0 {
		temp := config.Temperature
		genConfig.Temperature = &temp
	}

	return genConfig
}
