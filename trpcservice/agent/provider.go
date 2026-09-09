package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkmodel "trpc.group/trpc-go/trpc-agent-go/model"
	openaimodel "trpc.group/trpc-go/trpc-agent-go/model/openai"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type ModelConfig struct {
	TenantID      string
	AgentAppID    string
	ConfigVersion int64
	ConfigRef     string
	Provider      string
	Endpoint      string
	Model         string
	SecretRef     string
}

type ProviderResponseError struct {
	Type    string
	Code    string
	Message string
}

func (e *ProviderResponseError) Error() string {
	if e == nil {
		return "provider response error"
	}
	parts := []string{e.Type}
	if e.Code != "" {
		parts = append(parts, e.Code)
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	return strings.Join(parts, ": ")
}

type ModelConfigResolver interface {
	Resolve(context.Context, tenant.TenantContext, AgentSpec) (ModelConfig, error)
}

type SecretResolver interface {
	Resolve(context.Context, tenant.TenantContext, string) (string, error)
}

type ModelConfigResolverFunc func(context.Context, tenant.TenantContext, AgentSpec) (ModelConfig, error)

func (f ModelConfigResolverFunc) Resolve(ctx context.Context, tc tenant.TenantContext, spec AgentSpec) (ModelConfig, error) {
	if f == nil {
		return ModelConfig{}, errors.New("model config resolver is nil")
	}
	return f(ctx, tc, spec)
}

type SecretResolverFunc func(context.Context, tenant.TenantContext, string) (string, error)

func (f SecretResolverFunc) Resolve(ctx context.Context, tc tenant.TenantContext, ref string) (string, error) {
	if f == nil {
		return "", errors.New("secret resolver is nil")
	}
	return f(ctx, tc, ref)
}

type OpenAIProviderFactory struct {
	Configs ModelConfigResolver
	Secrets SecretResolver
}

func (f OpenAIProviderFactory) Build(ctx context.Context, tc tenant.TenantContext, spec AgentSpec) (Provider, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.Configs == nil || f.Secrets == nil {
		return nil, errors.New("model provider configuration is incomplete")
	}
	config, err := f.Configs.Resolve(ctx, tc, spec)
	if err != nil {
		return nil, fmt.Errorf("resolve model config: %w", err)
	}
	if config.TenantID != tc.TenantID || config.AgentAppID != tc.AgentAppID || config.ConfigVersion != tc.ConfigVersion || config.ConfigRef != spec.ModelConfigRef || config.Provider != spec.ModelProvider {
		return nil, fmt.Errorf("model config does not match tenant, agent, version, config ref, or provider")
	}
	if strings.TrimSpace(config.Endpoint) == "" || strings.TrimSpace(config.Model) == "" || strings.TrimSpace(config.SecretRef) == "" {
		return nil, errors.New("model endpoint, model, and secret reference are required")
	}
	secret, err := f.Secrets.Resolve(ctx, tc, config.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("resolve model secret: %w", err)
	}
	if strings.TrimSpace(secret) == "" {
		return nil, errors.New("model secret is empty")
	}
	return &openAIProvider{model: openaimodel.New(config.Model, openaimodel.WithBaseURL(config.Endpoint), openaimodel.WithAPIKey(secret))}, nil
}

type openAIProvider struct{ model frameworkmodel.Model }

func (p *openAIProvider) Complete(ctx context.Context, request ProviderRequest) (ProviderResponse, error) {
	if p == nil || p.model == nil {
		return ProviderResponse{}, errors.New("openai model is not configured")
	}
	messages := make([]frameworkmodel.Message, 0, len(request.Messages))
	for _, message := range request.Messages {
		role, err := frameworkRole(message.Role)
		if err != nil {
			return ProviderResponse{}, err
		}
		converted := frameworkmodel.Message{Role: role, Content: message.Content, ToolID: message.ToolID, ToolName: message.ToolName}
		for _, call := range message.ToolCalls {
			converted.ToolCalls = append(converted.ToolCalls, frameworkmodel.ToolCall{Type: "function", ID: call.ID, Function: frameworkmodel.FunctionDefinitionParam{Name: call.Name, Arguments: []byte(call.Arguments)}})
		}
		messages = append(messages, converted)
	}
	tools := make(map[string]frameworktool.Tool, len(request.Tools))
	for _, spec := range request.Tools {
		declaration := &frameworktool.Declaration{Name: spec.Name, Description: spec.Description, InputSchema: schemaFromValue(spec.InputSchema)}
		tools[spec.Name] = modelToolDeclaration{declaration: declaration}
	}
	responses, err := p.model.GenerateContent(ctx, &frameworkmodel.Request{Messages: messages, Tools: tools})
	if err != nil {
		return ProviderResponse{}, fmt.Errorf("%w: %w", ErrProviderFailure, err)
	}
	var result ProviderResponse
	for response := range responses {
		if response == nil {
			continue
		}
		if response.Error != nil {
			code := ""
			if response.Error.Code != nil {
				code = *response.Error.Code
			}
			responseErr := &ProviderResponseError{Type: response.Error.Type, Code: code, Message: response.Error.Message}
			return result, fmt.Errorf("%w: %w", ErrProviderFailure, responseErr)
		}
		if response.Usage != nil {
			result.InputTokens += int64(response.Usage.PromptTokens)
			result.OutputTokens += int64(response.Usage.CompletionTokens)
		}
		if len(response.Choices) > 0 {
			choice := response.Choices[0]
			result.Text += choice.Message.Content
			if result.Text == "" {
				result.Text += choice.Delta.Content
			}
			for _, call := range choice.Message.ToolCalls {
				result.ToolCalls = append(result.ToolCalls, ToolCall{ID: call.ID, Name: call.Function.Name, Arguments: string(call.Function.Arguments)})
			}
			if choice.FinishReason != nil {
				result.FinishType = *choice.FinishReason
			}
		}
	}
	return result, nil
}
