package agent

import (
	"context"
	"errors"
	"strings"

	"github.com/cyl6/trpc-agent-service/trpcservice/budget"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/privacy"

	"trpc.group/trpc-go/trpc-agent-go/model"
	modelfailover "trpc.group/trpc-go/trpc-agent-go/model/failover"
	modelopenai "trpc.group/trpc-go/trpc-agent-go/model/openai"
)

func buildConfiguredModel(cfg config.ModelConfig) (model.Model, error) {
	return buildConfiguredModelWithBudget(config.TenantConfig{Model: cfg}, nil)
}

func buildConfiguredModelWithBudget(tenant config.TenantConfig, ledger budget.Ledger) (model.Model, error) {
	cfg := tenant.Model
	apiKey, err := config.Secret(cfg.APIKeyEnv)
	if err != nil {
		if cfg.FallbackProvider == "mock" {
			fallback := model.Model(&mockFallbackModel{name: "mock-fallback"})
			return wrapConfiguredModel(tenant, fallback, ledger), nil
		}
		return nil, err
	}
	options := []modelopenai.Option{modelopenai.WithAPIKey(apiKey)}
	if cfg.BaseURL != "" {
		options = append(options, modelopenai.WithBaseURL(cfg.BaseURL))
	}
	if cfg.Variant != "" {
		options = append(options, modelopenai.WithVariant(modelopenai.Variant(cfg.Variant)))
	}
	primary := modelopenai.New(cfg.Name, options...)
	primaryModel := wrapConfiguredModel(tenant, primary, ledger)
	if cfg.FallbackProvider == "" {
		return primaryModel, nil
	}
	fallback := wrapConfiguredModel(tenant, &mockFallbackModel{name: "mock-fallback"}, ledger)
	return modelfailover.New(modelfailover.WithCandidates(
		primaryModel,
		fallback,
	))
}

func wrapConfiguredModel(tenant config.TenantConfig, next model.Model, ledger budget.Ledger) model.Model {
	if ledger != nil {
		next = budget.WrapModel(next, budget.ModelSpec{
			TenantID:          tenant.TenantID,
			AppNamespace:      domain.AppNamespace(tenant.TenantID, tenant.App.Name),
			ModelName:         next.Info().Name,
			MaxTokens:         tenant.Model.MaxTokens,
			InputPrice:        tenant.Model.InputPrice,
			OutputPrice:       tenant.Model.OutputPrice,
			MonthlyLimitUnits: budget.USDUnits(tenant.Budget.MonthlyCostUSD),
		}, ledger)
	}
	// Privacy runs before budget reservation/provider dispatch, including summary
	// calls and both failover candidates.
	return privacy.WrapModel(next, tenant)
}

// mockFallbackModel is deliberately simple and side-effect free. It is used
// only when the primary provider fails before producing any successful
// response, so a provider outage does not execute tools or duplicate work.
type mockFallbackModel struct {
	name string
}

func (m *mockFallbackModel) GenerateContent(
	ctx context.Context,
	request *model.Request,
) (<-chan *model.Response, error) {
	if request == nil {
		return nil, errors.New("mock fallback request is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	content := lastUserContent(request.Messages)
	message := "[mock fallback] 真实大模型暂时不可用。"
	if content != "" {
		message += "\n" + content
	}
	response := &model.Response{
		Model: m.Info().Name,
		Done:  true,
		Choices: []model.Choice{{Index: 0, Message: model.Message{
			Role: model.RoleAssistant, Content: message,
		}}},
	}
	out := make(chan *model.Response, 1)
	out <- response
	close(out)
	return out, nil
}

func (m *mockFallbackModel) Info() model.Info {
	name := strings.TrimSpace(m.name)
	if name == "" {
		name = "mock-fallback"
	}
	return model.Info{Name: name}
}

func lastUserContent(messages []model.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == model.RoleUser {
			return messages[i].Content
		}
	}
	return ""
}
