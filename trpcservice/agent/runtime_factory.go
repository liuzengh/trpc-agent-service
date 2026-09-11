package agent

import (
	"context"
	"fmt"

	"github.com/cyl6/trpc-agent-service/trpcservice/budget"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/memoryvisibility"
	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	platformskill "github.com/cyl6/trpc-agent-service/trpcservice/skill"
	platformtool "github.com/cyl6/trpc-agent-service/trpcservice/tool"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	summarypkg "trpc.group/trpc-go/trpc-agent-go/session/summary"
	agentskill "trpc.group/trpc-go/trpc-agent-go/skill"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func buildRuntime(ctx context.Context, tenant config.TenantConfig, skills agentskill.Repository) (*Runtime, error) {
	return buildRuntimeWithBudget(ctx, tenant, skills, nil)
}

func buildRuntimeWithBudget(ctx context.Context, tenant config.TenantConfig, skills agentskill.Repository, ledger budget.Ledger, visibility ...memoryvisibility.Store) (*Runtime, error) {
	appNamespace := domain.AppNamespace(tenant.TenantID, tenant.App.Name)
	var visibilityStore memoryvisibility.Store
	if len(visibility) > 0 {
		visibilityStore = visibility[0]
	}
	var llmModel model.Model
	switch tenant.Model.Provider {
	case "mock":
		llmModel = wrapConfiguredModel(tenant, &mockFallbackModel{name: "mock-summary"}, ledger)
	case "openai":
		var err error
		llmModel, err = buildConfiguredModelWithBudget(tenant, ledger)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported model provider %q", tenant.Model.Provider)
	}
	var summarizer summarypkg.SessionSummarizer
	if tenant.Data.Summary.Type != "" && tenant.Data.Summary.Type != "disabled" {
		// Avoid a new asynchronous model call on every short Redis/InMemory
		// turn. Keep the existing strict SQL job policy unchanged.
		options := []summarypkg.Option{}
		if tenant.Data.Session.Type != "sql" {
			options = append(options, summarypkg.WithEventThreshold(20))
		}
		summarizer = summarypkg.NewSummarizer(llmModel, options...)
	}
	backends, err := buildBackends(ctx, tenant, appNamespace, summarizer, visibilityStore)
	if err != nil {
		return nil, err
	}
	tools := platformtool.BuiltInTools()
	if len(backends.memory.Tools) > 0 {
		tools = append(tools, backends.memory.Tools...)
	}
	var rootAgent agent.Agent
	switch tenant.Model.Provider {
	case "mock":
		rootAgent = &mockAgent{name: tenant.App.AgentName, description: tenant.App.Description, tools: tools}
	case "openai":
		maxTokens := tenant.Model.MaxTokens
		temperature := tenant.Model.Temperature
		agentOptions := []llmagent.Option{
			llmagent.WithModel(llmModel),
			llmagent.WithDescription(tenant.App.Description),
			llmagent.WithInstruction(tenant.App.Instruction),
			llmagent.WithTools(tools),
			llmagent.WithGenerationConfig(model.GenerationConfig{
				MaxTokens: &maxTokens, Temperature: &temperature, Stream: tenant.Model.Streaming,
			}),
			llmagent.WithAddSessionSummary(summarizer != nil),
			llmagent.WithMaxLLMCalls(8),
			llmagent.WithMaxToolIterations(6),
		}
		if backends.memory.Reader != nil {
			agentOptions = append(agentOptions, llmagent.WithPreloadMemory(20))
		}
		if backends.knowledge != nil {
			agentOptions = append(agentOptions,
				llmagent.WithKnowledge(backends.knowledge),
				llmagent.WithKnowledgeFilter(map[string]any{
					"metadata.tenant_id": tenant.TenantID,
					"metadata.app_name":  tenant.App.Name,
				}),
			)
		}
		if skills != nil {
			// Knowledge-only profile exposes skill_load / skill_list_docs /
			// skill_select_docs without a code executor, so IM tenants never
			// gain local script execution through the framework's auto
			// fallback. Tenant tool policy still gates these tools at run
			// time like any other tool.
			agentOptions = append(agentOptions,
				llmagent.WithSkills(skills),
				llmagent.WithSkillFilter(platformskill.VisibilityFilter(tenant.Skills.Allow)),
				llmagent.WithSkillToolProfile(llmagent.SkillToolProfileKnowledgeOnly),
			)
		}
		rootAgent = llmagent.New(tenant.App.AgentName, agentOptions...)
	}
	runnerOptions := []runner.Option{runner.WithSessionService(observability.WrapSession(backends.session, tenant.Data.Session.Type, tenant.TenantID)), runner.WithArtifactService(backends.artifact)}
	if backends.memory.Service != nil {
		runnerOptions = append(runnerOptions, runner.WithMemoryService(backends.memory.Service))
	}
	if backends.memory.Ingestor != nil {
		runnerOptions = append(runnerOptions, runner.WithSessionIngestor(backends.memory.Ingestor))
	}
	r := runner.NewRunner(appNamespace, rootAgent, runnerOptions...)
	return &Runtime{
		Runner:                  r,
		AppNamespace:            appNamespace,
		Session:                 backends.session,
		TurnSession:             backends.turn,
		SessionDatabaseIdentity: backends.databaseIdentity,
		Memory:                  backends.memory.Service,
		MemoryReader:            backends.memory.Reader,
		MemoryIngestor:          backends.memory.Ingestor,
		Artifact:                backends.artifact,
		Knowledge:               backends.knowledge,
		backendClosers:          backends.closers,
	}, nil
}

// ToolNames is used by diagnostics and tests without exposing tool instances.
func ToolNames(tools []tool.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, candidate := range tools {
		if candidate != nil && candidate.Declaration() != nil {
			names = append(names, candidate.Declaration().Name)
		}
	}
	return names
}
