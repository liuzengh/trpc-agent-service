package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// RunnerConfig groups the dependencies used to materialize one external-agent
// Runner. Session, registries, factories, and Observability are borrowed by
// the returned Runner; the optional StorageFactory produces capabilities owned
// by that Runner.
type RunnerConfig struct {
	Input                RunnerInput
	SecretResolver       modelprofile.SecretResolver
	ModelFactory         modelprofile.ModelFactory
	Sessions             session.Service
	StorageFactory       storagefactory.StorageFactory
	Observability        observability.Provider
	ToolRegistry         *servicetool.Registry
	AgentFactories       *AgentFactoryRegistry
	EnableUsageCallbacks bool
}

// NewRunnerWithConfig materializes one Runner from an explicit dependency
// group.
func NewRunnerWithConfig(ctx context.Context, config RunnerConfig) (trpcrunner.Runner, error) {
	if err := validateRunnerConfig(ctx, config); err != nil {
		return nil, err
	}
	resources, err := materializeRunnerResources(ctx, config)
	if err != nil {
		return nil, err
	}
	owned := resources.capabilities != nil
	defer func() {
		if owned {
			_ = resources.capabilities.Close()
		}
	}()
	runner, err := assembleRunner(ctx, config, resources)
	if err != nil {
		return nil, err
	}
	owned = false
	return runner, nil
}

type runnerResources struct {
	sessions     session.Service
	capabilities *storagefactory.CapabilitySet
}

func validateRunnerConfig(ctx context.Context, config RunnerConfig) error {
	if ctx == nil {
		return errors.New("invalid runner: context is required")
	}
	if config.Sessions == nil && config.StorageFactory == nil {
		return errors.New("invalid runner: session service is required")
	}
	return nil
}

func materializeRunnerResources(ctx context.Context, config RunnerConfig) (runnerResources, error) {
	resources := runnerResources{sessions: config.Sessions}
	if config.StorageFactory == nil {
		return resources, nil
	}
	capabilities, err := materializeStorageCapabilities(ctx, config)
	if err != nil {
		return runnerResources{}, err
	}
	if capabilities == nil {
		return runnerResources{}, fmt.Errorf("build runner: storage capability: %w", storagefactory.ErrStorageFactory)
	}
	sessions, err := capabilities.Session()
	if err != nil {
		_ = capabilities.Close()
		return runnerResources{}, fmt.Errorf("build runner: session capability: %w", err)
	}
	resources.sessions = sessions
	resources.capabilities = capabilities
	return resources, nil
}

func materializeStorageCapabilities(ctx context.Context, config RunnerConfig) (*storagefactory.CapabilitySet, error) {
	storageCtx := ctx
	started := time.Now()
	var finishStorage func(error)
	storageMetrics := metrics.New(config.Observability)
	if config.Observability != nil {
		storageCtx, _, finishStorage = observability.StartOperation(ctx, config.Observability, observability.OperationStorageOperation, "storage")
		_ = storageMetrics.Request(storageCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other", "status": "started"})
	}
	capabilities, err := config.StorageFactory.New(storageCtx, config.Input.Storage)
	if finishStorage != nil {
		finishStorage(err)
		_ = storageMetrics.Operation(storageCtx, started, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": "other"}, err)
		status := "success"
		if err != nil {
			status = observability.ErrorClass(err)
			if status == "" {
				status = "error"
			}
		}
		_ = storageMetrics.BackendDuration(storageCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "provider": "other", "status": status, "error_class": observability.ErrorClass(err)})
	}
	if err != nil {
		return nil, fmt.Errorf("build runner: storage capability: %w", err)
	}
	return capabilities, nil
}

func assembleRunner(ctx context.Context, config RunnerConfig, resources runnerResources) (trpcrunner.Runner, error) {
	agentInput := config.Input.Agent.Clone()
	scopedSessions, err := NewTenantSessionService(config.Input.Tenant, resources.sessions)
	if err != nil {
		return nil, fmt.Errorf("build runner: session scope: %w", err)
	}
	model, err := modelruntime.ResolveAndBuild(ctx, config.Input.Model, config.SecretResolver, config.ModelFactory)
	if err != nil {
		return nil, fmt.Errorf("build runner: model: %w", err)
	}
	telemetryProvider := config.Observability
	if telemetryProvider == nil && config.EnableUsageCallbacks {
		telemetryProvider = observability.NewNoopProvider()
	}
	if telemetryProvider != nil {
		model = wrapTelemetryModel(model)
	}
	toolRegistry := config.ToolRegistry
	if toolRegistry == nil {
		toolRegistry = servicetool.DefaultRegistry()
	}
	tools, err := toolRegistry.Resolve(agentInput.Tools)
	if err != nil {
		return nil, fmt.Errorf("build runner: tools: %w", err)
	}
	modelOptions := []llmagent.Option(nil)
	if telemetryProvider != nil {
		modelOptions = append(modelOptions, telemetryOptions(telemetryProvider, config.Input.Model.Provider, config.Input.Model.Model)...)
	}
	factories := config.AgentFactories
	if factories == nil {
		factories = DefaultAgentFactoryRegistry()
	}
	builtAgent, err := factories.Build(ctx, AgentBuildInput{
		Definition: agentInput, Model: model, Tools: tools, ModelOptions: modelOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("build runner: Agent Factory: %w", err)
	}
	delegate := trpcrunner.NewRunner(agentInput.AppID, builtAgent, trpcrunner.WithSessionService(scopedSessions))
	return &policyRunner{
		delegate:     delegate,
		capabilities: resources.capabilities,
		runOptions: []trpcagent.RunOption{
			trpcagent.WithMaxRunDuration(time.Duration(agentInput.Runtime.ExecutionTimeoutSeconds) * time.Second),
		},
	}, nil
}
