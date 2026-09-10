package agent

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestNewRunnerWithConfigRejectsInvalidDependencies(t *testing.T) {
	var nilContext context.Context
	for _, test := range []struct {
		name   string
		ctx    context.Context
		config RunnerConfig
	}{
		{name: "nil context", ctx: nilContext, config: RunnerConfig{Sessions: inmemory.NewSessionService()}},
		{name: "missing session capability", ctx: context.Background()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.config.Sessions != nil {
				t.Cleanup(func() { _ = test.config.Sessions.Close() })
			}
			if _, err := NewRunnerWithConfig(test.ctx, test.config); err == nil {
				t.Fatal("invalid runner configuration unexpectedly succeeded")
			}
		})
	}
}

func TestNewRunnerWithConfigRejectsStorageMaterializationFailures(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	factoryErr := errors.New("storage provider failed")
	tests := []struct {
		name    string
		factory storagefactory.StorageFactory
		wantErr error
	}{
		{
			name: "factory error",
			factory: storagefactory.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
				return nil, factoryErr
			}),
			wantErr: factoryErr,
		},
		{
			name: "nil capability set",
			factory: storagefactory.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
				return nil, nil
			}),
			wantErr: storagefactory.ErrStorageFactory,
		},
		{
			name: "missing session capability",
			factory: storagefactory.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
				return storagefactory.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilityMemory: struct{}{}})
			}),
			wantErr: storagefactory.ErrCapabilityUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewRunnerWithConfig(context.Background(), RunnerConfig{Input: input, ModelFactory: runnerBuilderModelFactory{}, StorageFactory: test.factory})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("NewRunnerWithConfig() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestNewRunnerWithConfigCleansUpAfterAssemblyFailure(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	base := inmemory.NewSessionService()
	tracked := &agentCloseTrackingSession{Service: base}
	factory := storagefactory.StorageFactoryFunc(func(_ context.Context, value backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[backend.Capability]any{backend.CapabilitySession: tracked})
	})
	_, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, ModelFactory: runnerBuilderModelFactory{err: errors.New("model unavailable")}, StorageFactory: factory,
	})
	if err == nil {
		// ResolveAndBuild deliberately redacts provider failures to its stable
		// model-factory category; the important contract here is cleanup.
		t.Fatal("assembly failure unexpectedly succeeded")
	}
	if tracked.calls != 1 {
		t.Fatalf("storage capability close calls = %d, want 1", tracked.calls)
	}
}

func TestNewRunnerWithConfigCoversModelToolTelemetryAndStorageBoundaries(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = nil

	modelFailure := errors.New("model provider detail")
	modelSessions := inmemory.NewSessionService()
	t.Cleanup(func() { _ = modelSessions.Close() })
	if _, err := NewRunnerWithConfig(context.Background(), RunnerConfig{Input: input, Sessions: modelSessions, ModelFactory: runnerBuilderModelFactory{err: modelFailure}}); err == nil {
		// The adapter preserves the model runtime category rather than provider
		// details; assert only that the build failed and did not return a Runner.
		t.Fatal("model assembly unexpectedly succeeded")
	}

	toolInput := input
	toolInput.Agent.Tools = []appmodel.ToolAuthorization{{ToolID: "required-tool-not-installed", Required: true}}
	toolSessions := inmemory.NewSessionService()
	t.Cleanup(func() { _ = toolSessions.Close() })
	_, err := NewRunnerWithConfig(context.Background(), RunnerConfig{Input: toolInput, Sessions: toolSessions, ModelFactory: runnerBuilderModelFactory{}})
	if err == nil {
		t.Fatal("required unavailable tool unexpectedly succeeded")
	}

	telemetrySessions := inmemory.NewSessionService()
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, Sessions: telemetrySessions, ModelFactory: runnerBuilderModelFactory{}, Observability: observability.NewNoopProvider(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}

	storageSessions := inmemory.NewSessionService()
	storageFactory := storagefactory.StorageFactoryFunc(func(_ context.Context, value backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[backend.Capability]any{backend.CapabilitySession: storageSessions})
	})
	runner, err = NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, ModelFactory: runnerBuilderModelFactory{}, Observability: observability.NewNoopProvider(),
		StorageFactory: storageFactory,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
}

func runnerBuilderInputForTest(t *testing.T) RunnerInput {
	t.Helper()
	tenantRoot, tenantSnapshot, appRoot, revision := executionFixture(t)
	snapshot, err := NewAgentExecutionSnapshot(tenantSnapshot, appRoot, revision)
	if err != nil {
		t.Fatal(err)
	}
	agentInput, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	return RunnerInput{
		Tenant: *tenantRoot,
		Agent:  agentInput,
		Model: modelprofile.ModelFactoryInput{
			TenantID: tenantRoot.TenantID, TenantVersion: tenantRoot.Version,
			ProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileVersion: 1,
			ContentDigest: "model-digest", SchemaVersion: modelprofile.SchemaVersionV1,
			Provider: "fake", Model: "deterministic",
		},
		Storage: backend.StorageFactoryInput{
			TenantID: tenantRoot.TenantID, TenantVersion: tenantRoot.Version,
			ProfileID: "bp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileVersion: 1,
			ContentDigest: "backend-digest", SchemaVersion: 1,
		},
	}
}

type runnerBuilderModelFactory struct {
	err       error
	returnNil bool
}

func (factory runnerBuilderModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (model.Model, error) {
	if factory.err != nil {
		return nil, factory.err
	}
	if factory.returnNil {
		return nil, nil
	}
	return agentTestModel{}, nil
}
