package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestPolicyRunnerCloseReleasesDelegateAndCapabilities(t *testing.T) {
	delegate := &agentClosingRunner{err: errors.New("delegate close failure")}
	capability := &agentCloseTrackingSession{Service: inmemory.NewSessionService(), err: errors.New("capability close failure")}
	set, err := storagefactory.NewCapabilitySet("t_00000000000000000000000000", map[storagefactory.Capability]any{storagefactory.CapabilitySession: capability})
	if err != nil {
		t.Fatal(err)
	}
	runner := &policyRunner{delegate: delegate, capabilities: set}
	if err := runner.Close(); err == nil || !strings.Contains(err.Error(), "delegate close failure") || !strings.Contains(err.Error(), "backend storage factory failed") {
		t.Fatalf("Close() = %v", err)
	}
	if delegate.calls != 1 || capability.calls != 1 {
		t.Fatalf("close calls delegate=%d capability=%d", delegate.calls, capability.calls)
	}
	var nilRunner *policyRunner
	if err := nilRunner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWithUsageObserverPreservesOnlyValidObservers(t *testing.T) {
	ctx := context.Background()
	if WithUsageObserver(nil, func(context.Context, runtimebudget.Usage) {}) != nil {
		t.Fatal("nil context unexpectedly accepted")
	}
	if WithUsageObserver(ctx, nil) != ctx {
		t.Fatal("nil observer unexpectedly changed context")
	}
	called := false
	observed := WithUsageObserver(ctx, func(context.Context, runtimebudget.Usage) { called = true })
	observer := usageObserverFromContext(observed)
	if observer == nil {
		t.Fatal("observer missing from context")
	}
	observer(context.Background(), runtimebudget.Usage{InputTokens: 1})
	if !called {
		t.Fatal("observer was not callable")
	}
}

func TestNewRunnerCarriesPublishedRuntimePolicy(t *testing.T) {
	tenantRoot, tenantSnapshot, appRoot, revision := executionFixture(t)
	policy := appmodel.DefaultRuntimePolicy()
	policy.EnableParallelTools = true
	policy.MaxParallelTools = 7
	policy.ExecutionTimeoutSeconds = 9
	appRoot, revision = agentRuntimeFixtureWithPolicy(t, tenantRoot.TenantID, appRoot.AppID, revision.ModelProfileID, policy)
	agentSnapshot, err := NewAgentExecutionSnapshot(tenantSnapshot, appRoot, revision)
	if err != nil {
		t.Fatal(err)
	}
	agentInput, err := agentSnapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	options := llmAgentOptions(agentInput, agentTestModel{})
	llmOptions := llmagent.Options{}
	for _, option := range options {
		option(&llmOptions)
	}
	if !llmOptions.EnableParallelTools || llmOptions.ToolConcurrencyConfig.MaxConcurrency != policy.MaxParallelTools || llmOptions.MaxLLMCalls != policy.MaxLLMCalls || llmOptions.MaxToolIterations != policy.MaxToolCalls {
		t.Fatalf("LLMAgent runtime options = %+v", llmOptions)
	}

	sessions := inmemory.NewSessionService()
	defer func() {
		if err := sessions.Close(); err != nil {
			t.Errorf("sessions.Close() error = %v", err)
		}
	}()
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: RunnerInput{
			Tenant: *tenantRoot,
			Agent:  agentInput,
			Model: modelprofile.ModelFactoryInput{
				TenantID: tenantRoot.TenantID, TenantVersion: tenantRoot.Version,
				ProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ProfileVersion: 1,
				ContentDigest: "model-digest", SchemaVersion: modelprofile.SchemaVersionV1,
				Provider: "fake", Model: "deterministic",
			},
		},
		ModelFactory: agentTestModelFactory{},
		Sessions:     sessions,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("runner.Close() error = %v", err)
		}
	}()
	policyRunner, ok := runner.(*policyRunner)
	if !ok {
		t.Fatalf("NewRunner returned %T, want policyRunner", runner)
	}
	runOptions := trpcagent.NewRunOptions(policyRunner.runOptions...)
	if runOptions.MaxRunDuration != time.Duration(policy.ExecutionTimeoutSeconds)*time.Second {
		t.Fatalf("MaxRunDuration = %v, want %v", runOptions.MaxRunDuration, time.Duration(policy.ExecutionTimeoutSeconds)*time.Second)
	}
}

func agentRuntimeFixtureWithPolicy(t *testing.T, tenantID, appID, modelProfileID string, policy appmodel.RuntimePolicy) (*appmodel.App, *appmodel.Revision) {
	t.Helper()
	appRoot, err := appmodel.NewApp(appmodel.CreateInput{TenantID: tenantID, AppKey: "policy-app", DisplayName: "Policy App", Description: "Policy"})
	if err != nil {
		t.Fatal(err)
	}
	// Keep the fixture's stable App identity while replacing only its executable revision.
	appRoot.AppID = appID
	draft, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: tenantID, AppID: appID, Revision: 1,
		Configuration: appmodel.DraftConfiguration{Description: "Policy revision", Instruction: "Answer accurately.", ModelProfileID: modelProfileID, Runtime: policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	published, err := draft.Publish(draft.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	appRoot.Status = appmodel.StatusActive
	appRoot.CurrentRevision = &published.Revision
	appRoot.Version++
	appRoot.UpdatedAt = published.UpdatedAt
	if err := appRoot.Validate(); err != nil {
		t.Fatal(err)
	}
	return appRoot, &published
}

type agentClosingRunner struct {
	err   error
	calls int
}

func (runner *agentClosingRunner) Run(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	return nil, errors.New("unused test runner")
}

func (runner *agentClosingRunner) Close() error {
	runner.calls++
	return runner.err
}

type agentCloseTrackingSession struct {
	session.Service
	err   error
	calls int
}

func (service *agentCloseTrackingSession) Close() error {
	service.calls++
	if service.Service != nil {
		_ = service.Service.Close()
	}
	return service.err
}

type agentTestModelFactory struct{}

func (agentTestModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (trpcmodel.Model, error) {
	return agentTestModel{}, nil
}

type agentTestModel struct{}

func (agentTestModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "agent-test-model"} }

func (agentTestModel) GenerateContent(ctx context.Context, _ *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	responses := make(chan *trpcmodel.Response, 1)
	go func() {
		defer close(responses)
		select {
		case responses <- &trpcmodel.Response{Done: true}:
		case <-ctx.Done():
		}
	}()
	return responses, nil
}

var _ trpcrunner.Runner = (*agentClosingRunner)(nil)
