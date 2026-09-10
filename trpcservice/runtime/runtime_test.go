package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"trpc.group/trpc-go/trpc-agent-go/model"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/noop"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
	frameworktodo "trpc.group/trpc-go/trpc-agent-go/tool/todo"
)

func TestBudgetCallbacksStopLaterModelCalls(t *testing.T) {
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxTokensPerExecution: 3})
	if callbacks == nil {
		t.Fatal("budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("first model call was rejected: %v", err)
	}
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{Usage: &model.Usage{PromptTokens: 2, CompletionTokens: 1, TotalTokens: 3}},
	}); err != nil {
		t.Fatalf("record usage: %v", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("later model call error = %v, want ErrBudgetExceeded", err)
	}
}

func TestBudgetCallbacksReserveConcurrentModelCall(t *testing.T) {
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxTokensPerExecution: 3})
	if callbacks == nil {
		t.Fatal("budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("first model call was rejected: %v", err)
	}

	secondCall := make(chan error, 1)
	go func() {
		_, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{})
		secondCall <- err
	}()
	if err := <-secondCall; !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("concurrent model call error = %v, want ErrBudgetExceeded", err)
	}

	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{Usage: &model.Usage{TotalTokens: 1}},
	}); err != nil {
		t.Fatalf("record usage: %v", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("model call after reservation release was rejected: %v", err)
	}
}

func TestBudgetCallbacksKeepStreamingReservationUntilTerminalUsage(t *testing.T) {
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxTokensPerExecution: 3})
	if callbacks == nil {
		t.Fatal("budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("streaming model call was rejected: %v", err)
	}
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{IsPartial: true},
	}); err != nil {
		t.Fatalf("partial model response was rejected: %v", err)
	}
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{
			Done:  true,
			Usage: &model.Usage{TotalTokens: 2},
		},
	}); err != nil {
		t.Fatalf("terminal model usage was rejected: %v", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("remaining model budget was rejected: %v", err)
	}
}

func TestBudgetCallbacksFailClosedWithoutTerminalUsage(t *testing.T) {
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxTokensPerExecution: 3})
	if callbacks == nil {
		t.Fatal("budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("model call was rejected: %v", err)
	}
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{Done: true},
	}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("unmetered model response error = %v, want ErrBudgetExceeded", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("follow-up model call error = %v, want ErrBudgetExceeded", err)
	}
}

func TestBudgetCallbacksPreserveModelErrorWithoutUsage(t *testing.T) {
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxTokensPerExecution: 3})
	if callbacks == nil {
		t.Fatal("budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("model call was rejected: %v", err)
	}
	providerErr := errors.New("provider timeout")
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{Error: providerErr}); err != nil {
		t.Fatalf("after-model callback replaced model error with: %v", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("follow-up model call error = %v, want ErrBudgetExceeded", err)
	}
}

func TestBudgetCallbacksPreserveResponseErrorWithoutUsage(t *testing.T) {
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxTokensPerExecution: 3})
	if callbacks == nil {
		t.Fatal("budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("model call was rejected: %v", err)
	}
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{Error: &model.ResponseError{Message: "provider rejected request"}},
	}); err != nil {
		t.Fatalf("response error was replaced by budget error: %v", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("follow-up model call error = %v, want ErrBudgetExceeded", err)
	}
}

func TestBudgetCallbacksEnforceCostLimit(t *testing.T) {
	estimate := func(inputTokens, outputTokens int) (float64, bool) {
		return float64(inputTokens + outputTokens), true
	}
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxCostPerExecution: 3}, estimate)
	if callbacks == nil {
		t.Fatal("cost budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("first model call was rejected: %v", err)
	}
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{Usage: &model.Usage{PromptTokens: 2, CompletionTokens: 1}},
	}); err != nil {
		t.Fatalf("cost usage was rejected at the limit: %v", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("later model call error = %v, want ErrBudgetExceeded", err)
	}
}

func TestBudgetCallbacksFailClosedForUnknownCost(t *testing.T) {
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxCostPerExecution: 1})
	if callbacks == nil {
		t.Fatal("cost budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("model call was rejected: %v", err)
	}
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{Usage: &model.Usage{PromptTokens: 1, CompletionTokens: 1}},
	}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("unknown cost error = %v, want ErrBudgetExceeded", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("follow-up model call error = %v, want ErrBudgetExceeded", err)
	}
}

func TestBudgetCallbacksFailClosedWhenCostUsageLacksTokenBreakdown(t *testing.T) {
	estimate := func(inputTokens, outputTokens int) (float64, bool) {
		return float64(inputTokens + outputTokens), true
	}
	callbacks := newBudgetCallbacks(tenant.BudgetPolicy{MaxCostPerExecution: 1}, estimate)
	if callbacks == nil {
		t.Fatal("cost budget callbacks were not created")
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); err != nil {
		t.Fatalf("model call was rejected: %v", err)
	}
	if _, err := callbacks.RunAfterModel(context.Background(), &model.AfterModelArgs{
		Response: &model.Response{Usage: &model.Usage{TotalTokens: 2}},
	}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("incomplete cost usage error = %v, want ErrBudgetExceeded", err)
	}
	if _, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{}); !errors.Is(err, tenant.ErrBudgetExceeded) {
		t.Fatalf("follow-up model call error = %v, want ErrBudgetExceeded", err)
	}
}

func TestDefaultEndpointPolicyRejectsNonPublicAddresses(t *testing.T) {
	policy := DefaultEndpointPolicy{}
	for _, endpoint := range []string{
		"https://10.0.0.1/v1",
		"https://192.168.1.1/v1",
		"https://[fd00::1]/v1",
		"https://[::ffff:10.0.0.1]/v1",
	} {
		if _, err := policy.ResolveModelBaseURL(context.Background(), worker.Execution{}, endpoint); err == nil {
			t.Errorf("private endpoint %q was accepted", endpoint)
		}
	}
}

func TestDefaultEndpointPolicyAllowsPublicLiteral(t *testing.T) {
	endpoint := "https://8.8.8.8/v1"
	resolved, err := (DefaultEndpointPolicy{}).ResolveModelBaseURL(context.Background(), worker.Execution{}, endpoint)
	if err != nil {
		t.Fatalf("public endpoint rejected: %v", err)
	}
	if resolved != endpoint {
		t.Fatalf("resolved endpoint = %q, want %q", resolved, endpoint)
	}
}

func TestModelObservabilityCallbacksFinishOnTerminalResponse(t *testing.T) {
	metricsRecorder, err := platformmetrics.New(metricnoop.NewMeterProvider(), platformmetrics.PricingCatalog{})
	if err != nil {
		t.Fatalf("new metrics recorder: %v", err)
	}
	exec := worker.Execution{
		RequestID: "request-1",
		Tenant: tenant.RuntimeContext{
			TenantID:      "tenant-a",
			AppID:         "app-a",
			ConfigVersion: "v1",
			Channel:       "openai",
		},
		Config: tenant.AppConfig{Model: tenant.ModelConfig{
			Provider: "openai",
			Model:    "gpt-test",
		}},
	}
	callbacks := addModelObservabilityCallbacks(nil, exec, metricsRecorder)
	before, err := callbacks.RunBeforeModel(context.Background(), &model.BeforeModelArgs{})
	if err != nil || before == nil || before.Context == nil {
		t.Fatalf("before model callback result = %#v, error = %v", before, err)
	}
	state, _ := before.Context.Value(modelSpanStateKey{}).(*modelSpanState)
	if state == nil {
		t.Fatal("model span state was not attached to callback context")
	}
	if _, err := callbacks.RunAfterModel(before.Context, &model.AfterModelArgs{
		Response: &model.Response{IsPartial: true},
	}); err != nil {
		t.Fatalf("partial model response: %v", err)
	}
	state.mu.Lock()
	finishedAfterPartial := state.finished
	state.mu.Unlock()
	if finishedAfterPartial {
		t.Fatal("model span finished before terminal response")
	}
	if _, err := callbacks.RunAfterModel(before.Context, &model.AfterModelArgs{
		Response: &model.Response{
			Done:  true,
			Usage: &model.Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3},
		},
	}); err != nil {
		t.Fatalf("terminal model response: %v", err)
	}
	state.mu.Lock()
	finishedAfterTerminal := state.finished
	state.mu.Unlock()
	if !finishedAfterTerminal {
		t.Fatal("model span did not finish on terminal response")
	}

	withoutMetrics := addModelObservabilityCallbacks(nil, exec, nil)
	before, err = withoutMetrics.RunBeforeModel(context.Background(), &model.BeforeModelArgs{})
	if err != nil || before == nil || before.Context == nil {
		t.Fatalf("before model callback without metrics = %#v, error = %v", before, err)
	}
	if _, err := withoutMetrics.RunAfterModel(before.Context, &model.AfterModelArgs{
		Response: &model.Response{Usage: &model.Usage{TotalTokens: 1}},
	}); err != nil {
		t.Fatalf("terminal model response without metrics: %v", err)
	}
	state, _ = before.Context.Value(modelSpanStateKey{}).(*modelSpanState)
	state.mu.Lock()
	finishedWithoutMetrics := state.finished
	state.mu.Unlock()
	if !finishedWithoutMetrics {
		t.Fatal("model span did not finish when metrics were disabled")
	}
}

func TestVisibleToolsFiltersBeforeAgentConstruction(t *testing.T) {
	tools, err := visibleTools(tenant.ToolPolicy{VisibleTools: []string{"safe"}}, []frameworktool.Tool{
		testTool{name: "safe"}, testTool{name: "hidden"},
	})
	if err != nil {
		t.Fatalf("filter visible tools: %v", err)
	}
	if len(tools) != 1 || tools[0].Declaration().Name != "safe" {
		t.Fatalf("visible tools = %#v", tools)
	}
}

func TestVisibleToolsRejectsConfiguredUnavailableTool(t *testing.T) {
	_, err := visibleTools(tenant.ToolPolicy{VisibleTools: []string{"missing"}}, []frameworktool.Tool{
		testTool{name: "available"},
	})
	if err == nil {
		t.Fatal("visible tools succeeded with unavailable configured tool")
	}
}

func TestRuntimeBuildsFreshRunnerAndConfiguredTool(t *testing.T) {
	models, err := NewOpenAIModelResolver(testSecrets{}, nil)
	if err != nil {
		t.Fatalf("new model resolver: %v", err)
	}
	sessions, err := platformsession.NewRouter(testSessionProvider{}, testSessionProvider{})
	if err != nil {
		t.Fatalf("new session router: %v", err)
	}
	runtime, err := NewRuntime(models, sessions, nil, nil, nil, NewToolCatalog())
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	exec := testExecution()
	exec.Config.Tools = tenant.ToolPolicy{VisibleTools: []string{frameworktodo.DefaultToolName}}
	first, err := runtime.BuildRunner(context.Background(), exec)
	if err != nil {
		t.Fatalf("resolve first runner: %v", err)
	}
	second, err := runtime.BuildRunner(context.Background(), exec)
	if err != nil {
		t.Fatalf("resolve second runner: %v", err)
	}
	defer first.Close()
	defer second.Close()
	if first == second {
		t.Fatal("executions reused a runner")
	}
}

type testTool struct{ name string }

func (t testTool) Declaration() *frameworktool.Declaration {
	return &frameworktool.Declaration{Name: t.name}
}

type testSecrets struct{}

func (testSecrets) ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error) {
	return "test-model-key", nil
}

type testSessionProvider struct{}

func (testSessionProvider) ResolveSession(context.Context, worker.Execution) (frameworksession.Service, error) {
	return noop.NewService(), nil
}

func (testSessionProvider) Close() error { return nil }

func testExecution() worker.Execution {
	return worker.Execution{
		RequestID: "request-1",
		Tenant: tenant.RuntimeContext{
			TenantID:           "tenant-a",
			AppID:              "app-a",
			ConfigVersion:      "v1",
			SessionPrincipalID: "user-a",
			SessionID:          "session-a",
			UserID:             "user-a",
		},
		Config: tenant.AppConfig{
			TenantID: "tenant-a",
			AppID:    "app-a",
			Version:  "v1",
			Model: tenant.ModelConfig{
				Provider:  "openai",
				Model:     "gpt-test",
				APIKeyRef: tenant.SecretRef{Name: "model-key"},
			},
			BackendConfig: tenant.BackendConfig{
				Session: tenant.BackendRef{Kind: tenant.BackendSQL, Provider: "postgres", Name: "sessions"},
			},
		},
		Message: tenantMessage("hello"),
	}
}

func tenantMessage(text string) gateway.Message {
	return gateway.Message{Text: text}
}
