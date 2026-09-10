package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type registryTestRunner struct {
	closeCount atomic.Int32
}

func (runner *registryTestRunner) Run(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	events := make(chan *trpcevent.Event)
	close(events)
	return events, nil
}

func (runner *registryTestRunner) Close() error {
	runner.closeCount.Add(1)
	return nil
}

func TestRunnerRegistryOwnsRunnerLifecycle(t *testing.T) {
	plan := newRegistryTestPlan(t)
	runnerValue := &registryTestRunner{}
	var factoryCalls atomic.Int32
	registry, err := NewRunnerRegistry(RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (Runner, error) {
			factoryCalls.Add(1)
			return runnerValue, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !registry.Ready() {
		t.Fatal("new registry is not ready")
	}

	first, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if first.Runner() != runnerValue || second.Runner() != runnerValue || factoryCalls.Load() != 1 {
		t.Fatalf("runner reuse = first=%p second=%p calls=%d", first.Runner(), second.Runner(), factoryCalls.Load())
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	if runnerValue.closeCount.Load() != 0 {
		t.Fatal("borrowed Runner closed before its last lease released")
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
	if runnerValue.closeCount.Load() != 1 {
		t.Fatalf("Runner close count = %d, want 1", runnerValue.closeCount.Load())
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if registry.Ready() {
		t.Fatal("closed registry is ready")
	}
	if _, err := registry.Acquire(context.Background(), plan); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed registry acquire error = %v", err)
	}
}

func TestRunnerRegistryBoundaryErrors(t *testing.T) {
	plan := newRegistryTestPlan(t)
	if _, err := NewRunnerRegistry(RunnerRegistryConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing factory error = %v", err)
	}
	var nilRegistry *RunnerRegistry
	if _, err := nilRegistry.Acquire(context.Background(), plan); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil registry acquire error = %v", err)
	}
	if err := nilRegistry.Invalidate(runtime.CacheKey{}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil registry invalidation error = %v", err)
	}
	var nilLease *RunnerLease
	if nilLease.Runner() != nil || nilLease.Release() != nil {
		t.Fatal("nil lease boundary was not harmless")
	}
	if (&runnerEntry{}).close() != nil {
		t.Fatal("empty Runner entry close failed")
	}
}

func newRegistryTestPlan(t *testing.T) runtime.ExecutionPlan {
	t.Helper()
	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: model.FieldForbidden, SecretRefPolicy: model.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantValue, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "runner-test-tenant", DisplayName: "Runner Test Tenant", AuditRetentionDays: 30,
		LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	appRoot, err := appmodel.NewApp(appmodel.CreateInput{TenantID: tenantValue.TenantID, AppKey: "runner-test-app", DisplayName: "Runner Test App"})
	if err != nil {
		t.Fatal(err)
	}
	modelProfile, err := model.NewProfile(model.CreateInput{
		TenantID: tenantValue.TenantID, ProfileKey: "runner-test-model", DisplayName: "Runner Test Model",
		Configuration: model.Configuration{Provider: "fake", Model: "deterministic"},
	}, modelCatalog)
	if err != nil {
		t.Fatal(err)
	}
	backendProfile, err := backend.NewProfile(backend.CreateInput{
		TenantID: tenantValue.TenantID, ProfileKey: "runner-test-backend", DisplayName: "Runner Test Backend",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "runner-test"}}},
	}, backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	draft, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: tenantValue.TenantID, AppID: appRoot.AppID, Revision: 1,
		Configuration: appmodel.DraftConfiguration{Description: "runner test", Instruction: "Answer clearly.", ModelProfileID: modelProfile.ProfileID, Runtime: appmodel.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	published, err := draft.Publish(time.Now().UTC().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	appRoot.Status = appmodel.StatusActive
	appRoot.CurrentRevision = &published.Revision
	appID, backendID := appRoot.AppID, backendProfile.ProfileID
	tenantValue.DefaultAgentAppID = &appID
	tenantValue.DefaultBackendProfileID = &backendID
	snapshot, err := tenant.NewConfigurationSnapshot(tenantValue)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := runtime.NewExecutionPlanFromInput(runtime.ExecutionPlanInput{
		TenantSnapshot: snapshot, AppRoot: appRoot, Revision: &published,
		ModelProfile: modelProfile, ModelCatalog: modelCatalog,
		BackendProfile: backendProfile, BackendCatalog: backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
