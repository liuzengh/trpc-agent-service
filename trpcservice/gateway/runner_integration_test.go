package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentrunnerfactory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/runnerfactory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type testRunner struct {
	runFn      func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error)
	closeErr   error
	closeCount atomic.Int32
}

func (runner *testRunner) Run(ctx context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	if runner.runFn != nil {
		return runner.runFn(ctx, userID, sessionID, message, options...)
	}
	events := make(chan *trpcevent.Event)
	close(events)
	return events, nil
}

func (runner *testRunner) Close() error {
	runner.closeCount.Add(1)
	return runner.closeErr
}

func testExecutionPlan(t *testing.T) runtime.ExecutionPlan {
	t.Helper()
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.Resolve(context.Background(), mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID))
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestRunnerRegistryMergesConcurrentConstructionAndReusesCompleteKey(t *testing.T) {
	plan := testExecutionPlan(t)
	var calls atomic.Int32
	var started sync.Once
	startedCh := make(chan struct{})
	release := make(chan struct{})
	runnerValue := &testRunner{}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{
		Factory: func(ctx context.Context, got runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			calls.Add(1)
			started.Do(func() { close(startedCh) })
			select {
			case <-release:
				return runnerValue, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		MaxEntries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()

	const workers = 12
	leases := make(chan *runtimerunner.RunnerLease, workers)
	errorsCh := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			lease, acquireErr := registry.Acquire(context.Background(), plan)
			if acquireErr != nil {
				errorsCh <- acquireErr
				return
			}
			leases <- lease
		}()
	}
	<-startedCh
	close(release)
	wait.Wait()
	close(leases)
	close(errorsCh)
	for acquireErr := range errorsCh {
		t.Fatal(acquireErr)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}
	var first *runtimerunner.RunnerLease
	allLeases := make([]*runtimerunner.RunnerLease, 0, workers)
	for lease := range leases {
		allLeases = append(allLeases, lease)
		if first == nil {
			first = lease
		} else if lease.Runner() != first.Runner() {
			t.Fatal("same CacheKey returned different Runner instances")
		}
	}
	if first == nil {
		t.Fatal("no Runner leases returned")
	}
	for _, lease := range allLeases {
		if err := lease.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRunnerRegistryInvalidationDefersCloseUntilRelease(t *testing.T) {
	plan := testExecutionPlan(t)
	var calls atomic.Int32
	runners := make(chan *testRunner, 2)
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			runnerValue := &testRunner{}
			runners <- runnerValue
			calls.Add(1)
			return runnerValue, nil
		},
		MaxEntries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	oldRunner := <-runners
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	if got := oldRunner.closeCount.Load(); got != 0 {
		t.Fatalf("borrowed Runner close count = %d, want 0", got)
	}
	newLease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	newRunner := <-runners
	if newRunner == oldRunner || calls.Load() != 2 {
		t.Fatal("invalidation reused the old Runner")
	}
	if err := oldLease.Release(); err != nil {
		t.Fatal(err)
	}
	if got := oldRunner.closeCount.Load(); got != 1 {
		t.Fatalf("released invalidated Runner close count = %d, want 1", got)
	}
	if err := newLease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistrySelectiveInvalidationPreservesUnrelatedTenant(t *testing.T) {
	planOne := testExecutionPlan(t)
	planTwo := testExecutionPlan(t)
	var calls atomic.Int32
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		calls.Add(1)
		return &testRunner{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	first, err := registry.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Acquire(context.Background(), planTwo)
	if err != nil {
		t.Fatal(err)
	}
	secondRunner := second.Runner()
	if err := registry.InvalidateTenant(planOne.Tenant().TenantID); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
	reused, err := registry.Acquire(context.Background(), planTwo)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reused.Release() }()
	if reused.Runner() != secondRunner || calls.Load() != 2 {
		t.Fatalf("unrelated tenant entry was evicted: runner=%p want=%p calls=%d", reused.Runner(), secondRunner, calls.Load())
	}
}

func TestRunnerRegistrySeparatesBackendProfileVersions(t *testing.T) {
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
	firstPlan, err := resolver.Resolve(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	updated, _, err := fixture.backends.UpdateConfiguration(context.Background(), backend.UpdateConfigurationInput{
		TenantID: fixture.backend.TenantID, ProfileID: fixture.backend.ProfileID, ExpectedVersion: fixture.backend.Version,
		DisplayName: fixture.backend.DisplayName, Description: fixture.backend.Description, SchemaVersion: fixture.backend.SchemaVersion,
		Bindings: fixture.backend.Bindings, Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "registry", Reason: "version separation", CorrelationID: "registry-version"},
	})
	if err != nil {
		t.Fatal(err)
	}
	secondPlan, err := resolver.Resolve(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	firstKey, err := firstPlan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	secondKey, err := secondPlan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if firstKey.BackendProfileID != secondKey.BackendProfileID || firstKey.BackendProfileVersion == secondKey.BackendProfileVersion || secondKey.BackendProfileVersion != updated.Version {
		t.Fatalf("backend cache keys = %+v, %+v; updated=%+v", firstKey, secondKey, updated)
	}
	var calls atomic.Int32
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		calls.Add(1)
		return &testRunner{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	first, err := registry.Acquire(context.Background(), firstPlan)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Release() }()
	second, err := registry.Acquire(context.Background(), secondPlan)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Release() }()
	if first.Runner() == second.Runner() || calls.Load() != 2 {
		t.Fatalf("backend profile versions shared a Runner: first=%p second=%p calls=%d", first.Runner(), second.Runner(), calls.Load())
	}
}

func TestRunnerRegistryCapacityEvictsIdleButNotBorrowedEntries(t *testing.T) {
	planOne := testExecutionPlan(t)
	planTwo := testExecutionPlan(t)
	var runnersMu sync.Mutex
	var runners []*testRunner
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			runnerValue := &testRunner{}
			runnersMu.Lock()
			runners = append(runners, runnerValue)
			runnersMu.Unlock()
			return runnerValue, nil
		},
		MaxEntries: 1,
		IdleTTL:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	leaseOne, err := registry.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(context.Background(), planTwo); !errors.Is(err, runtimerunner.ErrRunnerCapacity) {
		t.Fatalf("borrowed capacity error = %v", err)
	}
	if err := leaseOne.Release(); err != nil {
		t.Fatal(err)
	}
	leaseTwo, err := registry.Acquire(context.Background(), planTwo)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(runners); got != 2 {
		t.Fatalf("factory calls = %d, want 2", got)
	}
	if got := runners[0].closeCount.Load(); got != 1 {
		t.Fatalf("evicted idle Runner close count = %d, want 1", got)
	}
	if err := leaseTwo.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryDoesNotCacheFactoryFailures(t *testing.T) {
	plan := testExecutionPlan(t)
	var calls atomic.Int32
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			if calls.Add(1) == 1 {
				return nil, errors.New("provider secret must not escape")
			}
			return &testRunner{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	if _, err := registry.Acquire(context.Background(), plan); !errors.Is(err, runtimerunner.ErrRunnerUnavailable) {
		t.Fatalf("first factory error = %v", err)
	}
	lease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("factory calls = %d, want 2", calls.Load())
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryCloseWaitsForBorrowedLease(t *testing.T) {
	plan := testExecutionPlan(t)
	runnerValue := &testRunner{}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{
		Factory:      func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil },
		CloseTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- registry.Close() }()
	time.Sleep(20 * time.Millisecond)
	if got := runnerValue.closeCount.Load(); got != 0 {
		t.Fatalf("Runner closed while lease borrowed: %d", got)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if got := runnerValue.closeCount.Load(); got != 1 {
		t.Fatalf("Runner close count = %d, want 1", got)
	}
}

func TestRunnerRegistryConfigurationAndBoundaryErrors(t *testing.T) {
	factory := func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return &testRunner{}, nil }
	for name, config := range map[string]runtimerunner.RunnerRegistryConfig{
		"missing factory":        {},
		"negative entries":       {Factory: factory, MaxEntries: -1},
		"negative idle ttl":      {Factory: factory, IdleTTL: -time.Second},
		"negative close timeout": {Factory: factory, CloseTimeout: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := runtimerunner.NewRunnerRegistry(config); !errors.Is(err, runtimerunner.ErrInvalid) {
				t.Fatalf("runtimerunner.NewRunnerRegistry() error = %v", err)
			}
		})
	}

	plan := testExecutionPlan(t)
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: factory})
	if err != nil {
		t.Fatal(err)
	}
	if !registry.Ready() {
		t.Fatal("new registry is not ready")
	}
	var nilContext context.Context
	if _, err := registry.Acquire(nilContext, plan); !errors.Is(err, runtimerunner.ErrInvalid) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Acquire(canceled, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	if _, err := registry.Acquire(context.Background(), runtime.ExecutionPlan{}); !errors.Is(err, runtimerunner.ErrInvalid) {
		t.Fatalf("invalid plan error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if registry.Ready() {
		t.Fatal("closed registry is ready")
	}
	if _, err := registry.Acquire(context.Background(), plan); !errors.Is(err, runtimerunner.ErrClosed) {
		t.Fatalf("closed registry acquire error = %v", err)
	}
	if err := registry.Invalidate(runtime.CacheKey{}); !errors.Is(err, runtimerunner.ErrClosed) {
		t.Fatalf("closed registry invalidation error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	var nilRegistry *runtimerunner.RunnerRegistry
	if nilRegistry.Ready() {
		t.Fatal("nil registry is ready")
	}
	if _, err := nilRegistry.Acquire(context.Background(), plan); !errors.Is(err, runtimerunner.ErrNotReady) {
		t.Fatalf("nil registry acquire error = %v", err)
	}
	if err := nilRegistry.Invalidate(runtime.CacheKey{}); !errors.Is(err, runtimerunner.ErrNotReady) {
		t.Fatalf("nil registry invalidation error = %v", err)
	}
}

func TestRunnerRegistryInvalidationWrappersAndPredicateBoundaries(t *testing.T) {
	plan := testExecutionPlan(t)
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return &testRunner{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	if err := registry.InvalidateMatching(nil); !errors.Is(err, runtimerunner.ErrInvalid) {
		t.Fatalf("nil predicate = %v", err)
	}
	lease, err := registry.Acquire(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.InvalidateTenant("other-tenant"); err != nil {
		t.Fatal(err)
	}
	if err := registry.InvalidateApp("other-tenant", "other-app"); err != nil {
		t.Fatal(err)
	}
	if err := registry.InvalidateModelProfile("other-tenant", "other-model"); err != nil {
		t.Fatal(err)
	}
	if err := registry.InvalidateBackendProfile("other-tenant", "other-backend"); err != nil {
		t.Fatal(err)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.InvalidateApp(key.TenantID, key.AppID); err != nil {
		t.Fatal(err)
	}
	if err := registry.InvalidateModelProfile(key.TenantID, key.ModelProfileID); err != nil {
		t.Fatal(err)
	}
	if err := registry.InvalidateBackendProfile(key.TenantID, key.BackendProfileID); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	var nilRegistry *runtimerunner.RunnerRegistry
	if err := nilRegistry.InvalidateMatching(func(runtime.CacheKey) bool { return true }); !errors.Is(err, runtimerunner.ErrNotReady) {
		t.Fatalf("nil registry matching = %v", err)
	}
	var nilLease *runtimerunner.RunnerLease
	if nilLease.Runner() != nil || nilLease.Release() != nil {
		t.Fatal("nil lease boundary was not harmless")
	}
}

type stage2ModelFactory struct{}

func (stage2ModelFactory) New(context.Context, model.ModelFactoryInput, model.SecretValue) (trpcmodel.Model, error) {
	return nil, nil
}

func TestRuntimeRunnerRegistryWiresBorrowedDependencies(t *testing.T) {
	if _, err := agentrunnerfactory.NewRuntimeRunnerRegistry(agentrunnerfactory.Config{}); !errors.Is(err, runtimerunner.ErrInvalid) {
		t.Fatalf("missing runtime dependency error = %v", err)
	}
	plan := testExecutionPlan(t)
	sessions := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = sessions.Close() })
	registry, err := agentrunnerfactory.NewRuntimeRunnerRegistry(agentrunnerfactory.Config{
		ModelFactory: stage2ModelFactory{},
		Sessions:     sessions,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !registry.Ready() {
		t.Fatal("runtime registry is not ready")
	}
	if _, err := registry.Acquire(context.Background(), plan); !errors.Is(err, runtimerunner.ErrRunnerUnavailable) {
		t.Fatalf("agent-backed runner construction error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	storageRegistry, err := agentrunnerfactory.NewRuntimeRunnerRegistry(agentrunnerfactory.Config{
		ModelFactory: stage2ModelFactory{},
		StorageFactory: storagefactory.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
			return nil, storagefactory.ErrStorageFactory
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storageRegistry.Acquire(context.Background(), plan); !errors.Is(err, runtimerunner.ErrRunnerUnavailable) {
		t.Fatalf("storage-backed runner construction error = %v", err)
	}
	if err := storageRegistry.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryFactoryAndPendingCancellationEdges(t *testing.T) {
	plan := testExecutionPlan(t)
	for name, factory := range map[string]runtimerunner.RunnerFactory{
		"nil runner": func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			return nil, nil
		},
		"context failure": func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			return nil, context.DeadlineExceeded
		},
		"runner with failure": func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			return &testRunner{}, errors.New("provider detail")
		},
	} {
		t.Run(name, func(t *testing.T) {
			registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: factory})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = registry.Close() }()
			_, err = registry.Acquire(context.Background(), plan)
			switch name {
			case "context failure":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("factory context error = %v", err)
				}
			default:
				if !errors.Is(err, runtimerunner.ErrRunnerUnavailable) {
					t.Fatalf("factory error = %v", err)
				}
			}
		})
	}

	started := make(chan struct{})
	release := make(chan struct{})
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{
		Factory: func(ctx context.Context, _ runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			close(started)
			select {
			case <-release:
				return &testRunner{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	first := make(chan *runtimerunner.RunnerLease, 1)
	go func() {
		lease, acquireErr := registry.Acquire(context.Background(), plan)
		if acquireErr != nil {
			t.Errorf("first acquire error = %v", acquireErr)
			return
		}
		first <- lease
	}()
	<-started
	canceled, cancel := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() {
		_, acquireErr := registry.Acquire(canceled, plan)
		second <- acquireErr
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatalf("pending canceled acquire error = %v", err)
	}
	close(release)
	lease := <-first
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryInvalidatesPendingBuildAndRetries(t *testing.T) {
	plan := testExecutionPlan(t)
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(ctx context.Context, _ runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &testRunner{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	result := make(chan *runtimerunner.RunnerLease, 1)
	go func() {
		lease, acquireErr := registry.Acquire(context.Background(), plan)
		if acquireErr != nil {
			t.Errorf("Acquire() = %v", acquireErr)
			return
		}
		result <- lease
	}()
	<-started
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	close(release)
	lease := <-result
	if calls.Load() != 2 {
		t.Fatalf("invalidated pending build calls = %d, want 2", calls.Load())
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRegistryIdleCloseErrorAndTimeoutEdges(t *testing.T) {
	planOne := testExecutionPlan(t)
	planTwo := testExecutionPlan(t)
	now := time.Unix(100, 0)
	var runners []*testRunner
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{
		Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			runner := &testRunner{}
			runners = append(runners, runner)
			return runner, nil
		},
		MaxEntries: 2, IdleTTL: time.Minute, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	otherLease, err := registry.Acquire(context.Background(), planTwo)
	if err != nil {
		t.Fatal(err)
	}
	if got := runners[0].closeCount.Load(); got != 1 {
		t.Fatalf("idle Runner close count = %d, want 1", got)
	}
	if err := otherLease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	closeFail := &testRunner{closeErr: errors.New("provider close detail")}
	failing, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return closeFail, nil }})
	if err != nil {
		t.Fatal(err)
	}
	failingLease, err := failing.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	if err := failingLease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := failing.Close(); !errors.Is(err, runtimerunner.ErrRunnerClose) {
		t.Fatalf("close failure = %v", err)
	}
	if err := failing.Close(); !errors.Is(err, runtimerunner.ErrRunnerClose) {
		t.Fatalf("repeated close failure = %v", err)
	}

	borrowed := &testRunner{}
	timeoutRegistry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{
		Factory:      func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return borrowed, nil },
		CloseTimeout: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	borrowedLease, err := timeoutRegistry.Acquire(context.Background(), planOne)
	if err != nil {
		t.Fatal(err)
	}
	if err := timeoutRegistry.Close(); !errors.Is(err, runtimerunner.ErrRegistryCloseTimeout) {
		t.Fatalf("close timeout = %v", err)
	}
	if err := borrowedLease.Release(); err != nil {
		t.Fatal(err)
	}
	if got := borrowed.closeCount.Load(); got != 1 {
		t.Fatalf("eventual close count = %d, want 1", got)
	}
}
