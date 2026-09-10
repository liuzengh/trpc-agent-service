package execution

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	serviceagent "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type executionTestRunner struct {
	mu         sync.Mutex
	runFn      func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error)
	userID     string
	sessionID  string
	requestID  string
	closeCount atomic.Int32
}

type executionStagedContext struct {
	context.Context
	done     chan struct{}
	errAfter int32
	calls    atomic.Int32
	once     sync.Once
}

func (ctx *executionStagedContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *executionStagedContext) Err() error {
	if ctx.calls.Add(1) > ctx.errAfter {
		ctx.once.Do(func() { close(ctx.done) })
		return context.Canceled
	}
	return nil
}

type executionRegistryStub struct {
	ready      bool
	acquireErr error
	lease      *runtimerunner.RunnerLease
}

func (stub *executionRegistryStub) Ready() bool {
	return stub != nil && stub.ready
}

func (stub *executionRegistryStub) Acquire(context.Context, runtime.ExecutionPlan) (*runtimerunner.RunnerLease, error) {
	if stub.acquireErr != nil {
		return nil, stub.acquireErr
	}
	return stub.lease, nil
}

type executionCancelAfterCheckContext struct {
	context.Context
	done    chan struct{}
	checked chan struct{}
	once    sync.Once
}

func (ctx *executionCancelAfterCheckContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *executionCancelAfterCheckContext) Err() error {
	ctx.once.Do(func() { close(ctx.checked) })
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func (runner *executionTestRunner) Run(ctx context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	settings := trpcagent.RunOptions{}
	for _, option := range options {
		option(&settings)
	}
	runner.mu.Lock()
	runner.userID, runner.sessionID, runner.requestID = userID, sessionID, settings.RequestID
	runner.mu.Unlock()
	if runner.runFn != nil {
		return runner.runFn(ctx, userID, sessionID, message, options...)
	}
	events := make(chan *trpcevent.Event)
	close(events)
	return events, nil
}

func (runner *executionTestRunner) Close() error {
	runner.closeCount.Add(1)
	return nil
}

func TestCoordinatorMapsEventsAndReleasesLease(t *testing.T) {
	plan := newExecutionTestPlan(t)
	runnerValue := &executionTestRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 2)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "hello"}}}}}
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	registry, coordinator, identity := newExecutionTestCoordinator(t, plan, runnerValue)
	stream, err := coordinator.Execute(context.Background(), Request{
		Plan: plan, Identity: identity, Message: trpcmodel.Message{Content: "question"}, RequestID: "request-1", TraceID: "trace-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectExecutionEvents(stream)
	if len(events) != 2 || events[0].Type != EventMessage || events[0].Text != "hello" || !events[1].Done {
		t.Fatalf("events=%+v", events)
	}
	if events[0].RequestID != "request-1" || events[0].TraceID != "trace-1" || events[1].RequestID != "request-1" {
		t.Fatalf("correlation=%+v", events)
	}
	runnerValue.mu.Lock()
	if runnerValue.userID != identity.UserID || runnerValue.sessionID != identity.SessionID || runnerValue.requestID != "request-1" {
		runnerValue.mu.Unlock()
		t.Fatalf("runner input user=%q session=%q request=%q", runnerValue.userID, runnerValue.sessionID, runnerValue.requestID)
	}
	runnerValue.mu.Unlock()

	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	if runnerValue.closeCount.Load() != 1 {
		t.Fatalf("runner close count=%d, want 1", runnerValue.closeCount.Load())
	}
}

func TestCoordinatorRedactsRunnerError(t *testing.T) {
	plan := newExecutionTestPlan(t)
	runnerValue := &executionTestRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: "provider secret"}}}
		close(events)
		return events, nil
	}}
	registry, coordinator, identity := newExecutionTestCoordinator(t, plan, runnerValue)
	stream, err := coordinator.Execute(context.Background(), Request{Plan: plan, Identity: identity, Message: trpcmodel.Message{Content: "question"}, RequestID: "request-error"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectExecutionEvents(stream)
	if len(events) != 2 || events[0].Type != EventError || !errors.Is(events[0].Err, ErrExecution) || events[1].Status != "error" {
		t.Fatalf("events=%+v", events)
	}
	if strings.Contains(events[0].Err.Error(), "secret") {
		t.Fatal("provider error escaped execution boundary")
	}

	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	if runnerValue.closeCount.Load() != 1 {
		t.Fatalf("runner close count=%d, want 1", runnerValue.closeCount.Load())
	}
}

func TestCoordinatorCancellationDrainsAndReleasesLease(t *testing.T) {
	plan := newExecutionTestPlan(t)
	runnerValue := &executionTestRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		return make(chan *trpcevent.Event), nil
	}}
	registry, coordinator, identity := newExecutionTestCoordinatorWithConfig(t, plan, runnerValue, Config{DrainTimeout: time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := coordinator.Execute(ctx, Request{Plan: plan, Identity: identity, Message: trpcmodel.Message{Content: "question"}, RequestID: "request-cancel"})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	events := collectExecutionEvents(stream)
	if len(events) != 2 || events[0].Type != EventError || !errors.Is(events[0].Err, context.Canceled) || events[1].Status != "canceled" {
		t.Fatalf("events=%+v", events)
	}

	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	if runnerValue.closeCount.Load() != 1 {
		t.Fatalf("runner close count=%d, want 1", runnerValue.closeCount.Load())
	}
}

func TestCoordinatorCancellationWinsReadyTerminalEvent(t *testing.T) {
	plan := newExecutionTestPlan(t)
	runnerValue := &executionTestRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		return events, nil
	}}
	registry, coordinator, identity := newExecutionTestCoordinatorWithConfig(t, plan, runnerValue, Config{DrainTimeout: time.Millisecond})
	ctx := &executionStagedContext{Context: context.Background(), done: make(chan struct{}), errAfter: 3}
	stream, err := coordinator.Execute(ctx, Request{Plan: plan, Identity: identity, Message: trpcmodel.Message{Content: "question"}, RequestID: "request-late-event"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectExecutionEvents(stream)
	if len(events) != 2 || !errors.Is(events[0].Err, context.Canceled) || events[1].Status != "canceled" {
		t.Fatalf("events=%+v", events)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	if runnerValue.closeCount.Load() != 1 {
		t.Fatalf("runner close count=%d, want 1", runnerValue.closeCount.Load())
	}
}

func TestCoordinatorCompletesClosedRunnerStream(t *testing.T) {
	plan := newExecutionTestPlan(t)
	runnerValue := &executionTestRunner{}
	registry, coordinator, identity := newExecutionTestCoordinator(t, plan, runnerValue)
	stream, err := coordinator.Execute(context.Background(), Request{Plan: plan, Identity: identity, Message: trpcmodel.Message{Content: "question"}, RequestID: "request-closed"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectExecutionEvents(stream)
	if len(events) != 1 || events[0].Type != EventDone || events[0].Status != "complete" || !events[0].Done {
		t.Fatalf("events=%+v", events)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
	if runnerValue.closeCount.Load() != 1 {
		t.Fatalf("runner close count=%d, want 1", runnerValue.closeCount.Load())
	}
}

func TestCoordinatorDeadlineCancellation(t *testing.T) {
	plan := newExecutionTestPlan(t)
	runnerValue := &executionTestRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		return make(chan *trpcevent.Event), nil
	}}
	registry, coordinator, identity := newExecutionTestCoordinatorWithConfig(t, plan, runnerValue, Config{DrainTimeout: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	stream, err := coordinator.Execute(ctx, Request{Plan: plan, Identity: identity, Message: trpcmodel.Message{Content: "question"}, RequestID: "request-deadline"})
	if err != nil {
		t.Fatal(err)
	}
	events := collectExecutionEvents(stream)
	if len(events) != 2 || !errors.Is(events[0].Err, context.DeadlineExceeded) || events[1].Status != "deadline_exceeded" {
		t.Fatalf("events=%+v", events)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Invalidate(key); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorRunBoundaryErrorsReleaseLease(t *testing.T) {
	for name, runFn := range map[string]func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error){
		"runner error": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return nil, errors.New("provider detail")
		},
		"runner canceled": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return nil, context.Canceled
		},
		"nil event stream": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return nil, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan := newExecutionTestPlan(t)
			runnerValue := &executionTestRunner{runFn: runFn}
			registry, coordinator, identity := newExecutionTestCoordinator(t, plan, runnerValue)
			stream, err := coordinator.Execute(context.Background(), Request{Plan: plan, Identity: identity, Message: trpcmodel.Message{Content: "question"}, RequestID: "request-error"})
			if stream != nil {
				t.Fatal("failed Runner invocation returned a stream")
			}
			switch name {
			case "runner error", "nil event stream":
				if !errors.Is(err, ErrExecution) {
					t.Fatalf("error=%v", err)
				}
			case "runner canceled":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error=%v", err)
				}
			}
			key, keyErr := plan.CacheKey()
			if keyErr != nil {
				t.Fatal(keyErr)
			}
			if err := registry.Invalidate(key); err != nil {
				t.Fatal(err)
			}
			if runnerValue.closeCount.Load() != 1 {
				t.Fatalf("runner close count=%d, want 1", runnerValue.closeCount.Load())
			}
		})
	}
}

func TestCoordinatorRejectsUnavailableExecution(t *testing.T) {
	plan := newExecutionTestPlan(t)
	identity, err := tenant.NewRunnerIdentity(plan.Tenant().TenantID, "external-user", "external-session")
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Plan: plan, Identity: identity, RequestID: "request-boundary"}

	var nilCoordinator *Coordinator
	if _, err := nilCoordinator.Execute(context.Background(), request); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil coordinator error=%v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	coordinator, err := NewCoordinator(Config{Registry: &executionRegistryStub{ready: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Execute(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error=%v", err)
	}

	acquireErr := errors.New("registry detail")
	coordinator, err = NewCoordinator(Config{Registry: &executionRegistryStub{ready: true, acquireErr: acquireErr}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Execute(context.Background(), request); !errors.Is(err, acquireErr) {
		t.Fatalf("acquire error=%v", err)
	}

	coordinator, err = NewCoordinator(Config{Registry: &executionRegistryStub{ready: true}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Execute(context.Background(), request); !errors.Is(err, runtimerunner.ErrRunnerUnavailable) {
		t.Fatalf("nil lease error=%v", err)
	}
}

func TestCoordinatorForwardCancellationCheckpoints(t *testing.T) {
	coordinator, err := NewCoordinator(Config{Registry: &executionRegistryStub{ready: true}, DrainTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{RequestID: "request-forward-cancel", TraceID: "trace-forward-cancel"}
	tests := []struct {
		name     string
		runner   func() chan serviceagent.RunnerEvent
		errAfter int32
	}{
		{
			name: "after receiving event",
			runner: func() chan serviceagent.RunnerEvent {
				events := make(chan serviceagent.RunnerEvent, 1)
				events <- serviceagent.RunnerEvent{Type: serviceagent.RunnerEventMessage, Text: "hello"}
				return events
			},
			errAfter: 1,
		},
		{
			name: "after closed channel",
			runner: func() chan serviceagent.RunnerEvent {
				events := make(chan serviceagent.RunnerEvent)
				close(events)
				return events
			},
			errAfter: 1,
		},
		{
			name: "before mapped event",
			runner: func() chan serviceagent.RunnerEvent {
				events := make(chan serviceagent.RunnerEvent, 1)
				events <- serviceagent.RunnerEvent{Type: serviceagent.RunnerEventMessage, Text: "hello"}
				return events
			},
			errAfter: 2,
		},
		{
			name: "while sending event",
			runner: func() chan serviceagent.RunnerEvent {
				events := make(chan serviceagent.RunnerEvent, 1)
				events <- serviceagent.RunnerEvent{Type: serviceagent.RunnerEventMessage, Text: "hello"}
				return events
			},
			errAfter: 3,
		},
		{
			name: "after terminal drain",
			runner: func() chan serviceagent.RunnerEvent {
				events := make(chan serviceagent.RunnerEvent, 1)
				events <- serviceagent.RunnerEvent{Type: serviceagent.RunnerEventDone, Status: "complete", Done: true}
				return events
			},
			errAfter: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &executionStagedContext{Context: context.Background(), done: make(chan struct{}), errAfter: tt.errAfter}
			output := make(chan Event, 4)
			coordinator.forward(ctx, executionStream{
				request: request, runnerEvents: tt.runner(), lease: &runtimerunner.RunnerLease{}, output: output,
				finishRunner: func(error) {}, started: time.Now(),
			})
			events := collectExecutionEvents(output)
			if len(events) != 2 || events[0].Type != EventError || !errors.Is(events[0].Err, context.Canceled) || events[1].Status != "canceled" || !events[1].Done {
				t.Fatalf("events=%+v", events)
			}
		})
	}
}

func TestCoordinatorForwardSelectCancellation(t *testing.T) {
	coordinator, err := NewCoordinator(Config{Registry: &executionRegistryStub{ready: true}, DrainTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx := &executionCancelAfterCheckContext{Context: context.Background(), done: make(chan struct{}), checked: make(chan struct{})}
	output := make(chan Event, 4)
	finished := make(chan struct{})
	go func() {
		coordinator.forward(ctx, executionStream{
			request: Request{RequestID: "request-select-cancel"}, runnerEvents: make(chan serviceagent.RunnerEvent),
			lease: &runtimerunner.RunnerLease{}, output: output, finishRunner: func(error) {}, started: time.Now(),
		})
		close(finished)
	}()
	<-ctx.checked
	close(ctx.done)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("forward did not finish after cancellation")
	}
	events := collectExecutionEvents(output)
	if len(events) != 2 || events[0].Type != EventError || !errors.Is(events[0].Err, context.Canceled) || events[1].Status != "canceled" || !events[1].Done {
		t.Fatalf("events=%+v", events)
	}
}

func TestCoordinatorInputAndConfigurationErrors(t *testing.T) {
	if _, err := NewCoordinator(Config{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing registry error=%v", err)
	}
	plan := newExecutionTestPlan(t)
	runnerValue := &executionTestRunner{}
	_, coordinator, identity := newExecutionTestCoordinator(t, plan, runnerValue)
	if !coordinator.Ready() {
		t.Fatal("coordinator is not ready")
	}
	var nilCoordinator *Coordinator
	if nilCoordinator.Ready() {
		t.Fatal("nil coordinator is ready")
	}
	if _, err := coordinator.Execute(nil, Request{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context error=%v", err)
	}
	if _, err := coordinator.Execute(context.Background(), Request{Plan: plan, Identity: identity}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing request ID error=%v", err)
	}
	if _, err := coordinator.Execute(context.Background(), Request{Plan: plan, RequestID: "request-missing-identity"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing identity error=%v", err)
	}
	if _, err := coordinator.Execute(context.Background(), Request{Identity: identity, RequestID: "request-invalid-plan"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid plan error=%v", err)
	}
	if _, err := NewCoordinator(Config{Registry: coordinator.registry, DrainTimeout: -time.Second}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative drain timeout error=%v", err)
	}
	if err := coordinator.registry.(*runtimerunner.RunnerRegistry).Close(); err != nil {
		t.Fatal(err)
	}
	if coordinator.Ready() {
		t.Fatal("closed coordinator is ready")
	}
}

func TestMapRunnerEvent(t *testing.T) {
	tests := []struct {
		name  string
		event serviceagent.RunnerEvent
		want  EventType
		done  bool
	}{
		{name: "zero event", event: serviceagent.RunnerEvent{}, want: EventStatus},
		{name: "partial response", event: serviceagent.RunnerEvent{Type: serviceagent.RunnerEventStatus, Status: "partial"}, want: EventStatus},
		{name: "message", event: serviceagent.RunnerEvent{Type: serviceagent.RunnerEventMessage, Text: "fallback"}, want: EventMessage},
		{name: "done", event: serviceagent.RunnerEvent{Type: serviceagent.RunnerEventDone, Status: "complete", Done: true}, want: EventDone, done: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapped, done := mapRunnerEvent(tt.event, "request", "trace")
			if len(mapped) == 0 || mapped[0].Type != tt.want || done != tt.done || mapped[0].RequestID != "request" || mapped[0].TraceID != "trace" {
				t.Fatalf("mapped=%+v done=%v", mapped, done)
			}
		})
	}
	errorEvents, done := mapRunnerEvent(serviceagent.RunnerEvent{Type: serviceagent.RunnerEventError, Err: serviceagent.ErrRunnerExecution}, "request", "trace")
	if done || len(errorEvents) != 1 || !errors.Is(errorEvents[0].Err, ErrExecution) {
		t.Fatalf("error events=%+v done=%v", errorEvents, done)
	}
}

func TestExecutionHelpersRespectCancellationAndBounds(t *testing.T) {
	if !errors.Is(cancellationError(nil), context.Canceled) {
		t.Fatal("nil context did not produce cancellation")
	}
	var nilContext context.Context
	if sendEvent(nilContext, make(chan Event, 1), Event{}) {
		t.Fatal("sendEvent accepted nil context")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if sendEvent(canceled, make(chan Event, 1), Event{}) {
		t.Fatal("sendEvent accepted canceled context")
	}

	blockedContext, stop := context.WithCancel(context.Background())
	result := make(chan bool, 1)
	go func() { result <- sendEvent(blockedContext, make(chan Event), Event{}) }()
	select {
	case <-result:
		t.Fatal("sendEvent unexpectedly completed before cancellation")
	case <-time.After(time.Millisecond):
	}
	stop()
	if <-result {
		t.Fatal("sendEvent succeeded after cancellation while blocked")
	}

	fullOutput := make(chan Event, 1)
	fullOutput <- Event{Status: "existing"}
	trySend(fullOutput, Event{Status: "discarded"})
	if event := <-fullOutput; event.Status != "existing" {
		t.Fatalf("trySend overwrote full output with %+v", event)
	}
}

func collectExecutionEvents(stream <-chan Event) []Event {
	events := make([]Event, 0, 4)
	for event := range stream {
		events = append(events, event)
	}
	return events
}

func newExecutionTestCoordinator(t *testing.T, plan runtime.ExecutionPlan, runnerValue *executionTestRunner) (*runtimerunner.RunnerRegistry, *Coordinator, tenant.RunnerIdentity) {
	return newExecutionTestCoordinatorWithConfig(t, plan, runnerValue, Config{})
}

func newExecutionTestCoordinatorWithConfig(t *testing.T, plan runtime.ExecutionPlan, runnerValue *executionTestRunner, config Config) (*runtimerunner.RunnerRegistry, *Coordinator, tenant.RunnerIdentity) {
	t.Helper()
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return runnerValue, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if config.Registry == nil {
		config.Registry = registry
	}
	coordinator, err := NewCoordinator(config)
	if err != nil {
		_ = registry.Close()
		t.Fatal(err)
	}
	identity, err := tenant.NewRunnerIdentity(plan.Tenant().TenantID, "external-user", "external-session")
	if err != nil {
		_ = registry.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return registry, coordinator, identity
}

func newExecutionTestPlan(t *testing.T) runtime.ExecutionPlan {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldForbidden,
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
		TenantKey: "execution-test-tenant", DisplayName: "Execution Test Tenant", AuditRetentionDays: 30,
		LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	appRoot, err := appmodel.NewApp(appmodel.CreateInput{TenantID: tenantValue.TenantID, AppKey: "execution-test-app", DisplayName: "Execution Test App"})
	if err != nil {
		t.Fatal(err)
	}
	modelProfile, err := modelprofile.NewProfile(modelprofile.CreateInput{
		TenantID: tenantValue.TenantID, ProfileKey: "execution-test-model", DisplayName: "Execution Test Model",
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"},
	}, modelCatalog)
	if err != nil {
		t.Fatal(err)
	}
	backendProfile, err := backend.NewProfile(backend.CreateInput{
		TenantID: tenantValue.TenantID, ProfileKey: "execution-test-backend", DisplayName: "Execution Test Backend",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "execution-test"}}},
	}, backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	draft, err := appmodel.NewRevision(appmodel.CreateRevisionInput{
		TenantID: tenantValue.TenantID, AppID: appRoot.AppID, Revision: 1,
		Configuration: appmodel.DraftConfiguration{Description: "execution test", Instruction: "Answer clearly.", ModelProfileID: modelProfile.ProfileID, Runtime: appmodel.DefaultRuntimePolicy()},
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
