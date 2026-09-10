package worker_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestWorkerPrepareResolvesBackendWithoutStickySession(t *testing.T) {
	w := testWorker(t, sharedBackendConfig())

	exec, err := w.Prepare(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	if exec.Config.BackendConfig.Name != "shared" {
		t.Fatalf("backend_config name = %q, want shared", exec.Config.BackendConfig.Name)
	}
	if exec.Config.Version != "v1" {
		t.Fatalf("config version = %q, want v1", exec.Config.Version)
	}
	if exec.TenantSource != gateway.TenantSourceAuthenticatedClaims {
		t.Fatalf("tenant source = %q, want authenticated claims", exec.TenantSource)
	}
	const want = "tenant:tenant-a:app:support:session:principal-1:session-1"
	if exec.PartitionKey != want {
		t.Fatalf("partition key = %q, want %q", exec.PartitionKey, want)
	}
	if exec.Config.BackendConfig.Session.Kind != tenant.BackendSQL {
		t.Fatalf("session backend kind = %q, want sql", exec.Config.BackendConfig.Session.Kind)
	}
	if exec.Config.BackendConfig.Memory.Name != "memory-redis" {
		t.Fatalf("memory backend name = %q, want memory-redis", exec.Config.BackendConfig.Memory.Name)
	}
}

func TestWorkersPrepareSameJobWithoutNodeAffinity(t *testing.T) {
	backend := sharedBackendConfig()
	workers := []worker.Worker{
		testWorker(t, backend),
		testWorker(t, backend),
	}
	job := testJob("request-1", "tenant-a", "session-1")

	first, err := workers[0].Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("first worker prepare: %v", err)
	}
	second, err := workers[1].Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("second worker prepare: %v", err)
	}
	if first.PartitionKey != second.PartitionKey {
		t.Fatalf("partition keys differ: %q and %q", first.PartitionKey, second.PartitionKey)
	}
	if !reflect.DeepEqual(first.Config, second.Config) {
		t.Fatalf("app configs differ: %#v and %#v", first.Config, second.Config)
	}
}

func TestWorkerResolvesExactConfigVersion(t *testing.T) {
	backend := sharedBackendConfig()
	v1 := testAppConfig("tenant-a", backend)
	v2 := testAppConfig("tenant-a", backend)
	v2.Version = "v2"
	v2.Model.Model = "gpt-4.1"
	configs, err := config.NewStaticResolver(v1, v2)
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	w := worker.Worker{
		Config: configs,
	}
	job := testJobWith("request-1", "tenant-a", "session-1", func(tc *tenant.RuntimeContext, _ *gateway.Message) {
		tc.ConfigVersion = "v2"
	})

	exec, err := w.Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	if exec.Config.Version != "v2" || exec.Config.Model.Model != "gpt-4.1" {
		t.Fatalf("resolved config = %q/%q, want v2/gpt-4.1", exec.Config.Version, exec.Config.Model.Model)
	}
}

func TestWorkerRunCallsRunnerAndDrainsEvents(t *testing.T) {
	runner := &recordingRunner{
		events: []*event.Event{
			event.New("invocation-1", "assistant"),
			runnerCompletionEvent(),
		},
	}
	sink := &recordingEventSink{}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.Events = sink

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("run job: %v", err)
	}
	if runner.userID != "principal-1" {
		t.Fatalf("runner user id = %q, want principal-1", runner.userID)
	}
	if runner.sessionID != "session-1" {
		t.Fatalf("runner session id = %q, want session-1", runner.sessionID)
	}
	if runner.message.Role != model.RoleUser || runner.message.Content != "hello" {
		t.Fatalf("runner message = %#v, want user hello", runner.message)
	}
	if runner.options.RequestID != "request-1" {
		t.Fatalf("runner request id = %q, want request-1", runner.options.RequestID)
	}
	if runner.options.AppName != "tenant:tenant-a:app:support:runner" {
		t.Fatalf(
			"runner app name = %q, want tenant:tenant-a:app:support:runner",
			runner.options.AppName,
		)
	}
	if got := runner.options.RuntimeState["tenant_id"]; got != "tenant-a" {
		t.Fatalf("runtime tenant_id = %v, want tenant-a", got)
	}
	if got := runner.options.RuntimeState["user_id"]; got != "user-1" {
		t.Fatalf("runtime user_id = %v, want user-1", got)
	}
	if got := runner.options.RuntimeState["session_principal_id"]; got != "principal-1" {
		t.Fatalf("runtime session_principal_id = %v, want principal-1", got)
	}
	if result.EventCount != 2 {
		t.Fatalf("event count = %d, want 2", result.EventCount)
	}
	if !result.RunnerCompleted {
		t.Fatal("runner completion was not observed")
	}
	if !result.TerminalEventPersisted {
		t.Fatal("durable terminal event was not recorded")
	}
	if len(sink.events) != 2 {
		t.Fatalf("sink event count = %d, want 2", len(sink.events))
	}
	if !runner.closed {
		t.Fatal("runner was not closed after events were drained")
	}
}

func TestWorkerMarksTerminalEventForAtomicProjection(t *testing.T) {
	runner := &recordingRunner{events: []*event.Event{runnerCompletionEvent()}}
	var projected worker.Execution
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.Events = eventSinkFunc(func(_ context.Context, exec worker.Execution, _ *event.Event) error {
		projected = exec
		return nil
	})

	if _, err := w.Run(context.Background(), testJob("request-terminal", "tenant-a", "session-1")); err != nil {
		t.Fatalf("run job: %v", err)
	}
	if projected.TerminalStatus != queue.CompletionSucceeded {
		t.Fatalf("projected terminal status = %q, want %q", projected.TerminalStatus, queue.CompletionSucceeded)
	}
}

func TestWorkerGuardrailRejectionHonorsAuditPolicy(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("audit-%t", enabled), func(t *testing.T) {
			appConfig := testAppConfig("tenant-a", sharedBackendConfig())
			appConfig.Audit.Enabled = enabled
			configs, err := config.NewStaticResolver(appConfig)
			if err != nil {
				t.Fatalf("new config resolver: %v", err)
			}
			audit := &recordingAuditSink{}
			w := *worker.New(configs, nil, testSessionLocker{}, nil, nil)
			w.Audit = audit
			_, err = w.Run(context.Background(), testJobWith("request-guardrail-audit", "tenant-a", "session-1", func(_ *tenant.RuntimeContext, message *gateway.Message) {
				message.Text = "password: hunter2"
			}))
			if err == nil || !errors.Is(err, guardrail.ErrInputBlocked) {
				t.Fatalf("guardrail error = %v", err)
			}
			want := 0
			if enabled {
				want = 1
			}
			if len(audit.events) != want {
				t.Fatalf("audit events = %d, want %d", len(audit.events), want)
			}
		})
	}
}

func TestWorkerPersistsRunnerBuildFailureOnlyOnFinalAttempt(t *testing.T) {
	wantErr := errors.New("resolve model failed")
	w := testWorker(t, sharedBackendConfig())
	w.Runner = func(context.Context, worker.Execution) (frameworkrunner.Runner, error) {
		return nil, wantErr
	}
	var projected []worker.Execution
	var events []*event.Event
	w.Events = eventSinkFunc(func(_ context.Context, exec worker.Execution, evt *event.Event) error {
		projected = append(projected, exec)
		events = append(events, evt)
		return nil
	})
	job := testJob("request-build-failure", "tenant-a", "session-1")
	if _, err := w.Run(worker.ContextWithFinalAttempt(context.Background(), false), job); !errors.Is(err, wantErr) {
		t.Fatalf("retryable build error = %v, want %v", err, wantErr)
	}
	if len(events) != 0 {
		t.Fatalf("non-final build failure persisted %d events", len(events))
	}
	if _, err := w.Run(worker.ContextWithFinalAttempt(context.Background(), true), job); !errors.Is(err, wantErr) {
		t.Fatalf("final build error = %v, want %v", err, wantErr)
	}
	if len(events) != 1 || !events[0].IsTerminalError() {
		t.Fatalf("final build failure events = %#v", events)
	}
	if projected[0].TerminalStatus != queue.CompletionFailed {
		t.Fatalf("final build status = %q, want FAILED", projected[0].TerminalStatus)
	}
}

func TestWorkerCleanupErrorsDoNotChangeBusinessResult(t *testing.T) {
	closeErr := errors.New("runner close failed")
	runner := &recordingRunner{
		events:   []*event.Event{runnerCompletionEvent()},
		closeErr: closeErr,
	}
	locker := &recordingSessionLocker{releaseErr: errors.New("session release failed")}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.SessionLocker = locker

	result, err := w.Run(context.Background(), testJob("request-cleanup", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("run job = %v, want nil business error", err)
	}
	if !errors.Is(result.CleanupError, closeErr) || !errors.Is(result.CleanupError, locker.releaseErr) {
		t.Fatalf("cleanup error = %v, want runner and session cleanup errors", result.CleanupError)
	}
}

func TestWorkerRunHoldsSessionLockUntilEventsAreDrained(t *testing.T) {
	runner := &recordingRunner{events: []*event.Event{runnerCompletionEvent()}}
	locker := &recordingSessionLocker{}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.SessionLocker = locker
	w.Events = eventSinkFunc(func(_ context.Context, _ worker.Execution, _ *event.Event) error {
		if locker.lock == nil {
			t.Fatal("event sink ran before session lock was acquired")
		}
		if locker.lock.released.Load() {
			t.Fatal("session lock was released before events were drained")
		}
		return nil
	})

	if _, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1")); err != nil {
		t.Fatalf("run job: %v", err)
	}
	if locker.partitionKey != "tenant:tenant-a:app:support:session:principal-1:session-1" {
		t.Fatalf("lock partition key = %q", locker.partitionKey)
	}
	if runner.ctx.Value(sessionLockTestContextKey{}) != "locked" {
		t.Fatal("runner did not receive the session lock context")
	}
	if locker.lock == nil || !locker.lock.released.Load() {
		t.Fatal("session lock was not released after the runner event channel closed")
	}
}

func TestWorkerRunKeepsPrivateSessionsSeparate(t *testing.T) {
	w, sessions := sessionContractWorker(t)
	const sessionID = "private-session"

	first := testJobWith("request-1", "tenant-a", sessionID, func(tc *tenant.RuntimeContext, message *gateway.Message) {
		tc.UserID = "user-1"
		tc.SessionPrincipalID = "user-1"
		message.Text = "from user 1"
	})
	if _, err := w.Run(context.Background(), first); err != nil {
		t.Fatalf("run first private message: %v", err)
	}

	firstKey := session.Key{
		AppName:   "tenant:tenant-a:app:support:runner",
		UserID:    "user-1",
		SessionID: sessionID,
	}
	firstSession, err := sessions.GetSession(context.Background(), firstKey)
	if err != nil {
		t.Fatalf("get first private session: %v", err)
	}
	if firstSession == nil {
		t.Fatal("first private session was not created")
	}
	firstEventCount := len(firstSession.GetEvents())

	second := testJobWith("request-2", "tenant-a", sessionID, func(tc *tenant.RuntimeContext, message *gateway.Message) {
		tc.UserID = "user-2"
		tc.SessionPrincipalID = "user-2"
		message.Text = "from user 2"
	})
	if _, err := w.Run(context.Background(), second); err != nil {
		t.Fatalf("run second private message: %v", err)
	}

	firstSession, err = sessions.GetSession(context.Background(), firstKey)
	if err != nil {
		t.Fatalf("get first private session again: %v", err)
	}
	if got := len(firstSession.GetEvents()); got != firstEventCount {
		t.Fatalf("first private session event count = %d, want %d", got, firstEventCount)
	}
	secondSession, err := sessions.GetSession(context.Background(), session.Key{
		AppName:   "tenant:tenant-a:app:support:runner",
		UserID:    "user-2",
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("get second private session: %v", err)
	}
	if secondSession == nil {
		t.Fatal("second private session was not created")
	}
}

func TestWorkerRunSharesGroupSessionAcrossUsers(t *testing.T) {
	w, sessions := sessionContractWorker(t)
	const (
		principalID = "group-1"
		sessionID   = "group-session"
	)

	first := testJobWith("request-1", "tenant-a", sessionID, func(tc *tenant.RuntimeContext, message *gateway.Message) {
		tc.UserID = "user-1"
		tc.SessionPrincipalID = principalID
		message.Text = "from user 1"
	})
	if _, err := w.Run(context.Background(), first); err != nil {
		t.Fatalf("run first group message: %v", err)
	}

	groupKey := session.Key{
		AppName:   "tenant:tenant-a:app:support:runner",
		UserID:    principalID,
		SessionID: sessionID,
	}
	groupSession, err := sessions.GetSession(context.Background(), groupKey)
	if err != nil {
		t.Fatalf("get group session: %v", err)
	}
	if groupSession == nil {
		t.Fatal("group session was not created")
	}
	firstEventCount := len(groupSession.GetEvents())

	second := testJobWith("request-2", "tenant-a", sessionID, func(tc *tenant.RuntimeContext, message *gateway.Message) {
		tc.UserID = "user-2"
		tc.SessionPrincipalID = principalID
		message.Text = "from user 2"
	})
	if _, err := w.Run(context.Background(), second); err != nil {
		t.Fatalf("run second group message: %v", err)
	}

	groupSession, err = sessions.GetSession(context.Background(), groupKey)
	if err != nil {
		t.Fatalf("get shared group session: %v", err)
	}
	if got := len(groupSession.GetEvents()); got <= firstEventCount {
		t.Fatalf("group session event count = %d, want more than %d", got, firstEventCount)
	}
	userSession, err := sessions.GetSession(context.Background(), session.Key{
		AppName:   "tenant:tenant-a:app:support:runner",
		UserID:    "user-2",
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("get real-user session: %v", err)
	}
	if userSession != nil {
		t.Fatal("group message created a session scoped to the real user")
	}
}

func TestWorkerRunDrainsEventsAfterSinkError(t *testing.T) {
	wantErr := errors.New("sink failed")
	runner := &recordingRunner{
		events: []*event.Event{
			event.New("invocation-1", "assistant"),
			runnerCompletionEvent(),
		},
	}
	sink := &recordingEventSink{err: wantErr}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.Events = sink

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("run error = %v, want sink error", err)
	}
	if !worker.IsSideEffectUncertainError(err) {
		t.Fatalf("run error = %v, want side-effect-uncertain classification", err)
	}
	if result.EventCount != 2 {
		t.Fatalf("event count = %d, want 2", result.EventCount)
	}
	if len(sink.events) != 1 {
		t.Fatalf("sink event count = %d, want 1 after cancellation", len(sink.events))
	}
}

func TestWorkerRunCancelsRunnerAndBoundsEventDrainAfterSinkFailure(t *testing.T) {
	wantErr := errors.New("sink failed")
	runner := &nonCooperativeEventRunner{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	defer func() {
		close(runner.release)
		select {
		case <-runner.done:
		case <-time.After(time.Second):
			t.Error("non-cooperative runner did not finish after test release")
		}
	}()
	sink := &recordingEventSink{err: wantErr}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.Events = sink
	w.EventSinkTimeout = 20 * time.Millisecond

	started := time.Now()
	result, err := w.Run(context.Background(), testJob("request-sink-failure", "tenant-a", "session-1"))
	if time.Since(started) > time.Second {
		t.Fatal("event drain exceeded the bounded shutdown period")
	}
	if !errors.Is(err, wantErr) || !worker.IsSideEffectUncertainError(err) {
		t.Fatalf("run error = %v, want side-effect-uncertain sink error", err)
	}
	select {
	case <-runner.canceled:
	case <-time.After(time.Second):
		t.Fatal("runner did not receive context cancellation")
	}
	if result.EventCount != 1 || result.RunnerCompleted {
		t.Fatalf("run result = %#v, want only the first event and no completion", result)
	}
	if len(sink.events) != 1 {
		t.Fatalf("sink event count = %d, want 1", len(sink.events))
	}
}

func TestWorkerRunReturnsRunnerCompletionError(t *testing.T) {
	wantErr := &model.ResponseError{
		Type:    model.ErrorTypeRunError,
		Message: "runner failed",
	}
	runner := &recordingRunner{
		events: []*event.Event{{
			Response: &model.Response{
				Object: model.ObjectTypeRunnerCompletion,
				Done:   true,
				Error:  wantErr,
			},
		}},
	}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("run error = %v, want runner error", err)
	}
	if !result.RunnerCompleted {
		t.Fatal("runner completion was not observed")
	}
	if result.EventCount != 1 {
		t.Fatalf("event count = %d, want 1", result.EventCount)
	}
}

func TestWorkerRunDrainsEventsAfterSinkTimeout(t *testing.T) {
	runner := &recordingRunner{
		events: []*event.Event{
			event.New("invocation-1", "assistant"),
			runnerCompletionEvent(),
		},
	}
	sink := &blockingEventSink{}

	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.Events = sink
	w.EventSinkTimeout = time.Millisecond

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run error = %v, want deadline exceeded", err)
	}
	if result.EventCount != 2 {
		t.Fatalf("event count = %d, want 2", result.EventCount)
	}
	if got := sink.calls.Load(); got > 1 {
		t.Fatalf("sink calls = %d, want at most 1 after timeout", got)
	}
}

func TestWorkerDoesNotPersistCompletionAfterApprovalBoundary(t *testing.T) {
	backend := sharedBackendConfig()
	appConfig := testAppConfig("tenant-a", backend)
	appConfig.Tools = tenant.ToolPolicy{
		ExecutableTools:     []string{"delete"},
		ReviewRequiredTools: []string{"delete"},
	}
	configs, err := config.NewStaticResolver(appConfig)
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	callID := "approval-call-1"
	runner := &recordingRunner{
		permissionRequest: &frameworktool.PermissionRequest{
			ToolName:   "delete",
			ToolCallID: callID,
			Arguments:  []byte(`{"resource":"record-1"}`),
		},
		events: []*event.Event{
			event.NewResponseEvent("invocation-1", "assistant", &model.Response{
				Object: model.ObjectTypeChatCompletion,
				Done:   true,
				Choices: []model.Choice{{Message: model.Message{
					Role: model.RoleAssistant,
					ToolCalls: []model.ToolCall{{
						ID: callID, Type: "function",
						Function: model.FunctionDefinitionParam{
							Name: "delete", Arguments: []byte(`{"resource":"record-1"}`),
						},
					}},
				}}},
			}),
			event.NewResponseEvent("invocation-1", "tool", &model.Response{
				Object: model.ObjectTypeToolResponse,
				Choices: []model.Choice{{Message: model.Message{
					Role:     model.RoleTool,
					ToolID:   callID,
					ToolName: "delete",
					Content:  `{"status":"approval_required"}`,
				}}},
			}),
			event.NewResponseEvent("invocation-1", "assistant", &model.Response{
				Object:  model.ObjectTypeChatCompletion,
				Done:    true,
				Choices: []model.Choice{{Message: model.NewAssistantMessage("must not be persisted")}},
			}),
			runnerCompletionEvent(),
		},
	}
	sink := &recordingEventSink{}
	w := *worker.New(configs, nil, testSessionLocker{}, sink, nil)
	w.Runner = fixedRunner(runner)
	w.Approvals = &pendingApprovalRepository{}

	result, err := w.Run(context.Background(), testJob("request-approval-boundary", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("run job: %v", err)
	}
	if !result.ApprovalPending || result.ApprovalID == "" {
		t.Fatalf("approval result = %#v, want durable pending approval", result)
	}
	if len(sink.events) != 3 {
		t.Fatalf("persisted event count = %d, want tool call, tool result, and runner completion", len(sink.events))
	}
	for _, persisted := range sink.events {
		if persisted.Response != nil && len(persisted.Response.Choices) > 0 &&
			persisted.Response.Choices[0].Message.Content == "must not be persisted" {
			t.Fatal("assistant completion after approval boundary was persisted")
		}
	}
}

func TestOpenAIModelResolverAppliesModelParametersWithoutConfigCredentials(t *testing.T) {
	w := testWorker(t, sharedBackendConfig())
	exec, err := w.Prepare(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	exec.Config.Model.Parameters = map[string]string{
		"base_url":    "https://model.example.test/v1",
		"max_tokens":  "256",
		"temperature": "0.2",
	}
	models, err := platformruntime.NewOpenAIModelResolver(
		staticSecretProvider("test-model-key"),
		allowConfiguredEndpoint{},
	)
	if err != nil {
		t.Fatalf("new model resolver: %v", err)
	}
	runtime, err := models.ResolveModel(context.Background(), exec)
	if err != nil {
		t.Fatalf("resolve model: %v", err)
	}
	if runtime.Model == nil {
		t.Fatal("resolved model is nil")
	}
	if runtime.GenerationConfig.Temperature == nil || *runtime.GenerationConfig.Temperature != 0.2 {
		t.Fatalf("temperature = %#v, want 0.2", runtime.GenerationConfig.Temperature)
	}
	if runtime.GenerationConfig.MaxTokens == nil || *runtime.GenerationConfig.MaxTokens != 256 {
		t.Fatalf("max tokens = %#v, want 256", runtime.GenerationConfig.MaxTokens)
	}

	withoutPolicy, err := platformruntime.NewOpenAIModelResolver(staticSecretProvider("test-model-key"), nil)
	if err != nil {
		t.Fatalf("new model resolver without endpoint policy: %v", err)
	}
	if _, err := withoutPolicy.ResolveModel(context.Background(), exec); err == nil {
		t.Fatal("resolve model succeeded with a configured base_url but no endpoint policy")
	}

	insecurePolicy, err := platformruntime.NewOpenAIModelResolver(
		staticSecretProvider("test-model-key"),
		staticEndpointPolicy("http://127.0.0.1:8080/v1"),
	)
	if err != nil {
		t.Fatalf("new model resolver with endpoint policy: %v", err)
	}
	if _, err := insecurePolicy.ResolveModel(context.Background(), exec); err == nil {
		t.Fatal("resolve model succeeded with an insecure resolved base_url")
	}

	exec.Config.Model.Parameters = map[string]string{"api_key": "must-not-be-stored-here"}
	if _, err := models.ResolveModel(context.Background(), exec); err == nil {
		t.Fatal("resolve model succeeded with api_key in immutable config")
	}
}

func TestWorkerRunSkipsSinkAfterLockContextCanceled(t *testing.T) {
	lockContext, cancelLock := context.WithCancel(context.Background())
	runner := &recordingRunner{
		events: []*event.Event{runnerCompletionEvent()},
		cancel: cancelLock,
	}
	sink := &recordingEventSink{}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.SessionLocker = staticSessionLocker{ctx: lockContext}
	w.Events = sink

	if _, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1")); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
	if len(sink.events) != 0 {
		t.Fatalf("sink received %d events after lock context cancellation", len(sink.events))
	}
}

func TestWorkerRunClassifiesLostSessionLeaseAsUncertain(t *testing.T) {
	lockContext, cancelLock := context.WithCancelCause(context.Background())
	cancelLock(worker.ErrSessionLeaseLost)
	runner := &recordingRunner{events: []*event.Event{runnerCompletionEvent()}}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.SessionLocker = staticSessionLocker{ctx: lockContext}

	result, err := w.Run(context.Background(), testJob("request-lease-lost", "tenant-a", "session-1"))
	if !worker.IsSideEffectUncertainError(err) {
		t.Fatalf("run error = %v, want side-effect-uncertain classification", err)
	}
	if !errors.Is(err, worker.ErrSessionLeaseLost) {
		t.Fatalf("run error = %v, want session lease lost cause", err)
	}
	if !result.RunnerStarted {
		t.Fatal("runner start was not recorded")
	}
}

func TestWorkerRunCancelsManagedRunnerWhenExecutionContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &managedBlockingRunner{
		started: make(chan struct{}),
		events:  make(chan *event.Event),
	}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)

	done := make(chan error, 1)
	go func() {
		_, err := w.Run(ctx, testJob("request-managed-cancel", "tenant-a", "session-1"))
		done <- err
	}()
	<-runner.started
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
	if !runner.canceled.Load() {
		t.Fatal("managed runner was not canceled")
	}
}

func TestWorkerRunModelTimeoutCancelsAndDrainsManagedRunner(t *testing.T) {
	runner := &managedBlockingRunner{
		started: make(chan struct{}),
		events:  make(chan *event.Event),
	}
	locker := &recordingSessionLocker{}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.SessionLocker = locker
	w.ModelTimeout = time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := w.Run(context.Background(), testJob("request-model-timeout", "tenant-a", "session-1"))
		done <- err
	}()
	<-runner.started
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("run error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("model timeout did not finish worker run")
	}
	if !runner.canceled.Load() {
		t.Fatal("model timeout did not cancel managed runner")
	}
	if !runner.closed.Load() {
		t.Fatal("runner was not closed after timeout event drain")
	}
	if locker.lock == nil || !locker.lock.released.Load() {
		t.Fatal("session lock was not released after model timeout")
	}
}

func TestWorkerRunBoundsDrainAfterExternalCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &nonCooperativeEventRunner{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	defer func() {
		close(runner.release)
		select {
		case <-runner.done:
		case <-time.After(time.Second):
			t.Error("non-cooperative runner did not finish after test release")
		}
	}()
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.EventSinkTimeout = 20 * time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := w.Run(ctx, testJob("request-external-cancel", "tenant-a", "session-1"))
		done <- err
	}()
	<-runner.started
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("external cancellation did not finish bounded event drain")
	}
	select {
	case <-runner.canceled:
	case <-time.After(time.Second):
		t.Fatal("runner did not observe context cancellation")
	}
}

func TestWorkerRunTimeoutCancelsManagedRunnerBlockedInStart(t *testing.T) {
	runner := &managedStartBlockingRunner{started: make(chan struct{})}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(runner)
	w.ModelTimeout = time.Millisecond

	done := make(chan error, 1)
	go func() {
		_, err := w.Run(context.Background(), testJob("request-timeout-start", "tenant-a", "session-1"))
		done <- err
	}()
	<-runner.started
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("run error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("model timeout did not stop a runner blocked in Run")
	}
	if !runner.canceled.Load() || !runner.closed.Load() {
		t.Fatal("blocked managed runner was not canceled and closed")
	}
}

func testJob(requestID, tenantID, sessionID string) execution.Job {
	return testJobWith(requestID, tenantID, sessionID, nil)
}

func testJobWith(
	requestID, tenantID, sessionID string,
	mutate func(*tenant.RuntimeContext, *gateway.Message),
) execution.Job {
	tenantContext := tenant.RuntimeContext{
		TenantID:           tenantID,
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          sessionID,
		SessionPrincipalID: "principal-1",
		UserID:             "user-1",
		TraceID:            "trace-1",
	}
	message := gateway.Message{Text: "hello"}
	if mutate != nil {
		mutate(&tenantContext, &message)
	}
	job, err := execution.NewJob(
		requestID,
		gateway.TenantSourceAuthenticatedClaims,
		tenantContext,
		message,
	)
	if err != nil {
		panic(err)
	}
	return job
}

func sharedBackendConfig() tenant.BackendConfig {
	return tenant.BackendConfig{
		Name: "shared",
		Session: tenant.BackendRef{
			Kind:     tenant.BackendSQL,
			Provider: "postgres",
			Name:     "session-sql",
			Options:  map[string]string{"schema": "agent"},
		},
		Memory: tenant.BackendRef{
			Kind:     tenant.BackendRedis,
			Provider: "redis",
			Name:     "memory-redis",
		},
	}
}

func testWorker(t *testing.T, backend tenant.BackendConfig) worker.Worker {
	t.Helper()
	configs, err := config.NewStaticResolver(testAppConfig("tenant-a", backend))
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	return *worker.New(configs, nil, testSessionLocker{}, nil, nil)
}

func sessionContractWorker(t *testing.T) (worker.Worker, *sessioninmemory.SessionService) {
	t.Helper()
	sessions := sessioninmemory.NewSessionService()
	r := frameworkrunner.NewRunner(
		"worker-session-contract",
		&sessionContractAgent{name: "assistant"},
		frameworkrunner.WithSessionService(sessions),
	)
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("close runner: %v", err)
		}
	})
	w := testWorker(t, sharedBackendConfig())
	w.Runner = fixedRunner(r)
	return w, sessions
}

func testAppConfig(tenantID string, backend tenant.BackendConfig) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:  "openai",
			APIKeyRef: tenant.SecretRef{Name: "model-key"},
			Model:     "gpt-4.1-mini",
		},
		BackendConfig: backend,
	}
}

func fixedRunner(value frameworkrunner.Runner) func(context.Context, worker.Execution) (frameworkrunner.Runner, error) {
	return func(context.Context, worker.Execution) (frameworkrunner.Runner, error) {
		return value, nil
	}
}

type recordingRunner struct {
	ctx                context.Context
	userID             string
	sessionID          string
	message            model.Message
	options            agent.RunOptions
	events             []*event.Event
	permissionRequest  *frameworktool.PermissionRequest
	permissionDecision frameworktool.PermissionDecision
	permissionErr      error
	err                error
	cancel             context.CancelFunc
	closed             bool
	closeErr           error
}

type managedBlockingRunner struct {
	started  chan struct{}
	events   chan *event.Event
	canceled atomic.Bool
	closed   atomic.Bool
}

type nonCooperativeEventRunner struct {
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	done     chan struct{}
}

func (r *nonCooperativeEventRunner) Run(
	ctx context.Context,
	_, _ string,
	_ model.Message,
	_ ...agent.RunOption,
) (<-chan *event.Event, error) {
	close(r.started)
	events := make(chan *event.Event, 1)
	events <- event.New("invocation-1", "assistant")
	go func() {
		defer close(r.done)
		<-ctx.Done()
		close(r.canceled)
		<-r.release
		close(events)
	}()
	return events, nil
}

func (*nonCooperativeEventRunner) Close() error { return nil }

func (r *managedBlockingRunner) Run(
	_ context.Context,
	_, _ string,
	_ model.Message,
	_ ...agent.RunOption,
) (<-chan *event.Event, error) {
	close(r.started)
	return r.events, nil
}

func (r *managedBlockingRunner) Cancel(requestID string) bool {
	if requestID == "" {
		return false
	}
	r.canceled.Store(true)
	close(r.events)
	return true
}

func (*managedBlockingRunner) RunStatus(string) (frameworkrunner.RunStatus, bool) {
	return frameworkrunner.RunStatus{}, false
}

func (r *managedBlockingRunner) Close() error {
	r.closed.Store(true)
	return nil
}

type managedStartBlockingRunner struct {
	started  chan struct{}
	canceled atomic.Bool
	closed   atomic.Bool
}

func (r *managedStartBlockingRunner) Run(ctx context.Context, _ string, _ string, _ model.Message, _ ...agent.RunOption) (<-chan *event.Event, error) {
	close(r.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *managedStartBlockingRunner) Cancel(requestID string) bool {
	if requestID == "" {
		return false
	}
	r.canceled.Store(true)
	return true
}

func (*managedStartBlockingRunner) RunStatus(string) (frameworkrunner.RunStatus, bool) {
	return frameworkrunner.RunStatus{}, false
}

func (r *managedStartBlockingRunner) Close() error {
	r.closed.Store(true)
	return nil
}

type sessionContractAgent struct {
	name string
}

func (a *sessionContractAgent) Run(
	_ context.Context,
	invocation *agent.Invocation,
) (<-chan *event.Event, error) {
	ch := make(chan *event.Event, 1)
	ch <- event.NewResponseEvent(
		invocation.InvocationID,
		a.name,
		&model.Response{
			Done: true,
			Choices: []model.Choice{{
				Index:   0,
				Message: model.NewAssistantMessage("ok"),
			}},
		},
	)
	close(ch)
	return ch, nil
}

func (a *sessionContractAgent) Tools() []frameworktool.Tool {
	return nil
}

func (a *sessionContractAgent) Info() agent.Info {
	return agent.Info{Name: a.name}
}

func (*sessionContractAgent) SubAgents() []agent.Agent {
	return nil
}

func (*sessionContractAgent) FindSubAgent(string) agent.Agent {
	return nil
}

func (r *recordingRunner) Run(
	ctx context.Context,
	userID string,
	sessionID string,
	message model.Message,
	runOpts ...agent.RunOption,
) (<-chan *event.Event, error) {
	r.ctx = ctx
	r.userID = userID
	r.sessionID = sessionID
	r.message = message
	r.options = agent.NewRunOptions(runOpts...)
	if r.permissionRequest != nil {
		if r.options.ToolPermissionPolicy == nil {
			r.permissionErr = errors.New("tool permission policy is missing")
		} else {
			r.permissionDecision, r.permissionErr = r.options.ToolPermissionPolicy.CheckToolPermission(ctx, r.permissionRequest)
		}
	}
	if r.cancel != nil {
		r.cancel()
	}
	if r.err != nil {
		return nil, r.err
	}
	ch := make(chan *event.Event, len(r.events))
	for _, evt := range r.events {
		ch <- evt
	}
	close(ch)
	return ch, nil
}

type pendingApprovalRepository struct{}

func (*pendingApprovalRepository) ResolveOrCreate(_ context.Context, request platformapproval.Request) (platformapproval.Record, error) {
	return platformapproval.Record{
		ApprovalID:     "approval-1",
		TenantID:       request.TenantID,
		AppID:          request.AppID,
		ConfigVersion:  request.ConfigVersion,
		RequestID:      request.RequestID,
		SessionID:      request.SessionID,
		ToolName:       request.ToolName,
		ToolCallID:     request.ToolCallID,
		ArgumentDigest: request.ArgumentDigest,
		Status:         platformapproval.StatusPending,
		ExpiresAt:      request.ExpiresAt,
		CreatedAt:      time.Now(),
	}, nil
}

func (*pendingApprovalRepository) List(context.Context, platformapproval.Query) ([]platformapproval.Record, error) {
	return nil, nil
}

func (*pendingApprovalRepository) Decide(context.Context, string, string, string, platformapproval.Status) (platformapproval.Record, error) {
	return platformapproval.Record{}, nil
}

type testSessionLocker struct{}

func (testSessionLocker) Lock(ctx context.Context, _ string) (worker.SessionLock, error) {
	return testSessionLock{ctx: ctx}, nil
}

type staticSessionLocker struct {
	ctx context.Context
}

func (l staticSessionLocker) Lock(context.Context, string) (worker.SessionLock, error) {
	return testSessionLock(l), nil
}

type testSessionLock struct {
	ctx context.Context
}

func (l testSessionLock) Context() context.Context {
	return l.ctx
}

func (testSessionLock) Release() error {
	return nil
}

type sessionLockTestContextKey struct{}

type recordingSessionLocker struct {
	partitionKey string
	lock         *recordingSessionLock
	releaseErr   error
}

func (l *recordingSessionLocker) Lock(ctx context.Context, partitionKey string) (worker.SessionLock, error) {
	l.partitionKey = partitionKey
	l.lock = &recordingSessionLock{
		ctx:        context.WithValue(ctx, sessionLockTestContextKey{}, "locked"),
		releaseErr: l.releaseErr,
	}
	return l.lock, nil
}

type recordingSessionLock struct {
	ctx        context.Context
	released   atomic.Bool
	releaseErr error
}

func (l *recordingSessionLock) Context() context.Context {
	return l.ctx
}

func (l *recordingSessionLock) Release() error {
	l.released.Store(true)
	return l.releaseErr
}

type eventSinkFunc func(context.Context, worker.Execution, *event.Event) error

func (f eventSinkFunc) HandleRunnerEvent(
	ctx context.Context,
	exec worker.Execution,
	evt *event.Event,
) error {
	return f(ctx, exec, evt)
}

type recordingAuditSink struct {
	events []platformaudit.Event
}

func (s *recordingAuditSink) Record(_ context.Context, event platformaudit.Event) error {
	s.events = append(s.events, event)
	return nil
}

type staticSecretProvider string

func (r staticSecretProvider) ResolveSecret(
	_ context.Context,
	_ tenant.Scope,
	_ tenant.SecretRef,
) (string, error) {
	return string(r), nil
}

type allowConfiguredEndpoint struct{}

func (allowConfiguredEndpoint) ResolveModelBaseURL(
	_ context.Context,
	_ worker.Execution,
	configuredURL string,
) (string, error) {
	return configuredURL, nil
}

type staticEndpointPolicy string

func (p staticEndpointPolicy) ResolveModelBaseURL(
	_ context.Context,
	_ worker.Execution,
	_ string,
) (string, error) {
	return string(p), nil
}

func (r *recordingRunner) Close() error {
	r.closed = true
	return r.closeErr
}

type recordingEventSink struct {
	events []*event.Event
	err    error
}

func (s *recordingEventSink) HandleRunnerEvent(
	_ context.Context,
	_ worker.Execution,
	evt *event.Event,
) error {
	s.events = append(s.events, evt)
	return s.err
}

type blockingEventSink struct {
	calls atomic.Int32
}

func (s *blockingEventSink) HandleRunnerEvent(
	ctx context.Context,
	_ worker.Execution,
	_ *event.Event,
) error {
	s.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func runnerCompletionEvent() *event.Event {
	return &event.Event{
		Response: &model.Response{
			Object: model.ObjectTypeRunnerCompletion,
			Done:   true,
		},
	}
}
