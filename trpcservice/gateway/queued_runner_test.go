package gateway_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestQueuedRunnerAdmitsAuthenticatedContextAndForwardsPersistedEvents(t *testing.T) {
	identity := validAdmissionIdentity()
	resolver := staticTenantResolver{
		tenant:       identity.Tenant,
		source:       identity.Source,
		identity:     identity,
		withIdentity: true,
	}
	ctx, err := gateway.ContextWithAuthenticatedRequest(context.Background(), gateway.AuthenticatedRequest{
		RequestID:      "request-queued-1",
		IdempotencyKey: "idempotency-queued-1",
		Tenant:         resolver,
	})
	if err != nil {
		t.Fatalf("attach authenticated request: %v", err)
	}
	admitter := &captureAdmitter{result: gateway.AdmissionResult{
		RequestID:     "request-queued-1",
		ConfigVersion: "v1",
		TurnSeq:       1,
	}}
	source := &staticExecutionEventSource{events: []gateway.ExecutionEvent{{
		Sequence: 1,
		Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			ID:      "event-1",
			Object:  model.ObjectTypeChatCompletion,
			Done:    true,
			Choices: []model.Choice{{Message: model.NewAssistantMessage("accepted")}},
		}),
	}}}
	queued, err := gateway.NewQueuedRunner(gateway.New(admitter), source)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}

	events, err := queued.Run(ctx, "untrusted-user", "untrusted-session", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatalf("run queued request: %v", err)
	}
	got, ok := <-events
	if !ok {
		t.Fatal("queued runner closed without persisted event")
	}
	if got.Response == nil || got.Response.ID != "event-1" {
		t.Fatalf("forwarded event = %#v", got)
	}
	if _, ok := <-events; ok {
		t.Fatal("queued runner did not close after persisted stream")
	}
	if admitter.request.RequestID != "request-queued-1" ||
		admitter.request.IdempotencyKey != "idempotency-queued-1" {
		t.Fatalf("admitted request = %#v", admitter.request)
	}
	if admitter.request.Identity.Tenant != identity.Tenant {
		t.Fatalf("admitted identity tenant = %#v, want %#v", admitter.request.Identity.Tenant, identity.Tenant)
	}
	if source.scope != identity.Tenant.Scope() || source.requestID != "request-queued-1" || source.after != 0 {
		t.Fatalf("event subscription = %#v", source)
	}
}

func TestQueuedRunnerSkipsInternalEventsWhenReplayingExecution(t *testing.T) {
	identity := validAdmissionIdentity()
	ctx, err := gateway.ContextWithAuthenticatedRequest(context.Background(), gateway.AuthenticatedRequest{
		RequestID:      "request-queued-replay",
		IdempotencyKey: "idempotency-queued-replay",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
	})
	if err != nil {
		t.Fatalf("attach authenticated request: %v", err)
	}
	source := &staticExecutionEventSource{events: []gateway.ExecutionEvent{
		{Sequence: 1, Event: event.New("invocation-1", "assistant")},
		{Sequence: 2, Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object:  model.ObjectTypeChatCompletion,
			Done:    true,
			Choices: []model.Choice{{Message: model.NewAssistantMessage("recovered")}},
		})},
		{Sequence: 3, Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object: model.ObjectTypeRunnerCompletion,
			Done:   true,
		})},
	}}
	queued, err := gateway.NewQueuedRunner(
		gateway.New(&captureAdmitter{result: gateway.AdmissionResult{
			RequestID: "request-queued-replay", ConfigVersion: "v1", TurnSeq: 1,
		}}),
		source,
	)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}

	forwarded, err := queued.Run(ctx, "", "", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatalf("run queued request: %v", err)
	}
	var got []*event.Event
	for evt := range forwarded {
		got = append(got, evt)
	}
	if len(got) != 1 || got[0].Response == nil ||
		got[0].Response.Object != model.ObjectTypeChatCompletion {
		t.Fatalf("forwarded replay events = %#v", got)
	}
}

func TestQueuedRunnerProjectsOnlyClientCompletionEvents(t *testing.T) {
	identity := validAdmissionIdentity()
	ctx, err := gateway.ContextWithAuthenticatedRequest(context.Background(), gateway.AuthenticatedRequest{
		RequestID: "request-queued-projection", IdempotencyKey: "idempotency-queued-projection",
		Tenant: staticTenantResolver{
			tenant: identity.Tenant, source: identity.Source, identity: identity, withIdentity: true,
		},
	})
	if err != nil {
		t.Fatalf("attach authenticated request: %v", err)
	}
	source := &staticExecutionEventSource{events: []gateway.ExecutionEvent{
		{Sequence: 1, Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object:  model.ObjectTypeChatCompletionChunk,
			Choices: []model.Choice{{Delta: model.Message{Role: model.RoleAssistant, Content: "part"}}},
		})},
		{Sequence: 2, Event: event.NewResponseEvent("invocation-1", "tool", &model.Response{
			Object:  model.ObjectTypeChatCompletion,
			Choices: []model.Choice{{Message: model.Message{ToolID: "tool-1"}}},
		})},
		{Sequence: 3, Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object: model.ObjectTypeError,
			Done:   true,
			Error:  &model.ResponseError{Type: "internal", Message: "internal failure"},
		})},
		{Sequence: 4, Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object:  model.ObjectTypeChatCompletion,
			Done:    true,
			Choices: []model.Choice{{Message: model.NewAssistantMessage("final")}},
		})},
		{Sequence: 5, Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object: model.ObjectTypeRunnerCompletion,
			Done:   true,
		})},
	}}
	queued, err := gateway.NewQueuedRunner(
		gateway.New(&captureAdmitter{result: gateway.AdmissionResult{
			RequestID: "request-queued-projection", ConfigVersion: "v1", TurnSeq: 1,
		}}),
		source,
	)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}
	forwarded, err := queued.Run(ctx, "", "", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatalf("run queued request: %v", err)
	}
	var got []*event.Event
	for evt := range forwarded {
		got = append(got, evt)
	}
	if len(got) != 2 || got[0].Response == nil || got[0].Response.Object != model.ObjectTypeChatCompletionChunk ||
		got[1].Response == nil || got[1].Response.Object != model.ObjectTypeChatCompletion {
		t.Fatalf("forwarded projection events = %#v", got)
	}
}

func TestQueuedRunnerRejectsRuntimeOptions(t *testing.T) {
	identity := validAdmissionIdentity()
	ctx, err := gateway.ContextWithAuthenticatedRequest(context.Background(), gateway.AuthenticatedRequest{
		RequestID:      "request-queued-2",
		IdempotencyKey: "idempotency-queued-2",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
	})
	if err != nil {
		t.Fatalf("attach authenticated request: %v", err)
	}
	admitter := &captureAdmitter{result: gateway.AdmissionResult{
		RequestID:     "request-queued-2",
		ConfigVersion: "v1",
		TurnSeq:       1,
	}}
	queued, err := gateway.NewQueuedRunner(
		gateway.New(admitter),
		&staticExecutionEventSource{},
	)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}

	_, err = queued.Run(
		ctx,
		"user-1",
		"session-1",
		model.NewUserMessage("hello"),
		agent.MergeRuntimeState(map[string]any{"tenant_id": "other"}),
	)
	if err == nil || !strings.Contains(err.Error(), "runner options are not supported") {
		t.Fatalf("queued runner error = %v", err)
	}
	if admitter.request.RequestID != "" {
		t.Fatal("queued runner admitted a request with runtime options")
	}
}

func TestForwardExecutionEventsStopsWhenSourceNeverCloses(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	identity := validAdmissionIdentity()
	ctx, err := gateway.ContextWithAuthenticatedRequest(base, gateway.AuthenticatedRequest{
		RequestID:      "request-queued-never-closes",
		IdempotencyKey: "idempotency-queued-never-closes",
		Tenant: staticTenantResolver{
			tenant: identity.Tenant, source: identity.Source, identity: identity, withIdentity: true,
		},
	})
	if err != nil {
		t.Fatalf("attach authenticated request: %v", err)
	}
	persisted := make(chan gateway.ExecutionEvent)
	queued, err := gateway.NewQueuedRunner(
		gateway.New(&captureAdmitter{result: gateway.AdmissionResult{
			RequestID: "request-queued-never-closes", ConfigVersion: "v1", TurnSeq: 1,
		}}),
		&neverClosingExecutionEventSource{events: persisted},
	)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}
	forwarded, err := queued.Run(ctx, "", "", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatalf("run queued request: %v", err)
	}
	cancel()
	select {
	case _, ok := <-forwarded:
		if ok {
			t.Fatal("forwarded event channel remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("forwarded event channel did not close after cancellation")
	}
}

type neverClosingExecutionEventSource struct {
	events <-chan gateway.ExecutionEvent
}

func (s *neverClosingExecutionEventSource) SubscribeExecutionEvents(
	context.Context,
	tenant.Scope,
	string,
	int64,
) (<-chan gateway.ExecutionEvent, error) {
	return s.events, nil
}

type staticExecutionEventSource struct {
	events    []gateway.ExecutionEvent
	scope     tenant.Scope
	requestID string
	after     int64
}

func (s *staticExecutionEventSource) SubscribeExecutionEvents(
	_ context.Context,
	scope tenant.Scope,
	requestID string,
	afterSequence int64,
) (<-chan gateway.ExecutionEvent, error) {
	s.scope = scope
	s.requestID = requestID
	s.after = afterSequence
	stream := make(chan gateway.ExecutionEvent, len(s.events))
	for _, item := range s.events {
		stream <- item
	}
	close(stream)
	return stream, nil
}
