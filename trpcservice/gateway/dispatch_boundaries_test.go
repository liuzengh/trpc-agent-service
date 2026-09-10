package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/execution"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
)

func TestDispatchStoreViewFailsClosedAndDelegatesNarrowCapabilities(t *testing.T) {
	var empty dispatchStoreView
	checks := []struct {
		name string
		call func() error
	}{
		{"GetSession", func() error { _, err := empty.GetSession(context.Background(), "tenant", "session"); return err }},
		{"CreateSession", func() error {
			_, err := empty.CreateSession(context.Background(), "tenant", "session", nil)
			return err
		}},
		{"UpdateSessionState", func() error {
			_, err := empty.UpdateSessionState(context.Background(), "tenant", "session", 1, nil)
			return err
		}},
		{"DeleteSession", func() error { return empty.DeleteSession(context.Background(), "tenant", "session") }},
		{"RecordMessage", func() error {
			_, _, err := empty.RecordMessage(context.Background(), runtimestorage.MessageEventInput{})
			return err
		}},
		{"GetMessage", func() error { _, err := empty.GetMessage(context.Background(), "tenant", "event"); return err }},
		{"TransitionMessage", func() error {
			_, err := empty.TransitionMessage(context.Background(), runtimestorage.MessageTransition{})
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if !errors.Is(check.call(), runtimestorage.ErrInvalid) {
				t.Fatalf("%s accepted a missing capability", check.name)
			}
		})
	}

	store := inmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	view := dispatchStoreView{sessions: store, messages: store}
	if _, err := view.GetSession(context.Background(), "tenant", "session"); err == nil {
		t.Fatal("delegated GetSession unexpectedly succeeded for a missing session")
	}
	if _, err := view.CreateSession(context.Background(), "tenant", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := view.UpdateSessionState(context.Background(), "tenant", "session", 1, map[string]any{"key": "value"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := view.RecordMessage(context.Background(), runtimestorage.MessageEventInput{
		TenantID: "tenant", EventID: "event", SessionID: "session", BindingID: "binding", ExternalMessageID: "external",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := view.GetMessage(context.Background(), "tenant", "event"); err != nil {
		t.Fatal(err)
	}
	if _, err := view.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "owner", LeaseDuration: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if err := view.DeleteSession(context.Background(), "tenant", "session"); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeDispatchRequestAndDetachedCorrelationBoundaries(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
	valid := DispatchRequest{Principal: principal, Message: InboundMessage{
		Content: " hello ", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer",
	}, RequestID: "request", TraceID: "trace"}
	message, requestID, traceID, err := normalizeDispatchRequest(context.Background(), valid)
	if err != nil || message.Content != "hello" || requestID != "request" || traceID != "trace" {
		t.Fatalf("normalized request = %+v requestID=%q traceID=%q err=%v", message, requestID, traceID, err)
	}
	generated, requestID, traceID, err := normalizeDispatchRequest(context.Background(), DispatchRequest{Principal: principal, Message: valid.Message})
	if err != nil || generated.Content != "hello" || requestID == "" || traceID != "" {
		t.Fatalf("generated correlation = %+v requestID=%q traceID=%q err=%v", generated, requestID, traceID, err)
	}

	var nilContext context.Context
	if _, _, _, err := normalizeDispatchRequest(nilContext, valid); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, _, err := normalizeDispatchRequest(canceled, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	if _, _, _, err := normalizeDispatchRequest(context.Background(), DispatchRequest{Message: valid.Message}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("invalid principal error = %v", err)
	}
	invalidMessage := valid
	invalidMessage.Message.ContentType = "unsupported"
	if _, _, _, err := normalizeDispatchRequest(context.Background(), invalidMessage); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid message error = %v", err)
	}
	if got := observability.RequestID(detachedCorrelationContext(nil, "request", "trace")); got != "request" {
		t.Fatalf("detached request ID = %q", got)
	}
	if got := observability.TraceID(detachedCorrelationContext(context.Background(), "request", "trace")); got != "trace" {
		t.Fatalf("detached trace ID = %q", got)
	}
}

func TestClaimInboundAndPrepareInboundEventDefensiveBranches(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal := mustChannelPrincipal(t, target)
	store := inmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	dispatcher := &Dispatcher{runtimeStore: store, telemetry: observability.NewNoopProvider(), metrics: metrics.New(observability.NewNoopProvider())}
	metadata := dispatchMetadata{
		principal: principal,
		message:   InboundMessage{ExternalMessageID: "external", ConversationKind: "unsupported"},
	}
	metadata.identity, _ = tenant.NewRunnerIdentity(principal.TenantID(), "user", "session")
	if _, err := dispatcher.claimInboundWithLease(context.Background(), metadata, time.Minute); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid reply target error = %v", err)
	}
	metadata.message.ConversationKind = channels.ConversationDirect
	metadata.message.ExternalPeerID = "peer"
	metadata.message.ExternalMessageID = ""
	if _, err := dispatcher.claimInboundWithLease(context.Background(), metadata, time.Minute); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing external message ID error = %v", err)
	}

	transitionStore := &duplicateClaimStore{gatewayStore: store, event: runtimestorage.MessageEvent{TenantID: principal.TenantID(), EventID: "event", SessionID: "session", Status: runtimestorage.EventReceived, ReplyTarget: runtimestorage.ReplyTarget{BindingID: target.BindingID, ConversationKind: string(channels.ConversationDirect), ReceiverID: "peer"}}, transitionErr: runtimestorage.ErrConflict}
	dispatcher.runtimeStore = transitionStore
	metadata.message.ExternalMessageID = "external"
	if _, err := dispatcher.claimInboundWithLease(context.Background(), metadata, time.Minute); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate conflict error = %v", err)
	}

	expired := time.Now().UTC().Add(-time.Minute)
	if _, err := prepareInboundEvent(context.Background(), transitionStore, inboundEventPreparation{
		tenantID: principal.TenantID(), event: runtimestorage.MessageEvent{EventID: "event", Status: runtimestorage.EventRunning, LeaseExpiresAt: &expired}, duplicate: true, owner: "owner",
	}); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("failed reclaim transition error = %v", err)
	}
	if _, err := prepareInboundEvent(context.Background(), transitionStore, inboundEventPreparation{
		event: runtimestorage.MessageEvent{EventID: "event", Status: runtimestorage.EventCompleted}, duplicate: true,
	}); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("terminal duplicate error = %v", err)
	}
}

func TestExecutionForwardStateClassifiesTerminalOutcomes(t *testing.T) {
	tests := []struct {
		name      string
		event     execution.Event
		wantErr   error
		wantEvent audit.EventType
		wantClass string
	}{
		{name: "nil provider error", event: execution.Event{Type: execution.EventError}, wantErr: ErrExecution, wantEvent: audit.EventExecutionFailed, wantClass: string(audit.ErrorUnavailable)},
		{name: "canceled error", event: execution.Event{Type: execution.EventError, Err: context.Canceled}, wantErr: context.Canceled, wantEvent: audit.EventExecutionCanceled, wantClass: string(audit.ErrorCanceled)},
		{name: "error status", event: execution.Event{Type: execution.EventDone, Status: "error", Done: true}, wantErr: ErrExecution, wantEvent: audit.EventExecutionFailed, wantClass: string(audit.ErrorUnavailable)},
		{name: "canceled status", event: execution.Event{Type: execution.EventDone, Status: "canceled", Done: true}, wantErr: context.Canceled, wantEvent: audit.EventExecutionCanceled, wantClass: string(audit.ErrorCanceled)},
		{name: "deadline status", event: execution.Event{Type: execution.EventDone, Status: "deadline_exceeded", Done: true}, wantErr: context.DeadlineExceeded, wantEvent: audit.EventExecutionCanceled, wantClass: string(audit.ErrorCanceled)},
		{name: "complete status", event: execution.Event{Type: execution.EventDone, Status: "complete", Done: true}, wantEvent: audit.EventExecutionCompleted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := executionForwardState{}
			state.observe(test.event)
			if test.wantErr != nil && !errors.Is(state.terminalErr, test.wantErr) {
				t.Fatalf("terminal error = %v, want %v", state.terminalErr, test.wantErr)
			}
			if state.terminalEventType != test.wantEvent || state.terminalErrorType != test.wantClass {
				t.Fatalf("terminal audit = %q/%q, want %q/%q", state.terminalEventType, state.terminalErrorType, test.wantEvent, test.wantClass)
			}
		})
	}
	state := executionForwardState{}
	state.observe(execution.Event{Type: execution.EventMessage, Text: "hello"})
	state.observe(execution.Event{Type: execution.EventStatus, Status: "progress"})
	if state.reply.String() != "hello" || state.terminalSeen {
		t.Fatalf("non-terminal state = %+v", state)
	}
	if !state.skip(execution.Event{Type: execution.EventDone, Done: true}) || !state.skip(execution.Event{Type: execution.EventError, Err: context.Canceled}) || state.skip(execution.Event{Type: execution.EventMessage}) {
		t.Fatal("execution event skip policy is incorrect")
	}
}

func TestExecutionForwardStateEnsuresTerminalAndMarksSendFailure(t *testing.T) {
	state := executionForwardState{}
	state.ensureTerminal(context.Background())
	if state.terminalEventType != audit.EventExecutionCompleted {
		t.Fatalf("default terminal event = %q", state.terminalEventType)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	state = executionForwardState{}
	state.ensureTerminal(canceled)
	if !errors.Is(state.terminalErr, context.Canceled) || state.terminalEventType != audit.EventExecutionCanceled {
		t.Fatalf("canceled terminal state = %+v", state)
	}
	state = executionForwardState{terminalSeen: true}
	state.ensureTerminal(canceled)
	if state.terminalErr != nil {
		t.Fatal("existing terminal event was overwritten")
	}
	state = executionForwardState{}
	state.markSendFailure(context.Background())
	if !errors.Is(state.terminalErr, context.Canceled) || state.terminalEventType != audit.EventExecutionCanceled || state.terminalErrorEmitted {
		t.Fatalf("send failure state = %+v", state)
	}
}

func TestForwardExecutionStopsOnCanceledOutputContext(t *testing.T) {
	dispatcher, _ := newTestDispatcher(t, &testRunner{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	events := make(chan execution.Event, 1)
	events <- execution.Event{Type: execution.EventMessage, Text: "not delivered"}
	close(events)
	_, span := dispatcher.telemetry.Tracer("test").Start(context.Background(), "execution")
	output := make(chan DispatchEvent, 4)
	dispatcher.forwardExecution(ctx, &dispatchExecution{
		metadata: dispatchMetadata{requestID: "request", traceID: "trace"}, executionEvents: events,
		output: output, span: span, started: time.Now(),
	})
	closeEvents := collectDispatchEvents(output)
	if len(closeEvents) != 2 || closeEvents[0].Type != DispatchEventError || closeEvents[1].Type != DispatchEventDone {
		t.Fatalf("canceled output events = %+v", closeEvents)
	}
}

func TestFinishDurableUsesMediaFallbackAfterMaterializationFailure(t *testing.T) {
	batch := &fallbackBatchStore{}
	materializer, err := outbox.NewMaterializer(outbox.MaterializerConfig{BatchStore: batch})
	if err != nil {
		t.Fatal(err)
	}
	store := &durableTransitionStore{}
	provider := observability.NewNoopProvider()
	dispatcher := &Dispatcher{materializer: materializer, telemetry: provider, metrics: metrics.New(provider)}
	target := runtimestorage.ReplyTarget{BindingID: "binding", ConversationKind: string(channels.ConversationDirect), ReceiverID: "peer"}
	dispatcher.finishDurable(context.Background(), dispatchMetadata{requestID: "request", traceID: "trace"}, &durableExecution{
		store: store, tenantID: "tenant", eventID: "event", owner: "owner", fencingToken: 1, replyTarget: target,
	}, nil, "ignored", []servicetool.ReplyIntent{{Kind: runtimestorage.ReplyKindText, Payload: "media intent"}})
	if batch.calls != 2 {
		t.Fatalf("materializer calls = %d, want fallback retry", batch.calls)
	}
	if len(store.transitions) != 1 || store.transitions[0].To != runtimestorage.EventCompleted || store.transitions[0].ReplyID != "event" || store.transitions[0].SegmentCount != 1 {
		t.Fatalf("durable transition = %+v", store.transitions)
	}
}

func TestObserveStorageReportsOperationAndBoundaryErrors(t *testing.T) {
	var nilDispatcher *Dispatcher
	if err := nilDispatcher.observeStorage(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil dispatcher error = %v", err)
	}
	provider := observability.NewNoopProvider()
	dispatcher := &Dispatcher{telemetry: provider, metrics: metrics.New(provider)}
	wantErr := errors.New("storage detail")
	if err := dispatcher.observeStorage(context.Background(), func(context.Context) error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("storage operation error = %v", err)
	}
}

func TestMapExecutionEventAndCorrelationCancellationVariants(t *testing.T) {
	if got := mapExecutionEvent(execution.Event{Type: execution.EventError, Err: context.Canceled, RequestID: "request"}); got.Error != ErrExecutionCanceled.Error() {
		t.Fatalf("canceled mapped event = %+v", got)
	}
	if got := mapExecutionEvent(execution.Event{Type: execution.EventError, Err: errors.New("provider secret")}); got.Error != ErrExecution.Error() {
		t.Fatalf("failed mapped event = %+v", got)
	}
	if got := cancellationStatus(context.Background()); got != "canceled" {
		t.Fatalf("normal cancellation status = %q", got)
	}
	deadline, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if got := cancellationStatus(deadline); got != "deadline_exceeded" {
		t.Fatalf("deadline cancellation status = %q", got)
	}
	if sendDispatchEvent(nil, make(chan DispatchEvent, 1), DispatchEvent{}) {
		t.Fatal("nil context accepted dispatch event")
	}
}

type duplicateClaimStore struct {
	gatewayStore
	event         runtimestorage.MessageEvent
	transitionErr error
}

func (store *duplicateClaimStore) RecordMessage(context.Context, runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	return store.event, true, nil
}

func (store *duplicateClaimStore) TransitionMessage(context.Context, runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	return store.event, store.transitionErr
}

type durableTransitionStore struct {
	transitions []runtimestorage.MessageTransition
}

func (store *durableTransitionStore) RecordMessage(context.Context, runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	return runtimestorage.MessageEvent{}, false, nil
}

func (store *durableTransitionStore) GetMessage(context.Context, string, string) (runtimestorage.MessageEvent, error) {
	return runtimestorage.MessageEvent{}, runtimestorage.ErrNotFound
}

func (store *durableTransitionStore) TransitionMessage(_ context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	store.transitions = append(store.transitions, transition)
	return runtimestorage.MessageEvent{Status: transition.To}, nil
}

type fallbackBatchStore struct {
	calls int
}

func (store *fallbackBatchStore) EnqueueReplies(context.Context, []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	return nil, nil
}

func (store *fallbackBatchStore) EnqueueRepliesWithCorrelation(_ context.Context, _ runtimestorage.ReplyCorrelation, replies []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	store.calls++
	if store.calls == 1 {
		return nil, runtimestorage.ErrStorage
	}
	return replies, nil
}
