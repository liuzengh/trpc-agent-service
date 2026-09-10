package gateway

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type capturedRun struct {
	mu        sync.Mutex
	userID    string
	sessionID string
	message   trpcmodel.Message
	requestID string
}

type durableOutboxProvider struct {
	deliveries []runtimestorage.ReplyOutbox
}

type auditWriterFailure struct {
	calls     int
	failAfter int
}

type handoffStub struct {
	reserveErr, finalizeErr error
	reserved, finalized     int32
}

func (s *handoffStub) Reserve(context.Context, audit.ExecutionHandoff) (audit.ExecutionHandoff, error) {
	atomic.AddInt32(&s.reserved, 1)
	if s.reserveErr != nil {
		return audit.ExecutionHandoff{}, s.reserveErr
	}
	return audit.ExecutionHandoff{State: audit.HandoffPending}, nil
}
func (s *handoffStub) Finalize(context.Context, audit.ExecutionHandoff) (audit.ExecutionHandoff, error) {
	atomic.AddInt32(&s.finalized, 1)
	if s.finalizeErr != nil {
		return audit.ExecutionHandoff{}, s.finalizeErr
	}
	return audit.ExecutionHandoff{State: audit.HandoffFinalized}, nil
}
func (*handoffStub) Get(context.Context, string, string) (audit.ExecutionHandoff, error) {
	return audit.ExecutionHandoff{}, audit.ErrHandoffNotFound
}

func (w *auditWriterFailure) Append(_ context.Context, event audit.Event) (audit.AppendResult, error) {
	w.calls++
	if w.calls > w.failAfter {
		return audit.AppendResult{}, errors.New("audit unavailable")
	}
	return audit.AppendResult{Event: event}, nil
}

func (p *durableOutboxProvider) Deliver(_ context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	p.deliveries = append(p.deliveries, value)
	return "provider-" + value.ReplyID, nil
}

func (*durableOutboxProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryUnknown, "", nil
}

type gatewayStore interface {
	sessionstorage.SessionStateStore
	runtimestorage.MessageStore
	runtimestorage.ReplyStore
}

type claimStoreStub struct {
	gatewayStore
	getErr, createErr, recordErr, transitionErr error
}

type transitionCaptureStore struct {
	gatewayStore
	mu          sync.Mutex
	transitions []runtimestorage.MessageTransition
}

type dispatchAttachmentStore struct {
	bindFn func(context.Context, string, string, []attachment.Reference) error
	loadFn func(context.Context, string, string, attachment.Reference) (attachment.Content, error)
}

type failingToolAttachmentStore struct {
	runtimestorage.AttachmentStore
}

func (failingToolAttachmentStore) PutAttachment(context.Context, string, attachment.Upload, io.Reader) (attachment.Reference, error) {
	return attachment.Reference{}, errors.New("attachment storage failed")
}

func (s dispatchAttachmentStore) BindAttachments(ctx context.Context, tenantID, eventID string, references []attachment.Reference) error {
	if s.bindFn != nil {
		return s.bindFn(ctx, tenantID, eventID, references)
	}
	return nil
}

func (s dispatchAttachmentStore) Load(ctx context.Context, tenantID, eventID string, reference attachment.Reference) (attachment.Content, error) {
	if s.loadFn != nil {
		return s.loadFn(ctx, tenantID, eventID, reference)
	}
	return attachment.Content{}, errors.New("attachment unavailable")
}
func (s *claimStoreStub) GetSession(context.Context, string, string) (sessionstorage.Session, error) {
	if s.getErr != nil {
		return sessionstorage.Session{}, s.getErr
	}
	return s.gatewayStore.GetSession(context.Background(), "unused", "unused")
}
func (s *claimStoreStub) CreateSession(ctx context.Context, tenantID, sessionID string, state map[string]any) (sessionstorage.Session, error) {
	if s.createErr != nil {
		return sessionstorage.Session{}, s.createErr
	}
	return s.gatewayStore.CreateSession(ctx, tenantID, sessionID, state)
}
func (s *claimStoreStub) RecordMessage(context.Context, runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	if s.recordErr != nil {
		return runtimestorage.MessageEvent{}, false, s.recordErr
	}
	return runtimestorage.MessageEvent{}, false, nil
}
func (s *claimStoreStub) TransitionMessage(context.Context, runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	if s.transitionErr != nil {
		return runtimestorage.MessageEvent{}, s.transitionErr
	}
	return runtimestorage.MessageEvent{}, nil
}

func (s *transitionCaptureStore) TransitionMessage(ctx context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	s.mu.Lock()
	s.transitions = append(s.transitions, transition)
	s.mu.Unlock()
	return s.gatewayStore.TransitionMessage(ctx, transition)
}

func (s *transitionCaptureStore) runningLease() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, transition := range s.transitions {
		if transition.To == runtimestorage.EventRunning {
			return transition.LeaseDuration, true
		}
	}
	return 0, false
}

func newTestDispatcher(t *testing.T, runnerValue *testRunner) (*Dispatcher, Principal) {
	return newTestDispatcherWithFactory(t, func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return runnerValue, nil
	})
}

func newTestDispatcherWithFactory(t *testing.T, factory runtimerunner.RunnerFactory) (*Dispatcher, Principal) {
	t.Helper()
	fixture := newGatewayFixture(t)
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: factory})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, DrainTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return dispatcher, mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
}

func TestReplyTargetUsesConversationDestination(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	tests := []struct {
		name    string
		message InboundMessage
		want    runtimestorage.ReplyTarget
		wantErr bool
	}{
		{
			name:    "direct peer",
			message: InboundMessage{ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1", ExternalThreadID: "thread-1"},
			want:    runtimestorage.ReplyTarget{BindingID: target.BindingID, ConversationKind: "direct", ReceiverID: "peer-1", ThreadID: "thread-1"},
		},
		{
			name:    "group chat",
			message: InboundMessage{ConversationKind: channels.ConversationGroup, ExternalChatID: "chat-1", ExternalThreadID: "thread-1"},
			want:    runtimestorage.ReplyTarget{BindingID: target.BindingID, ConversationKind: "group", ReceiverID: "chat-1", ThreadID: "thread-1"},
		},
		{name: "unknown conversation", message: InboundMessage{ConversationKind: "unknown"}, wantErr: true},
		{name: "missing direct peer", message: InboundMessage{ConversationKind: channels.ConversationDirect}, wantErr: true},
		{name: "blank group chat", message: InboundMessage{ConversationKind: channels.ConversationGroup, ExternalChatID: " "}, wantErr: true},
		{name: "invalid thread", message: InboundMessage{ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1", ExternalThreadID: "thread\n"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := replyTarget(target, tt.message)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("error = %v, want invalid", err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("reply target = %+v err %v, want %+v", got, err, tt.want)
			}
		})
	}
}

func TestDispatcherMapsEventsAndPropagatesIdentityAndRequestID(t *testing.T) {
	runnerValue := &testRunner{}
	var captured capturedRun
	runnerValue.runFn = func(_ context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		settings := trpcagent.RunOptions{}
		for _, option := range options {
			option(&settings)
		}
		captured.mu.Lock()
		captured.userID, captured.sessionID, captured.message, captured.requestID = userID, sessionID, message, settings.RequestID
		captured.mu.Unlock()
		events := make(chan *trpcevent.Event, 2)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "hello"}}}}}
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}
	dispatcher, principal := newTestDispatcher(t, runnerValue)
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal,
		Message: InboundMessage{
			Content: "  hello  ", ExternalUserID: "external-user", ConversationKind: channels.ConversationDirect,
			ExternalPeerID: "external-peer",
		},
		TraceID: "trace-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Type != DispatchEventMessage || events[0].Text != "hello" || !events[1].Done {
		t.Fatalf("dispatch events = %+v", events)
	}
	requestID := events[0].RequestID
	if requestID == "" || requestID != events[1].RequestID || events[0].TraceID != "trace-1" {
		t.Fatalf("event correlation = %+v", events)
	}
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if captured.userID == "" || captured.sessionID == "" || captured.message.Content != "hello" || captured.requestID != requestID {
		t.Fatalf("captured Runner call user=%q session=%q content=%q request=%q", captured.userID, captured.sessionID, captured.message.Content, captured.requestID)
	}
}

func TestDispatcherWritesExecutionAuditLifecycle(t *testing.T) {
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}})
	writer, err := audit.NewInMemory(principal.TenantID())
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.auditWriter = writer
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = collectDispatchEvents(stream)
	events, err := writer.List(context.Background(), audit.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || !hasAuditEventTypes(events, audit.EventExecutionStarted, audit.EventExecutionCompleted) {
		t.Fatalf("audit lifecycle = %#v", events)
	}
}

func TestDispatcherAuditFailureIsRedactedAndCorrelated(t *testing.T) {
	runner := &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		return nil, errors.New("provider secret should not escape")
	}}
	dispatcher, principal := newTestDispatcher(t, runner)
	dispatcher.auditWriter = &auditWriterFailure{failAfter: 1}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if !errors.Is(err, ErrExecution) {
		t.Fatalf("dispatch error = %v", err)
	}
	if stream != nil {
		t.Fatal("failed pre-stream audit should not return a stream")
	}

	runner = &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	dispatcher, principal = newTestDispatcher(t, runner)
	dispatcher.auditWriter = &auditWriterFailure{failAfter: 1}
	stream, err = dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Error != ErrAuditWriteFailed.Error() || events[0].RequestID == "" || events[1].Status != "error" || events[1].RequestID != events[0].RequestID {
		t.Fatalf("audit failure events = %#v", events)
	}
}

func TestDispatcherHandoffReserveFailurePreventsRunner(t *testing.T) {
	var calls atomic.Int32
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		calls.Add(1)
		return nil, nil
	}})
	dispatcher.handoffStore = &handoffStub{reserveErr: errors.New("handoff unavailable")}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if !errors.Is(err, ErrAuditWriteFailed) || stream != nil {
		t.Fatalf("stream=%v err=%v", stream, err)
	}
	if calls.Load() != 0 {
		t.Fatal("Runner started before handoff reserve")
	}
}

func TestDispatcherHandoffFinalizeFailureIsRedacted(t *testing.T) {
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}})
	dispatcher.handoffStore = &handoffStub{finalizeErr: errors.New("handoff finalize unavailable")}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Error != ErrAuditWriteFailed.Error() || events[1].Status != "error" {
		t.Fatalf("events=%+v", events)
	}
}

func TestDispatcherCancellationFinalizesAuditAndHandoff(t *testing.T) {
	runnerEvents := make(chan *trpcevent.Event)
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		return runnerEvents, nil
	}})
	writer, err := audit.NewInMemory(principal.TenantID())
	if err != nil {
		t.Fatal(err)
	}
	handoffs := audit.NewInMemoryHandoffStore()
	dispatcher.auditWriter, dispatcher.handoffStore = writer, handoffs
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := dispatcher.Dispatch(ctx, DispatchRequest{Principal: principal, RequestID: "cancel-request", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	close(runnerEvents)
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Error != ErrExecutionCanceled.Error() {
		t.Fatalf("events=%+v", events)
	}
	handoff, err := handoffs.Get(context.Background(), principal.TenantID(), audit.NewEventID("cancel-request", "handoff"))
	if err != nil {
		t.Fatal(err)
	}
	if handoff.State != audit.HandoffFinalized || handoff.Result != audit.ResultCanceled {
		t.Fatalf("handoff=%+v", handoff)
	}
	auditEvents, err := writer.List(context.Background(), audit.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasAuditEventTypes(auditEvents, audit.EventExecutionStarted, audit.EventExecutionCanceled) {
		t.Fatalf("audit=%+v", auditEvents)
	}
}

func TestDispatcherAuditsCanarySelectionBeforeExecutionStart(t *testing.T) {
	fixture := newGatewayFixture(t)
	current, candidate := int64(1), int64(2)
	appRoot := fixture.app.Clone()
	appRoot.CurrentRevision, appRoot.CanaryRevision = &current, &candidate
	candidateRevision := fixture.revision.Clone()
	candidateRevision.Revision = candidate
	resolverConfig := resolverTestConfig(fixture)
	resolverConfig.Apps = resolverAgentRepository{Repository: fixture.apps, getFn: func(context.Context, string, string) (*appmodel.App, error) { return &appRoot, nil }, getRevisionFn: func(_ context.Context, _ string, _ string, revision int64) (*appmodel.Revision, error) {
		if revision == candidate {
			return &candidateRevision, nil
		}
		return fixture.revision, nil
	}}
	resolver, err := NewPlanResolver(resolverConfig)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return events, nil
		}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := audit.NewInMemory(fixture.tenant.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.auditWriter = writer
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, appRoot.AppID)
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, RequestID: "canary-audit", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = collectDispatchEvents(stream)
	events, err := writer.List(context.Background(), audit.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 3 || events[0].EventType != audit.EventCanarySelected || events[0].Revision == nil || *events[0].Revision != candidate || events[1].EventType != audit.EventExecutionStarted {
		t.Fatalf("audit events=%+v", events)
	}
}

func TestDispatcherSelectCancellationBranchFinalizesCanceledOutcome(t *testing.T) {
	runnerStarted := make(chan struct{})
	runnerEvents := make(chan *trpcevent.Event)
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		close(runnerStarted)
		return runnerEvents, nil
	}})
	writer, err := audit.NewInMemory(principal.TenantID())
	if err != nil {
		t.Fatal(err)
	}
	handoffs := audit.NewInMemoryHandoffStore()
	dispatcher.auditWriter, dispatcher.handoffStore = writer, handoffs
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := dispatcher.Dispatch(ctx, DispatchRequest{Principal: principal, RequestID: "select-cancel", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-runnerStarted:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}
	cancel()
	events := collectDispatchEvents(stream)
	close(runnerEvents)
	if len(events) != 2 || events[0].Error != ErrExecutionCanceled.Error() || !events[1].Done {
		t.Fatalf("events=%+v", events)
	}
	handoff, err := handoffs.Get(context.Background(), principal.TenantID(), audit.NewEventID("select-cancel", "handoff"))
	if err != nil {
		t.Fatal(err)
	}
	if handoff.Result != audit.ResultCanceled || handoff.State != audit.HandoffFinalized {
		t.Fatalf("handoff=%+v", handoff)
	}
}

func TestDispatcherSelectCancellationAuditFailure(t *testing.T) {
	runnerStarted := make(chan struct{})
	runnerEvents := make(chan *trpcevent.Event)
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		close(runnerStarted)
		return runnerEvents, nil
	}})
	dispatcher.auditWriter = &auditWriterFailure{failAfter: 1}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := dispatcher.Dispatch(ctx, DispatchRequest{Principal: principal, RequestID: "select-cancel-audit-fail", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	<-runnerStarted
	cancel()
	events := collectDispatchEvents(stream)
	close(runnerEvents)
	if len(events) != 2 || events[0].Error != ErrAuditWriteFailed.Error() || events[1].Status != "error" {
		t.Fatalf("events=%+v", events)
	}
}

func TestDispatcherSelectCancellationHandoffFailure(t *testing.T) {
	runnerStarted := make(chan struct{})
	runnerEvents := make(chan *trpcevent.Event)
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		close(runnerStarted)
		return runnerEvents, nil
	}})
	dispatcher.handoffStore = &handoffStub{finalizeErr: errors.New("handoff unavailable")}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := dispatcher.Dispatch(ctx, DispatchRequest{Principal: principal, RequestID: "select-cancel-handoff-fail", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	<-runnerStarted
	cancel()
	events := collectDispatchEvents(stream)
	close(runnerEvents)
	if len(events) != 2 || events[0].Error != ErrAuditWriteFailed.Error() || events[1].Status != "error" {
		t.Fatalf("events=%+v", events)
	}
}

func TestDispatcherTerminalErrorFinalizesFailureHandoff(t *testing.T) {
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: "provider secret"}}}
		close(events)
		return events, nil
	}})
	handoffs := audit.NewInMemoryHandoffStore()
	dispatcher.handoffStore = handoffs
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, RequestID: "failure-request", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Error != ErrExecution.Error() || events[1].Status != "error" {
		t.Fatalf("events=%+v", events)
	}
	handoff, err := handoffs.Get(context.Background(), principal.TenantID(), audit.NewEventID("failure-request", "handoff"))
	if err != nil {
		t.Fatal(err)
	}
	if handoff.Result != audit.ResultFailure || handoff.ErrorType != string(audit.ErrorUnavailable) {
		t.Fatalf("handoff=%+v", handoff)
	}
}

func TestDispatcherRunnerBoundaryAuditFailures(t *testing.T) {
	for name, runFn := range map[string]func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error){
		"run error": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return nil, errors.New("provider error")
		},
		"nil stream": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return nil, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: runFn})
			dispatcher.auditWriter = &auditWriterFailure{failAfter: 1}
			stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
			if stream != nil || !errors.Is(err, ErrAuditWriteFailed) {
				t.Fatalf("stream=%v err=%v", stream, err)
			}
		})
	}
}

func TestDispatcherAcquireFailureWritesTerminalAudit(t *testing.T) {
	dispatcher, principal := newTestDispatcherWithFactory(t, func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return nil, errors.New("factory provider detail")
	})
	writer := &auditWriterFailure{failAfter: 1}
	dispatcher.auditWriter = writer
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, RequestID: "acquire-audit", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if stream != nil || !errors.Is(err, ErrAuditWriteFailed) {
		t.Fatalf("stream=%v err=%v", stream, err)
	}
}

func TestDispatcherAcquireFailureWithSuccessfulTerminalAudit(t *testing.T) {
	dispatcher, principal := newTestDispatcherWithFactory(t, func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return nil, errors.New("factory unavailable")
	})
	writer, err := audit.NewInMemory(principal.TenantID())
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.auditWriter = writer
	_, err = dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, RequestID: "acquire-success-audit", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if err == nil || errors.Is(err, ErrAuditWriteFailed) {
		t.Fatalf("unexpected acquire error=%v", err)
	}
	entries, err := writer.List(context.Background(), audit.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasAuditEventTypes(entries, audit.EventExecutionStarted, audit.EventExecutionFailed) {
		t.Fatalf("audit entries=%+v", entries)
	}
}

func TestWriteExecutionAuditUsesVerifiedChannelRoute(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, _ := newTestDispatcher(t, &testRunner{})
	writer, err := audit.NewInMemory(principal.TenantID())
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.auditWriter = writer
	identity := tenant.RunnerIdentity{SessionID: "session"}
	metadata := dispatchMetadata{principal: principal, message: InboundMessage{Content: "hello", ExternalUserID: "user"}, identity: identity, requestID: "request", traceID: "trace"}
	if err := dispatcher.writeExecutionAudit(context.Background(), metadata, audit.EventExecutionStarted, ""); err != nil {
		t.Fatal(err)
	}
	events, err := writer.List(context.Background(), audit.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Channel != string(target.Channel) {
		t.Fatalf("events=%+v", events)
	}
}

func TestDispatcherRunnerRunFailureAuditWriteIsRedacted(t *testing.T) {
	dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		return nil, errors.New("provider secret")
	}})
	dispatcher.auditWriter = &auditWriterFailure{failAfter: 1}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, RequestID: "run-audit", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if stream != nil || !errors.Is(err, ErrAuditWriteFailed) {
		t.Fatalf("stream=%v err=%v", stream, err)
	}
}

func TestDispatcherDefensiveNilRunnerAuditFailure(t *testing.T) {
	dispatcher, principal := newTestDispatcherWithFactory(t, func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return nil, nil
	})
	dispatcher.auditWriter = &auditWriterFailure{failAfter: 1}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, RequestID: "nil-runner-audit", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if stream != nil || !errors.Is(err, ErrAuditWriteFailed) {
		t.Fatalf("stream=%v err=%v", stream, err)
	}
}

func TestDispatcherDefensiveNilRunnerWritesFailedAudit(t *testing.T) {
	dispatcher, principal := newTestDispatcherWithFactory(t, func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return nil, nil
	})
	writer, err := audit.NewInMemory(principal.TenantID())
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.auditWriter = writer
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, RequestID: "nil-runner-terminal", Message: InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}})
	if stream != nil || !errors.Is(err, runtimerunner.ErrRunnerUnavailable) {
		t.Fatalf("stream=%v err=%v", stream, err)
	}
	events, err := writer.List(context.Background(), audit.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if !hasAuditEventTypes(events, audit.EventExecutionStarted, audit.EventExecutionFailed) {
		t.Fatalf("events=%+v", events)
	}
}

func TestAuditHelpers(t *testing.T) {
	if terminalAuditError(nil) != "" || terminalAuditError(context.Canceled) != string(audit.ErrorCanceled) || terminalAuditError(ErrExecution) != string(audit.ErrorUnavailable) {
		t.Fatal("unexpected terminal audit error mapping")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	output := make(chan DispatchEvent, 2)
	run := &dispatchExecution{metadata: dispatchMetadata{requestID: "request-1", traceID: "trace-1"}, output: output}
	run.finishForwardOutput(ctx, context.Canceled, false)
	close(output)
	events := collectDispatchEvents(output)
	if len(events) != 2 || events[0].RequestID != "request-1" || events[0].TraceID != "trace-1" || events[1].RequestID != "request-1" {
		t.Fatalf("helper events = %#v", events)
	}
}

func TestExecutionAuditEventIDIsBoundedForMaximumRequestID(t *testing.T) {
	if got := audit.NewEventID(strings.Repeat("r", 256), string(audit.EventExecutionStarted)); len(got) > 256 {
		t.Fatalf("audit event id length = %d", len(got))
	}
}

func hasAuditEventTypes(events []audit.Event, want ...audit.EventType) bool {
	seen := map[audit.EventType]bool{}
	for _, event := range events {
		seen[event.EventType] = true
	}
	for _, eventType := range want {
		if !seen[eventType] {
			return false
		}
	}
	return true
}

func TestDispatcherUsesVerifiedChannelIdentity(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	channelPrincipal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	var captured capturedRun
	runnerValue := &testRunner{}
	runnerValue.runFn = func(_ context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		captured.mu.Lock()
		captured.userID, captured.sessionID, captured.message = userID, sessionID, message
		captured.mu.Unlock()
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, DrainTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: channelPrincipal,
		Message: InboundMessage{
			Content: "group message", ExternalUserID: "user-1", ConversationKind: channels.ConversationGroup,
			ExternalChatID: "chat-1", ExternalThreadID: "thread-1",
		},
		RequestID: "request-channel",
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 1 || !events[0].Done {
		t.Fatalf("channel dispatch events = %+v", events)
	}
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if !strings.Contains(captured.userID, target.BindingID) || !strings.Contains(captured.sessionID, target.BindingID) {
		t.Fatalf("channel identity omitted Binding scope: user=%q session=%q", captured.userID, captured.sessionID)
	}
}

func TestDispatcherDurableChannelClaimSuppressesDuplicateRunner(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	runnerValue := &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = registry.Close() }()
	store := inmemory.New()
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, DrainTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	request := DispatchRequest{Principal: principal, Message: InboundMessage{Content: "duplicate", ExternalMessageID: "channel-message-1", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}, RequestID: "channel-message-1"}
	firstDone := make(chan error, 1)
	go func() {
		stream, dispatchErr := dispatcher.Dispatch(context.Background(), request)
		if dispatchErr != nil {
			firstDone <- dispatchErr
			return
		}
		collectDispatchEvents(stream)
		firstDone <- nil
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first Runner did not start")
	}
	if _, err := dispatcher.Dispatch(context.Background(), request); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate dispatch error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("duplicate started %d Runner calls", got)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherBindsStoredAttachmentBeforePassingVerifiedContentToRunner(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("image")
	store := inmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	reference, err := store.PutAttachment(context.Background(), principal.TenantID(), attachment.Upload{ID: "attachment-1", Kind: attachment.KindImage, MIMEType: "image/png", Size: int64(len(data)), Provider: "telegram", ProviderID: "file-1"}, strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("PutAttachment = %v", err)
	}
	if _, err := store.Load(context.Background(), principal.TenantID(), "unbound-event", reference); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("unbound Load = %v", err)
	}
	var captured trpcmodel.Message
	runnerValue := &testRunner{runFn: func(_ context.Context, _ string, _ string, message trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		captured = message
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, Attachments: store, AttachmentStore: store})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: InboundMessage{Content: "describe", ExternalMessageID: "attachment-message", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1", Attachments: []attachment.Reference{reference}}})
	if err != nil {
		t.Fatalf("Dispatch = %v", err)
	}
	if events := collectDispatchEvents(stream); len(events) != 1 || !events[0].Done {
		t.Fatalf("dispatch events = %+v", events)
	}
	if captured.Content != "describe" || len(captured.ContentParts) != 1 || captured.ContentParts[0].Type != trpcmodel.ContentTypeImage || captured.ContentParts[0].Image == nil || string(captured.ContentParts[0].Image.Data) != string(data) {
		t.Fatalf("Runner message = %+v", captured)
	}
}

func TestDispatcherMaterializesDurableChannelReplyAndWorkerCompletesLifecycle(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	runnerValue := &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 3)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "abc"}}}}}
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "def"}}}}}
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	store := inmemory.New()
	materializer, err := outbox.NewMaterializer(outbox.MaterializerConfig{BatchStore: store, SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, Materializer: materializer, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	message := dispatchAndAssertDurableReply(t, dispatcher, principal, store)
	assertDurableReplyWorkerCompletes(t, store, principal.TenantID(), message.EventID)
}

func TestDispatcherMaterializesToolMediaReplyAndReplaysIdempotently(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal := mustChannelPrincipal(t, target)
	dispatcher, store, runs := newToolMediaDispatcher(t, fixture)
	request := DispatchRequest{Principal: principal, RequestID: "media-request", TraceID: "media-trace", Message: InboundMessage{Content: "send a test image", ExternalMessageID: "media-message", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}}
	assertToolMediaDispatchCompletes(t, dispatcher, request)
	reply := assertToolMediaOutbox(t, store, principal.TenantID(), target)
	assertToolMediaCorrelation(t, store, principal.TenantID(), reply.EventID)
	assertToolMediaDuplicateIsRejected(t, dispatcher, request, runs, store, principal.TenantID())
}

func mustChannelPrincipal(t *testing.T, target channels.RoutingTarget) Principal {
	t.Helper()
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	return principal
}

func newToolMediaDispatcher(t *testing.T, fixture gatewayFixture) (*Dispatcher, *inmemory.Store, *atomic.Int32) {
	t.Helper()
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends, ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog})
	if err != nil {
		t.Fatal(err)
	}
	runs := &atomic.Int32{}
	runnerValue := &testRunner{runFn: func(ctx context.Context, _ string, _ string, _ trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		runs.Add(1)
		return testImageToolEvents(ctx)
	}}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	store := inmemory.New()
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, Attachments: store, AttachmentStore: store, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher, store, runs
}

func testImageToolEvents(ctx context.Context) (<-chan *trpcevent.Event, error) {
	tools, err := servicetool.DefaultRegistry().Resolve([]appmodel.ToolAuthorization{{ToolID: servicetool.SendTestImageID, Required: true}})
	if err != nil {
		return nil, err
	}
	callable, ok := tools[0].(trpctool.CallableTool)
	if !ok {
		return nil, errors.New("tool is not callable")
	}
	events := make(chan *trpcevent.Event, 1)
	if _, err := callable.Call(ctx, []byte("{}")); err != nil {
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: err.Error()}}}
	} else {
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
	}
	close(events)
	return events, nil
}

func assertToolMediaDispatchCompletes(t *testing.T, dispatcher *Dispatcher, request DispatchRequest) {
	t.Helper()
	stream, err := dispatcher.Dispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if events := collectDispatchEvents(stream); len(events) != 1 || !events[0].Done {
		t.Fatalf("tool dispatch events = %+v", events)
	}
}

func assertToolMediaOutbox(t *testing.T, store *inmemory.Store, tenantID string, target channels.RoutingTarget) runtimestorage.ReplyOutbox {
	t.Helper()
	replies, err := store.ListReplyCandidates(context.Background(), tenantID)
	if err != nil || len(replies) != 1 {
		t.Fatalf("media replies = %+v, err=%v", replies, err)
	}
	reply := replies[0]
	if reply.Kind != runtimestorage.ReplyKindImage || reply.Fallback == "" || reply.ReplyTarget.BindingID != target.BindingID || reply.ReplyTarget.ReceiverID != "peer-1" {
		t.Fatalf("media outbox row = %+v", reply)
	}
	if _, err := store.Load(context.Background(), tenantID, reply.EventID, reply.Attachment); err != nil {
		t.Fatalf("outbox attachment is not tenant/event scoped: %v", err)
	}
	return reply
}

func assertToolMediaCorrelation(t *testing.T, store *inmemory.Store, tenantID, eventID string) {
	t.Helper()
	correlation, err := store.GetReplyCorrelation(context.Background(), tenantID, eventID)
	if err != nil || correlation.RequestID != "media-request" || correlation.TraceID != "media-trace" {
		t.Fatalf("media reply correlation = %+v, err=%v", correlation, err)
	}
}

func assertToolMediaDuplicateIsRejected(t *testing.T, dispatcher *Dispatcher, request DispatchRequest, runs *atomic.Int32, store *inmemory.Store, tenantID string) {
	t.Helper()
	if _, err := dispatcher.Dispatch(context.Background(), request); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate media dispatch error = %v", err)
	}
	if runs.Load() != 1 {
		t.Fatalf("tool runner calls = %d, want one durable execution", runs.Load())
	}
	replies, err := store.ListReplyCandidates(context.Background(), tenantID)
	if err != nil || len(replies) != 1 {
		t.Fatalf("replayed media replies = %+v, err=%v", replies, err)
	}
}

func TestDispatcherToolFailureMaterializesFallback(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends, ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog})
	if err != nil {
		t.Fatal(err)
	}
	runnerValue := &testRunner{runFn: func(ctx context.Context, _ string, _ string, _ trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		tools, resolveErr := servicetool.DefaultRegistry().Resolve([]appmodel.ToolAuthorization{{ToolID: servicetool.SendTestImageID}})
		if resolveErr != nil {
			return nil, resolveErr
		}
		_, callErr := tools[0].(trpctool.CallableTool).Call(ctx, []byte("{}"))
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: callErr.Error()}}}
		close(events)
		return events, nil
	}}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	store := inmemory.New()
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, AttachmentStore: failingToolAttachmentStore{AttachmentStore: store}, DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, RequestID: "tool-fallback", TraceID: "tool-fallback-trace", Message: InboundMessage{Content: "send a test image", ExternalMessageID: "tool-fallback-message", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = collectDispatchEvents(stream)
	replies, err := store.ListReplyCandidates(context.Background(), principal.TenantID())
	if err != nil || len(replies) != 1 || replies[0].Kind != runtimestorage.ReplyKindText || replies[0].Payload != durableFailureFallbackReply {
		t.Fatalf("tool failure fallback = %+v, err=%v", replies, err)
	}
}

func TestDispatcherDurableInboundLeaseCoversAgentRuntimeTimeout(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	runnerValue := &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	baseStore := inmemory.New()
	t.Cleanup(func() { _ = baseStore.Close() })
	store := &transitionCaptureStore{gatewayStore: baseStore}
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: baseStore, DrainTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal, RequestID: "durable-lease",
		Message: InboundMessage{Content: "inbound", ExternalMessageID: "durable-lease", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	collectDispatchEvents(stream)
	got, ok := store.runningLease()
	want := time.Duration(fixture.revision.Runtime.ExecutionTimeoutSeconds)*time.Second + durableInboundLeaseGrace
	if !ok || got != want || got <= 30*time.Second {
		t.Fatalf("durable inbound lease = %v ok=%v, want %v and longer than 30s", got, ok, want)
	}
}

func TestDispatcherDurableChannelModelErrorMaterializesFallbackReply(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	runnerValue := &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: "provider secret"}}}
		close(events)
		return events, nil
	}}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) { return runnerValue, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	store := inmemory.New()
	dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, DrainTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal, RequestID: "durable-error-fallback",
		Message: InboundMessage{Content: "inbound", ExternalMessageID: "durable-error-fallback", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Error != ErrExecution.Error() || events[1].Status != "error" {
		t.Fatalf("dispatch events = %+v", events)
	}
	rows, err := store.ListReplyCandidates(context.Background(), principal.TenantID())
	if err != nil || len(rows) != 1 {
		t.Fatalf("outbox rows = %+v / %v", rows, err)
	}
	row := rows[0]
	if row.Payload != durableFailureFallbackReply || row.ReplyTarget != (runtimestorage.ReplyTarget{BindingID: target.BindingID, ConversationKind: "direct", ReceiverID: "peer-1"}) {
		t.Fatalf("fallback row = %+v", row)
	}
	message, err := store.GetMessage(context.Background(), principal.TenantID(), row.EventID)
	if err != nil || message.Status != runtimestorage.EventCompleted || message.ReplyID != row.ReplyID || message.SegmentCount != 1 {
		t.Fatalf("fallback message = %+v / %v", message, err)
	}
	assertDurableReplyWorkerCompletesCount(t, store, principal.TenantID(), row.EventID, 1)
}

func dispatchAndAssertDurableReply(t *testing.T, dispatcher *Dispatcher, principal Principal, store gatewayStore) runtimestorage.MessageEvent {
	t.Helper()
	target, ok := principal.RoutingTarget()
	if !ok {
		t.Fatal("channel principal has no routing target")
	}
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal, RequestID: "durable-reply",
		Message: InboundMessage{Content: "inbound", ExternalMessageID: "durable-reply", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 3 || !events[2].Done {
		t.Fatalf("dispatch events = %+v", events)
	}
	rows, err := store.ListReplyCandidates(context.Background(), principal.TenantID())
	if err != nil || len(rows) != 2 {
		t.Fatalf("outbox rows = %+v / %v", rows, err)
	}
	segments := make(map[int]runtimestorage.ReplyOutbox, len(rows))
	for _, row := range rows {
		segments[row.SegmentIndex] = row
	}
	first, second := segments[0], segments[1]
	if first.Payload != "abc" || second.Payload != "def" || first.SegmentCount != 2 || second.SegmentCount != 2 || first.ReplyID != second.ReplyID || first.ReplyTarget != (runtimestorage.ReplyTarget{BindingID: target.BindingID, ConversationKind: "direct", ReceiverID: "peer-1"}) || second.ReplyTarget != first.ReplyTarget {
		t.Fatalf("materialized rows = %+v", rows)
	}
	message, err := store.GetMessage(context.Background(), principal.TenantID(), first.EventID)
	if err != nil || message.Status != runtimestorage.EventCompleted || message.ReplyID != first.ReplyID || message.SegmentCount != 2 || message.ReplyTarget != first.ReplyTarget {
		t.Fatalf("materialized message = %+v / %v", message, err)
	}
	return message
}

func assertDurableReplyWorkerCompletes(t *testing.T, store gatewayStore, tenantID, eventID string) {
	t.Helper()
	assertDurableReplyWorkerCompletesCount(t, store, tenantID, eventID, 2)
}

func assertDurableReplyWorkerCompletesCount(t *testing.T, store gatewayStore, tenantID, eventID string, want int) {
	t.Helper()
	provider := &durableOutboxProvider{}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: tenantID, Owner: "worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != want || len(provider.deliveries) != want {
		t.Fatalf("worker = processed %d deliveries %d err %v", processed, len(provider.deliveries), err)
	}
	message, err := store.GetMessage(context.Background(), tenantID, eventID)
	if err != nil || message.Status != runtimestorage.EventReplied {
		t.Fatalf("final message = %+v / %v", message, err)
	}
}

func TestDispatcherDurableClaimReclaimsReceivedAndExpiredRunning(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	store := inmemory.New()
	dispatcher := &Dispatcher{runtimeStore: store}
	message := InboundMessage{Content: "claim", ExternalMessageID: "claim-received", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}
	identity, err := dispatchRunnerIdentity(principal, message)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(context.Background(), principal.TenantID(), identity.SessionID, nil); err != nil {
		t.Fatal(err)
	}
	assertDurableClaimReclaimsLeases(t, dispatcher, principal, message, identity, store, target.BindingID)
	assertDurableClaimRejectsTerminalStates(t, dispatcher, principal, message, identity, store, target.BindingID)
	assertDurableClaimReclaimsReconcilingAndValidatesIDs(t, dispatcher, principal, message, identity, store, target.BindingID)

	message.ExternalMessageID = "claim-default-lease"
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{
		TenantID: principal.TenantID(), EventID: "default-lease-event", SessionID: identity.SessionID,
		BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID,
	}); err != nil {
		t.Fatal(err)
	}
	claimed, err := dispatcher.claimInboundWithLease(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity}, 0)
	if err != nil || claimed == nil {
		t.Fatalf("default durable lease claim = %+v err=%v", claimed, err)
	}
	dispatcher.failDurable(claimed, errors.New("runner unavailable"))
}

func TestDurableInboundLeaseUsesRuntimePolicyDefaults(t *testing.T) {
	defaultPolicy := appmodel.DefaultRuntimePolicy()
	if got, want := durableInboundLeaseForRuntime(appmodel.RuntimePolicy{}), time.Duration(defaultPolicy.ExecutionTimeoutSeconds)*time.Second+durableInboundLeaseGrace; got != want {
		t.Fatalf("zero runtime policy lease = %v, want %v", got, want)
	}
	custom := appmodel.RuntimePolicy{ExecutionTimeoutSeconds: 7}
	if got, want := durableInboundLeaseForRuntime(custom), 7*time.Second+durableInboundLeaseGrace; got != want {
		t.Fatalf("custom runtime policy lease = %v, want %v", got, want)
	}
}

func assertDurableClaimReclaimsLeases(t *testing.T, dispatcher *Dispatcher, principal Principal, message InboundMessage, identity tenant.RunnerIdentity, store gatewayStore, bindingID string) {
	t.Helper()
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: principal.TenantID(), EventID: "received-event", SessionID: identity.SessionID, BindingID: bindingID, ExternalMessageID: message.ExternalMessageID}); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := dispatcher.claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity})
	if err != nil || reclaimed == nil {
		t.Fatalf("received reclaim = %+v err=%v", reclaimed, err)
	}
	dispatcher.failDurable(reclaimed, errors.New("runner unavailable"))
	message.ExternalMessageID = "claim-expired"
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: principal.TenantID(), EventID: "expired-event", SessionID: identity.SessionID, BindingID: bindingID, ExternalMessageID: message.ExternalMessageID}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: "expired-event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "old", LeaseDuration: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	recovered, err := dispatcher.claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity})
	if err != nil || recovered == nil {
		t.Fatalf("expired reclaim = %+v err=%v", recovered, err)
	}
	if _, err := dispatcher.claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity}); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("active duplicate error = %v", err)
	}
}

func assertDurableClaimRejectsTerminalStates(t *testing.T, dispatcher *Dispatcher, principal Principal, message InboundMessage, identity tenant.RunnerIdentity, store gatewayStore, bindingID string) {
	t.Helper()
	seedClaimEvent := func(eventID, externalID string, status string) {
		t.Helper()
		if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: principal.TenantID(), EventID: eventID, SessionID: identity.SessionID, BindingID: bindingID, ExternalMessageID: externalID}); err != nil {
			t.Fatal(err)
		}
		if status == runtimestorage.EventFailed {
			if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: eventID, From: runtimestorage.EventReceived, To: runtimestorage.EventFailed, Owner: "seed"}); err != nil {
				t.Fatal(err)
			}
			return
		}
		running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: eventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "seed", LeaseDuration: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if status == runtimestorage.EventCompleted {
			if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: eventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "seed", FencingToken: running.FencingToken}); err != nil {
				t.Fatal(err)
			}
			return
		}
		if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: eventID, From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: "seed", FencingToken: running.FencingToken}); err == nil {
			t.Fatal("expected live lease reconciliation to fail")
		}
	}
	seedClaimEvent("completed-event", "claim-completed", runtimestorage.EventCompleted)
	message.ExternalMessageID = "claim-completed"
	if _, err := dispatcher.claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity}); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("completed duplicate error = %v", err)
	}
	seedClaimEvent("failed-event", "claim-failed", runtimestorage.EventFailed)
	message.ExternalMessageID = "claim-failed"
	if _, err := dispatcher.claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity}); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("failed duplicate error = %v", err)
	}
}

func assertDurableClaimReclaimsReconcilingAndValidatesIDs(t *testing.T, dispatcher *Dispatcher, principal Principal, message InboundMessage, identity tenant.RunnerIdentity, store gatewayStore, bindingID string) {
	t.Helper()
	message.ExternalMessageID = "claim-reconciling"
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: principal.TenantID(), EventID: "reconciling-event", SessionID: identity.SessionID, BindingID: bindingID, ExternalMessageID: message.ExternalMessageID}); err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: "reconciling-event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "seed", LeaseDuration: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: principal.TenantID(), EventID: "reconciling-event", From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: "recovery", FencingToken: running.FencingToken}); err != nil {
		t.Fatal(err)
	}
	recoveredReconciling, err := dispatcher.claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity})
	if err != nil || recoveredReconciling == nil {
		t.Fatalf("reconciling reclaim = %+v err=%v", recoveredReconciling, err)
	}
	dispatcher.finishDurable(context.Background(), dispatchMetadata{}, recoveredReconciling, errors.New("execution failed"), "", nil)
	message.ExternalMessageID = ""
	if _, err := dispatcher.claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing external ID error = %v", err)
	}
	message.ExternalMessageID = strings.Repeat("x", maxDurableExternalMessageIDRunes+1)
	if _, err := dispatcher.claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized durable external ID error = %v", err)
	}
}

func TestDispatcherDurableClaimMapsStorageErrors(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	message := InboundMessage{Content: "claim-errors", ExternalMessageID: "claim-errors", ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}
	identity, err := dispatchRunnerIdentity(principal, message)
	if err != nil {
		t.Fatal(err)
	}
	base := inmemory.New()
	claim := func(store gatewayStore) error {
		_, err := (&Dispatcher{runtimeStore: store}).claimInbound(context.Background(), dispatchMetadata{principal: principal, message: message, identity: identity})
		return err
	}
	if err := claim(&claimStoreStub{gatewayStore: base, getErr: errors.New("storage")}); err == nil {
		t.Fatal("expected GetSession storage error")
	}
	if err := claim(&claimStoreStub{gatewayStore: base, getErr: runtimestorage.ErrNotFound, createErr: errors.New("create")}); err == nil {
		t.Fatal("expected CreateSession storage error")
	}
	if err := claim(&claimStoreStub{gatewayStore: base, recordErr: errors.New("record")}); err == nil {
		t.Fatal("expected RecordMessage storage error")
	}
	if err := claim(&claimStoreStub{gatewayStore: base, transitionErr: errors.New("transition")}); err == nil {
		t.Fatal("expected TransitionMessage storage error")
	}
}

func TestDispatcherDurableDispatchFailurePaths(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends, ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog})
	if err != nil {
		t.Fatal(err)
	}
	message := func(id string) InboundMessage {
		return InboundMessage{Content: "failure", ExternalMessageID: id, ExternalUserID: "user-1", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1"}
	}
	newDispatcher := func(runFn func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error), factoryNil bool) (*Dispatcher, *runtimerunner.RunnerRegistry) {
		runner := &testRunner{runFn: runFn}
		registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			if factoryNil {
				return nil, nil
			}
			return runner, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		store := inmemory.New()
		t.Cleanup(func() { _ = store.Close() })
		dispatcher, err := NewDispatcher(DispatchConfig{Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store, DrainTimeout: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return dispatcher, registry
	}
	runError := func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		return nil, errors.New("runner")
	}
	dispatcher, registry := newDispatcher(runError, false)
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: message("run-error")}); !errors.Is(err, ErrExecution) {
		t.Fatalf("runner error = %v", err)
	}
	_ = registry.Close()
	nilEvents := func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		return nil, nil
	}
	dispatcher, registry = newDispatcher(nilEvents, false)
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: message("nil-events")}); !errors.Is(err, ErrExecution) {
		t.Fatalf("nil events = %v", err)
	}
	_ = registry.Close()
	dispatcher, registry = newDispatcher(nil, true)
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: message("nil-runner")}); !errors.Is(err, runtimerunner.ErrRunnerUnavailable) {
		t.Fatalf("nil runner = %v", err)
	}
	_ = registry.Close()
	dispatcher, registry = newDispatcher(nilEvents, false)
	_ = registry.Close()
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: message("closed-registry")}); err == nil {
		t.Fatal("expected closed registry error")
	}
	_ = registry.Close()
	badPrincipal := mustAPIPrincipal(t, fixture.tenant.TenantID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAW")
	dispatcher, registry = newDispatcher(nilEvents, false)
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: badPrincipal, Message: InboundMessage{Content: "resolver", ExternalMessageID: "resolver", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}}); !errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("resolver error = %v", err)
	}
	_ = registry.Close()
}

func TestDispatcherDurableAttachmentFailurePaths(t *testing.T) {
	fixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, fixture)
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends, ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog})
	if err != nil {
		t.Fatal(err)
	}
	reference := testAttachmentReference(t, attachment.KindImage, "image/png", []byte("image"))
	newDispatcher := func(t *testing.T, store *inmemory.Store, attachments attachment.Reader) (*Dispatcher, *runtimerunner.RunnerRegistry, *atomic.Int32) {
		t.Helper()
		var runnerCalls atomic.Int32
		registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
			return &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
				runnerCalls.Add(1)
				return nil, nil
			}}, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		dispatcher, err := NewDispatcher(DispatchConfig{
			Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store,
			Attachments: attachments, DrainTimeout: time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return dispatcher, registry, &runnerCalls
	}
	for _, tt := range []struct {
		name        string
		attachments attachment.Reader
		want        error
	}{
		{
			name: "missing binder",
			attachments: attachmentReaderFunc(func(context.Context, string, string, attachment.Reference) (attachment.Content, error) {
				return attachment.Content{}, errors.New("unexpected load")
			}),
			want: ErrExecution,
		},
		{
			name: "binder failure",
			attachments: dispatchAttachmentStore{bindFn: func(context.Context, string, string, []attachment.Reference) error {
				return errors.New("bind failed")
			}},
			want: ErrExecution,
		},
		{
			name: "load failure",
			attachments: dispatchAttachmentStore{loadFn: func(context.Context, string, string, attachment.Reference) (attachment.Content, error) {
				return attachment.Content{}, errors.New("load failed")
			}},
			want: ErrExecution,
		},
		{
			name: "load cancellation",
			attachments: dispatchAttachmentStore{loadFn: func(context.Context, string, string, attachment.Reference) (attachment.Content, error) {
				return attachment.Content{}, context.Canceled
			}},
			want: context.Canceled,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := inmemory.New()
			dispatcher, registry, runnerCalls := newDispatcher(t, store, tt.attachments)
			defer func() { _ = registry.Close() }()
			message := InboundMessage{
				Content: "caption", ExternalMessageID: "attachment-" + strings.ReplaceAll(tt.name, " ", "-"), ExternalUserID: "user-1",
				ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1", Attachments: []attachment.Reference{reference},
			}
			stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal, Message: message})
			if !errors.Is(err, tt.want) || stream != nil {
				t.Fatalf("Dispatch() stream=%v err=%v, want %v", stream, err, tt.want)
			}
			if runnerCalls.Load() != 0 {
				t.Fatal("Runner started after attachment preparation failed")
			}
			assertDurableMessageStatus(t, store, principal, target, message, runtimestorage.EventFailed)
		})
	}
}

func assertDurableMessageStatus(t *testing.T, store gatewayStore, principal Principal, target channels.RoutingTarget, message InboundMessage, status string) {
	t.Helper()
	identity, err := dispatchRunnerIdentity(principal, message)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := replyTarget(target, message)
	if err != nil {
		t.Fatal(err)
	}
	event, duplicate, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{
		TenantID: principal.TenantID(), EventID: "probe-" + message.ExternalMessageID, SessionID: identity.SessionID,
		BindingID: target.BindingID, ExternalMessageID: message.ExternalMessageID, ReplyTarget: reply,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate || event.Status != status {
		t.Fatalf("durable message duplicate=%v status=%q, want %q", duplicate, event.Status, status)
	}
}

func TestDispatcherRedactsRunnerErrors(t *testing.T) {
	runnerValue := &testRunner{}
	runnerValue.runFn = func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 1)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Error: &trpcmodel.ResponseError{Message: "provider secret endpoint"}}}
		close(events)
		return events, nil
	}
	dispatcher, principal := newTestDispatcher(t, runnerValue)
	stream, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal,
		Message:   InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		RequestID: "request-error",
	})
	if err != nil {
		t.Fatal(err)
	}
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Type != DispatchEventError || events[0].Error != ErrExecution.Error() || !events[1].Done {
		t.Fatalf("error dispatch events = %+v", events)
	}
	if strings.Contains(events[0].Error, "secret") {
		t.Fatal("Runner provider detail escaped into Dispatch event")
	}
}

func TestDispatcherCancellationDrainsRunnerEventsAndReleasesLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	senderFinished := make(chan struct{})
	runnerValue := &testRunner{}
	runnerValue.runFn = func(ctx context.Context, _ string, _ string, _ trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event)
		go func() {
			defer close(senderFinished)
			<-ctx.Done()
			events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "late"}}}}}
			close(events)
		}()
		return events, nil
	}
	dispatcher, principal := newTestDispatcher(t, runnerValue)
	stream, err := dispatcher.Dispatch(ctx, DispatchRequest{
		Principal: principal,
		Message:   InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		RequestID: "request-cancel",
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	events := collectDispatchEvents(stream)
	if len(events) != 2 || events[0].Error != ErrExecutionCanceled.Error() || !events[1].Done {
		t.Fatalf("cancellation events = %+v", events)
	}
	select {
	case <-senderFinished:
	case <-time.After(time.Second):
		t.Fatal("Runner event sender was not drained")
	}
}

func TestDispatcherCancellationWinsLateReadyRunnerEvent(t *testing.T) {
	for attempt := 0; attempt < 64; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		events := make(chan *trpcevent.Event, 1)
		dispatcher, principal := newTestDispatcher(t, &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			return events, nil
		}})
		stream, err := dispatcher.Dispatch(ctx, DispatchRequest{
			Principal: principal,
			Message:   InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
			RequestID: "request-cancel-late-event",
		})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "late"}}}}}
		close(events)
		got := collectDispatchEvents(stream)
		if len(got) != 2 || got[0].Error != ErrExecutionCanceled.Error() || !got[1].Done {
			t.Fatalf("attempt %d cancellation events = %+v", attempt, got)
		}
	}
}

func TestDispatcherRejectsInvalidBoundaryInputs(t *testing.T) {
	dispatcher, principal := newTestDispatcher(t, &testRunner{})
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{Message: InboundMessage{Content: "hello"}}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("invalid principal error = %v", err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), DispatchRequest{
		Principal: principal,
		Message:   InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"},
		TraceID:   "bad\ntrace",
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid trace ID error = %v", err)
	}
}

func collectDispatchEvents(stream <-chan DispatchEvent) []DispatchEvent {
	events := make([]DispatchEvent, 0, 4)
	for event := range stream {
		events = append(events, event)
	}
	return events
}

func TestDispatcherConfigurationAndEventMappingEdges(t *testing.T) {
	if _, err := NewDispatcher(DispatchConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing dispatcher dependency error = %v", err)
	}
	dispatcher, principal := newTestDispatcher(t, &testRunner{})
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return &testRunner{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	if _, err := NewDispatcher(DispatchConfig{Resolver: dispatcher.resolver, Registry: registry, DrainTimeout: -time.Second}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative drain timeout error = %v", err)
	}
	readyDispatcher, err := NewDispatcher(DispatchConfig{Resolver: dispatcher.resolver, Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	if !readyDispatcher.Ready() {
		t.Fatal("configured dispatcher is not ready")
	}
	narrowStore := inmemory.New()
	t.Cleanup(func() { _ = narrowStore.Close() })
	if _, err := NewDispatcher(DispatchConfig{
		Resolver: dispatcher.resolver, Registry: registry,
		SessionStore: narrowStore, MessageStore: narrowStore,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("incomplete durable capabilities error = %v", err)
	}
	narrowDispatcher, err := NewDispatcher(DispatchConfig{
		Resolver: dispatcher.resolver, Registry: registry,
		SessionStore: narrowStore, MessageStore: narrowStore, ReplyBatchStore: narrowStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	if narrowDispatcher.runtimeStore == nil || narrowDispatcher.materializer == nil {
		t.Fatalf("narrow dispatcher capabilities = store:%v materializer:%v", narrowDispatcher.runtimeStore, narrowDispatcher.materializer)
	}
	var nilDispatcher *Dispatcher
	if nilDispatcher.Ready() {
		t.Fatal("nil dispatcher is ready")
	}
	if _, err := nilDispatcher.Dispatch(context.Background(), DispatchRequest{Principal: principal}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil dispatcher error = %v", err)
	}
	var nilContext context.Context
	if _, err := dispatcher.Dispatch(nilContext, DispatchRequest{Principal: principal}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil dispatch context error = %v", err)
	}

	if _, err := dispatchRunnerIdentity(Principal{}, InboundMessage{}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unknown principal identity error = %v", err)
	}
	if got := encodeDispatchIdentity("a", "bc"); got != "1:a2:bc" {
		t.Fatalf("encoded identity = %q", got)
	}
	groupMessage := InboundMessage{
		Content: "group", ExternalUserID: "user", ConversationKind: channels.ConversationGroup, ExternalChatID: "chat",
	}
	if _, err := dispatchRunnerIdentity(principal, groupMessage); err != nil {
		t.Fatalf("API group identity error = %v", err)
	}
}

func TestDispatcherRunAndChannelTerminalEdges(t *testing.T) {
	request := func(principal Principal) DispatchRequest {
		return DispatchRequest{Principal: principal, Message: InboundMessage{
			Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer",
		}}
	}
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
		"closed event stream": func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			events := make(chan *trpcevent.Event)
			close(events)
			return events, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			runner := &testRunner{runFn: runFn}
			dispatcher, principal := newTestDispatcher(t, runner)
			stream, err := dispatcher.Dispatch(context.Background(), request(principal))
			switch name {
			case "runner error":
				if !errors.Is(err, ErrExecution) {
					t.Fatalf("runner error = %v", err)
				}
			case "runner canceled":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("runner cancellation = %v", err)
				}
			case "nil event stream":
				if !errors.Is(err, ErrExecution) {
					t.Fatalf("nil event stream error = %v", err)
				}
			case "closed event stream":
				if err != nil {
					t.Fatal(err)
				}
				events := collectDispatchEvents(stream)
				if len(events) != 1 || !events[0].Done {
					t.Fatalf("closed stream events = %+v", events)
				}
			}
		})
	}

	dispatcher, principal := newTestDispatcher(t, &testRunner{})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := dispatcher.Dispatch(canceled, request(principal)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dispatch error = %v", err)
	}

	ctx, stop := context.WithCancel(context.Background())
	stop()
	if sendDispatchEvent(ctx, make(chan DispatchEvent), DispatchEvent{}) {
		t.Fatal("sendDispatchEvent succeeded after cancellation")
	}

	var nilLease *runtimerunner.RunnerLease
	if nilLease.Runner() != nil || nilLease.Release() != nil {
		t.Fatal("nil lease was not safe")
	}
	if (&runtimerunner.RunnerLease{}).Runner() != nil || (&runtimerunner.RunnerLease{}).Release() != nil {
		t.Fatal("empty lease was not safe")
	}
}
