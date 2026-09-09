package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type recordingAdapter struct {
	name    string
	failFor int

	mu       sync.Mutex
	started  bool
	attempts int
	messages []message.OutboundMessage
}

type rejectingAuthorizer struct{ err error }

func (a rejectingAuthorizer) AuthorizeTask(context.Context, message.ExecutionTask) error {
	return a.err
}

func (a *recordingAdapter) Name() string { return a.name }
func (a *recordingAdapter) Start(ctx context.Context, _ channels.IngressSink) error {
	a.mu.Lock()
	a.started = true
	a.mu.Unlock()
	<-ctx.Done()
	return nil
}
func (a *recordingAdapter) Ready(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started {
		return errors.New("adapter is not started")
	}
	return nil
}
func (a *recordingAdapter) Send(_ context.Context, outbound message.OutboundMessage) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.attempts++
	a.messages = append(a.messages, outbound)
	if a.attempts <= a.failFor {
		return errors.New("injected send failure")
	}
	return nil
}
func (a *recordingAdapter) Close() error { return nil }
func (a *recordingAdapter) snapshot() (int, []message.OutboundMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.attempts, append([]message.OutboundMessage(nil), a.messages...)
}

func TestGatewayWaitsForReliableReplyAndCachesResult(t *testing.T) {
	service, store, cancel, done := newGatewayService(t, time.Second)
	defer func() { cancel(); _ = <-done }()
	inbound := gatewayInbound("message-1", "hello", "request-1")
	replyCh := make(chan message.OutboundMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		reply, err := service.Handle(context.Background(), inbound)
		replyCh <- reply
		errCh <- err
	}()
	delivery, err := store.ReadTask(context.Background(), "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(context.Background(), delivery, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), lease, message.OutboundMessage{
		Channel: "demo", BindingID: "binding", RequestID: delivery.Task.RequestID,
		TraceID: delivery.Task.TraceID, SessionID: delivery.Task.SessionID, Text: "answer",
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if reply := <-replyCh; reply.Text != "answer" || reply.RequestID != "request-1" || reply.BindingID != "binding" {
		t.Fatalf("unexpected reply: %#v", reply)
	}

	duplicate := inbound
	duplicate.RequestID = "request-2"
	reply, err := service.Handle(context.Background(), duplicate)
	if err != nil || reply.RequestID != "request-2" || reply.Text != "answer" {
		t.Fatalf("cached Handle() = (%#v, %v)", reply, err)
	}
	conflict := duplicate
	conflict.Text = "changed"
	conflictReply, err := service.Handle(context.Background(), conflict)
	if !errors.Is(err, ErrMessageConflict) || conflictReply.TraceID != inbound.TraceID {
		t.Fatalf("conflicting Handle() error = %v", err)
	}
}

func TestGatewayTimeoutLeavesTaskPending(t *testing.T) {
	service, store, cancel, done := newGatewayService(t, 50*time.Millisecond)
	defer func() { cancel(); _ = <-done }()
	inbound := gatewayInbound("message-timeout", "hello", "request-timeout")
	reply, err := service.Handle(context.Background(), inbound)
	if !errors.Is(err, ErrTaskPending) {
		t.Fatalf("Handle() error = %v, want ErrTaskPending", err)
	}
	if reply.TraceID != inbound.TraceID {
		t.Fatalf("pending trace_id = %q, want original %q", reply.TraceID, inbound.TraceID)
	}
	snapshotTask, err := service.router.Resolve(context.Background(), inbound)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), snapshotTask.InboxID())
	if err != nil || snapshot.State != messaging.StateQueued {
		t.Fatalf("pending snapshot = (%#v, %v)", snapshot, err)
	}
}

func TestGatewayDigestV2RetryReusesOriginalTaskTrace(t *testing.T) {
	service, store, cancel, done := newGatewayServiceWithDigestV2(t, time.Second, true)
	defer func() { cancel(); _ = <-done }()
	first := gatewayInbound("message-v2-retry", "hello", "request-first")
	first.TraceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	result, err := service.Accept(context.Background(), first)
	if err != nil || result.Duplicate {
		t.Fatalf("first Accept() = (%#v, %v)", result, err)
	}
	task, err := store.ReadTask(context.Background(), "worker-v2-retry", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if task.Task.TraceParent == "" {
		t.Fatal("first task did not receive a trace parent")
	}

	duplicate := first
	duplicate.RequestID = "request-second"
	duplicate.TraceID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	result, err = service.Accept(context.Background(), duplicate)
	if err != nil || !result.Duplicate || result.TaskID != task.Task.TaskID || result.TraceID != task.Task.TraceID {
		t.Fatalf("duplicate Accept() = (%#v, %v), want original task %q", result, err, task.Task.TaskID)
	}

	conflict := duplicate
	conflict.Text = "different payload"
	if _, err := service.Accept(context.Background(), conflict); !errors.Is(err, ErrMessageConflict) {
		t.Fatalf("conflicting Accept() error = %v, want ErrMessageConflict", err)
	}
}

func TestGatewayPolicyRejectionDoesNotSubmit(t *testing.T) {
	service, store, cancel, done := newGatewayService(t, time.Second)
	defer func() { cancel(); _ = <-done }()
	service.SetTaskAuthorizer(rejectingAuthorizer{err: governance.ErrActorForbidden})
	inbound := gatewayInbound("message-denied", "hello", "request-denied")
	reply, err := service.Handle(context.Background(), inbound)
	if !errors.Is(err, governance.ErrActorForbidden) || reply.TraceID != inbound.TraceID {
		t.Fatalf("Handle() = (%#v, %v)", reply, err)
	}
	task, resolveErr := service.router.Resolve(context.Background(), inbound)
	if resolveErr != nil {
		t.Fatal(resolveErr)
	}
	if _, snapshotErr := store.Snapshot(context.Background(), task.InboxID()); !errors.Is(snapshotErr, messaging.ErrInboxMissing) {
		t.Fatalf("denied task was submitted: %v", snapshotErr)
	}
}

func TestGatewayCachedFailureKeepsOriginalTaskTrace(t *testing.T) {
	service, store, cancel, done := newGatewayService(t, time.Second)
	defer func() { cancel(); _ = <-done }()
	inbound := gatewayInbound("message-failed", "hello", "request-first")
	firstReply := make(chan message.OutboundMessage, 1)
	firstErr := make(chan error, 1)
	go func() {
		reply, err := service.Handle(context.Background(), inbound)
		firstReply <- reply
		firstErr <- err
	}()
	delivery, err := store.ReadTask(context.Background(), "worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(context.Background(), delivery, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(context.Background(), lease, "model_timeout"); err != nil {
		t.Fatal(err)
	}
	if err := <-firstErr; !errors.Is(err, executor.ErrAgentTimeout) {
		t.Fatalf("first Handle() error = %v", err)
	}
	if reply := <-firstReply; reply.TraceID != inbound.TraceID {
		t.Fatalf("first failure trace_id = %q", reply.TraceID)
	}

	duplicate := inbound
	duplicate.RequestID = "request-second"
	duplicate.TraceID = "trace-second"
	reply, err := service.Handle(context.Background(), duplicate)
	if !errors.Is(err, executor.ErrAgentTimeout) || reply.TraceID != inbound.TraceID {
		t.Fatalf("cached failure = (%#v, %v)", reply, err)
	}
}

func TestGatewayReliableOutboundRetriesAndRedactsAgentFailure(t *testing.T) {
	store, router := newIMGatewayDependencies(t, 5, 5*time.Millisecond, 5*time.Millisecond)
	adapter := &recordingAdapter{name: "telegram-binding", failFor: 2}
	service, cancel, done := startGatewayWithAdapter(t, store, router, "gateway-outbound", adapter)
	defer func() { cancel(); _ = service.Close(); _ = <-done }()

	task := submitAndFinishIMTask(t, service, store, "outbound-success", true)
	state := waitOutboundTerminal(t, store, task.TaskID)
	attempts, messages := adapter.snapshot()
	if state.Status != "succeeded" || state.Attempts != 3 || attempts != 3 || state.AckedAt.IsZero() {
		t.Fatalf("outbound state=%#v adapter attempts=%d", state, attempts)
	}
	for _, current := range messages {
		if current.Text != "agent answer" || current.ConversationID != "chat-a" || current.BindingID != "telegram-binding" {
			t.Fatalf("trusted outbound = %#v", current)
		}
	}

	failureTask := submitAndFinishIMTask(t, service, store, "outbound-agent-failure", false)
	_ = waitOutboundTerminal(t, store, failureTask.TaskID)
	_, messages = adapter.snapshot()
	failure := messages[len(messages)-1]
	if failure.Text != agentFailureText || failure.RequestID != "" || failure.TraceID != "" || failure.SessionID != "" {
		t.Fatalf("user-visible failure = %#v", failure)
	}
}

func TestGatewayReliableOutboundStopsAtAttemptLimit(t *testing.T) {
	store, router := newIMGatewayDependencies(t, 3, 2*time.Millisecond, 2*time.Millisecond)
	adapter := &recordingAdapter{name: "telegram-binding", failFor: 100}
	service, cancel, done := startGatewayWithAdapter(t, store, router, "gateway-terminal", adapter)
	defer func() { cancel(); _ = service.Close(); _ = <-done }()

	task := submitAndFinishIMTask(t, service, store, "outbound-terminal", true)
	state := waitOutboundTerminal(t, store, task.TaskID)
	attempts, _ := adapter.snapshot()
	if state.Status != "failed_terminal" || state.Attempts != 3 || attempts != 3 || state.LastError != "send_failed" || !state.AckedAt.IsZero() {
		t.Fatalf("outbound terminal state=%#v adapter attempts=%d", state, attempts)
	}
}

func TestGatewayOutboundAttemptsSurviveRestart(t *testing.T) {
	store, router := newIMGatewayDependencies(t, 5, 150*time.Millisecond, 150*time.Millisecond)
	firstAdapter := &recordingAdapter{name: "telegram-binding", failFor: 100}
	first, cancelFirst, firstDone := startGatewayWithAdapter(t, store, router, "gateway-before-restart", firstAdapter)
	task := submitAndFinishIMTask(t, first, store, "outbound-restart", true)
	waitOutboundAttempts(t, store, task.TaskID, 1)
	cancelFirst()
	_ = first.Close()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)
	secondAdapter := &recordingAdapter{name: "telegram-binding"}
	second, cancelSecond, secondDone := startGatewayWithAdapter(t, store, router, "gateway-after-restart", secondAdapter)
	defer func() { cancelSecond(); _ = second.Close(); _ = <-secondDone }()
	state := waitOutboundTerminal(t, store, task.TaskID)
	secondAttempts, _ := secondAdapter.snapshot()
	if state.Status != "succeeded" || state.Attempts != 2 || secondAttempts != 1 {
		t.Fatalf("recovered outbound state=%#v second attempts=%d", state, secondAttempts)
	}
}

func newIMGatewayDependencies(t *testing.T, maxAttempts int, initialBackoff, maxBackoff time.Duration) (*messaging.Store, *routing.Router) {
	t.Helper()
	server := miniredis.RunT(t)
	cfg := config.MessagingConfig{
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "gateway-im-" + fmt.Sprint(time.Now().UnixNano()),
		LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: time.Second, MaxBackoff: 5 * time.Second, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: time.Second,
		OutboundMaxAttempts: maxAttempts, OutboundInitialBackoff: initialBackoff, OutboundMaxBackoff: maxBackoff,
		OutboundSendTimeout: 100 * time.Millisecond, OutboundClaimIdle: 5 * time.Millisecond,
	}
	store, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	catalog := gatewayCatalog()
	catalog.ChannelBindings[0] = tenant.ChannelBinding{
		ID: "telegram-binding", Channel: "telegram", ExternalAccountID: "123", CredentialRef: "env:TELEGRAM_TOKEN",
		TenantID: "tenant", AgentAppID: "app", Enabled: true,
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	router, err := routing.New(repository, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}
	return store, router
}

func startGatewayWithAdapter(t *testing.T, store *messaging.Store, router *routing.Router, consumer string, adapter channels.Adapter) (*Service, context.CancelFunc, <-chan error) {
	t.Helper()
	service, err := NewWithAdapters(router, store, consumer, adapter)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if service.Ready(context.Background()) == nil {
			return service, cancel, done
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	t.Fatal("IM Gateway did not become ready")
	return nil, nil, nil
}

func submitAndFinishIMTask(t *testing.T, service *Service, store *messaging.Store, id string, succeeded bool) message.ExecutionTask {
	t.Helper()
	inbound := message.InboundMessage{
		Channel: "telegram", BindingID: "telegram-binding", ExternalAccountID: "123", PlatformMessageID: id,
		ActorUserID: "user-a", ConversationID: "chat-a", ConversationType: message.ConversationDirect,
		Text: "hello", RequestID: "request-" + id, TraceID: "trace-" + id, ReceivedAt: time.Now().UTC(),
	}
	if _, err := service.Accept(context.Background(), inbound); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-"+id, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(context.Background(), delivery, "worker-"+id)
	if err != nil {
		t.Fatal(err)
	}
	if succeeded {
		err = store.Complete(context.Background(), lease, message.OutboundMessage{Text: "agent answer", TraceID: "untrusted", BindingID: "untrusted"})
	} else {
		err = store.Fail(context.Background(), lease, "model_timeout")
	}
	if err != nil {
		t.Fatal(err)
	}
	return delivery.Task
}

func waitOutboundAttempts(t *testing.T, store *messaging.Store, taskID string, attempts int) messaging.OutboundState {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state, err := store.OutboundSnapshot(context.Background(), taskID)
		if err == nil && state.Attempts >= attempts {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("outbound attempts did not advance")
	return messaging.OutboundState{}
}

func waitOutboundTerminal(t *testing.T, store *messaging.Store, taskID string) messaging.OutboundState {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, err := store.OutboundSnapshot(context.Background(), taskID)
		if err == nil && state.Terminal {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("outbound delivery did not reach terminal state")
	return messaging.OutboundState{}
}

func newGatewayService(t *testing.T, wait time.Duration) (*Service, *messaging.Store, context.CancelFunc, <-chan error) {
	return newGatewayServiceWithDigestV2(t, wait, false)
}

func newGatewayServiceWithDigestV2(t *testing.T, wait time.Duration, digestV2 bool) (*Service, *messaging.Store, context.CancelFunc, <-chan error) {
	t.Helper()
	server := miniredis.RunT(t)
	cfg := config.MessagingConfig{
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "gateway-" + fmt.Sprint(time.Now().UnixNano()),
		LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: time.Second, MaxBackoff: 5 * time.Second, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: wait,
	}
	store, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	repository, err := tenant.NewPresetRepository(gatewayCatalog())
	if err != nil {
		t.Fatal(err)
	}
	router, err := routing.NewWithDigestV2(repository, []byte("01234567890123456789012345678901"), digestV2)
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(router, store, "gateway-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if service.Ready(context.Background()) == nil {
			return service, store, cancel, done
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	t.Fatal("gateway did not become ready")
	return nil, nil, nil, nil
}

func gatewayInbound(messageID, text, requestID string) message.InboundMessage {
	return message.InboundMessage{
		Channel: "demo", BindingID: "binding", MessageID: messageID,
		ExternalUserID: "user", ConversationID: "conversation", Text: text,
		RequestID: requestID, TraceID: "trace", ReceivedAt: time.Now().UTC(),
	}
}

func gatewayCatalog() tenant.Catalog {
	return tenant.Catalog{
		Tenants:         []tenant.Tenant{{ID: "tenant", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{TenantID: "tenant", ID: "memory", Kind: tenant.StorageKindInMemory}},
		AgentApps:       []tenant.AgentApp{{TenantID: "tenant", ID: "app", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []tenant.ConfigVersion{{
			TenantID: "tenant", AgentAppID: "app", Version: "v1", StorageProfileID: "memory", Instruction: "test",
			Model: tenant.ModelConfig{Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL", RequestTimeout: time.Second, MaxOutputTokens: 32},
		}},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: "binding", Channel: "demo", ExternalAccountID: "binding", TenantID: "tenant", AgentAppID: "app", Enabled: true,
		}},
	}
}
