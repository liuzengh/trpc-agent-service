package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestHandleNewCommandBypassesGateway(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newWeComRecordingAdmitter()
	handler := &wecomNewSessionHandler{}
	adapter, err := NewAdapter(
		source,
		gateway.New(admitter),
		testWeComSecrets{key: binding, value: "bot-secret"},
		WithCommandHandler(handler),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), Message{
		MessageID:   "msg-new",
		AIBotID:     "bot-a",
		ChatType:    "single",
		From:        MessageFrom{UserID: "user-a"},
		MessageType: "text",
		Text:        MessageText{Content: "/new"},
	}); err != nil {
		t.Fatalf("handle /new: %v", err)
	}
	if admitter.admittedCount() != 0 {
		t.Fatal("/new entered Gateway admission")
	}
	if handler.input.Text != "/new" || handler.requestID == "" {
		t.Fatalf("command handler request = %#v", handler)
	}
}

func TestHandleNewCommandFailureRecordsReply(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("switch session failed")
	admitter := newWeComRecordingAdmitter()
	adapter, err := NewAdapter(source, gateway.New(admitter), testWeComSecrets{key: binding, value: "bot-secret"},
		WithCommandHandler(&wecomNewSessionHandler{err: wantErr}))
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), Message{
		MessageID: "msg-new-failed", AIBotID: "bot-a", ChatType: "single",
		From: MessageFrom{UserID: "user-a"}, MessageType: "text", Text: MessageText{Content: "/new"},
	}); !errors.Is(err, wantErr) {
		t.Fatalf("handle failed /new = %v, want %v", err, wantErr)
	}
	if admitter.failureRequest.RequestID == "" || admitter.failureRequest.ChannelInput == nil {
		t.Fatalf("failure request = %#v", admitter.failureRequest)
	}
}

func TestHandleMessageUsesBotIDBindingAndSendsChannelInputToGateway(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newWeComRecordingAdmitter()
	adapter := newWeComTestAdapter(t, source, admitter, testWeComSecrets{
		key:   binding,
		value: "bot-secret-a",
	})

	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), Message{
		MessageID:   "msg-1",
		AIBotID:     "bot-a",
		ChatType:    "single",
		From:        MessageFrom{UserID: "user-a"},
		MessageType: "text",
		Text:        MessageText{Content: "hello"},
	}); err != nil {
		t.Fatalf("handle WeCom WS event: %v", err)
	}

	request := admitter.lastRequest(t)
	if request.Identity.Tenant.TenantID != "tenant-a" || request.Identity.Tenant.AppID != "support" || request.Identity.Tenant.BindingID != "binding-a" {
		t.Fatalf("admission scope = %#v, want tenant-a/support/binding-a", request.Identity.Tenant)
	}
	if request.ChannelInput == nil || request.ChannelInput.ExternalMessageID != "msg-1" || request.ChannelInput.Text != "hello" {
		t.Fatalf("channel input = %#v", request.ChannelInput)
	}
}

func TestHandleMessageDuplicateEventUsesGatewayIdempotency(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newWeComRecordingAdmitter()
	adapter := newWeComTestAdapter(t, source, admitter, testWeComSecrets{key: binding, value: "bot-secret"})
	event := Message{
		MessageID:   "msg-duplicate",
		AIBotID:     "bot-a",
		ChatType:    "single",
		From:        MessageFrom{UserID: "user-a"},
		MessageType: "text",
		Text:        MessageText{Content: "same"},
	}
	for range 2 {
		if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), event); err != nil {
			t.Fatalf("handle duplicate event: %v", err)
		}
	}
	if admitter.admittedCount() != 1 {
		t.Fatalf("admitted count = %d, want 1", admitter.admittedCount())
	}
}

func TestRunReconcilesNewBindingWhileExistingClientBlocks(t *testing.T) {
	bindingA := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "secret-a")
	bindingB := testWeComBinding("tenant-b", "support", "binding-b", "bot-b", "secret-b")
	source := newMutableWeComBindingSource(bindingA)
	secrets := testWeComSecrets{
		key:   bindingA,
		value: "secret-a",
		more:  map[string]string{testWeComSecretKey(bindingB): "secret-b"},
	}
	started := make(chan channels.BindingSnapshot, 2)
	adapter, err := NewAdapter(
		source,
		gateway.New(newWeComRecordingAdmitter()),
		secrets,
		WithReconcileInterval(10*time.Millisecond),
		WithClientFactory(func(binding channels.BindingSnapshot, secret string, _ MessageHandler) (ClientRunner, error) {
			if secret == "" {
				return nil, errors.New("empty test secret")
			}
			started <- binding
			return &wecomBlockingRunner{closed: make(chan struct{})}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	select {
	case binding := <-started:
		if binding.BindingID != bindingA.BindingID {
			t.Fatalf("first binding = %q, want %q", binding.BindingID, bindingA.BindingID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first binding")
	}
	source.Set(bindingB)
	select {
	case binding := <-started:
		if binding.BindingID != bindingB.BindingID {
			t.Fatalf("reconciled binding = %q, want %q", binding.BindingID, bindingB.BindingID)
		}
	case <-time.After(time.Second):
		t.Fatal("new active WeCom binding waited for unrelated client")
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for adapter shutdown")
	}
}

func TestCloseLetsRunOwnActiveClientShutdown(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "secret-a")
	source := newMutableWeComBindingSource(binding)
	client := &wecomCloseCountingRunner{
		started:      make(chan struct{}),
		closeStarted: make(chan struct{}),
		closeGate:    make(chan struct{}),
	}
	adapter, err := NewAdapter(
		source,
		gateway.New(newWeComRecordingAdmitter()),
		testWeComSecrets{key: binding, value: "secret-a"},
		WithClientFactory(func(channels.BindingSnapshot, string, MessageHandler) (ClientRunner, error) {
			return client, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom client")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- adapter.Close(context.Background()) }()
	select {
	case <-client.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("adapter Close did not close the active WeCom client")
	}
	close(client.closeGate)
	if err := <-closeDone; err != nil {
		t.Fatalf("close adapter: %v", err)
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context canceled", err)
	}
	if got := client.closeCalls.Load(); got != 1 {
		t.Fatalf("WeCom client close calls = %d, want 1", got)
	}
}

func TestCloseBoundsNonCooperativeBindingShutdown(t *testing.T) {
	bindings := []channels.Binding{
		testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "secret-a"),
		testWeComBinding("tenant-b", "support", "binding-b", "bot-b", "secret-b"),
		testWeComBinding("tenant-c", "support", "binding-c", "bot-c", "secret-c"),
	}
	source := newMutableWeComBindingSource(bindings...)
	secrets := testWeComSecrets{key: bindings[0], value: "secret-a", more: map[string]string{}}
	for _, binding := range bindings[1:] {
		secrets.more[testWeComSecretKey(binding)] = "secret-" + string(binding.BindingID[len(binding.BindingID)-1])
	}
	started := make(chan struct{}, len(bindings))
	release := make(chan struct{})
	adapter, err := NewAdapter(
		source,
		gateway.New(newWeComRecordingAdmitter()),
		secrets,
		WithClientFactory(func(channels.BindingSnapshot, string, MessageHandler) (ClientRunner, error) {
			return &wecomNonCooperativeRunner{started: started, release: release}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	for range bindings {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for non-cooperative binding")
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Millisecond)
	startedAt := time.Now()
	err = adapter.Close(shutdownCtx)
	cancelShutdown()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("close elapsed = %s, want one bounded grace period", elapsed)
	}

	close(release)
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("adapter run did not finish after releasing non-cooperative clients")
	}
}

func TestTimedOutBindingRunRemainsUntilFinished(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "secret-a")
	adapter := newWeComTestAdapter(t, newMutableWeComBindingSource(binding), newWeComRecordingAdmitter(), testWeComSecrets{key: binding, value: "secret-a"})
	key := bindingKey(binding.Snapshot())
	_, cancelRun := context.WithCancel(context.Background())
	run := &wecomBindingRun{snapshot: binding, cancel: cancelRun, done: make(chan struct{})}
	adapter.runsMu.Lock()
	adapter.runs[key] = run
	adapter.runsMu.Unlock()

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	adapter.stopBinding(stopCtx, key, run)
	cancelStop()
	adapter.runsMu.Lock()
	_, stillPresent := adapter.runs[key]
	adapter.runsMu.Unlock()
	if !stillPresent {
		t.Fatal("timed-out binding run was removed before it finished")
	}

	var factoryCalls atomic.Int32
	adapter.clientFactory = func(channels.BindingSnapshot, string, MessageHandler) (ClientRunner, error) {
		factoryCalls.Add(1)
		return &wecomFailingRunner{}, nil
	}
	if err := adapter.reconcileBindings(context.Background(), []channels.Binding{binding}, make(chan wecomBindingRunResult, 1)); err != nil {
		t.Fatalf("reconcile binding: %v", err)
	}
	if got := factoryCalls.Load(); got != 0 {
		t.Fatalf("replacement client factory calls = %d, want 0", got)
	}

	close(run.done)
	adapter.finishBindingRun(wecomBindingRunResult{key: key, run: run})
}

func TestRunUsesCappedReconnectBackoff(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAdapter(
		source,
		gateway.New(newWeComRecordingAdmitter()),
		testWeComSecrets{key: binding, value: "secret-a"},
		WithReconnectDelay(5*time.Millisecond, 20*time.Millisecond),
		WithClientFactory(func(channels.BindingSnapshot, string, MessageHandler) (ClientRunner, error) {
			return &wecomFailingRunner{}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	stopErr := errors.New("stop reconnect test")
	var delays []time.Duration
	adapter.waitReconnect = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == 4 {
			return stopErr
		}
		return nil
	}
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(context.Background()) }()
	select {
	case err := <-runDone:
		if !errors.Is(err, stopErr) {
			t.Fatalf("run error = %v, want reconnect test stop error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconnect test did not finish")
	}
	expected := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}
	if len(delays) != len(expected) {
		t.Fatalf("reconnect delays = %v, want %v", delays, expected)
	}
	for i := range expected {
		if delays[i] != expected[i] {
			t.Fatalf("reconnect delay[%d] = %s, want %s", i, delays[i], expected[i])
		}
	}
}

func TestRunRebuildsChangedBindingAndStopsSuspendedBinding(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "secret-a")
	updated := binding
	updated.BindingRevision = 2
	updated.Secret = tenant.SecretRef{Name: "secret-b", Version: "v1"}
	source := newMutableWeComBindingSource(binding)
	secrets := testWeComSecrets{
		key:   binding,
		value: "secret-a",
		more:  map[string]string{testWeComSecretKey(updated): "secret-b"},
	}
	started := make(chan channels.BindingSnapshot, 2)
	clients := make(chan *wecomBlockingRunner, 2)
	adapter, err := NewAdapter(
		source,
		gateway.New(newWeComRecordingAdmitter()),
		secrets,
		WithReconcileInterval(10*time.Millisecond),
		WithClientFactory(func(binding channels.BindingSnapshot, secret string, _ MessageHandler) (ClientRunner, error) {
			if secret == "" {
				return nil, errors.New("empty test secret")
			}
			client := &wecomBlockingRunner{closed: make(chan struct{})}
			started <- binding
			clients <- client
			return client, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	first := <-clients
	firstBinding := <-started
	if firstBinding.BindingRevision != 1 {
		t.Fatalf("initial binding revision = %d", firstBinding.BindingRevision)
	}
	source.Set(updated)
	second := <-clients
	secondBinding := <-started
	if secondBinding.BindingRevision != 2 || secondBinding.Secret.Name != "secret-b" {
		t.Fatalf("updated binding = %#v", secondBinding)
	}
	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), Message{
		MessageID: "stale-message", AIBotID: "bot-a", ChatType: "single",
		From: MessageFrom{UserID: "user-a"}, MessageType: "text", Text: MessageText{Content: "stale"},
	}); !errors.Is(err, gateway.ErrChannelBindingSnapshotStale) {
		t.Fatalf("stale WeCom message error = %v", err)
	}
	select {
	case <-first.closed:
	case <-time.After(time.Second):
		t.Fatal("old WeCom client was not closed after revision change")
	}

	suspended := updated
	suspended.Status = channels.BindingSuspended
	source.Set(suspended)
	select {
	case <-second.closed:
	case <-time.After(time.Second):
		t.Fatal("active WeCom client was not closed after suspension")
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom shutdown")
	}
}

func TestWebSocketClientAuthMessageReplyReconnectAndCancellation(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	reconnected := make(chan struct{})
	var reconnectOnce sync.Once
	serverErrors := make(chan error, 4)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		connectionNumber := connections.Add(1)
		auth, err := readWeComFrame(conn)
		if err != nil {
			serverErrors <- err
			return
		}
		if auth.Cmd != "aibot_subscribe" {
			serverErrors <- errors.New("first client frame was not aibot_subscribe")
			return
		}
		var credentials struct {
			BotID  string `json:"bot_id"`
			Secret string `json:"secret"`
		}
		if err := json.Unmarshal(auth.Body, &credentials); err != nil {
			serverErrors <- err
			return
		}
		if credentials.BotID != "bot-id" || credentials.Secret != "bot-secret" {
			serverErrors <- errors.New("wrong WeCom credentials")
			return
		}
		if err := writeWeComAck(conn, auth.Headers.ReqID); err != nil {
			serverErrors <- err
			return
		}
		if connectionNumber == 1 {
			message := Message{
				MessageID:   "msg-ws-1",
				AIBotID:     "bot-id",
				ChatType:    "single",
				From:        MessageFrom{UserID: "user-1"},
				MessageType: "text",
				Text:        MessageText{Content: "from websocket"},
			}
			if err := writeWeComFrame(conn, protocolFrame{
				Cmd:     "aibot_msg_callback",
				Headers: frameHeaders{ReqID: "incoming-1"},
				Body:    bodyBytes(message),
			}); err != nil {
				serverErrors <- err
				return
			}
			send, err := readWeComFrame(conn)
			if err != nil {
				serverErrors <- err
				return
			}
			if send.Cmd != "aibot_send_msg" {
				serverErrors <- errors.New("reply frame was not aibot_send_msg")
				return
			}
			var body struct {
				ChatID   string `json:"chatid"`
				MsgType  string `json:"msgtype"`
				Markdown struct {
					Content string `json:"content"`
				} `json:"markdown"`
			}
			if err := json.Unmarshal(send.Body, &body); err != nil {
				serverErrors <- err
				return
			}
			if body.ChatID != "user-1" || body.MsgType != "markdown" || body.Markdown.Content != "reply" {
				serverErrors <- errors.New("wrong aibot_send_msg body")
				return
			}
			if err := writeWeComAck(conn, send.Headers.ReqID); err != nil {
				serverErrors <- err
				return
			}
			return
		}
		reconnectOnce.Do(func() { close(reconnected) })
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	received := make(chan Message, 1)
	client, err := NewClient(
		"bot-id",
		"bot-secret",
		WithWebSocketURL("ws"+strings.TrimPrefix(server.URL, "http")),
		WithWebSocketReconnectDelay(time.Millisecond, 5*time.Millisecond),
		WithMessageHandler(func(_ context.Context, message Message) error {
			received <- message
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(runCtx) }()

	select {
	case message := <-received:
		if message.MessageID != "msg-ws-1" || message.Text.Content != "from websocket" {
			t.Fatalf("received message = %#v", message)
		}
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom message")
	}
	if _, err := client.SendMessage(context.Background(), "user-1", "reply"); err != nil {
		t.Fatalf("send WeCom reply: %v", err)
	}
	select {
	case <-reconnected:
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom reconnect")
	}
	closeErr := client.Close(context.Background())
	if closeErr != nil {
		t.Fatalf("close WeCom client: %v", closeErr)
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom shutdown")
	}
	select {
	case err := <-serverErrors:
		t.Fatal(err)
	default:
	}
}

func TestOutboundClientSendsThroughBindingScopedSender(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	sender := &recordingWeComSender{messageID: "provider-msg-1"}
	client, err := NewOutboundClient(
		context.Background(),
		testWeComSecrets{key: binding, value: "bot-secret"},
		binding.Snapshot(),
		WithMessageSender(sender),
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := client.SendOnce(context.Background(), channels.Reply{
		TenantID:        binding.TenantID,
		AppID:           binding.AppID,
		RequestID:       "request-1",
		SourceEventID:   "msg-1",
		Channel:         channels.ChannelWeCom,
		BindingID:       binding.BindingID,
		BindingRevision: binding.BindingRevision,
		ReplyID:         "reply-1",
		Revision:        1,
		Target:          channels.ReplyTarget{Kind: channels.TargetKindUser, InternalEntityID: "entity-1"},
		Text:            "answer",
	}, "user-1")
	if err != nil {
		t.Fatalf("send reply: %v", err)
	}
	if receipt.ProviderMessageID != "provider-msg-1" || sender.target != "user-1" || sender.text != "answer" {
		t.Fatalf("receipt=%#v sender=%#v", receipt, sender)
	}
}

func TestAdapterResolvesTheLiveBindingSender(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	sender := &recordingWeComSender{messageID: "provider-msg-1"}
	adapter := &Adapter{
		runs: map[string]*wecomBindingRun{
			bindingKey(binding.Snapshot()): {
				snapshot: binding,
				client:   &wecomClientHandle{client: sender},
			},
		},
	}

	resolved, err := adapter.ResolveOutboundSender(context.Background(), binding.Snapshot())
	if err != nil {
		t.Fatalf("resolve live sender: %v", err)
	}
	if resolved != sender {
		t.Fatalf("resolved sender = %T %p, want shared sender %T %p", resolved, resolved, sender, sender)
	}
}

type recordingWeComSender struct {
	target    string
	text      string
	messageID string
}

func (r *recordingWeComSender) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (*recordingWeComSender) Close(context.Context) error { return nil }

var _ ClientRunner = (*recordingWeComSender)(nil)
var _ MessageSender = (*recordingWeComSender)(nil)

type wecomBlockingRunner struct {
	closed chan struct{}
}

type wecomCloseCountingRunner struct {
	started      chan struct{}
	closeStarted chan struct{}
	closeGate    chan struct{}
	closeCalls   atomic.Int32
}

type wecomNonCooperativeRunner struct {
	started chan<- struct{}
	release <-chan struct{}
}

type wecomFailingRunner struct{}

func (r *wecomNonCooperativeRunner) Run(ctx context.Context) error {
	r.started <- struct{}{}
	<-r.release
	return ctx.Err()
}

func (*wecomNonCooperativeRunner) Close(context.Context) error { return nil }

func (*wecomFailingRunner) Run(context.Context) error { return errors.New("fake client stopped") }

func (*wecomFailingRunner) Close(context.Context) error { return nil }

var _ ClientRunner = (*wecomNonCooperativeRunner)(nil)
var _ ClientRunner = (*wecomFailingRunner)(nil)

func (r *wecomCloseCountingRunner) Run(ctx context.Context) error {
	close(r.started)
	<-ctx.Done()
	return ctx.Err()
}

func (r *wecomCloseCountingRunner) Close(ctx context.Context) error {
	if r.closeCalls.Add(1) == 1 {
		close(r.closeStarted)
	}
	select {
	case <-r.closeGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ ClientRunner = (*wecomCloseCountingRunner)(nil)

func (r *wecomBlockingRunner) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.closed:
		return nil
	}
}

func (r *wecomBlockingRunner) Close(context.Context) error {
	select {
	case <-r.closed:
	default:
		close(r.closed)
	}
	return nil
}

var _ ClientRunner = (*wecomBlockingRunner)(nil)

func (s *recordingWeComSender) SendMessage(_ context.Context, target, text string) (string, error) {
	s.target = target
	s.text = text
	return s.messageID, nil
}

type wecomRecordingAdmitter struct {
	mu             sync.Mutex
	requests       []gateway.AdmissionRequest
	failureRequest gateway.AdmissionRequest
	results        map[string]gateway.AdmissionResult
}

type wecomNewSessionHandler struct {
	requestID string
	input     channels.ChannelInput
	err       error
}

func (h *wecomNewSessionHandler) HandleNewSession(_ context.Context, request channels.NewSessionRequest) error {
	h.requestID = request.RequestID
	h.input = request.Input
	return h.err
}

func newWeComRecordingAdmitter() *wecomRecordingAdmitter {
	return &wecomRecordingAdmitter{results: make(map[string]gateway.AdmissionResult)}
}

func (a *wecomRecordingAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, request)
	if result, ok := a.results[request.IdempotencyKey]; ok {
		result.Replayed = true
		return result, nil
	}
	result := gateway.AdmissionResult{
		RequestID:     request.RequestID,
		ConfigVersion: "v1",
		TurnSeq:       int64(len(a.results) + 1),
		Status:        gateway.AdmissionStatusAdmitted,
	}
	a.results[request.IdempotencyKey] = result
	return result, nil
}

func (a *wecomRecordingAdmitter) RecordChannelFailure(_ context.Context, request gateway.AdmissionRequest) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failureRequest = request
	return nil
}

func (a *wecomRecordingAdmitter) lastRequest(t *testing.T) gateway.AdmissionRequest {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.requests) == 0 {
		t.Fatal("gateway received no request")
	}
	return a.requests[len(a.requests)-1]
}

func (a *wecomRecordingAdmitter) admittedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.results)
}

type testWeComSecrets struct {
	key   channels.Binding
	value string
	more  map[string]string
}

func (p testWeComSecrets) ResolveSecret(_ context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	key := scope.TenantID + "\x00" + scope.AppID + "\x00" + ref.Name
	if key == testWeComSecretKey(p.key) {
		return p.value, nil
	}
	if value := p.more[key]; value != "" {
		return value, nil
	}
	return "", errors.New("test WeCom secret not found")
}

func testWeComSecretKey(binding channels.Binding) string {
	return binding.TenantID + "\x00" + binding.AppID + "\x00" + binding.Secret.Name
}

type mutableWeComBindingSource struct {
	mu       sync.RWMutex
	bindings map[string]channels.Binding
}

func newMutableWeComBindingSource(bindings ...channels.Binding) *mutableWeComBindingSource {
	source := &mutableWeComBindingSource{bindings: make(map[string]channels.Binding)}
	for _, binding := range bindings {
		source.bindings[bindingKey(binding.Snapshot())] = binding
	}
	return source
}

func (s *mutableWeComBindingSource) Set(bindings ...channels.Binding) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, binding := range bindings {
		s.bindings[bindingKey(binding.Snapshot())] = binding
	}
}

func (s *mutableWeComBindingSource) ResolveBinding(_ context.Context, tenantID, appID, bindingID string) (channels.Binding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	binding, ok := s.bindings[tenantID+"\x00"+appID+"\x00"+bindingID]
	if !ok {
		return channels.Binding{}, errors.New("binding not found")
	}
	return binding, nil
}

func (s *mutableWeComBindingSource) ListActiveChannelBindings(_ context.Context, channel channels.Channel) ([]channels.Binding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]channels.Binding, 0, len(s.bindings))
	for _, binding := range s.bindings {
		if binding.Channel == channel && binding.Status == channels.BindingActive {
			result = append(result, binding)
		}
	}
	return result, nil
}

func newWeComTestAdapter(
	t *testing.T,
	source channels.BindingSource,
	admitter *wecomRecordingAdmitter,
	secrets platformsecret.SecretProvider,
) *Adapter {
	t.Helper()
	adapter, err := NewAdapter(source, gateway.New(admitter), secrets)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func testWeComBinding(tenantID, appID, bindingID, botID, secret string) channels.Binding {
	return channels.Binding{
		TenantID:        tenantID,
		AppID:           appID,
		BindingID:       bindingID,
		Channel:         channels.ChannelWeCom,
		ExternalAccount: botID,
		Secret:          tenant.SecretRef{Name: secret, Version: "v1"},
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}

func readWeComFrame(conn *websocket.Conn) (protocolFrame, error) {
	_, payload, err := conn.ReadMessage()
	if err != nil {
		return protocolFrame{}, err
	}
	var frame protocolFrame
	if err := json.Unmarshal(payload, &frame); err != nil {
		return protocolFrame{}, err
	}
	return frame, nil
}

func writeWeComFrame(conn *websocket.Conn, frame protocolFrame) error {
	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func writeWeComAck(conn *websocket.Conn, requestID string) error {
	return writeWeComFrame(conn, protocolFrame{
		Headers: frameHeaders{ReqID: requestID},
		ErrCode: 0,
		ErrMsg:  "ok",
	})
}

var _ platformsecret.SecretProvider = testWeComSecrets{}
