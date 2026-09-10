package telegram

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	attachmentmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

type telegramAuditWriter struct {
	events     []audit.Event
	failAfter  int
	alwaysFail bool
}

func (w *telegramAuditWriter) Append(_ context.Context, event audit.Event) (audit.AppendResult, error) {
	if w.alwaysFail || (w.failAfter > 0 && len(w.events) >= w.failAfter) {
		return audit.AppendResult{}, errors.New("audit unavailable")
	}
	w.events = append(w.events, event)
	return audit.AppendResult{Event: event}, nil
}

func TestAuditWriterFailureAfterDeliveryIsRedacted(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "audit-failure", "12345")
	writer := &telegramAuditWriter{failAfter: 1}
	dispatcher := &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventMessage, Text: "reply"}, {Type: gateway.DispatchEventDone, Done: true}}}
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	adapter, err := New(context.Background(), Config{BotToken: "12345:runtime-secret", Target: target, Dispatcher: dispatcher, Factory: &fakeFactory{client: client}, AuditWriter: writer})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	if err := adapter.HandleUpdate(context.Background(), textUpdate(40, models.ChatTypePrivate, 100, 42, "input", 0)); !errors.Is(err, ErrDispatch) {
		t.Fatalf("err=%v", err)
	}
}

func TestHandleUpdateAuditFailureBranches(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "audit-branches", "12345")
	update := textUpdate(41, models.ChatTypePrivate, 100, 42, "input", 0)
	admission := newTestAdapter(t, target, &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventDone, Done: true}}}, &fakeBot{me: &models.User{ID: 12345, IsBot: true}})
	admission.audit = audit.NewRecorder(&telegramAuditWriter{alwaysFail: true}, target.TenantID)
	if err := admission.HandleUpdate(context.Background(), update); !errors.Is(err, ErrDispatch) {
		t.Fatalf("admission audit err=%v", err)
	}
	replayWriter := &telegramAuditWriter{failAfter: 2}
	replayDispatcher := &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventMessage, Text: "reply"}, {Type: gateway.DispatchEventDone, Done: true}}}
	replay := newTestAdapter(t, target, replayDispatcher, &fakeBot{me: &models.User{ID: 12345, IsBot: true}})
	replay.audit = audit.NewRecorder(replayWriter, target.TenantID)
	if err := replay.HandleUpdate(context.Background(), textUpdate(42, models.ChatTypePrivate, 100, 42, "replay", 0)); err != nil {
		t.Fatal(err)
	}
	if err := replay.HandleUpdate(context.Background(), textUpdate(42, models.ChatTypePrivate, 100, 42, "replay", 0)); !errors.Is(err, ErrDispatch) {
		t.Fatalf("replay audit err=%v", err)
	}
}

func TestHandleUpdateDuplicateAuditAndDispatchSendFailure(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "audit-duplicate", "12345")
	entered, release := make(chan struct{}), make(chan struct{})
	dispatcher := &dispatchStub{stream: func(ctx context.Context, _ gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		close(entered)
		<-release
		return eventStream(gateway.DispatchEvent{Type: gateway.DispatchEventDone, Done: true}), nil
	}}
	adapter := newTestAdapter(t, target, dispatcher, &fakeBot{me: &models.User{ID: 12345, IsBot: true}})
	adapter.audit = audit.NewRecorder(&telegramAuditWriter{failAfter: 1}, target.TenantID)
	update := textUpdate(43, models.ChatTypePrivate, 100, 42, "pending", 0)
	first := make(chan error, 1)
	go func() { first <- adapter.HandleUpdate(context.Background(), update) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start")
	}
	if err := adapter.HandleUpdate(context.Background(), update); !errors.Is(err, ErrDispatch) {
		t.Fatalf("duplicate audit err=%v", err)
	}
	close(release)
	<-first
	recorder := &errorRecorder{}
	failing := newTestAdapterWithHook(t, target, &dispatchStub{err: errors.New("provider")}, &fakeBot{me: &models.User{ID: 12345, IsBot: true}, sendErr: errors.New("send")}, recorder.hook)
	if err := failing.HandleUpdate(context.Background(), textUpdate(44, models.ChatTypePrivate, 100, 42, "failure", 0)); !errors.Is(err, ErrDispatch) {
		t.Fatalf("dispatch/send err=%v", err)
	}
	if len(recorder.snapshot()) != 2 {
		t.Fatalf("error hooks=%+v", recorder.snapshot())
	}
}

func TestHandleUpdatePreservesDispatchAndSendCancellation(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "cancel-branches", "12345")
	entered := make(chan struct{})
	dispatcher := &dispatchStub{stream: func(ctx context.Context, _ gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	adapter := newTestAdapter(t, target, dispatcher, &fakeBot{me: &models.User{ID: 12345, IsBot: true}})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- adapter.HandleUpdate(ctx, textUpdate(45, models.ChatTypePrivate, 100, 42, "cancel", 0))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatch cancellation err=%v", err)
	}
	sendCtx, sendCancel := context.WithCancel(context.Background())
	sendDispatcher := &dispatchStub{stream: func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		sendCancel()
		return eventStream(gateway.DispatchEvent{Type: gateway.DispatchEventMessage, Text: "reply"}, gateway.DispatchEvent{Type: gateway.DispatchEventDone, Done: true}), nil
	}}
	sendAdapter := newTestAdapter(t, target, sendDispatcher, &fakeBot{me: &models.User{ID: 12345, IsBot: true}})
	if err := sendAdapter.HandleUpdate(sendCtx, textUpdate(46, models.ChatTypePrivate, 100, 42, "send-cancel", 0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("send cancellation err=%v", err)
	}
}

func TestNewInjectsFactoryAndRejectsBotIdentityMismatch(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "constructor", "12345")
	dispatcher := &dispatchStub{}
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	factory := &fakeFactory{client: client}
	adapter, err := New(context.Background(), Config{
		BotToken: "12345:runtime-secret", Target: target, Dispatcher: dispatcher, Factory: factory,
		APIBaseURL: "https://api.example.test", PollTimeout: 3 * time.Second, Workers: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if factory.token != "12345:runtime-secret" || factory.config.Handler == nil {
		t.Fatal("factory did not receive the runtime token and update handler")
	}
	if factory.config.APIBaseURL != "https://api.example.test" || factory.config.PollTimeout != 3*time.Second || factory.config.Workers != 2 {
		t.Fatalf("factory received unexpected options: %+v", factory.config)
	}
	if adapter == nil || adapter.principal.Kind() != gateway.PrincipalChannel {
		t.Fatal("adapter did not retain a trusted channel principal")
	}

	recorder := &errorRecorder{}
	mismatched := &fakeFactory{client: &fakeBot{me: &models.User{ID: 54321, IsBot: true}}}
	_, err = New(context.Background(), Config{
		BotToken: "12345:runtime-secret", Target: target, Dispatcher: dispatcher, Factory: mismatched,
		ErrorHook: recorder.hook,
	})
	if !errors.Is(err, ErrBotIdentityMismatch) {
		t.Fatalf("identity mismatch was accepted or leaked another error: %v", err)
	}
	events := recorder.snapshot()
	if len(events) != 1 || events[0].Operation != ErrorOperationInitialization || !errors.Is(events[0].Err, ErrBotIdentityMismatch) {
		t.Fatalf("unexpected identity error hook: %+v", events)
	}
	if len(dispatcher.requests()) != 0 {
		t.Fatal("identity mismatch reached Dispatch")
	}
}

func TestNewRejectsNonTelegramTargetAndInvalidRuntimeOptions(t *testing.T) {
	wecomTarget := newTrustedTarget(t, channels.ChannelWeCom, "wrong-channel", "corp-1")
	factoryCalls := 0
	factory := BotFactoryFunc(func(string, BotFactoryConfig) (BotClient, error) {
		factoryCalls++
		return &fakeBot{me: &models.User{ID: 1, IsBot: true}}, nil
	})
	_, err := New(context.Background(), Config{
		BotToken: "token", Target: wecomTarget, Dispatcher: &dispatchStub{}, Factory: factory,
	})
	if !errors.Is(err, ErrInvalid) || factoryCalls != 0 {
		t.Fatalf("non-Telegram target was not rejected before factory: err=%v calls=%d", err, factoryCalls)
	}

	target := newTrustedTarget(t, channels.ChannelTelegram, "options", "12345")
	for name, config := range map[string]Config{
		"negative worker":           {BotToken: "token", Target: target, Dispatcher: &dispatchStub{}, Workers: -1},
		"short poll":                {BotToken: "token", Target: target, Dispatcher: &dispatchStub{}, PollTimeout: time.Second},
		"http api":                  {BotToken: "token", Target: target, Dispatcher: &dispatchStub{}, APIBaseURL: "http://insecure.example"},
		"negative attachment bytes": {BotToken: "token", Target: target, Dispatcher: &dispatchStub{}, MaxAttachmentBytes: -1},
		"oversized attachment bytes": {
			BotToken: "token", Target: target, Dispatcher: &dispatchStub{}, MaxAttachmentBytes: maximumAttachmentBytes + 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid option was accepted: %v", err)
			}
		})
	}
}

func TestBotFactoryContractsAndSDKOptions(t *testing.T) {
	var nilFactory BotFactoryFunc
	if _, err := nilFactory.New("token", BotFactoryConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil BotFactoryFunc returned unexpected error: %v", err)
	}
	called := false
	expected := &fakeBot{}
	factory := BotFactoryFunc(func(token string, config BotFactoryConfig) (BotClient, error) {
		called = token == "runtime-token" && config.Workers == 2
		return expected, nil
	})
	client, err := factory.New("runtime-token", BotFactoryConfig{Workers: 2})
	if err != nil || client != expected || !called {
		t.Fatalf("BotFactoryFunc did not delegate: client=%v err=%v called=%v", client, err, called)
	}

	handler := func(context.Context, *bot.Bot, *models.Update) {}
	transport := &http.Client{}
	if configuredHTTPClient(transport, time.Second) != transport || configuredHTTPClient(nil, time.Second) == nil {
		t.Fatal("configured HTTP client did not preserve or create a client")
	}
	sdkClient, err := (sdkBotFactory{}).New("runtime-token", BotFactoryConfig{
		Handler: handler, APIBaseURL: "https://api.example.test", HTTPClient: transport,
		PollTimeout: minimumPollTimeout, Workers: 1, OnPollingError: func() {},
	})
	if err != nil {
		t.Fatalf("SDK factory rejected valid options: %v", err)
	}
	if _, ok := sdkClient.(*bot.Bot); !ok {
		t.Fatalf("SDK factory returned unexpected client type %T", sdkClient)
	}
	for name, config := range map[string]BotFactoryConfig{
		"missing handler": {PollTimeout: minimumPollTimeout, Workers: 1},
		"invalid worker":  {Handler: handler, PollTimeout: minimumPollTimeout, Workers: 0},
		"short timeout":   {Handler: handler, PollTimeout: minimumPollTimeout - time.Nanosecond, Workers: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (sdkBotFactory{}).New("runtime-token", config); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid SDK factory options were accepted: %v", err)
			}
		})
	}
}

func TestNewRedactsConstructionFailuresAndRejectsPreconditions(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "construction-failures", "12345")
	dispatcher := &dispatchStub{}
	var nilContext context.Context
	if _, err := New(nilContext, Config{BotToken: "token", Target: target, Dispatcher: dispatcher}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil construction context returned unexpected error: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(canceled, Config{BotToken: "token", Target: target, Dispatcher: dispatcher}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled construction context returned unexpected error: %v", err)
	}
	for name, config := range map[string]Config{
		"invalid token":        {BotToken: " token", Target: target, Dispatcher: dispatcher},
		"missing dispatcher":   {BotToken: "token", Target: target},
		"untrusted target":     {BotToken: "token", Dispatcher: dispatcher},
		"noncanonical account": {BotToken: "token", Target: newTrustedTarget(t, channels.ChannelTelegram, "noncanonical", "012345"), Dispatcher: dispatcher},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid construction input was accepted: %v", err)
			}
		})
	}
	for name, factory := range map[string]BotFactory{
		"factory error": BotFactoryFunc(func(string, BotFactoryConfig) (BotClient, error) {
			return nil, errors.New("provider token=secret")
		}),
		"nil client": BotFactoryFunc(func(string, BotFactoryConfig) (BotClient, error) {
			return nil, nil
		}),
		"getMe error":          &fakeFactory{client: &fakeBot{meErr: errors.New("provider token=secret")}},
		"missing bot identity": &fakeFactory{client: &fakeBot{}},
		"not a bot":            &fakeFactory{client: &fakeBot{me: &models.User{ID: 12345}}},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := &errorRecorder{}
			_, err := New(context.Background(), Config{
				BotToken: "runtime-token", Target: target, Dispatcher: dispatcher, Factory: factory,
				ErrorHook: recorder.hook,
			})
			want := ErrInitialization
			if name == "missing bot identity" || name == "not a bot" {
				want = ErrBotIdentityMismatch
			}
			if !errors.Is(err, want) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("construction failure was not stable/redacted: err=%v want=%v", err, want)
			}
			if len(recorder.snapshot()) != 1 {
				t.Fatalf("construction failure did not report exactly one hook event: %+v", recorder.snapshot())
			}
		})
	}
}

func TestNewPreservesCancellationDuringGetMe(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "getme-cancel", "12345")
	started := make(chan struct{})
	client := &fakeBot{getMeFn: func(ctx context.Context) (*models.User, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	factory := &fakeFactory{client: client}
	recorder := &errorRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := New(ctx, Config{
			BotToken: "runtime-token", Target: target, Dispatcher: &dispatchStub{}, Factory: factory,
			ErrorHook: recorder.hook,
		})
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("getMe did not start")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("getMe cancellation was remapped: %v", err)
	}
	if events := recorder.snapshot(); len(events) != 0 {
		t.Fatalf("getMe cancellation emitted an initialization failure hook: %+v", events)
	}
}

func TestPollingErrorsUseStableRedactedHook(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "polling-hook", "12345")
	recorder := &errorRecorder{}
	factory := &fakeFactory{client: &fakeBot{me: &models.User{ID: 12345, IsBot: true}}}
	adapter, err := New(context.Background(), Config{
		BotToken: "12345:runtime-secret", Target: target, Dispatcher: &dispatchStub{}, Factory: factory,
		ErrorHook: recorder.hook,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	factory.config.OnPollingError()
	events := recorder.snapshot()
	if len(events) != 1 || events[0].Operation != ErrorOperationPolling || !errors.Is(events[0].Err, ErrPolling) {
		t.Fatalf("unexpected polling error hook: %+v", events)
	}
	if strings.Contains(events[0].Err.Error(), "runtime-secret") {
		t.Fatal("polling error hook leaked the runtime token")
	}
}

func TestHandleUpdateMapsPrivateTextAndAggregatesDispatchEvents(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "private", "12345")
	dispatcher := &dispatchStub{events: []gateway.DispatchEvent{
		{Type: gateway.DispatchEventMessage, Text: "hello "},
		{Type: gateway.DispatchEventStatus, Status: "partial"},
		{Type: gateway.DispatchEventMessage, Text: "world"},
		{Type: gateway.DispatchEventDone, Done: true},
	}}
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	adapter := newTestAdapter(t, target, dispatcher, client)
	aw := &telegramAuditWriter{}
	adapter.audit = audit.NewRecorder(aw, target.TenantID)
	key := contextKey("request-context")
	ctx := context.WithValue(context.Background(), key, "preserved")
	update := textUpdate(7, models.ChatTypePrivate, 100, 42, "  hello  ", 0)
	if err := adapter.HandleUpdate(ctx, update); err != nil {
		t.Fatal(err)
	}

	requests := dispatcher.requests()
	if len(requests) != 1 {
		t.Fatalf("expected one Dispatch call, got %d", len(requests))
	}
	request := requests[0]
	assertPrivateInboundRequest(t, target, request, dispatcher.contextValue(key), key)
	sent := client.sent()
	assertPrivateReply(t, sent, aw.events)
}

func assertPrivateInboundRequest(t *testing.T, target channels.RoutingTarget, request gateway.DispatchRequest, contextValue any, key contextKey) {
	t.Helper()
	if request.Principal.Kind() != gateway.PrincipalChannel || request.Principal.TenantID() != target.TenantID || request.Principal.AppID() != target.AppID {
		t.Fatalf("Dispatch did not receive the trusted principal: %+v", request.Principal)
	}
	if request.Message.Content != "hello" || request.Message.ContentType != gateway.ContentTypeText || request.Message.ExternalUserID != "42" || request.Message.ExternalPeerID != "100" || request.Message.ConversationKind != channels.ConversationDirect {
		t.Fatalf("unexpected private inbound message: %+v", request.Message)
	}
	expectedID := externalMessageID(target, 7)
	if request.Message.ExternalMessageID != expectedID || request.RequestID != expectedID {
		t.Fatalf("unexpected stable message/request ID: message=%q request=%q expected=%q", request.Message.ExternalMessageID, request.RequestID, expectedID)
	}
	if contextValue != "preserved" {
		t.Fatalf("Dispatch did not receive the caller Context: %v", contextValue)
	}
}

func assertPrivateReply(t *testing.T, sent []sentMessage, events []audit.Event) {
	t.Helper()
	if len(sent) != 1 || sent[0].Text != "hello world" || sent[0].ChatID != 100 || sent[0].ThreadID != 0 {
		t.Fatalf("unexpected aggregated Telegram reply: %+v", sent)
	}
	if len(events) != 2 || events[0].EventType != audit.EventIMIngressAccepted || events[1].EventType != audit.EventIMDeliverySent {
		t.Fatalf("audit events = %#v", events)
	}
}

func TestHandleUpdateMapsGroupThreadAndSplitsUnicodeReply(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "group", "12345")
	reply := strings.Repeat("界", maximumReplyRunes) + "🙂"
	dispatcher := &dispatchStub{events: []gateway.DispatchEvent{
		{Type: gateway.DispatchEventMessage, Text: reply},
		{Type: gateway.DispatchEventDone, Done: true},
	}}
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	adapter := newTestAdapter(t, target, dispatcher, client)
	update := textUpdate(8, models.ChatTypeSupergroup, -100, 42, "group text", 9)
	if err := adapter.HandleUpdate(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	request := dispatcher.requests()[0]
	if request.Message.ConversationKind != channels.ConversationGroup || request.Message.ExternalChatID != "-100" || request.Message.ExternalThreadID != "9" {
		t.Fatalf("unexpected group/thread mapping: %+v", request.Message)
	}
	sent := client.sent()
	if len(sent) != 2 || len([]rune(sent[0].Text)) != maximumReplyRunes || len([]rune(sent[1].Text)) != 1 {
		t.Fatalf("reply was not split at Unicode code-point boundary: %+v", sent)
	}
	for _, message := range sent {
		if message.ChatID != -100 || message.ThreadID != 9 {
			t.Fatalf("group/thread routing was not preserved: %+v", sent)
		}
	}
}

func TestDuplicateDeliveryUsesProcessLocalIdempotency(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "duplicate", "12345")
	dispatcher := &dispatchStub{events: []gateway.DispatchEvent{
		{Type: gateway.DispatchEventMessage, Text: "once"}, {Type: gateway.DispatchEventDone, Done: true},
	}}
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	adapter := newTestAdapter(t, target, dispatcher, client)
	update := textUpdate(9, models.ChatTypePrivate, 100, 42, "duplicate", 0)
	if err := adapter.HandleUpdate(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if err := adapter.HandleUpdate(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.requests()) != 1 || len(client.sent()) != 2 {
		t.Fatalf("completed duplicate did not replay the cached logical reply: dispatch=%d sends=%d", len(dispatcher.requests()), len(client.sent()))
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	pendingDispatcher := &dispatchStub{stream: func(ctx context.Context, _ gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return eventStream(gateway.DispatchEvent{Type: gateway.DispatchEventMessage, Text: "pending"}, gateway.DispatchEvent{Type: gateway.DispatchEventDone, Done: true}), nil
	}}
	pendingClient := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	pendingAdapter := newTestAdapter(t, target, pendingDispatcher, pendingClient)
	firstDone := make(chan error, 1)
	go func() { firstDone <- pendingAdapter.HandleUpdate(context.Background(), update) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first update did not reach Dispatch")
	}
	if err := pendingAdapter.HandleUpdate(context.Background(), update); !errors.Is(err, ErrDuplicateUpdate) {
		t.Fatalf("pending duplicate was dispatched instead of rejected: %v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if len(pendingDispatcher.requests()) != 1 {
		t.Fatalf("pending duplicate started more than one dispatch: %d", len(pendingDispatcher.requests()))
	}
}

func TestUnsupportedAndMalformedUpdatesNeverDispatch(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "unsupported", "12345")
	dispatcher := &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventDone, Done: true}}}
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	adapter := newTestAdapter(t, target, dispatcher, client)
	valid := textUpdate(10, models.ChatTypePrivate, 100, 42, "valid", 0)
	privateCommand := textUpdate(101, models.ChatTypePrivate, 100, 42, "/start", 0)
	privateCommand.Message.Entities = []models.MessageEntity{{Type: models.MessageEntityTypeBotCommand, Offset: 0, Length: 6}}
	groupCommand := textUpdate(102, models.ChatTypeGroup, -100, 42, "/help@bot", 0)
	groupCommand.Message.Entities = []models.MessageEntity{{Type: models.MessageEntityTypeBotCommand, Offset: 0, Length: 10}}
	cases := []struct {
		name   string
		update *models.Update
		want   error
	}{
		{name: "nil", update: nil, want: ErrInvalidUpdate},
		{name: "empty update", update: &models.Update{ID: 11}, want: ErrUnsupportedUpdate},
		{name: "edited", update: &models.Update{ID: 12, EditedMessage: valid.Message}, want: ErrUnsupportedUpdate},
		{name: "callback", update: &models.Update{ID: 13, CallbackQuery: &models.CallbackQuery{}}, want: ErrUnsupportedUpdate},
		{name: "media only", update: &models.Update{ID: 14, Message: &models.Message{Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate}, From: &models.User{ID: 42}, Photo: []models.PhotoSize{{}}}}, want: ErrUnsupportedUpdate},
		{name: "service with text", update: &models.Update{ID: 141, Message: &models.Message{Text: "system", Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate}, From: &models.User{ID: 42}, NewChatTitle: "renamed"}}, want: ErrUnsupportedUpdate},
		{name: "private command", update: privateCommand, want: ErrUnsupportedUpdate},
		{name: "group command", update: groupCommand, want: ErrUnsupportedUpdate},
		{name: "missing sender", update: &models.Update{ID: 15, Message: &models.Message{Text: "x", Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate}}}, want: ErrInvalidUpdate},
		{name: "channel", update: textUpdate(16, models.ChatTypeChannel, 100, 42, "x", 0), want: ErrUnsupportedUpdate},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := adapter.HandleUpdate(context.Background(), test.update); !errors.Is(err, test.want) {
				t.Fatalf("unexpected rejection: got=%v want=%v", err, test.want)
			}
		})
	}
	if len(dispatcher.requests()) != 0 || len(client.sent()) != 0 {
		t.Fatalf("unsupported updates reached Gateway or Telegram: dispatch=%d sends=%d", len(dispatcher.requests()), len(client.sent()))
	}
}

func TestMessageContentAndHasMediaBranches(t *testing.T) {
	cases := []struct {
		name        string
		message     *models.Message
		wantContent string
		wantType    string
		wantOK      bool
	}{
		{name: "nil", message: nil},
		{name: "text", message: &models.Message{Text: " hello "}, wantContent: " hello ", wantType: gateway.ContentTypeText, wantOK: true},
		{name: "caption media", message: &models.Message{Caption: "caption", Photo: []models.PhotoSize{{FileID: "photo"}}}, wantContent: "caption", wantType: gateway.ContentTypeMedia, wantOK: true},
		{name: "media marker", message: &models.Message{Video: &models.Video{}}, wantContent: "[telegram media]", wantType: gateway.ContentTypeMedia, wantOK: true},
		{name: "rich marker", message: &models.Message{RichMessage: &models.RichMessage{}}, wantContent: "[telegram rich message]", wantType: gateway.ContentTypeRich, wantOK: true},
		{name: "caption without media", message: &models.Message{Caption: "caption"}},
		{name: "empty", message: &models.Message{}},
		{name: "empty photo", message: &models.Message{Photo: []models.PhotoSize{{FileID: " "}}}},
		{name: "text wins", message: &models.Message{Text: "text", Video: &models.Video{}}, wantContent: "text", wantType: gateway.ContentTypeText, wantOK: true},
		{name: "secondary media", message: &models.Message{Dice: &models.Dice{}}, wantContent: "[telegram media]", wantType: gateway.ContentTypeMedia, wantOK: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			content, contentType, ok := messageContent(test.message)
			if content != test.wantContent || contentType != test.wantType || ok != test.wantOK {
				t.Fatalf("messageContent = (%q, %q, %t), want (%q, %q, %t)", content, contentType, ok, test.wantContent, test.wantType, test.wantOK)
			}
		})
	}

	if !hasMedia(&models.Message{Photo: []models.PhotoSize{{}, {FileID: "photo"}}}) {
		t.Fatal("photo with a later valid file ID was not detected")
	}
	if hasMedia(&models.Message{Photo: []models.PhotoSize{{}}}) {
		t.Fatal("photo without a file ID was detected as media")
	}
}

func TestNativeMediaIsPersistedAndDispatchedAsReference(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "native-media", "12345")
	dispatcher := &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventDone, Done: true}}}
	data := []byte("telegram-image")
	adapter, err := New(context.Background(), Config{
		BotToken: "12345:runtime-secret", Target: target, Dispatcher: dispatcher,
		Factory:     &fakeFactory{client: &fakeBot{me: &models.User{ID: 12345, IsBot: true}}},
		Attachments: attachmentmemory.New(), MediaDownloader: fakeMediaDownloader{data: data},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	update := &models.Update{ID: 201, Message: &models.Message{
		ID: 201, From: &models.User{ID: 42}, Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate},
		Caption: "please inspect", Photo: []models.PhotoSize{{FileID: "photo-small", FileSize: 3}, {FileID: "photo-large", FileSize: len(data)}},
	}}
	if err := adapter.HandleUpdate(context.Background(), update); err != nil {
		t.Fatalf("HandleUpdate = %v", err)
	}
	requests := dispatcher.requests()
	if len(requests) != 1 || requests[0].Message.Content != "please inspect" || len(requests[0].Message.Attachments) != 1 {
		t.Fatalf("dispatch request = %+v", requests)
	}
	reference := requests[0].Message.Attachments[0]
	if reference.Kind != attachment.KindImage || reference.MIMEType != "image/jpeg" || reference.Provider != "telegram" || reference.ProviderID != "photo-large" || reference.Size != int64(len(data)) {
		t.Fatalf("attachment reference = %+v", reference)
	}
	if !strings.HasPrefix(reference.ID, "att_") || len(reference.SHA256) != sha256.Size*2 {
		t.Fatalf("attachment identity = %+v", reference)
	}
}

func TestNativeMediaFailuresAreRedactedAndCancellationPreserved(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "native-media-errors", "12345")
	base := Config{BotToken: "12345:runtime-secret", Target: target, Factory: &fakeFactory{client: &fakeBot{me: &models.User{ID: 12345, IsBot: true}}}, Attachments: attachmentmemory.New()}
	tooLarge := base
	tooLarge.Dispatcher = &dispatchStub{}
	tooLarge.MediaDownloader = fakeMediaDownloader{data: []byte("123456"), err: nil}
	tooLarge.MaxAttachmentBytes = 5
	adapter, err := New(context.Background(), tooLarge)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.HandleUpdate(context.Background(), mediaUpdate(202, "oversized")); !errors.Is(err, ErrAttachment) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("oversized media err = %v", err)
	}
	if got := len(tooLarge.Dispatcher.(*dispatchStub).requests()); got != 0 {
		t.Fatalf("oversized media reached dispatch: %d", got)
	}
	_ = adapter.Close()

	canceled := base
	canceled.Dispatcher = &dispatchStub{}
	ctx, cancel := context.WithCancel(context.Background())
	canceled.MediaDownloader = fakeMediaDownloader{wait: func(context.Context) { cancel() }}
	adapter, err = New(context.Background(), canceled)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	if err := adapter.HandleUpdate(ctx, mediaUpdate(203, "cancel")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled media err = %v", err)
	}
	if got := len(canceled.Dispatcher.(*dispatchStub).requests()); got != 0 {
		t.Fatalf("canceled media reached dispatch: %d", got)
	}
}

func TestTelegramMediaDownloaderFetchesBoundedHTTPSFile(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/file/photos/chart.jpg" {
			t.Fatalf("download path = %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, "telegram-bytes")
	}))
	defer server.Close()
	client := &fakeTelegramFileClient{file: &models.File{FileID: "file-1", FilePath: "photos/chart.jpg"}, link: server.URL + "/file/photos/chart.jpg"}
	downloader := telegramMediaDownloader{client: client, httpClient: server.Client(), maximum: 64}
	body, err := downloader.Download(context.Background(), "file-1")
	if err != nil {
		t.Fatalf("Download = %v", err)
	}
	data, err := io.ReadAll(body)
	closeErr := body.Close()
	if err != nil || closeErr != nil || string(data) != "telegram-bytes" {
		t.Fatalf("download body = %q read=%v close=%v", data, err, closeErr)
	}
	if client.fileID != "file-1" {
		t.Fatalf("GetFile id = %q", client.fileID)
	}
}

func TestTelegramMediaDownloaderRejectsUnsafeOrUnavailableFiles(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			http.Error(w, "nope", http.StatusBadGateway)
		case "/declared":
			w.Header().Set("Content-Length", "99")
			_, _ = io.WriteString(w, "x")
		case "/large":
			_, _ = io.WriteString(w, "123456")
		default:
			_, _ = io.WriteString(w, "ok")
		}
	}))
	defer server.Close()

	for _, test := range []struct {
		name       string
		ctx        context.Context
		downloader telegramMediaDownloader
		fileID     string
		want       error
	}{
		{name: "nil context", downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{}, httpClient: server.Client(), maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "missing client", ctx: context.Background(), downloader: telegramMediaDownloader{httpClient: server.Client(), maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "empty file id", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{}, httpClient: server.Client(), maximum: 8}, want: ErrAttachment},
		{name: "zero maximum", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{}, httpClient: server.Client()}, fileID: "file", want: ErrAttachment},
		{name: "get file error", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{err: errors.New("secret token")}, httpClient: server.Client(), maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "missing file path", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{file: &models.File{FileID: "file"}}, httpClient: server.Client(), maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "plain http URL", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{file: &models.File{FilePath: "file"}, link: "http://example.com/file"}, httpClient: server.Client(), maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "URL with query", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{file: &models.File{FilePath: "file"}, link: server.URL + "/file?token=secret"}, httpClient: server.Client(), maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "transport error", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{file: &models.File{FilePath: "file"}, link: server.URL + "/file"}, httpClient: &http.Client{Transport: telegramRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("transport secret") })}, maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "provider status", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{file: &models.File{FilePath: "file"}, link: server.URL + "/status"}, httpClient: server.Client(), maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "declared size", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{file: &models.File{FilePath: "file"}, link: server.URL + "/declared"}, httpClient: server.Client(), maximum: 8}, fileID: "file", want: ErrAttachment},
		{name: "body too large", ctx: context.Background(), downloader: telegramMediaDownloader{client: &fakeTelegramFileClient{file: &models.File{FilePath: "file"}, link: server.URL + "/large"}, httpClient: server.Client(), maximum: 5}, fileID: "file", want: ErrAttachment},
	} {
		t.Run(test.name, func(t *testing.T) {
			body, err := test.downloader.Download(test.ctx, test.fileID)
			if body != nil {
				_ = body.Close()
			}
			if !errors.Is(err, test.want) || strings.Contains(fmt.Sprint(err), "secret") {
				t.Fatalf("Download error = %v, want %v", err, test.want)
			}
		})
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	downloader := telegramMediaDownloader{client: &fakeTelegramFileClient{file: &models.File{FilePath: "file"}, link: server.URL + "/file"}, httpClient: server.Client(), maximum: 8}
	if _, err := downloader.Download(canceled, "file"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Download = %v", err)
	}
	if _, err := readTelegramMediaResponse(context.Background(), nil, 1); !errors.Is(err, ErrAttachment) {
		t.Fatalf("nil response = %v", err)
	}
	if _, err := readTelegramMediaResponse(context.Background(), &http.Response{}, 1); !errors.Is(err, ErrAttachment) {
		t.Fatalf("nil body response = %v", err)
	}
}

func TestNativeAttachmentsClassifyTelegramMediaFamilies(t *testing.T) {
	for _, test := range []struct {
		name    string
		message *models.Message
		want    telegramAttachment
	}{
		{name: "nil message"},
		{name: "largest photo", message: &models.Message{Photo: []models.PhotoSize{{FileID: ""}, {FileID: "small", FileSize: 1, Width: 10, Height: 10}, {FileID: "large", FileSize: 2, Width: 2, Height: 2}}}, want: telegramAttachment{fileID: "large", kind: attachment.KindImage, mimeType: "image/jpeg", name: "large.jpg"}},
		{name: "video default mime and safe name", message: &models.Message{Video: &models.Video{FileID: "video-1", FileName: "bad/name", MimeType: "application/octet-stream"}}, want: telegramAttachment{fileID: "video-1", kind: attachment.KindVideo, mimeType: "video/mp4", name: "video-1.mp4"}},
		{name: "animation keeps mime", message: &models.Message{Animation: &models.Animation{FileID: "anim-1", FileName: "clip.mp4", MimeType: "video/mp4"}}, want: telegramAttachment{fileID: "anim-1", kind: attachment.KindVideo, mimeType: "video/mp4", name: "clip.mp4"}},
		{name: "audio default suffix", message: &models.Message{Audio: &models.Audio{FileID: "audio-1", MimeType: "audio/mpeg"}}, want: telegramAttachment{fileID: "audio-1", kind: attachment.KindAudio, mimeType: "audio/mpeg", name: "audio-1.mp3"}},
		{name: "voice default", message: &models.Message{Voice: &models.Voice{FileID: "voice-1", MimeType: "audio/ogg"}}, want: telegramAttachment{fileID: "voice-1", kind: attachment.KindAudio, mimeType: "audio/ogg", name: "voice-1.ogg"}},
		{name: "document image", message: &models.Message{Document: &models.Document{FileID: "doc-image", FileName: "scan.png", MimeType: "image/png"}}, want: telegramAttachment{fileID: "doc-image", kind: attachment.KindImage, mimeType: "image/png", name: "scan.png"}},
		{name: "document video", message: &models.Message{Document: &models.Document{FileID: "doc-video", FileName: "clip.mov", MimeType: "video/quicktime"}}, want: telegramAttachment{fileID: "doc-video", kind: attachment.KindVideo, mimeType: "video/quicktime", name: "clip.mov"}},
		{name: "document audio", message: &models.Message{Document: &models.Document{FileID: "doc-audio", FileName: "voice.mp3", MimeType: "audio/mpeg"}}, want: telegramAttachment{fileID: "doc-audio", kind: attachment.KindAudio, mimeType: "audio/mpeg", name: "voice.mp3"}},
		{name: "document invalid MIME fallback", message: &models.Message{Document: &models.Document{FileID: "doc-1", FileName: "bad/name", MimeType: "image/png; charset=utf-8"}}, want: telegramAttachment{fileID: "doc-1", kind: attachment.KindDocument, mimeType: "application/octet-stream", name: "doc-1"}},
		{name: "video note", message: &models.Message{VideoNote: &models.VideoNote{FileID: "round-1"}}, want: telegramAttachment{fileID: "round-1", kind: attachment.KindVideo, mimeType: "video/mp4", name: "round-1.mp4"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := nativeAttachments(test.message)
			if test.want == (telegramAttachment{}) {
				if got != nil {
					t.Fatalf("nativeAttachments = %+v, want nil", got)
				}
				return
			}
			if len(got) != 1 || got[0] != test.want {
				t.Fatalf("nativeAttachments = %+v, want %+v", got, test.want)
			}
		})
	}
	if got := nativeAttachments(&models.Message{Document: &models.Document{FileID: ""}}); got != nil {
		t.Fatalf("empty document file ID = %+v", got)
	}
	if kindSupportsMIME(attachment.Kind("unknown"), "application/octet-stream") {
		t.Fatal("unknown attachment kind matched MIME")
	}
	if kindSupportsMIME(attachment.KindDocument, "image/png") {
		t.Fatal("document attachment accepted image MIME")
	}
	if got := mediaMIME(attachment.KindAudio, "application/octet-stream"); got != "audio/mpeg" {
		t.Fatalf("audio fallback MIME = %q", got)
	}
	if got := mediaMIME(attachment.KindImage, "application/octet-stream"); got != "application/octet-stream" {
		t.Fatalf("image fallback MIME = %q", got)
	}
}

type fakeMediaDownloader struct {
	data []byte
	err  error
	wait func(context.Context)
}

func (downloader fakeMediaDownloader) Download(ctx context.Context, _ string) (io.ReadCloser, error) {
	if downloader.wait != nil {
		downloader.wait(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if downloader.err != nil {
		return nil, downloader.err
	}
	return io.NopCloser(bytes.NewReader(downloader.data)), nil
}

func mediaUpdate(updateID int64, fileID string) *models.Update {
	return &models.Update{ID: updateID, Message: &models.Message{
		ID: int(updateID), From: &models.User{ID: 42}, Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate},
		Video: &models.Video{FileID: fileID},
	}}
}

type fakeTelegramFileClient struct {
	file   *models.File
	err    error
	link   string
	fileID string
}

func (client *fakeTelegramFileClient) GetFile(ctx context.Context, params *bot.GetFileParams) (*models.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.fileID = params.FileID
	if client.err != nil {
		return nil, client.err
	}
	return client.file, nil
}

func (client *fakeTelegramFileClient) FileDownloadLink(*models.File) string {
	return client.link
}

type telegramRoundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip telegramRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestDispatchAndSendFailuresAreRedacted(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "failure", "12345")
	recorder := &errorRecorder{}
	dispatcher := &dispatchStub{err: errors.New("provider token=secret stack=private")}
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	adapter := newTestAdapterWithHook(t, target, dispatcher, client, recorder.hook)
	update := textUpdate(17, models.ChatTypePrivate, 100, 42, "fail", 0)
	if err := adapter.HandleUpdate(context.Background(), update); !errors.Is(err, ErrDispatch) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("dispatch failure was not redacted: %v", err)
	}
	sent := client.sent()
	if len(sent) != 1 || sent[0].Text != failureReply || strings.Contains(sent[0].Text, "secret") {
		t.Fatalf("dispatch failure did not produce a fixed reply: %+v", sent)
	}
	if events := recorder.snapshot(); len(events) != 1 || events[0].Operation != ErrorOperationDispatch || !errors.Is(events[0].Err, ErrDispatch) {
		t.Fatalf("unexpected dispatch failure hook: %+v", events)
	}

	sendRecorder := &errorRecorder{}
	sendDispatcher := &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventMessage, Text: "reply"}, {Type: gateway.DispatchEventDone, Done: true}}}
	sendClient := &fakeBot{me: &models.User{ID: 12345, IsBot: true}, sendErr: errors.New("provider token=secret")}
	sendAdapter := newTestAdapterWithHook(t, target, sendDispatcher, sendClient, sendRecorder.hook)
	if err := sendAdapter.HandleUpdate(context.Background(), update); !errors.Is(err, ErrSendMessage) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("send failure was not redacted: %v", err)
	}
	if events := sendRecorder.snapshot(); len(events) != 1 || events[0].Operation != ErrorOperationSend || !errors.Is(events[0].Err, ErrSendMessage) {
		t.Fatalf("unexpected send failure hook: %+v", events)
	}
}

func TestAuditWriterRecordsTelegramIngressAndDelivery(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "audit-hooks", "12345")
	writer := &telegramAuditWriter{}
	dispatcher := &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventMessage, Text: "reply"}, {Type: gateway.DispatchEventDone, Done: true}}}
	adapter, err := New(context.Background(), Config{BotToken: "12345:runtime-secret", Target: target, Dispatcher: dispatcher, Factory: &fakeFactory{client: &fakeBot{me: &models.User{ID: 12345, IsBot: true}}}, AuditWriter: writer})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	if err := adapter.HandleUpdate(context.Background(), textUpdate(30, models.ChatTypePrivate, 100, 42, "input", 0)); err != nil {
		t.Fatal(err)
	}
	if err := adapter.HandleUpdate(context.Background(), textUpdate(30, models.ChatTypePrivate, 100, 42, "input", 0)); err != nil {
		t.Fatalf("replay err=%v", err)
	}
	events := writer.events
	seen := map[audit.EventType]bool{}
	for _, event := range events {
		seen[event.EventType] = true
	}
	if !seen[audit.EventIMIngressAccepted] || !seen[audit.EventIMDeliverySent] {
		t.Fatalf("audit events=%v", events)
	}
}

func TestRunCancellationAndCloseStopPolling(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "lifecycle", "12345")
	started := make(chan struct{})
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}, startFn: func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	}}
	adapter := newTestAdapter(t, target, &dispatchStub{}, client)
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Run did not start polling")
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned an unexpected error after Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel polling")
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := adapter.HandleUpdate(context.Background(), textUpdate(18, models.ChatTypePrivate, 100, 42, "closed", 0)); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed adapter accepted an update: %v", err)
	}
}

func TestRunAndHandleUpdatePreconditions(t *testing.T) {
	var nilAdapter *Adapter
	if err := nilAdapter.Run(context.Background()); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil adapter Run returned unexpected error: %v", err)
	}
	if err := nilAdapter.HandleUpdate(context.Background(), nil); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil adapter HandleUpdate returned unexpected error: %v", err)
	}
	if err := nilAdapter.Close(); err != nil {
		t.Fatalf("nil adapter Close returned unexpected error: %v", err)
	}

	bare := &Adapter{}
	var nilContext context.Context
	if err := bare.Run(nilContext); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil Run context returned unexpected error: %v", err)
	}
	if err := bare.Run(context.Background()); !errors.Is(err, ErrNotReady) {
		t.Fatalf("bare adapter Run returned unexpected error: %v", err)
	}
	if err := bare.HandleUpdate(context.Background(), nil); !errors.Is(err, ErrNotReady) {
		t.Fatalf("bare adapter HandleUpdate returned unexpected error: %v", err)
	}

	target := newTrustedTarget(t, channels.ChannelTelegram, "preconditions", "12345")
	adapter := newTestAdapter(t, target, &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventDone, Done: true}}}, &fakeBot{me: &models.User{ID: 12345, IsBot: true}})
	if err := adapter.HandleUpdate(nilContext, textUpdate(19, models.ChatTypePrivate, 100, 42, "text", 0)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil HandleUpdate context returned unexpected error: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := adapter.HandleUpdate(canceled, textUpdate(20, models.ChatTypePrivate, 100, 42, "text", 0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled HandleUpdate context returned unexpected error: %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunRejectsConcurrentStart(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "concurrent-run", "12345")
	started := make(chan struct{})
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}, startFn: func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	}}
	adapter := newTestAdapter(t, target, &dispatchStub{}, client)
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Run did not start")
	}
	if err := adapter.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("concurrent Run returned unexpected error: %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("first Run returned unexpected error: %v", err)
	}
}

func TestDispatchTerminalAndReplyHelpers(t *testing.T) {
	message := gateway.InboundMessage{Content: "text", ContentType: gateway.ContentTypeText, ExternalMessageID: "message-1", ExternalUserID: "42", ConversationKind: channels.ConversationDirect, ExternalPeerID: "100"}
	for name, stub := range map[string]*dispatchStub{
		"nil stream":     {stream: func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) { return nil, nil }},
		"incomplete":     {events: []gateway.DispatchEvent{{Type: gateway.DispatchEventMessage, Text: "partial"}}},
		"event error":    {events: []gateway.DispatchEvent{{Type: gateway.DispatchEventError, Error: "redacted"}, {Type: gateway.DispatchEventDone, Done: true}}},
		"provider error": {err: errors.New("provider token=secret")},
	} {
		t.Run(name, func(t *testing.T) {
			adapter := &Adapter{dispatcher: stub}
			if _, err := adapter.dispatch(context.Background(), message); !errors.Is(err, ErrDispatch) {
				t.Fatalf("dispatch returned unexpected error: %v", err)
			}
		})
	}
	contextError := &Adapter{dispatcher: &dispatchStub{err: context.Canceled}}
	if _, err := contextError.dispatch(context.Background(), message); !errors.Is(err, context.Canceled) {
		t.Fatalf("context dispatch error was not preserved: %v", err)
	}
	waiting := make(chan gateway.DispatchEvent)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	cancelAdapter := &Adapter{dispatcher: &dispatchStub{stream: func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
		return waiting, nil
	}}}
	if _, err := cancelAdapter.dispatch(canceled, message); !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatch cancellation was not preserved: %v", err)
	}

	target := newTrustedTarget(t, channels.ChannelTelegram, "reply-helpers", "12345")
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	adapter := newTestAdapter(t, target, &dispatchStub{}, client)
	tgMessage := &models.Message{Chat: models.Chat{ID: 100, Type: models.ChatTypePrivate}}
	if err := adapter.sendEvents(context.Background(), tgMessage, []gateway.DispatchEvent{{Type: gateway.DispatchEventError}}); err != nil {
		t.Fatalf("error event did not send fixed reply: %v", err)
	}
	if err := adapter.sendEvents(context.Background(), tgMessage, []gateway.DispatchEvent{{Type: gateway.DispatchEventStatus, Status: "empty"}}); err != nil {
		t.Fatalf("empty event stream produced unexpected reply error: %v", err)
	}
	if err := adapter.sendText(context.Background(), nil, "text"); !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("nil outbound message returned unexpected error: %v", err)
	}
	if err := (*Adapter)(nil).sendText(context.Background(), tgMessage, "text"); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil outbound adapter returned unexpected error: %v", err)
	}
	canceledSend, cancelSend := context.WithCancel(context.Background())
	cancelSend()
	if err := adapter.sendText(canceledSend, tgMessage, "text"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled outbound context returned unexpected error: %v", err)
	}
	if got := splitText("", maximumReplyRunes); got != nil {
		t.Fatalf("empty reply produced chunks: %#v", got)
	}
}

func TestSDKHandlerUsesSingleUpdatePath(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "sdk-handler", "12345")
	client := &fakeBot{me: &models.User{ID: 12345, IsBot: true}}
	dispatcher := &dispatchStub{events: []gateway.DispatchEvent{{Type: gateway.DispatchEventMessage, Text: "reply"}, {Type: gateway.DispatchEventDone, Done: true}}}
	factory := &fakeFactory{client: client}
	adapter, err := New(context.Background(), Config{BotToken: "runtime-token", Target: target, Dispatcher: dispatcher, Factory: factory})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	factory.config.Handler(context.Background(), nil, textUpdate(21, models.ChatTypePrivate, 100, 42, "input", 0))
	if len(dispatcher.requests()) != 1 || len(client.sent()) != 1 || client.sent()[0].Text != "reply" {
		t.Fatalf("SDK handler did not route one update: dispatch=%d sends=%v", len(dispatcher.requests()), client.sent())
	}
}

func TestBindingIsolationAndStableUnicodeChunking(t *testing.T) {
	first := newTrustedTarget(t, channels.ChannelTelegram, "isolation-one", "12345")
	second := newTrustedTarget(t, channels.ChannelTelegram, "isolation-two", "12345")
	input := channels.IdentityInput{ExternalUserID: "42", Kind: channels.ConversationGroup, ExternalChatID: "-100", ExternalThreadID: "9"}
	firstIdentity, err := first.RunnerIdentity(input)
	if err != nil {
		t.Fatal(err)
	}
	secondIdentity, err := second.RunnerIdentity(input)
	if err != nil {
		t.Fatal(err)
	}
	if firstIdentity.UserID == secondIdentity.UserID || firstIdentity.SessionID == secondIdentity.SessionID || first.TenantID == second.TenantID || first.BindingID == second.BindingID {
		t.Fatal("same external IDs crossed tenant or Binding identity boundaries")
	}
	chunks := splitText(strings.Repeat("界", maximumReplyRunes+1), maximumReplyRunes)
	if len(chunks) != 2 || len([]rune(chunks[0])) != maximumReplyRunes || len([]rune(chunks[1])) != 1 {
		t.Fatalf("unexpected Unicode chunks: lengths=%d,%d", len([]rune(chunks[0])), len([]rune(chunks[1])))
	}
}

type contextKey string

type dispatchStub struct {
	mu           sync.Mutex
	requestsList []gateway.DispatchRequest
	contexts     []context.Context
	events       []gateway.DispatchEvent
	err          error
	stream       func(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error)
}

func (stub *dispatchStub) Dispatch(ctx context.Context, request gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	stub.mu.Lock()
	stub.requestsList = append(stub.requestsList, request)
	stub.contexts = append(stub.contexts, ctx)
	err, stream, events := stub.err, stub.stream, append([]gateway.DispatchEvent(nil), stub.events...)
	stub.mu.Unlock()
	if stream != nil {
		return stream(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	return eventStream(events...), nil
}

func (stub *dispatchStub) requests() []gateway.DispatchRequest {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return append([]gateway.DispatchRequest(nil), stub.requestsList...)
}

func (stub *dispatchStub) contextValue(key contextKey) any {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.contexts) == 0 {
		return nil
	}
	return stub.contexts[0].Value(key)
}

func eventStream(events ...gateway.DispatchEvent) <-chan gateway.DispatchEvent {
	stream := make(chan gateway.DispatchEvent, len(events))
	for _, event := range events {
		stream <- event
	}
	close(stream)
	return stream
}

type fakeFactory struct {
	client *fakeBot
	token  string
	config BotFactoryConfig
}

func (factory *fakeFactory) New(token string, config BotFactoryConfig) (BotClient, error) {
	factory.token = token
	factory.config = config
	return factory.client, nil
}

type fakeBot struct {
	mu      sync.Mutex
	me      *models.User
	meErr   error
	getMeFn func(context.Context) (*models.User, error)
	sendErr error
	sends   []sentMessage
	startFn func(context.Context)
}

type sentMessage struct {
	ChatID   int64
	ThreadID int
	Text     string
}

func (client *fakeBot) Start(ctx context.Context) {
	if client.startFn != nil {
		client.startFn(ctx)
		return
	}
	<-ctx.Done()
}

func (client *fakeBot) GetMe(ctx context.Context) (*models.User, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client.getMeFn != nil {
		return client.getMeFn(ctx)
	}
	return client.me, client.meErr
}

func (client *fakeBot) SendMessage(ctx context.Context, params *bot.SendMessageParams) (*models.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.sendErr != nil {
		return nil, client.sendErr
	}
	chatID, ok := params.ChatID.(int64)
	if !ok {
		return nil, fmt.Errorf("unexpected fake chat ID type %T", params.ChatID)
	}
	client.sends = append(client.sends, sentMessage{ChatID: chatID, ThreadID: params.MessageThreadID, Text: params.Text})
	return &models.Message{}, nil
}

func (client *fakeBot) sent() []sentMessage {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]sentMessage(nil), client.sends...)
}

type errorRecorder struct {
	mu     sync.Mutex
	events []ErrorEvent
}

func (recorder *errorRecorder) hook(event ErrorEvent) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.events = append(recorder.events, event)
}

func (recorder *errorRecorder) snapshot() []ErrorEvent {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]ErrorEvent(nil), recorder.events...)
}

func newTestAdapter(t *testing.T, target channels.RoutingTarget, dispatcher gateway.DispatchService, client *fakeBot) *Adapter {
	return newTestAdapterWithHook(t, target, dispatcher, client, nil)
}

func newTestAdapterWithHook(t *testing.T, target channels.RoutingTarget, dispatcher gateway.DispatchService, client *fakeBot, hook ErrorHook) *Adapter {
	t.Helper()
	adapter, err := New(context.Background(), Config{BotToken: "12345:runtime-secret", Target: target, Dispatcher: dispatcher, Factory: &fakeFactory{client: client}, ErrorHook: hook})
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func textUpdate(updateID int64, chatType models.ChatType, chatID, userID int64, text string, threadID int) *models.Update {
	return &models.Update{ID: updateID, Message: &models.Message{
		ID: int(updateID), MessageThreadID: threadID, From: &models.User{ID: userID},
		Chat: models.Chat{ID: chatID, Type: chatType}, Text: text,
	}}
}

func newTrustedTarget(t *testing.T, channel channels.Channel, tenantKey, providerAccountID string) channels.RoutingTarget {
	t.Helper()
	root, snapshot, app := activeTenantApp(t, tenantKey)
	repo := inmemory.NewInMemoryRepository()
	routeDigest, err := channels.DigestPublicRouteKey(channel, "route-"+tenantKey)
	if err != nil {
		t.Fatal(err)
	}
	protocol := channels.ProtocolConfiguration{}
	if channel == channels.ChannelTelegram {
		protocol.Telegram = &channels.TelegramProtocolConfiguration{WebhookPath: "/reserved"}
	} else {
		protocol.WeCom = &channels.WeComProtocolConfiguration{CorpID: providerAccountID, ReceiveID: "receive"}
	}
	binding, _, err := repo.Create(context.Background(), channels.CreateInput{
		TenantID: root.TenantID, BindingKey: "binding-" + tenantKey, Channel: channel,
		ProviderAccountID: providerAccountID, PublicRouteKeyDigest: routeDigest, AppID: app.AppID,
		SecretRef: "secret/" + tenantKey, Protocol: protocol, Metadata: validMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err = repo.Activate(context.Background(), channels.TransitionStatusInput{TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: validMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	secret := "offline-secret"
	resolver := inmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{{TenantID: root.TenantID, SecretRef: binding.SecretRef}: secret})
	candidates, err := repo.LookupCandidates(context.Background(), channel, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidate lookup failed: %d candidates, %v", len(candidates), err)
	}
	digest := sha256.Sum256([]byte("test message"))
	request := channels.VerificationRequest{
		Purpose: channels.PurposeWebhookVerification, Timestamp: time.Now().UTC(), Nonce: "nonce-" + tenantKey,
		MessageDigest: hex.EncodeToString(digest[:]), ReceiveID: "receive",
	}
	request.Signature = inmemory.SignFakeRequest(secret, request)
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := resolver.Verify(context.Background(), handle, request)
	if err != nil {
		t.Fatal(err)
	}
	target, err := channels.NewRoutingTarget(snapshot, binding, app, verified)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func activeTenantApp(t *testing.T, key string) (*tenant.Tenant, tenant.ConfigurationSnapshot, *appmodel.App) {
	t.Helper()
	root, err := tenant.NewTenant(tenant.CreateInput{TenantKey: key, DisplayName: "Telegram Test Tenant", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	app, err := appmodel.NewApp(appmodel.CreateInput{TenantID: root.TenantID, AppKey: "support", DisplayName: "Support", Description: "offline Telegram test"})
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(1)
	app.Status = appmodel.StatusActive
	app.CurrentRevision = &revision
	app.Version = 2
	app.UpdatedAt = app.CreatedAt.Add(time.Second)
	if err := app.Validate(); err != nil {
		t.Fatal(err)
	}
	return root, snapshot, app
}

func validMetadata() channels.ChangeMetadata {
	return channels.ChangeMetadata{ActorType: "test", ActorID: "telegram", Reason: "telegram adapter test", CorrelationID: "telegram-test-correlation"}
}
