package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type fakeResponse struct {
	status int
	body   io.ReadCloser
	err    error
}

type fakeHTTPDoer struct {
	mu        sync.Mutex
	responses []fakeResponse
	requests  []*http.Request
	bodies    [][]byte
}

func (f *fakeHTTPDoer) Do(request *http.Request) (*http.Response, error) {
	var body []byte
	if request.Body != nil {
		var err error
		body, err = io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
	}
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.bodies = append(f.bodies, append([]byte(nil), body...))
	index := len(f.requests) - 1
	response := f.responses[len(f.responses)-1]
	if index < len(f.responses) {
		response = f.responses[index]
	}
	f.mu.Unlock()
	if response.err != nil {
		return nil, response.err
	}
	return &http.Response{StatusCode: response.status, Body: response.body, Header: make(http.Header)}, nil
}

func body(value string) io.ReadCloser {
	return io.NopCloser(strings.NewReader(value))
}

func bindingForTest(enabled bool) Binding {
	return Binding{
		TenantID: "tenant-telegram", BindingID: "binding-telegram", Channel: Channel,
		BotTokenSecretRef: "env://TELEGRAM_B2_BOT_TOKEN", WebhookSecretRef: "env://TELEGRAM_B2_WEBHOOK_SECRET", Enabled: enabled,
	}
}

func payloadForTest(threadID *int64) channels.ReplyOutboxPayload {
	return channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: "tenant-telegram", SessionID: "session-telegram", JobID: "job-telegram", ExecutionID: "execution-telegram",
		RequestID: "request-telegram", MessageID: "message-telegram", TraceID: "trace-telegram", BindingID: "binding-telegram",
		Channel: Channel, DestinationType: channels.DestinationTypeChat, DestinationID: "-100123456789",
		MessageThreadID: threadID, ReplyText: "plain text", SenderRoutingVersion: channels.SenderRoutingVersion,
	}
}

func outboxForTest(t *testing.T, payload channels.ReplyOutboxPayload, encode bool) storage.OutboxMessage {
	t.Helper()
	var encoded []byte
	var err error
	if encode {
		encoded, err = channels.EncodeReplyOutboxPayload(payload)
	} else {
		encoded, err = json.Marshal(payload)
	}
	if err != nil {
		t.Fatal("could not construct test payload")
	}
	return storage.OutboxMessage{
		TenantID: payload.TenantID, ID: "reply-" + payload.ExecutionID, Kind: channels.ReplyOutboxKind,
		AggregateID: payload.ExecutionID, DedupKey: payload.TenantID + "|" + payload.ExecutionID + "|" + channels.ReplyOutboxKind, Payload: encoded,
	}
}

func senderForTest(t *testing.T, doer *fakeHTTPDoer, enabled bool) *Sender {
	t.Helper()
	sender, err := NewSender(SenderConfig{
		Bindings: []Binding{bindingForTest(enabled)},
		Tokens: TokenResolverFunc(func(_ context.Context, binding Binding) (string, error) {
			if binding.BotTokenSecretRef != "env://TELEGRAM_B2_BOT_TOKEN" {
				t.Fatal("sender used an unexpected secret reference")
			}
			return "123456:token_for_test", nil
		}),
		Client: doer, Timeout: time.Second,
	})
	if err != nil {
		t.Fatal("could not construct Telegram sender")
	}
	return sender
}

func TestSenderGetMeAndSendMessageContract(t *testing.T) {
	threadID := int64(42)
	doer := &fakeHTTPDoer{responses: []fakeResponse{
		{status: http.StatusOK, body: body(`{"ok":true,"result":{"id":123,"is_bot":true,"username":"not-used"}}`)},
		{status: http.StatusOK, body: body(`{"ok":true,"result":{"message_id":9,"chat":{"id":-100123456789,"type":"supergroup"},"text":"plain text"}}`)},
	}}
	sender := senderForTest(t, doer, true)
	if err := sender.GetMe(context.Background(), "binding-telegram"); err != nil {
		t.Fatal("getMe did not satisfy the success contract")
	}
	outcome := sender.Send(context.Background(), outboxForTest(t, payloadForTest(&threadID), true))
	if outcome.Class != channels.OutcomeDelivered || outcome.Code != "" {
		t.Fatalf("send outcome class=%s code=%q", outcome.Class, outcome.Code)
	}
	if len(doer.requests) != 2 {
		t.Fatalf("unexpected request count: %d", len(doer.requests))
	}
	sendRequest := doer.requests[1]
	if sendRequest.Method != http.MethodPost || sendRequest.URL.Scheme != "https" || sendRequest.URL.Host != "api.telegram.org" ||
		!strings.HasSuffix(sendRequest.URL.Path, "/sendMessage") || sendRequest.URL.RawQuery != "" || sendRequest.Header.Get("Authorization") != "" {
		t.Fatal("sendMessage request metadata violated the Telegram contract")
	}
	var sent map[string]any
	if err := json.Unmarshal(doer.bodies[1], &sent); err != nil {
		t.Fatal("sendMessage body was not valid JSON")
	}
	if len(sent) != 3 || sent["chat_id"] != "-100123456789" || sent["text"] != "plain text" || sent["message_thread_id"] != float64(42) {
		t.Fatal("sendMessage body contained the wrong projection")
	}
	if _, ok := sent["uuid"]; ok {
		t.Fatal("Telegram request contained an unsupported idempotency field")
	}
}

func TestSenderOutcomeClassification(t *testing.T) {
	tests := []struct {
		name     string
		response fakeResponse
		class    channels.OutcomeClass
		code     string
	}{
		{name: "429", response: fakeResponse{status: http.StatusTooManyRequests, body: body(`{"ok":false,"error_code":429,"parameters":{"retry_after":2}}`)}, class: channels.OutcomeRetryableFailure, code: channels.TelegramSenderRateLimitedCode},
		{name: "5xx", response: fakeResponse{status: http.StatusBadGateway, body: body(`{"ok":false,"error_code":502}`)}, class: channels.OutcomeRetryableFailure, code: channels.TelegramSenderUnavailableCode},
		{name: "401", response: fakeResponse{status: http.StatusUnauthorized, body: body(`{}`)}, class: channels.OutcomePermanentFailure, code: channels.TelegramSenderAuthFailedCode},
		{name: "403", response: fakeResponse{status: http.StatusForbidden, body: body(`{}`)}, class: channels.OutcomePermanentFailure, code: channels.TelegramSenderForbiddenCode},
		{name: "400", response: fakeResponse{status: http.StatusBadRequest, body: body(`{"ok":false,"error_code":400}`)}, class: channels.OutcomePermanentFailure, code: channels.TelegramSenderRejectedCode},
		{name: "200 provider rejection", response: fakeResponse{status: http.StatusOK, body: body(`{"ok":false,"error_code":400}`)}, class: channels.OutcomePermanentFailure, code: channels.TelegramSenderRejectedCode},
		{name: "malformed success", response: fakeResponse{status: http.StatusOK, body: body(`{"ok":true,"result":{}}`)}, class: channels.OutcomePermanentFailure, code: channels.TelegramSenderMalformedResponseCode},
		{name: "nil response", response: fakeResponse{status: http.StatusOK, body: nil}, class: channels.OutcomeUnknown, code: channels.TelegramDeliveryOutcomeUnknownCode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			doer := &fakeHTTPDoer{responses: []fakeResponse{test.response}}
			sender := senderForTest(t, doer, true)
			outcome := sender.Send(context.Background(), outboxForTest(t, payloadForTest(nil), true))
			if outcome.Class != test.class || outcome.Code != test.code {
				t.Fatalf("outcome class=%s code=%q", outcome.Class, outcome.Code)
			}
		})
	}
}

func TestSenderUnknownAndExplicitUnsentTransport(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		class channels.OutcomeClass
		code  string
	}{
		{name: "response read interruption", err: nil, class: channels.OutcomeUnknown, code: channels.TelegramDeliveryOutcomeUnknownCode},
		{name: "sent timeout", err: TransportError{Err: context.DeadlineExceeded, Sent: true}, class: channels.OutcomeUnknown, code: channels.TelegramDeliveryOutcomeUnknownCode},
		{name: "unsent timeout", err: TransportError{Err: context.DeadlineExceeded, Sent: false}, class: channels.OutcomeRetryableFailure, code: channels.TelegramSenderTimeoutCode},
		{name: "unsent transport", err: TransportError{Err: errors.New("transport unavailable"), Sent: false}, class: channels.OutcomeRetryableFailure, code: channels.TelegramSenderUnavailableCode},
		{name: "plain transport", err: errors.New("transport result may have been accepted"), class: channels.OutcomeUnknown, code: channels.TelegramDeliveryOutcomeUnknownCode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var response fakeResponse
			if test.name == "response read interruption" {
				response = fakeResponse{status: http.StatusOK, body: &failingBody{}}
			} else {
				response = fakeResponse{err: test.err}
			}
			sender := senderForTest(t, &fakeHTTPDoer{responses: []fakeResponse{response}}, true)
			outcome := sender.Send(context.Background(), outboxForTest(t, payloadForTest(nil), true))
			if outcome.Class != test.class || outcome.Code != test.code {
				t.Fatalf("outcome class=%s code=%q", outcome.Class, outcome.Code)
			}
		})
	}
}

type failingBody struct{}

func (*failingBody) Read([]byte) (int, error) { return 0, errors.New("body read failed") }
func (*failingBody) Close() error             { return nil }

func TestSenderRejectsInvalidDestinationLengthAndBinding(t *testing.T) {
	longText := strings.Repeat("x", MaxMessageTextRunes+1)
	payload := payloadForTest(nil)
	payload.ReplyText = longText
	doer := &fakeHTTPDoer{responses: []fakeResponse{{status: http.StatusOK, body: body(`{"ok":true,"result":{"message_id":1}}`)}}}
	sender := senderForTest(t, doer, true)
	outcome := sender.Send(context.Background(), outboxForTest(t, payload, true))
	if outcome.Class != channels.OutcomePermanentFailure || outcome.Code != channels.TelegramSenderMessageTooLongCode || len(doer.requests) != 0 {
		t.Fatal("oversized text was not rejected before transport")
	}
	invalid := payloadForTest(nil)
	invalid.DestinationID = "@approved_username"
	outcome = sender.Send(context.Background(), outboxForTest(t, invalid, false))
	if outcome.Class != channels.OutcomePermanentFailure || outcome.Code != channels.TelegramSenderInvalidDestinationCode || len(doer.requests) != 0 {
		t.Fatal("username destination was accepted")
	}
	disabled := senderForTest(t, doer, false)
	outcome = disabled.Send(context.Background(), outboxForTest(t, payloadForTest(nil), true))
	if outcome.Class != channels.OutcomePermanentFailure || outcome.Code != channels.TelegramSenderNotConfiguredCode {
		t.Fatal("disabled binding was not fail-closed")
	}
}

func TestReplyRoutingCarriesTopicOnlyFromTenantContext(t *testing.T) {
	threadID := "42"
	routing, err := channels.RoutingFromTenantContext(tenantContextForTest(threadID))
	if err != nil || routing.MessageThreadID == nil || *routing.MessageThreadID != 42 {
		t.Fatal("Telegram thread was not projected from the verified tenant context")
	}
	payload := payloadForTest(routing.MessageThreadID)
	payload.SchemaVersion = channels.ReplyOutboxSchemaVersion
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil || !strings.Contains(string(encoded), "message_thread_id") {
		t.Fatal("topic field was not encoded in the current schema")
	}
	old := payloadForTest(nil)
	old.SchemaVersion = channels.PreviousReplyOutboxSchemaVersion
	old.Channel = "web"
	old.DestinationType = channels.DestinationTypeUser
	old.DestinationID = "legacy-user"
	if _, err := channels.EncodeReplyOutboxPayload(old); err != nil {
		t.Fatal("previous Web/Lark-compatible payload schema stopped decoding")
	}
}

func tenantContextForTest(threadID string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: "tenant-telegram", BindingID: "binding-telegram", Channel: Channel, ExternalChat: "-100123456789", ExternalThreadID: threadID}
}

func TestWebhookSecretAndUpdateParsing(t *testing.T) {
	adapter, err := NewWebhookAdapter(WebhookConfig{Binding: bindingForTest(true), Secret: "webhook-test-secret"})
	if err != nil {
		t.Fatal("could not construct webhook adapter")
	}
	request, _ := http.NewRequest(http.MethodPost, "https://service.invalid/webhook", nil)
	if err := adapter.Verify(request, nil); !errors.Is(err, ErrInvalidWebhookSecret) {
		t.Fatal("missing webhook secret was accepted")
	}
	request.Header.Set(HeaderSecretToken, "wrong")
	if err := adapter.Verify(request, nil); !errors.Is(err, ErrInvalidWebhookSecret) {
		t.Fatal("wrong webhook secret was accepted")
	}
	request.Header.Set(HeaderSecretToken, "webhook-test-secret")
	if err := adapter.Verify(request, nil); err != nil {
		t.Fatal("valid webhook secret was rejected")
	}

	threadID := int64(77)
	text := "hello"
	encoded, err := json.Marshal(Update{UpdateID: pointer(int64(1001)), Message: &Message{MessageID: 12, MessageThreadID: &threadID, From: &User{ID: 33}, Chat: Chat{ID: -100123456789, Type: "supergroup"}, Text: &text}})
	if err != nil {
		t.Fatal(err)
	}
	incoming, err := adapter.Parse(encoded)
	if err != nil || incoming.UpdateID != 1001 || incoming.MessageID != 12 || incoming.UserID != 33 || incoming.ChatID != -100123456789 || incoming.ChatType != "supergroup" || incoming.MessageThreadID == nil || *incoming.MessageThreadID != 77 || incoming.Text != text {
		t.Fatal("valid Telegram update was not parsed")
	}
	key, err := incoming.DedupKey("tenant-telegram", "binding-telegram")
	if err != nil || key != "tenant-telegram|telegram|binding-telegram|1001" {
		t.Fatal("update_id dedup key was not stable")
	}

	for _, scope := range []struct {
		name     string
		id       int64
		typeName string
	}{
		{name: "private", id: 12345, typeName: "private"},
		{name: "group", id: -12345, typeName: "group"},
		{name: "supergroup", id: -123456789, typeName: "supergroup"},
	} {
		t.Run(scope.name, func(t *testing.T) {
			updateText := text
			encoded, err := json.Marshal(Update{UpdateID: pointer(2000), Message: &Message{MessageID: 13, From: &User{ID: 33}, Chat: Chat{ID: scope.id, Type: scope.typeName}, Text: &updateText}})
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := adapter.Parse(encoded)
			if err != nil || parsed.ChatType != scope.typeName || parsed.ChatID != scope.id {
				t.Fatal("supported Telegram chat scope was rejected")
			}
		})
	}
}

func TestWebhookRejectsUnsupportedUpdatesAndScopes(t *testing.T) {
	adapter, err := NewWebhookAdapter(WebhookConfig{Binding: bindingForTest(true), Secret: "webhook-test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	text := "hello"
	cases := []struct {
		name   string
		update Update
		want   error
	}{
		{name: "non-text", update: Update{UpdateID: pointer(int64(1)), Message: &Message{MessageID: 1, From: &User{ID: 2}, Chat: Chat{ID: 3, Type: "private"}}}, want: ErrUnsupportedUpdate},
		{name: "channel", update: Update{UpdateID: pointer(int64(2)), Message: &Message{MessageID: 1, From: &User{ID: 2}, Chat: Chat{ID: 3, Type: "channel"}, Text: &text}}, want: ErrUnsupportedChat},
		{name: "group thread", update: Update{UpdateID: pointer(int64(3)), Message: &Message{MessageID: 1, MessageThreadID: pointer(int64(4)), From: &User{ID: 2}, Chat: Chat{ID: 3, Type: "group"}, Text: &text}}, want: ErrUnsupportedThread},
		{name: "missing message", update: Update{UpdateID: pointer(int64(4))}, want: ErrUnsupportedUpdate},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.update)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := adapter.Parse(encoded); !errors.Is(err, test.want) {
				t.Fatalf("parse error did not match the safe contract: %v", err)
			}
		})
	}
	if _, err := adapter.Parse([]byte(`{"update_id":1}{}`)); !errors.Is(err, ErrInvalidUpdate) {
		t.Fatal("trailing JSON was accepted")
	}
}

func TestEnvironmentSecretResolverRequiresExplicitEnvReference(t *testing.T) {
	t.Setenv("TELEGRAM_B2_BOT_TOKEN", "test-value")
	resolver := EnvironmentSecretResolver{}
	value, err := resolver.Resolve(context.Background(), "env://TELEGRAM_B2_BOT_TOKEN")
	if err != nil || value != "test-value" {
		t.Fatal("explicit environment SecretRef did not resolve")
	}
	if _, err := resolver.Resolve(context.Background(), "TELEGRAM_B2_BOT_TOKEN"); !errors.Is(err, ErrCredentialResolution) {
		t.Fatal("bare environment variable name was resolved without env://")
	}
}

func pointer(value int64) *int64 { return &value }
