package channels

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type fakeHTTPDoer struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	respond  func(*http.Request) (*http.Response, error)
}

func (f *fakeHTTPDoer) Do(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.bodies = append(f.bodies, append([]byte(nil), body...))
	respond := f.respond
	f.mu.Unlock()
	if respond == nil {
		return nil, errors.New("fake transport has no response")
	}
	return respond(request)
}

func validReplyPayload(channel, destinationType, destinationID string) ReplyOutboxPayload {
	return ReplyOutboxPayload{
		SchemaVersion: ReplyOutboxSchemaVersion, Kind: ReplyOutboxKind,
		TenantID: "tenant-channel", SessionID: "session-channel", JobID: "job-channel", ExecutionID: "execution-channel",
		RequestID: "request-channel", MessageID: "message-channel", TraceID: "trace-channel", BindingID: "binding-channel",
		Channel: channel, DestinationType: destinationType, DestinationID: destinationID,
		ReplyText: "reply text", FinishType: "stop", SenderRoutingVersion: SenderRoutingVersion,
	}
}

func outboxForPayload(t *testing.T, payload ReplyOutboxPayload) storage.OutboxMessage {
	t.Helper()
	encoded, err := EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return storage.OutboxMessage{
		TenantID: payload.TenantID, ID: "reply-" + payload.ExecutionID, Kind: ReplyOutboxKind,
		AggregateID: payload.ExecutionID, DedupKey: payload.TenantID + "|" + payload.ExecutionID + "|" + ReplyOutboxKind,
		Payload: encoded,
	}
}

func newTestHTTPSender(t *testing.T, doer *fakeHTTPDoer, registry *Registry) *HTTPSender {
	t.Helper()
	sender, err := NewHTTPSender(HTTPSenderConfig{
		Registry: registry, Client: doer,
		Endpoints: map[string]string{
			"web":      "https://web.invalid/base",
			"telegram": "https://telegram.invalid/base",
			"wecom":    "https://wecom.invalid/base",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return sender
}

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func TestReplyPayloadRoundTripAndCallerMutationIsolation(t *testing.T) {
	payload := validReplyPayload("telegram", DestinationTypeChat, "-100123")
	encoded, err := EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	payload.ReplyText = "changed after encoding"
	decoded, err := DecodeReplyOutboxPayload(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ReplyText != "reply text" || decoded.Channel != "telegram" || decoded.DestinationID != "-100123" {
		t.Fatalf("decoded payload=%+v", decoded)
	}
	encoded[0] = 'X'
	if _, err := DecodeReplyOutboxPayload(encoded); err == nil {
		t.Fatal("mutating encoded bytes unexpectedly preserved valid JSON")
	}
}

func TestReplyPayloadRejectsOversizeAndUnknownFields(t *testing.T) {
	if _, err := EncodeReplyOutboxPayload(func() ReplyOutboxPayload {
		payload := validReplyPayload("web", DestinationTypeUser, "user-channel")
		payload.ReplyText = strings.Repeat("x", MaxReplyTextBytes+1)
		return payload
	}()); !errors.Is(err, ErrInvalidReplyPayload) {
		t.Fatalf("oversize payload error=%v", err)
	}
	encoded := []byte(`{"schema_version":3,"kind":"agent.reply","tenant_id":"tenant-channel","session_id":"session-channel","job_id":"job-channel","execution_id":"execution-channel","request_id":"request-channel","message_id":"message-channel","trace_id":"trace-channel","binding_id":"binding-channel","channel":"web","destination_type":"user","destination_id":"user-channel","reply_text":"reply text","sender_routing_version":1,"authorization":"must reject"}`)
	if _, err := DecodeReplyOutboxPayload(encoded); !errors.Is(err, ErrInvalidReplyPayload) {
		t.Fatalf("unknown field error=%v", err)
	}
	if len(encoded) > MaxReplyPayloadBytes {
		t.Fatal("test payload unexpectedly exceeds the configured payload limit")
	}
}

func TestRoutingFromTenantContextRequiresExplicitDestination(t *testing.T) {
	tests := []struct {
		name     string
		context  tenant.TenantContext
		wantType string
		wantID   string
		wantErr  error
	}{
		{name: "web user", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a", Channel: "web", ExternalUser: "user-a"}, wantType: DestinationTypeUser, wantID: "user-a"},
		{name: "web chat", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a", Channel: "web", ExternalChat: "chat-a", ExternalUser: "user-a"}, wantType: DestinationTypeChat, wantID: "chat-a"},
		{name: "telegram chat", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a", Channel: "telegram", ExternalChat: "-100123"}, wantType: DestinationTypeChat, wantID: "-100123"},
		{name: "lark user", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a", Channel: "lark", ExternalUser: "ou_a"}, wantType: DestinationTypeUser, wantID: "ou_a"},
		{name: "lark chat", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a", Channel: "lark", ExternalChat: "oc_a"}, wantType: DestinationTypeChat, wantID: "oc_a"},
		{name: "wecom user", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a", Channel: "wecom", ExternalUser: "user-a"}, wantType: DestinationTypeUser, wantID: "user-a"},
		{name: "missing channel", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a"}, wantErr: ErrUnknownChannel},
		{name: "missing destination", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a", Channel: "web"}, wantErr: ErrInvalidDestination},
		{name: "telegram user is unsupported", context: tenant.TenantContext{TenantID: "tenant-a", BindingID: "binding-a", Channel: "telegram", ExternalUser: "user-a"}, wantErr: ErrInvalidDestination},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			routing, err := RoutingFromTenantContext(test.context)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("error=%v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil || routing.DestinationType != test.wantType || routing.DestinationID != test.wantID {
				t.Fatalf("routing=%+v err=%v", routing, err)
			}
		})
	}
	if err := ValidateDestination("web", "room", "room-a"); !errors.Is(err, ErrUnsupportedDestination) {
		t.Fatalf("unsupported destination error=%v", err)
	}
	if err := ValidateDestination("web", DestinationTypeUser, ""); !errors.Is(err, ErrInvalidDestination) {
		t.Fatalf("empty destination error=%v", err)
	}
	if err := ValidateDestination("telegram", DestinationTypeChat, "not-numeric"); !errors.Is(err, ErrInvalidDestination) {
		t.Fatalf("telegram destination error=%v", err)
	}
}

func TestRegistryRejectsDuplicatesAndConcurrentLookup(t *testing.T) {
	if _, err := NewRegistry(WebAdapter{}, WebAdapter{}); !errors.Is(err, ErrDuplicateChannel) {
		t.Fatalf("duplicate registry error=%v", err)
	}
	registry, err := NewRegistry(WebAdapter{}, TelegramAdapter{}, WeComAdapter{}, larkPlaceholderAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Lookup("lark"); err != nil {
		t.Fatalf("default registry did not include lark: %v", err)
	}
	if _, err := registry.Lookup("unknown"); !errors.Is(err, ErrUnknownChannel) {
		t.Fatalf("unknown lookup error=%v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, err := registry.Lookup("telegram"); err != nil {
					t.Errorf("concurrent lookup error=%v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestRegistryTenantPolicyIsCheckedBeforeTransport(t *testing.T) {
	registry, err := NewRegistryWithPolicy([]SenderAdapter{WebAdapter{}}, map[string][]string{"tenant-a": {"web"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.ValidateTenantChannel("tenant-a", "web"); err != nil {
		t.Fatal(err)
	}
	if err := registry.ValidateTenantChannel("tenant-b", "web"); !errors.Is(err, ErrChannelPolicy) {
		t.Fatalf("unlisted tenant policy error=%v", err)
	}
	if err := registry.ValidateTenantChannel("tenant-a", "telegram"); !errors.Is(err, ErrUnknownChannel) {
		t.Fatalf("unregistered channel policy error=%v", err)
	}
}

func TestChannelBuildersProduceBoundedProviderRequests(t *testing.T) {
	tests := []struct {
		name       string
		adapter    SenderAdapter
		payload    ReplyOutboxPayload
		path       string
		wantFields map[string]any
	}{
		{name: "web", adapter: WebAdapter{}, payload: validReplyPayload("web", DestinationTypeUser, "user-a"), path: "/replies", wantFields: map[string]any{"destination_type": "user", "destination_id": "user-a", "text": "reply text"}},
		{name: "telegram", adapter: TelegramAdapter{}, payload: validReplyPayload("telegram", DestinationTypeChat, "-100123"), path: "/sendMessage", wantFields: map[string]any{"chat_id": "-100123", "text": "reply text"}},
		{name: "wecom user", adapter: WeComAdapter{}, payload: validReplyPayload("wecom", DestinationTypeUser, "user-a"), path: "/message/send", wantFields: map[string]any{"msgtype": "text", "touser": "user-a"}},
		{name: "wecom chat", adapter: WeComAdapter{}, payload: validReplyPayload("wecom", DestinationTypeChat, "chat-a"), path: "/message/send", wantFields: map[string]any{"msgtype": "text", "chatid": "chat-a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := test.adapter.BuildOutbound(OutboundMessage{Payload: test.payload, IdempotencyKey: "stable-key"})
			if err != nil {
				t.Fatal(err)
			}
			if request.Method != http.MethodPost || request.Path != test.path || request.Headers.Get("Content-Type") != "application/json" {
				t.Fatalf("request metadata=%+v", request)
			}
			var actual map[string]any
			if err := json.Unmarshal(request.Body, &actual); err != nil {
				t.Fatal(err)
			}
			for key, want := range test.wantFields {
				if actual[key] != want {
					t.Fatalf("body=%v, field %s=%v want %v", actual, key, actual[key], want)
				}
			}
			if strings.Contains(string(request.Body), "authorization") || strings.Contains(string(request.Body), "token") {
				t.Fatalf("provider request contains forbidden credential field: %s", request.Body)
			}
		})
	}
}

func TestHTTPSenderMapsHTTPOutcomesAndUsesStableIdempotency(t *testing.T) {
	tests := []struct {
		name      string
		channel   string
		status    int
		body      string
		class     OutcomeClass
		code      string
		transport error
	}{
		{name: "web success", channel: "web", status: http.StatusOK, body: `{"accepted":true}`, class: OutcomeDelivered},
		{name: "telegram success", channel: "telegram", status: http.StatusOK, body: `{"ok":true}`, class: OutcomeDelivered},
		{name: "wecom success", channel: "wecom", status: http.StatusOK, body: `{"errcode":0}`, class: OutcomeDelivered},
		{name: "rate limited", channel: "web", status: http.StatusTooManyRequests, body: `{}`, class: OutcomeRetryableFailure, code: SenderRateLimitedCode},
		{name: "server failure", channel: "web", status: http.StatusBadGateway, body: `{}`, class: OutcomeRetryableFailure, code: SenderUnavailableCode},
		{name: "client rejection", channel: "web", status: http.StatusBadRequest, body: `{}`, class: OutcomePermanentFailure, code: SenderRejectedCode},
		{name: "malformed success", channel: "web", status: http.StatusOK, body: `not-json`, class: OutcomePermanentFailure, code: SenderMalformedResponseCode},
		{name: "deadline", channel: "web", transport: context.DeadlineExceeded, class: OutcomeRetryableFailure, code: SenderTimeoutCode},
		{name: "connection interruption", channel: "web", transport: errors.New("connection interrupted Authorization Bearer secret"), class: OutcomeUnknown, code: DeliveryOutcomeUnknownCode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channel := test.channel
			if channel == "" {
				channel = "web"
			}
			destinationType, destinationID := DestinationTypeUser, "user-a"
			if channel == "telegram" {
				destinationType, destinationID = DestinationTypeChat, "-100123"
			}
			payload := validReplyPayload(channel, destinationType, destinationID)
			doer := &fakeHTTPDoer{respond: func(*http.Request) (*http.Response, error) {
				if test.transport != nil {
					return nil, test.transport
				}
				return response(test.status, test.body), nil
			}}
			sender := newTestHTTPSender(t, doer, DefaultRegistry())
			outcome := sender.Send(context.Background(), outboxForPayload(t, payload))
			if outcome.Class != test.class || outcome.Code != test.code {
				t.Fatalf("outcome=%+v, want class=%s code=%q", outcome, test.class, test.code)
			}
			if len(doer.requests) > 0 {
				request := doer.requests[0]
				if request.Header.Get("Idempotency-Key") != ExternalIdempotencyKey(payload) || request.Header.Get("Authorization") != "" {
					t.Fatalf("request headers=%v", request.Header)
				}
			}
		})
	}
}

func TestHTTPSenderRejectsUnknownChannelDestinationAndDoesNotExposeResponse(t *testing.T) {
	webOnly, err := NewRegistry(WebAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	doer := &fakeHTTPDoer{respond: func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, `{"accepted":true}`), nil
	}}
	sender, err := NewHTTPSender(HTTPSenderConfig{
		Registry: webOnly, Client: doer, Endpoints: map[string]string{"web": "https://web.invalid/base"},
	})
	if err != nil {
		t.Fatal(err)
	}
	unknownChannel := validReplyPayload("web", DestinationTypeUser, "user-a")
	unknownChannel.TenantID = "tenant-a"
	message := outboxForPayload(t, unknownChannel)
	message.Payload = append(message.Payload, []byte(` `)...)
	if outcome := sender.Send(context.Background(), message); outcome.Class != OutcomeDelivered {
		t.Fatalf("whitespace should remain valid JSON, outcome=%+v", outcome)
	}
	invalid := outboxForPayload(t, validReplyPayload("telegram", DestinationTypeChat, "123"))
	if outcome := sender.Send(context.Background(), invalid); outcome.Class != OutcomePermanentFailure || outcome.Code != SenderUnknownChannelCode {
		t.Fatalf("unknown channel outcome=%+v", outcome)
	}
	badDestination := validReplyPayload("web", DestinationTypeUser, "user-a")
	badDestination.DestinationID = ""
	encoded, _ := json.Marshal(badDestination)
	invalid = outboxForPayload(t, validReplyPayload("web", DestinationTypeUser, "user-a"))
	invalid.Payload = encoded
	if outcome := sender.Send(context.Background(), invalid); outcome.Class != OutcomePermanentFailure || outcome.Code != SenderInvalidDestinationCode {
		t.Fatalf("invalid destination outcome=%+v", outcome)
	}
	if len(doer.requests) != 1 {
		t.Fatalf("invalid routing reached transport: requests=%d", len(doer.requests))
	}
}

func TestHTTPSenderValidatesEndpoints(t *testing.T) {
	_, err := NewHTTPSender(HTTPSenderConfig{Client: &fakeHTTPDoer{}, Endpoints: map[string]string{"web": "https://web.invalid/path?secret=bad"}})
	if !errors.Is(err, ErrInvalidEndpoint) {
		t.Fatalf("query endpoint error=%v", err)
	}
}
