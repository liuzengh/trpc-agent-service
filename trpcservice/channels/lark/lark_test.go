package lark

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type fakeDoer struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	respond  func(*http.Request) (*http.Response, error)
}

func (f *fakeDoer) Do(request *http.Request) (*http.Response, error) {
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
		return nil, errors.New("fake transport unavailable")
	}
	return respond(request)
}

type tokenResolverFunc func(context.Context, Binding) (string, error)

func (f tokenResolverFunc) Resolve(ctx context.Context, binding Binding) (string, error) {
	return f(ctx, binding)
}

type secretResolverFunc func(context.Context, string) (string, error)

func (f secretResolverFunc) Resolve(ctx context.Context, ref string) (string, error) {
	return f(ctx, ref)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("response body interrupted") }
func (errorReader) Close() error             { return nil }

func validBinding() Binding {
	return Binding{
		TenantID: "tenant-lark", BindingID: "binding-lark", Channel: Channel,
		AppID: "cli_test", SecretRef: "secret-ref", ReceiverIDType: ReceiverIDTypeOpenID, Enabled: true,
	}
}

func validPayload(destinationType, destinationID string) channels.ReplyOutboxPayload {
	return channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: "tenant-lark", SessionID: "session-lark", JobID: "job-lark", ExecutionID: "execution-lark",
		RequestID: "request-lark", MessageID: "message-lark", TraceID: "trace-lark", BindingID: "binding-lark",
		Channel: Channel, DestinationType: destinationType, DestinationID: destinationID,
		ReplyText: "reply text", SenderRoutingVersion: channels.SenderRoutingVersion,
	}
}

func outboxForPayload(t *testing.T, payload channels.ReplyOutboxPayload) storage.OutboxMessage {
	t.Helper()
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return storage.OutboxMessage{
		TenantID: payload.TenantID, ID: "reply-" + payload.ExecutionID, Kind: channels.ReplyOutboxKind,
		AggregateID: payload.ExecutionID, DedupKey: payload.TenantID + "|" + payload.ExecutionID + "|" + channels.ReplyOutboxKind,
		Payload: encoded,
	}
}

func newTestSender(t *testing.T, doer *fakeDoer, binding Binding) *Sender {
	t.Helper()
	sender, err := NewSender(SenderConfig{
		Bindings: []Binding{binding}, Tokens: tokenResolverFunc(func(context.Context, Binding) (string, error) {
			return "tenant-token", nil
		}), Client: doer,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sender
}

func larkResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestSenderBuildsOfficialMessageRequestAndUsesStableUUID(t *testing.T) {
	doer := &fakeDoer{respond: func(*http.Request) (*http.Response, error) {
		return larkResponse(http.StatusOK, `{"code":0,"data":{"message_id":"om_test"}}`), nil
	}}
	sender := newTestSender(t, doer, validBinding())
	message := outboxForPayload(t, validPayload(channels.DestinationTypeUser, "ou_test"))
	first := sender.Send(context.Background(), message)
	second := sender.Send(context.Background(), message)
	if first.Class != channels.OutcomeDelivered || second.Class != channels.OutcomeDelivered {
		t.Fatalf("outcomes=%+v,%+v", first, second)
	}
	doer.mu.Lock()
	requests := append([]*http.Request(nil), doer.requests...)
	bodies := append([][]byte(nil), doer.bodies...)
	doer.mu.Unlock()
	if len(requests) != 2 || len(bodies) != 2 {
		t.Fatalf("request count=%d body count=%d", len(requests), len(bodies))
	}
	for _, request := range requests {
		parsed, err := url.Parse(request.URL.String())
		if err != nil {
			t.Fatal(err)
		}
		if request.Method != http.MethodPost || parsed.Path != messageCreatePath || parsed.Query().Get("receive_id_type") != ReceiverIDTypeOpenID {
			t.Fatalf("request method/path/query=%s %s %v", request.Method, parsed.Path, parsed.Query())
		}
		if request.Header.Get("Authorization") != "Bearer tenant-token" {
			t.Fatal("request did not carry in-memory bearer token")
		}
	}
	var body struct {
		ReceiveID string `json:"receive_id"`
		MsgType   string `json:"msg_type"`
		Content   string `json:"content"`
		UUID      string `json:"uuid"`
	}
	if err := json.Unmarshal(bodies[0], &body); err != nil {
		t.Fatal(err)
	}
	if body.ReceiveID != "ou_test" || body.MsgType != "text" || body.UUID == "" || body.UUID != uuidFromRequestBody(t, bodies[1]) {
		t.Fatalf("request projection has unexpected identity: receive=%q type=%q uuid-present=%t", body.ReceiveID, body.MsgType, body.UUID != "")
	}
	var content struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(body.Content), &content); err != nil || content.Text != "reply text" {
		t.Fatalf("message content contract invalid: text=%q err=%v", content.Text, err)
	}
}

func uuidFromRequestBody(t *testing.T, body []byte) string {
	t.Helper()
	var request struct {
		UUID string `json:"uuid"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	return request.UUID
}

func TestSenderEnforcesBindingTenantAndDestinationBoundaries(t *testing.T) {
	doer := &fakeDoer{respond: func(*http.Request) (*http.Response, error) {
		return larkResponse(http.StatusOK, `{"code":0,"data":{"message_id":"om_test"}}`), nil
	}}
	sender := newTestSender(t, doer, validBinding())
	tests := []struct {
		name string
		make func() storage.OutboxMessage
		code string
	}{
		{"unknown binding", func() storage.OutboxMessage {
			payload := validPayload(channels.DestinationTypeUser, "ou_test")
			payload.BindingID = "binding-missing"
			return outboxForPayload(t, payload)
		}, channels.LarkSenderNotConfiguredCode},
		{"tenant mismatch", func() storage.OutboxMessage {
			payload := validPayload(channels.DestinationTypeUser, "ou_test")
			payload.TenantID = "tenant-other"
			return outboxForPayload(t, payload)
		}, channels.LarkSenderInvalidDestinationCode},
		{"binding receiver type mismatch", func() storage.OutboxMessage {
			payload := validPayload(channels.DestinationTypeChat, "oc_test")
			return outboxForPayload(t, payload)
		}, channels.LarkSenderInvalidDestinationCode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := sender.Send(context.Background(), test.make())
			if outcome.Class != channels.OutcomePermanentFailure || outcome.Code != test.code {
				t.Fatalf("outcome=%+v want code=%s", outcome, test.code)
			}
		})
	}
	if len(doer.requests) != 0 {
		t.Fatalf("invalid routing reached provider: requests=%d", len(doer.requests))
	}
}

func TestSenderMissingLarkConfigurationIsPermanentAndSafe(t *testing.T) {
	doer := &fakeDoer{respond: func(*http.Request) (*http.Response, error) { return larkResponse(http.StatusOK, `{}`), nil }}
	sender, err := NewSender(SenderConfig{
		Tokens: tokenResolverFunc(func(context.Context, Binding) (string, error) { return "ignored", nil }),
		Client: doer,
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := sender.Send(context.Background(), outboxForPayload(t, validPayload(channels.DestinationTypeUser, "ou_test")))
	if outcome.Class != channels.OutcomePermanentFailure || outcome.Code != channels.LarkSenderNotConfiguredCode {
		t.Fatalf("missing configuration outcome=%+v", outcome)
	}
	if len(doer.requests) != 0 {
		t.Fatal("missing configuration reached provider")
	}
}

func TestSenderMapsTokenResolverFailureToSafeOutcome(t *testing.T) {
	doer := &fakeDoer{respond: func(*http.Request) (*http.Response, error) {
		return larkResponse(http.StatusOK, `{"code":0,"data":{"message_id":"should-not-send"}}`), nil
	}}
	sender, err := NewSender(SenderConfig{
		Bindings: []Binding{validBinding()},
		Tokens: TokenResolverFunc(func(context.Context, Binding) (string, error) {
			return "", errors.New("resolver failed with secret-value")
		}),
		Client: doer,
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := sender.Send(context.Background(), outboxForPayload(t, validPayload(channels.DestinationTypeUser, "ou_test")))
	if outcome.Class != channels.OutcomePermanentFailure || outcome.Code != channels.LarkSenderAuthFailedCode {
		t.Fatalf("token resolver outcome=%+v", outcome)
	}
	if len(doer.requests) != 0 {
		t.Fatal("token resolver failure reached message provider")
	}
}

func TestSenderEnforcesResponseBodyLimit(t *testing.T) {
	doer := &fakeDoer{respond: func(*http.Request) (*http.Response, error) {
		return larkResponse(http.StatusOK, strings.Repeat("x", 17)), nil
	}}
	sender, err := NewSender(SenderConfig{
		Bindings: []Binding{validBinding()}, Tokens: tokenResolverFunc(func(context.Context, Binding) (string, error) { return "tenant-token", nil }),
		Client: doer, MaxResponseBytes: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := sender.Send(context.Background(), outboxForPayload(t, validPayload(channels.DestinationTypeUser, "ou_test")))
	if outcome.Class != channels.OutcomePermanentFailure || outcome.Code != channels.LarkSenderMalformedResponseCode {
		t.Fatalf("oversized response outcome=%+v", outcome)
	}
}

func TestSenderMapsProviderResponsesToSafeOutcomes(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		class     channels.OutcomeClass
		code      string
		bodyError bool
	}{
		{"success", http.StatusOK, `{"code":0,"data":{"message_id":"om_test"}}`, channels.OutcomeDelivered, "", false},
		{"malformed success", http.StatusOK, `{"code":0,"data":{}}`, channels.OutcomePermanentFailure, channels.LarkSenderMalformedResponseCode, false},
		{"empty success", http.StatusOK, ``, channels.OutcomeUnknown, channels.LarkDeliveryOutcomeUnknownCode, false},
		{"rate limited", http.StatusTooManyRequests, `{}`, channels.OutcomeRetryableFailure, channels.LarkSenderRateLimitedCode, false},
		{"unavailable", http.StatusBadGateway, `{}`, channels.OutcomeRetryableFailure, channels.LarkSenderUnavailableCode, false},
		{"expired token", http.StatusUnauthorized, `{"code":99991663}`, channels.OutcomePermanentFailure, channels.LarkSenderAuthFailedCode, false},
		{"forbidden", http.StatusForbidden, `{}`, channels.OutcomePermanentFailure, channels.LarkSenderForbiddenCode, false},
		{"invalid receiver", http.StatusBadRequest, `{}`, channels.OutcomePermanentFailure, channels.LarkSenderRejectedCode, false},
		{"body interrupted", http.StatusOK, ``, channels.OutcomeUnknown, channels.LarkDeliveryOutcomeUnknownCode, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			doer := &fakeDoer{respond: func(*http.Request) (*http.Response, error) {
				response := larkResponse(test.status, test.body)
				if test.bodyError {
					response.Body = errorReader{}
				}
				if test.status == http.StatusTooManyRequests {
					response.Header.Set("Retry-After", "3600")
				}
				return response, nil
			}}
			outcome := newTestSender(t, doer, validBinding()).Send(context.Background(), outboxForPayload(t, validPayload(channels.DestinationTypeUser, "ou_test")))
			if outcome.Class != test.class || outcome.Code != test.code {
				t.Fatalf("outcome=%+v want class=%s code=%q", outcome, test.class, test.code)
			}
		})
	}
}

func TestSenderMapsTransportStateAndTimeoutWithoutRawError(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		class channels.OutcomeClass
		code  string
	}{
		{"not sent", TransportError{Err: errors.New("connection failed with secret-value"), Sent: false}, channels.OutcomeRetryableFailure, channels.LarkSenderUnavailableCode},
		{"possibly accepted", TransportError{Err: errors.New("connection failed with secret-value"), Sent: true}, channels.OutcomeUnknown, channels.LarkDeliveryOutcomeUnknownCode},
		{"plain connection error", errors.New("connection failed with secret-value"), channels.OutcomeUnknown, channels.LarkDeliveryOutcomeUnknownCode},
		{"deadline", context.DeadlineExceeded, channels.OutcomeRetryableFailure, channels.LarkSenderTimeoutCode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			doer := &fakeDoer{respond: func(*http.Request) (*http.Response, error) { return nil, test.err }}
			outcome := newTestSender(t, doer, validBinding()).Send(context.Background(), outboxForPayload(t, validPayload(channels.DestinationTypeUser, "ou_test")))
			if outcome.Class != test.class || outcome.Code != test.code {
				t.Fatalf("outcome=%+v want class=%s code=%q", outcome, test.class, test.code)
			}
		})
	}
}

func TestHTTPAccessTokenResolverUsesSecretRefWithoutReturningSecret(t *testing.T) {
	secretSeen := false
	doer := &fakeDoer{respond: func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != tenantAccessTokenPath || request.Header.Get("Authorization") != "" {
			t.Fatal("token request used an unexpected endpoint or authorization header")
		}
		for _, body := range [][]byte{{}} {
			_ = body
		}
		return larkResponse(http.StatusOK, `{"code":0,"tenant_access_token":"tenant-token","expire":7200}`), nil
	}}
	resolver, err := NewHTTPAccessTokenResolver(TokenResolverConfig{
		Secrets: secretResolverFunc(func(_ context.Context, ref string) (string, error) {
			if ref != validBinding().SecretRef {
				t.Fatal("resolver received an unexpected secret reference")
			}
			return "secret-value", nil
		}),
		Client: doer,
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := resolver.Resolve(context.Background(), validBinding())
	if err != nil || token != "tenant-token" {
		t.Fatalf("token resolution failed: token-present=%t err=%v", token != "", err)
	}
	doer.mu.Lock()
	body := string(doer.bodies[0])
	doer.mu.Unlock()
	secretSeen = strings.Contains(body, "secret-value")
	if !secretSeen {
		t.Fatal("token request did not use the resolved secret in memory")
	}
}

func TestHTTPAccessTokenResolverRedactsResolverAndProviderFailures(t *testing.T) {
	resolver, err := NewHTTPAccessTokenResolver(TokenResolverConfig{
		Secrets: secretResolverFunc(func(context.Context, string) (string, error) {
			return "secret-value", errors.New("resolver failed with secret-value")
		}), Client: &fakeDoer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolver.Resolve(context.Background(), validBinding())
	if !errors.Is(err, ErrCredentialResolution) || strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("credential error is not safe: type=%T contains-secret=%t", err, strings.Contains(err.Error(), "secret-value"))
	}

	malformed := &fakeDoer{respond: func(*http.Request) (*http.Response, error) {
		return larkResponse(http.StatusOK, `{"code":0,"expire":7200}`), nil
	}}
	resolver, err = NewHTTPAccessTokenResolver(TokenResolverConfig{
		Secrets: secretResolverFunc(func(context.Context, string) (string, error) { return "secret-value", nil }), Client: malformed,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolver.Resolve(context.Background(), validBinding())
	if !errors.Is(err, ErrTokenMalformed) {
		t.Fatalf("malformed token response error=%v", err)
	}
}

func TestLarkConfigRejectsArbitraryBaseURLAndInvalidBinding(t *testing.T) {
	_, err := NewSender(SenderConfig{
		Tokens: tokenResolverFunc(func(context.Context, Binding) (string, error) { return "token", nil }),
		Client: &fakeDoer{}, BaseURL: "https://attacker.invalid",
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("arbitrary base URL error=%v", err)
	}
	bad := validBinding()
	bad.ReceiverIDType = "arbitrary"
	_, err = NewSender(SenderConfig{
		Bindings: []Binding{bad}, Tokens: tokenResolverFunc(func(context.Context, Binding) (string, error) { return "token", nil }), Client: &fakeDoer{},
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid receiver type error=%v", err)
	}
}
