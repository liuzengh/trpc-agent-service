package channel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestTelegramProviderErrorDoesNotPersistResponseBody(t *testing.T) {
	adapter := NewTelegramAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":false,"error_code":502,"description":"provider-secret-body"}`)),
			Request:    req,
		}, nil
	})
	telegramToken := "123456789:" + strings.Repeat("A", 35)
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{Token: telegramToken}, &OutboundMessage{
		ConversationID: "chat-1",
		Content:        "hello",
	})
	if err == nil || strings.Contains(err.Error(), "provider-secret-body") || strings.Contains(err.Error(), telegramToken) {
		t.Fatalf("provider response leaked through error: %v", err)
	}
}

func TestTelegramRetryAfterBoundsMalformedAndOversizedValues(t *testing.T) {
	maxDuration := time.Duration(maxTelegramRetryAfterSeconds) * time.Second
	maxInt := int(^uint(0) >> 1)
	wantMaxInt := time.Duration(int64(maxInt)) * time.Second
	if int64(maxInt) > maxTelegramRetryAfterSeconds {
		wantMaxInt = maxDuration
	}
	for _, test := range []struct {
		name    string
		seconds int
		want    time.Duration
	}{
		{name: "negative", seconds: -1, want: 0},
		{name: "zero", seconds: 0, want: 0},
		{name: "positive", seconds: 7, want: 7 * time.Second},
		{name: "max int", seconds: maxInt, want: wantMaxInt},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := telegramRetryAfter(test.seconds); got != test.want {
				t.Fatalf("telegramRetryAfter(%d) = %s, want %s", test.seconds, got, test.want)
			}
		})
	}
}

func TestTelegramOversizedResponseIsOutcomeUnknown(t *testing.T) {
	adapter := NewTelegramAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}` + strings.Repeat(" ", maxTelegramResponseBytes))),
			Request:    req,
		}, nil
	})
	token := "123456789:" + strings.Repeat("A", 35)
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{Token: token}, &OutboundMessage{
		ConversationID: "chat-1",
		Content:        "hello",
	})
	if err == nil || !DeliveryOutcomeUnknown(err) {
		t.Fatalf("oversized Telegram response error=%v, want outcome-unknown", err)
	}
}

func TestTelegramReplyUsesCurrentReplyParametersShape(t *testing.T) {
	adapter := NewTelegramAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var payload map[string]interface{}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			t.Fatalf("decode Telegram request: %v", err)
		}
		if _, found := payload["reply_to_message_id"]; found {
			t.Fatalf("legacy reply field was sent: %#v", payload)
		}
		replyParameters, ok := payload["reply_parameters"].(map[string]interface{})
		if !ok {
			t.Fatalf("reply_parameters=%T, want object", payload["reply_parameters"])
		}
		messageID, ok := replyParameters["message_id"].(float64)
		if !ok || messageID != 42 {
			t.Fatalf("reply_parameters.message_id=%#v, want JSON number 42", replyParameters["message_id"])
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})

	token := "123456789:" + strings.Repeat("A", 35)
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{Token: token}, &OutboundMessage{
		ConversationID: "chat-1",
		Content:        "hello",
		ReplyToID:      "42",
	})
	if err != nil {
		t.Fatalf("SendReply: %v", err)
	}
}

func TestTelegramReplyRejectsNonNumericTarget(t *testing.T) {
	adapter := NewTelegramAdapter()
	called := false
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		return nil, fmt.Errorf("transport must not be called")
	})
	token := "123456789:" + strings.Repeat("A", 35)
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{Token: token}, &OutboundMessage{
		ConversationID: "chat-1",
		Content:        "hello",
		ReplyToID:      "message-42",
	})
	var deliveryErr *DeliveryError
	if err == nil || !errors.As(err, &deliveryErr) || !deliveryErr.Permanent {
		t.Fatalf("error=%v, want permanent delivery error", err)
	}
	if called {
		t.Fatal("invalid reply target reached transport")
	}
}

func TestTelegramStreamChunkOnlyIgnoresMessageNotModified(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{
			name: "unchanged content is idempotent",
			body: `{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`,
		},
		{
			name:    "invalid message is permanent",
			body:    `{"ok":false,"error_code":400,"description":"Bad Request: message to edit not found"}`,
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := NewTelegramAdapter()
			adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(test.body)),
					Request:    req,
				}, nil
			})
			token := "123456789:" + strings.Repeat("A", 35)
			err := adapter.SendStreamChunk(context.Background(), &tenant.ChannelBinding{Token: token}, &StreamChunk{
				ConversationID: "chat-1",
				MessageID:      "42",
				Content:        "hello",
			})
			if test.wantErr {
				var deliveryErr *DeliveryError
				if err == nil || !errors.As(err, &deliveryErr) || !deliveryErr.Permanent {
					t.Fatalf("expected permanent stream error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unchanged stream content returned error: %v", err)
			}
		})
	}
}

// StreamChunk has no content-type contract. Model output must therefore stay
// plain text instead of being interpreted as Telegram Markdown, whose syntax
// would reject otherwise valid streamed content such as an unmatched '*'.
func TestTelegramStreamChunkUsesPlainTextByDefault(t *testing.T) {
	adapter := NewTelegramAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var payload map[string]interface{}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			t.Fatalf("decode Telegram stream request: %v", err)
		}
		messageID, ok := payload["message_id"].(float64)
		if !ok || messageID != 42 {
			t.Fatalf("message_id=%#v, want JSON number 42", payload["message_id"])
		}
		if _, found := payload["parse_mode"]; found {
			t.Fatalf("stream payload unexpectedly enables Telegram Markdown: %#v", payload)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    req,
		}, nil
	})

	token := "123456789:" + strings.Repeat("A", 35)
	err := adapter.SendStreamChunk(context.Background(), &tenant.ChannelBinding{Token: token}, &StreamChunk{
		ConversationID: "chat-1",
		MessageID:      "42",
		Content:        "unmatched * model text",
	})
	if err != nil {
		t.Fatalf("SendStreamChunk: %v", err)
	}
}

func TestTelegramStreamChunkUsesDeliveryBoundaryAndIdempotencyKey(t *testing.T) {
	adapter := NewTelegramAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get(DeliveryIdempotencyKeyHeader); got != "stream-delivery-7" {
			t.Fatalf("stream idempotency key=%q, want stream-delivery-7", got)
		}
		if trace := httptrace.ContextClientTrace(req.Context()); trace != nil {
			trace.WroteRequest(httptrace.WroteRequestInfo{})
		}
		return nil, errors.New("connection reset after write")
	})
	token := "123456789:" + strings.Repeat("A", 35)
	err := adapter.SendStreamChunk(context.Background(), &tenant.ChannelBinding{Token: token}, &StreamChunk{
		ConversationID: "chat-1",
		MessageID:      "42",
		Content:        "partial",
		DeliveryID:     "stream-delivery-7",
	})
	if err == nil || !DeliveryOutcomeUnknown(err) {
		t.Fatalf("stream transport error=%v, want outcome-unknown", err)
	}
}

func TestTelegramStreamChunkRejectsMissingProviderBodyWithoutPanic(t *testing.T) {
	adapter := NewTelegramAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Request: req}, nil
	})
	token := "123456789:" + strings.Repeat("A", 35)
	err := adapter.SendStreamChunk(context.Background(), &tenant.ChannelBinding{Token: token}, &StreamChunk{
		ConversationID: "chat-1",
		MessageID:      "42",
		Content:        "partial",
	})
	if err == nil || !DeliveryOutcomeUnknown(err) {
		t.Fatalf("missing-body error=%v, want outcome-unknown", err)
	}
}

func TestWeWorkProviderErrorDoesNotPersistResponseBody(t *testing.T) {
	adapter := NewWeWorkAdapter()
	adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/gettoken") {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"errcode":0,"access_token":"access-token","expires_in":3600}`)),
				Request:    req,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"errcode":502,"errmsg":"provider-secret-body"}`)),
			Request:    req,
		}, nil
	})
	err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{
		AppID:  "1000002",
		Secret: strings.Repeat("S", 43),
		Config: map[string]string{"corp_id": "ww0123456789abcdef"},
	}, &OutboundMessage{
		ConversationID: "user-1",
		Content:        "hello",
	})
	if err == nil || strings.Contains(err.Error(), "provider-secret-body") || strings.Contains(err.Error(), strings.Repeat("S", 43)) || strings.Contains(err.Error(), "access-token") {
		t.Fatalf("provider response leaked through error: %v", err)
	}
}

func TestWeWorkRateLimitHonorsRetryAfter(t *testing.T) {
	for _, test := range []struct {
		name        string
		rateLimited string
		statusCode  int
		body        string
	}{
		{name: "message HTTP 429", rateLimited: "/cgi-bin/message/send", statusCode: http.StatusTooManyRequests, body: `{}`},
		{name: "token HTTP 429", rateLimited: "/cgi-bin/gettoken", statusCode: http.StatusTooManyRequests, body: `{}`},
		{name: "message API errcode", rateLimited: "/cgi-bin/message/send", statusCode: http.StatusOK, body: `{"errcode":45009}`},
		{name: "token API errcode", rateLimited: "/cgi-bin/gettoken", statusCode: http.StatusOK, body: `{"errcode":45011}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := NewWeWorkAdapter()
			adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/cgi-bin/gettoken" && req.URL.Path != test.rateLimited {
					return weWorkJSONResponse(req, `{"errcode":0,"access_token":"access-token","expires_in":3600}`), nil
				}
				if req.URL.Path != test.rateLimited {
					return nil, fmt.Errorf("unexpected WeWork path %q", req.URL.Path)
				}
				header := make(http.Header)
				header.Set("Retry-After", "17")
				return &http.Response{
					StatusCode: test.statusCode,
					Header:     header,
					Body:       io.NopCloser(strings.NewReader(test.body)),
					Request:    req,
				}, nil
			})

			err := adapter.SendReply(context.Background(), weWorkBinding(99), &OutboundMessage{
				ConversationID: "user-1",
				Content:        "hello",
			})
			permanent, delay := DeliveryFailure(err)
			if err == nil || permanent || delay != 17*time.Second {
				t.Fatalf("rate-limited delivery classification err=%v permanent=%v delay=%s, want retryable after 17s", err, permanent, delay)
			}
		})
	}
}

func TestWeWorkRetryAfter(t *testing.T) {
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	duplicate := make(http.Header)
	duplicate.Add("Retry-After", "17")
	duplicate.Add("Retry-After", "18")

	for _, test := range []struct {
		name   string
		header http.Header
		want   time.Duration
	}{
		{name: "delta seconds", header: http.Header{"Retry-After": []string{"17"}}, want: 17 * time.Second},
		{name: "future HTTP date", header: http.Header{"Retry-After": []string{now.Add(17 * time.Second).Format(http.TimeFormat)}}, want: 17 * time.Second},
		{name: "past HTTP date", header: http.Header{"Retry-After": []string{now.Add(-time.Second).Format(http.TimeFormat)}}, want: defaultWeWorkRateLimitRetryAfter},
		{name: "invalid value", header: http.Header{"Retry-After": []string{"later"}}, want: defaultWeWorkRateLimitRetryAfter},
		{name: "duplicate values", header: duplicate, want: defaultWeWorkRateLimitRetryAfter},
		{name: "oversized delta seconds", header: http.Header{"Retry-After": []string{"999999999999999999999"}}, want: time.Duration(maxWeWorkRetryAfterSeconds) * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := weWorkRetryAfter(test.header, now); got != test.want {
				t.Fatalf("weWorkRetryAfter() = %s, want %s", got, test.want)
			}
		})
	}
}
