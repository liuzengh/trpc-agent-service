package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

type diagnosticTransport func(*http.Request) (*http.Response, error)

func (f diagnosticTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSendTransportDiagnosticsDropEndpointAndCause(t *testing.T) {
	var calls int
	client := &http.Client{Transport: diagnosticTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		return nil, &url.Error{Op: "POST", URL: r.URL.String(), Err: &net.DNSError{Err: "private-error-canary", Name: "private-host-canary"}}
	})}
	adapter, _ := New(secret.StaticStore{"secret://bot": "private-token-canary"}, client)
	_, err := adapter.Send(context.Background(), testBinding(t, ""), channels.OutboundMessage{Text: "private-reply-canary", ReplyTarget: `{"chat_id":42}`})
	var delivery *channels.DeliveryError
	if !errors.As(err, &delivery) || !delivery.Unknown || delivery.Retryable || calls != 1 || delivery.Diagnostics.Kind != "dns" {
		t.Fatalf("unexpected delivery: %v calls=%d", err, calls)
	}
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if strings.Contains(cause.Error(), "canary") || strings.Contains(cause.Error(), "https://") {
			t.Fatal("original HTTP error escaped safe boundary")
		}
	}
}

func TestSendTimeoutAfterWriteStaysUnknown(t *testing.T) {
	var received atomic.Bool
	stop := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		received.Store(true)
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(stop) })
	client := server.Client()
	client.Timeout = 200 * time.Millisecond
	adapter, _ := New(secret.StaticStore{"secret://bot": "private-token-canary"}, client)
	_, err := adapter.Send(context.Background(), testBinding(t, server.URL), channels.OutboundMessage{Text: "private-reply-canary", ReplyTarget: `{"chat_id":42}`})
	var delivery *channels.DeliveryError
	if !errors.As(err, &delivery) || !delivery.Unknown || delivery.Retryable || !received.Load() {
		t.Fatal("lost response must remain unknown", err)
	}
	if delivery.Diagnostics.Kind != "timeout" || delivery.Diagnostics.Phase != "wait_response" || strings.Contains(err.Error(), "canary") {
		t.Fatal("timeout phase missing or unsafe", err)
	}
}

func TestSendResponseDiagnosticsOmitProviderPayload(t *testing.T) {
	for _, tc := range []struct {
		name, body, kind string
		code             int
		unknown, retry   bool
	}{
		{"malformed", `private-provider-canary`, "invalid_response", 502, true, false},
		{"incomplete", `{"ok":true,"result":{}}`, "invalid_response", 200, true, false},
		{"rejected", `{"ok":false,"error_code":403,"description":"private-provider-canary"}`, "provider_rejected", 403, false, false},
		{"rate_limited", `{"ok":false,"error_code":429,"description":"private-provider-canary","parameters":{"retry_after":3}}`, "provider_rejected", 429, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: diagnosticTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}
			adapter, _ := New(secret.StaticStore{"secret://bot": "private-token-canary"}, client)
			_, err := adapter.Send(context.Background(), testBinding(t, ""), channels.OutboundMessage{ReplyTarget: `{"chat_id":42}`})
			var d *channels.DeliveryError
			if !errors.As(err, &d) || d.Unknown != tc.unknown || d.Retryable != tc.retry || d.Diagnostics.Kind != tc.kind || d.Diagnostics.HTTPStatus != tc.code {
				t.Fatal("response classification lost", err)
			}
			raw, _ := json.Marshal(d)
			if strings.Contains(err.Error(), "canary") || strings.Contains(string(raw), "canary") {
				t.Fatal("provider description leaked")
			}
		})
	}
}
