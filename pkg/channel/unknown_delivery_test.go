package channel

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func TestUnknownDeliveryErrorPreservesStableClassification(t *testing.T) {
	err := UnknownDeliveryError(retryableTransportError("test"))
	if !DeliveryOutcomeUnknown(err) {
		t.Fatal("unknown delivery was not classified as outcome-unknown")
	}
	if !errors.Is(err, ErrDeliveryOutcomeUnknown) {
		t.Fatal("unknown delivery did not unwrap ErrDeliveryOutcomeUnknown")
	}
	if !errors.Is(err, ErrChannelTransport) {
		t.Fatal("unknown delivery did not preserve the stable transport category")
	}
	if permanent, delay := DeliveryFailure(err); permanent || delay != 0 {
		t.Fatalf("unknown delivery classification permanent=%v delay=%s, want non-permanent without retry hint", permanent, delay)
	}
}

func TestUnknownDeliveryErrorDoesNotExposeExtensionCause(t *testing.T) {
	secret := "provider failed https://user:password@example.test/send?token=live-secret"
	err := UnknownDeliveryError(errors.New(secret))
	if got := err.Error(); got != ErrDeliveryOutcomeUnknown.Error() {
		t.Fatalf("unknown delivery error=%q, want opaque classification", got)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "live-secret") {
		t.Fatalf("unknown delivery exposed extension cause: %q", err)
	}
	if errors.Is(err, ErrChannelTransport) {
		t.Fatal("arbitrary extension cause unexpectedly acquired transport classification")
	}
}

func TestTelegramMessageSendFailuresAreOutcomeUnknown(t *testing.T) {
	token := "123456789:" + strings.Repeat("A", 35)
	tests := []struct {
		name         string
		resp         *http.Response
		err          error
		requestWrote bool
		wantUnknown  bool
	}{
		{name: "transport before write", err: errors.New("no route to host")},
		{name: "transport after write", err: errors.New("connection reset"), requestWrote: true, wantUnknown: true},
		{name: "server error", resp: &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":false}`))}, wantUnknown: true},
		{name: "malformed response", resp: &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("not-json"))}, wantUnknown: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewTelegramAdapter()
			adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if tc.requestWrote {
					if trace := httptrace.ContextClientTrace(req.Context()); trace != nil {
						trace.WroteRequest(httptrace.WroteRequestInfo{})
					}
				}
				if tc.resp != nil {
					tc.resp.Request = req
				}
				return tc.resp, tc.err
			})
			err := adapter.SendReply(context.Background(), &tenant.ChannelBinding{Token: token}, &OutboundMessage{
				ConversationID: "chat-1",
				Content:        "hello",
			})
			if err == nil || DeliveryOutcomeUnknown(err) != tc.wantUnknown {
				t.Fatalf("SendReply error=%v, outcome-unknown=%v, want %v", err, DeliveryOutcomeUnknown(err), tc.wantUnknown)
			}
		})
	}
}

func TestWeWorkMessageSendFailuresAreOutcomeUnknown(t *testing.T) {
	tests := []struct {
		name         string
		resp         *http.Response
		err          error
		requestWrote bool
		wantUnknown  bool
	}{
		{name: "transport before write", err: errors.New("no route to host")},
		{name: "transport after write", err: errors.New("connection reset"), requestWrote: true, wantUnknown: true},
		{name: "server error", resp: &http.Response{StatusCode: http.StatusBadGateway, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, wantUnknown: true},
		{name: "malformed response", resp: weWorkJSONResponse(nil, "not-json"), wantUnknown: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewWeWorkAdapter()
			adapter.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/cgi-bin/gettoken" {
					return weWorkJSONResponse(req, `{"errcode":0,"access_token":"token-valid","expires_in":3600}`), nil
				}
				if tc.resp != nil {
					tc.resp.Request = req
				}
				if tc.requestWrote {
					if trace := httptrace.ContextClientTrace(req.Context()); trace != nil {
						trace.WroteRequest(httptrace.WroteRequestInfo{})
					}
				}
				return tc.resp, tc.err
			})
			err := adapter.SendReply(context.Background(), weWorkBinding(90), &OutboundMessage{
				ConversationID: "user-1",
				Content:        "hello",
			})
			if err == nil || DeliveryOutcomeUnknown(err) != tc.wantUnknown {
				t.Fatalf("SendReply error=%v, outcome-unknown=%v, want %v", err, DeliveryOutcomeUnknown(err), tc.wantUnknown)
			}
		})
	}
}
