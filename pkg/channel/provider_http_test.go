package channel

import (
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"testing"
)

func TestProviderRedirectPolicyRequiresExactHTTPSOrigin(t *testing.T) {
	policy := providerRedirectPolicy("api.example.test")
	for _, test := range []struct {
		name    string
		target  string
		allowed bool
	}{
		{name: "same host default port", target: "https://api.example.test/path", allowed: true},
		{name: "same host explicit tls port", target: "https://api.example.test:443/path", allowed: true},
		{name: "same host alternate port", target: "https://api.example.test:8443/path"},
		{name: "wrong scheme", target: "http://api.example.test/path"},
		{name: "wrong host", target: "https://evil.example.test/path"},
		{name: "userinfo", target: "https://token@api.example.test/path"},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, err := url.Parse(test.target)
			if err != nil {
				t.Fatal(err)
			}
			err = policy(&http.Request{URL: target}, nil)
			if (err == nil) != test.allowed {
				t.Fatalf("policy error=%v allowed=%v", err, test.allowed)
			}
		})
	}
}

func TestSetDeliveryIdempotencyKey(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "https://provider.example.test/send", nil)
	if err := setDeliveryIdempotencyKey(req, &OutboundMessage{DeliveryID: "trpc-outbox-v1-42-0"}); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get(DeliveryIdempotencyKeyHeader); got != "trpc-outbox-v1-42-0" {
		t.Fatalf("idempotency header=%q", got)
	}
	for _, invalid := range []string{"bad\r\nvalue", strings.Repeat("x", maxDeliveryIDBytes+1)} {
		req := httptest.NewRequest(http.MethodPost, "https://provider.example.test/send", nil)
		if err := setDeliveryIdempotencyKey(req, &OutboundMessage{DeliveryID: invalid}); err == nil {
			t.Fatalf("invalid delivery id %q was accepted", invalid)
		}
	}
	if err := setDeliveryIdempotencyKey(req, nil); err != nil {
		t.Fatalf("nil message: %v", err)
	}
}

func TestDoProviderRequestInvokesWriteTrace(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(req.Context())
		if trace == nil {
			t.Fatal("request trace was not installed")
		}
		trace.WroteRequest(httptrace.WroteRequestInfo{})
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody, Request: req}, nil
	})}
	req, err := http.NewRequest(http.MethodPost, "https://provider.example.test/send", strings.NewReader("body"))
	if err != nil {
		t.Fatal(err)
	}
	_, wrote, err := doProviderRequest(client, req)
	if err != nil {
		t.Fatal(err)
	}
	if !wrote {
		t.Fatal("write trace was not observed")
	}
	if got := providerTransportFailure("test", false); DeliveryOutcomeUnknown(got) {
		t.Fatalf("pre-write transport failure was marked unknown: %v", got)
	}
	if got := providerTransportFailure("test", true); !DeliveryOutcomeUnknown(got) {
		t.Fatalf("post-write transport failure was not marked unknown: %v", got)
	}
}
