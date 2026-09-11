package web

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// fakeWebhookSource returns a canned result, standing in for the IM manager.
type fakeWebhookSource struct {
	event channels.WebhookEvent
	err   error

	gotBinding string
	gotHeaders channels.WebhookCallback
	calls      int
}

func (f *fakeWebhookSource) HandleWebhook(_ context.Context, bindingID string, cb channels.WebhookCallback) (channels.WebhookEvent, error) {
	f.calls++
	f.gotBinding = bindingID
	f.gotHeaders = cb
	return f.event, f.err
}

func newWebhookServer(t *testing.T, src *fakeWebhookSource) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	NewWebhookAPI(src).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, srv *httptest.Server, path string, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-Lark-Request-Timestamp", "1700000000")
	req.Header.Set("X-Lark-Request-Nonce", "nonce-1")
	req.Header.Set("X-Lark-Signature", "sig-1")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookHandlerForwardsBindingAndHeaders(t *testing.T) {
	src := &fakeWebhookSource{event: channels.WebhookEvent{Payload: []byte("{}")}}
	srv := newWebhookServer(t, src)
	resp := post(t, srv, "/webhooks/im/b-42", `{"header":{}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if src.gotBinding != "b-42" {
		t.Errorf("binding = %q, want b-42", src.gotBinding)
	}
	// The signature headers are the platform's only credential: losing them
	// would make every callback unverifiable.
	if src.gotHeaders.Timestamp != "1700000000" || src.gotHeaders.Nonce != "nonce-1" || src.gotHeaders.Signature != "sig-1" {
		t.Errorf("signature headers lost: %+v", src.gotHeaders)
	}
	if string(src.gotHeaders.Body) != `{"header":{}}` {
		t.Errorf("body = %s", src.gotHeaders.Body)
	}
}

func TestWebhookHandlerEchoesChallenge(t *testing.T) {
	src := &fakeWebhookSource{event: channels.WebhookEvent{Challenge: "chal-9"}}
	srv := newWebhookServer(t, src)
	resp := post(t, srv, "/webhooks/im/b1", `{"type":"url_verification"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(readBody(t, resp), "chal-9") {
		t.Errorf("challenge not echoed: %s", readBody(t, resp))
	}
}

// TestWebhookHandlerStatusMapping pins the answer the platform reads for every
// refusal; getting these wrong either hides a misconfiguration or invites the
// platform to retry forever.
func TestWebhookHandlerStatusMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"unknown binding", channels.ErrBindingNotFound, http.StatusNotFound},
		{"unserved channel", channels.ErrWebhookUnsupported, http.StatusNotImplemented},
		{"unauthenticated", channels.ErrWebhookUnauthorized, http.StatusUnauthorized},
		{"busy", channels.ErrWebhookBusy, http.StatusServiceUnavailable},
		{"wrapped unauthenticated", errors.Join(errors.New("ctx"), channels.ErrWebhookUnauthorized), http.StatusUnauthorized},
		{"other", errors.New("boom"), http.StatusInternalServerError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &fakeWebhookSource{err: c.err}
			srv := newWebhookServer(t, src)
			resp := post(t, srv, "/webhooks/im/b1", `{}`)
			if resp.StatusCode != c.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, c.want)
			}
		})
	}
}

func TestWebhookHandlerRejectsBadRequests(t *testing.T) {
	src := &fakeWebhookSource{}
	srv := newWebhookServer(t, src)

	// Missing binding id.
	resp := post(t, srv, "/webhooks/im/", `{}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("empty binding id: status = %d, want 404", resp.StatusCode)
	}
	// A deeper path is not a callback.
	resp = post(t, srv, "/webhooks/im/b1/extra", `{}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("extra path segment: status = %d, want 404", resp.StatusCode)
	}
	// GET is a probe, not an event.
	getResp, err := srv.Client().Get(srv.URL + "/webhooks/im/b1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET: status = %d, want 405", getResp.StatusCode)
	}
	if src.calls != 0 {
		t.Errorf("the ingress was called %d time(s) for rejected requests", src.calls)
	}
}

func TestWebhookHandlerBoundsBody(t *testing.T) {
	src := &fakeWebhookSource{}
	srv := newWebhookServer(t, src)
	huge := strings.Repeat("a", maxWebhookBody+16)
	resp := post(t, srv, "/webhooks/im/b1", huge)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
	if src.calls != 0 {
		t.Error("an oversized body must not reach the ingress")
	}
}

// readBody drains a response body for assertions.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}
