package channels

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// TestDeliverySenderRoutesByChannel pins the wiring contract: every channel
// with a configured sender goes to it, and anything else — including a
// channel whose sender is missing — is a named Rejected, never a silent
// success.
func TestDeliverySenderRoutesByChannel(t *testing.T) {
	d := &DeliverySender{}
	cases := []struct {
		channel string
		field   string
	}{
		{"wechat_kf", "wechat_kf sender"},
		{"wecom", "wecom sender"},
		{"webchat", "webchat sender"},
		{"telegram", "not wired"},
	}
	for _, tc := range cases {
		outcome, err := d.Send(context.Background(), &outbox.ClaimedReply{ChannelType: tc.channel})
		if outcome != outbox.Rejected || err == nil {
			t.Fatalf("%s without a sender = (%v, %v), want Rejected with a reason", tc.channel, outcome, err)
		}
		if !strings.Contains(err.Error(), tc.field) {
			t.Fatalf("%s error = %q, want it to name %q", tc.channel, err, tc.field)
		}
	}
	// With senders present the routing reaches them; the webchat sender is
	// the cheapest proof because it answers without any network.
	d.WebChat = &WebChatSender{}
	if outcome, err := d.Send(context.Background(), &outbox.ClaimedReply{ChannelType: "webchat"}); outcome != outbox.Sent || err != nil {
		t.Fatalf("webchat with a sender = (%v, %v), want Sent", outcome, err)
	}
}

// wecomMock serves the two endpoints the sender talks to — gettoken and
// message/send — with scriptable answers on the send.
type wecomMock struct {
	srv      *httptest.Server
	sendCode atomic.Int32
	delayMS  atomic.Int64
	sent     atomic.Int32
}

func newWeComMock(t *testing.T) *wecomMock {
	t.Helper()
	m := &wecomMock{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cgi-bin/gettoken":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"errcode": 0, "access_token": "mock-token", "expires_in": 7200,
			})
		case "/cgi-bin/message/send":
			if d := m.delayMS.Load(); d > 0 {
				time.Sleep(time.Duration(d) * time.Millisecond)
			}
			code := m.sendCode.Load()
			if code == 0 {
				m.sent.Add(1)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"errcode": code, "errmsg": "scripted"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(m.srv.Close)
	return m
}

// TestWeComSenderClassifiesAnswers pins the outbox contract the delivery role
// relies on: an answer from the platform is Rejected (retryable/dead-letter),
// no answer is Unknown (never auto-retried, the message may have arrived).
func TestWeComSenderClassifiesAnswers(t *testing.T) {
	mock := newWeComMock(t)
	s := NewWeComSender(func(string) (*tenant.WeComBinding, bool) { return testWecomBinding(), true }).
		WithAPIBase(mock.srv.URL)
	s.delegate.client = &http.Client{Timeout: 500 * time.Millisecond}
	reply := &outbox.ClaimedReply{TenantID: "t1", Target: "u1", Text: "hi"}

	outcome, err := s.Send(context.Background(), reply)
	if outcome != outbox.Sent || err != nil {
		t.Fatalf("success send = (%v, %v), want Sent", outcome, err)
	}
	if mock.sent.Load() != 1 {
		t.Fatalf("send count = %d, want 1", mock.sent.Load())
	}

	mock.sendCode.Store(40003) // answered rejection: invalid user
	outcome, err = s.Send(context.Background(), reply)
	if outcome != outbox.Rejected || err == nil {
		t.Fatalf("answered rejection = (%v, %v), want Rejected", outcome, err)
	}

	mock.sendCode.Store(0)
	mock.delayMS.Store(2000) // far past the 500ms client timeout
	outcome, err = s.Send(context.Background(), reply)
	if outcome != outbox.Unknown || err == nil {
		t.Fatalf("unanswered send = (%v, %v), want Unknown", outcome, err)
	}
}

// TestWeComSenderWithoutBinding: no binding is permanent, so it must be
// Rejected (dead-letter material), not Unknown (a human's inbox).
func TestWeComSenderWithoutBinding(t *testing.T) {
	s := NewWeComSender(func(string) (*tenant.WeComBinding, bool) { return nil, false })
	outcome, err := s.Send(context.Background(), &outbox.ClaimedReply{TenantID: "t1", Target: "u1", Text: "hi"})
	if outcome != outbox.Rejected || err == nil {
		t.Fatalf("no binding = (%v, %v), want Rejected", outcome, err)
	}
}
