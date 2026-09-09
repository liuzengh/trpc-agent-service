package mock

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// recordingHandler implements channels.Handler, keeping the inbound messages
// it received for assertions (the mock callback runs on the server goroutine).
type recordingHandler struct {
	mu    sync.Mutex
	got   []channels.InboundMessage
	reply channels.OutboundMessage
	err   error
}

func (h *recordingHandler) Handle(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.got = append(h.got, msg)
	return h.reply, h.err
}

func (h *recordingHandler) received() []channels.InboundMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]channels.InboundMessage(nil), h.got...)
}

func postJSON(t *testing.T, url, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func TestMockNameSendSent(t *testing.T) {
	c := New()
	if c.Name() != "mock" {
		t.Fatalf("channel name: got %q, want mock", c.Name())
	}

	msg := channels.OutboundMessage{
		Channel: "mock", MsgID: "m1", SessionKey: "dm:mock:u1",
		UserID: "u1", Text: "你好", TraceID: "tr-1",
	}
	if err := c.Send(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	sent := c.Sent()
	if len(sent) != 1 || sent[0].Text != "你好" || sent[0].SessionKey != "dm:mock:u1" {
		t.Fatalf("unexpected inbox: %+v", sent)
	}
	// Sent returns a copy: mutating the result must not touch the inbox.
	sent[0].Text = "mutated"
	if got := c.Sent(); len(got) != 1 || got[0].Text != "你好" {
		t.Fatalf("Sent must hand out a copy, got %+v", got)
	}
	if len(New().Sent()) != 0 {
		t.Fatal("a fresh channel must have an empty inbox")
	}
}

// The full callback path: payload decode → normalization → handler → sync
// reply recorded in the inbox and echoed in the response body.
func TestMockCallbackSyncReply(t *testing.T) {
	c := New()
	h := &recordingHandler{reply: channels.OutboundMessage{Channel: "mock", Text: "同步回复"}}
	handler, err := c.CallbackHandler(h, channels.BindingCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()

	code, body := postJSON(t, srv.URL, `{"msg_id":"m1","user_id":"u1","chat_id":"g1","text":"你好"}`)
	if code != http.StatusOK {
		t.Fatalf("status %d, body %s", code, body)
	}
	if !strings.Contains(body, `"status":"ok"`) || !strings.Contains(body, `"reply":"同步回复"`) {
		t.Fatalf("unexpected reply body: %s", body)
	}

	got := h.received()
	if len(got) != 1 {
		t.Fatalf("handler got %d messages, want 1", len(got))
	}
	msg := got[0]
	if msg.Channel != "mock" || msg.MsgID != "m1" || msg.UserID != "u1" ||
		msg.ChatID != "g1" || msg.Text != "你好" ||
		msg.SessionKey != "group:mock:g1" || msg.TraceID != "m1" ||
		msg.WebhookPath != "/" || msg.ReceivedAt.IsZero() {
		t.Fatalf("inbound not normalized correctly: %+v", msg)
	}

	// The sync reply went through Send into the inspectable inbox, verbatim
	// as the handler produced it.
	sent := c.Sent()
	if len(sent) != 1 || sent[0].Text != "同步回复" {
		t.Fatalf("unexpected inbox: %+v", sent)
	}
}

// Per the Handler contract: an empty reply text means "accepted" (async
// delivery), a duplicate is answered with success so the IM stops
// redelivering, and a handler failure surfaces as 500.
func TestMockCallbackStatuses(t *testing.T) {
	c := New()
	h := &recordingHandler{}
	handler, err := c.CallbackHandler(h, channels.BindingCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Accepted: empty reply text, nothing enters the inbox.
	h.reply, h.err = channels.OutboundMessage{}, nil
	code, body := postJSON(t, srv.URL, `{"msg_id":"m1","user_id":"u1","text":"hi"}`)
	if code != http.StatusOK || !strings.Contains(body, `"status":"accepted"`) {
		t.Fatalf("accepted: status %d body %s", code, body)
	}
	if got := c.Sent(); len(got) != 0 {
		t.Fatalf("async path must not record a send, got %+v", got)
	}

	// Duplicate: still a 200 with status "duplicate".
	h.err = channels.ErrDuplicate
	code, body = postJSON(t, srv.URL, `{"msg_id":"m2","user_id":"u1","text":"hi"}`)
	if code != http.StatusOK || !strings.Contains(body, `"status":"duplicate"`) {
		t.Fatalf("duplicate: status %d body %s", code, body)
	}

	// Handler error: 500.
	h.err = errors.New("boom")
	if code, _ = postJSON(t, srv.URL, `{"msg_id":"m3","user_id":"u1","text":"hi"}`); code != http.StatusInternalServerError {
		t.Fatalf("handler error: status %d, want 500", code)
	}

	// Malformed JSON body: 400.
	if code, _ = postJSON(t, srv.URL, `{not json`); code != http.StatusBadRequest {
		t.Fatalf("bad json: status %d, want 400", code)
	}

	// msg_id / user_id are required: 400.
	for _, body := range []string{
		`{"msg_id":"","user_id":"u1","text":"hi"}`,
		`{"msg_id":"m4","user_id":"","text":"hi"}`,
	} {
		if code, _ = postJSON(t, srv.URL, body); code != http.StatusBadRequest {
			t.Fatalf("missing required fields (%s): status %d, want 400", body, code)
		}
	}
}

// RegisterRoutes mounts POST /mock/callback carrying the request's path as
// the webhook path (the gateway routes the tenant through it).
func TestMockRegisterRoutes(t *testing.T) {
	c := New()
	h := &recordingHandler{reply: channels.OutboundMessage{Text: "ok"}}
	mux := http.NewServeMux()
	c.RegisterRoutes(mux, h)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	code, body := postJSON(t, srv.URL+"/mock/callback", `{"msg_id":"m9","user_id":"u9","text":"hello"}`)
	if code != http.StatusOK || !strings.Contains(body, `"status":"ok"`) {
		t.Fatalf("status %d body %s", code, body)
	}
	got := h.received()
	if len(got) != 1 || got[0].WebhookPath != "/mock/callback" {
		t.Fatalf("webhook path not carried: %+v", got)
	}
	if time.Since(got[0].ReceivedAt) > time.Minute {
		t.Fatalf("ReceivedAt must be stamped at arrival: %+v", got[0].ReceivedAt)
	}
}
