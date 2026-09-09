package wecombot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func testConfig(t *testing.T, gateway string) Config {
	return Config{BotID: "bot-test", BotSecret: "private-test-secret", BridgeToken: strings.Repeat("b", 32), GatewayBaseURL: gateway, RouteKey: "test-route", StateDir: t.TempDir()}
}
func incoming() []byte {
	return []byte(`{"cmd":"aibot_msg_callback","headers":{"req_id":"req-message"},"body":{"msgid":"message-1","aibotid":"bot-test","chattype":"single","from":{"userid":"allowed-user"},"msgtype":"text","text":{"content":"hello"}}}`)
}
func ack(ws *websocket.Conn, id string, code int) error {
	return ws.WriteJSON(map[string]any{"headers": map[string]string{"req_id": id}, "errcode": code})
}
func attachFixture(c *Connector, url string) {
	c.dial = func(ctx context.Context) (*websocket.Conn, *http.Response, error) {
		return websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(url, "http"), nil)
	}
}
func awaitStatus(t *testing.T, c *Connector, wanted string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		status := c.status
		c.mu.RUnlock()
		if status == wanted {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("connector did not reach expected state")
}

func TestConnectorForwardsRealAdapterEnvelopeAndConfirmsReply(t *testing.T) {
	received := make(chan *channel.InboundMessage, 1)
	adapter := channel.NewWeComBotAdapter("")
	binding := &tenant.ChannelBinding{Type: "wecom_bot", AccountID: "bot-test", Token: strings.Repeat("b", 32)}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("token") != "test-route" || adapter.VerifySignature(r, binding) != nil {
			w.WriteHeader(401)
			return
		}
		inbound, err := adapter.ParseInbound(r, binding)
		if err != nil {
			t.Errorf("actual adapter parse: %v", err)
			w.WriteHeader(400)
			return
		}
		received <- inbound
		w.WriteHeader(200)
	}))
	defer gateway.Close()
	var replyCount atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var f frame
			if json.Unmarshal(raw, &f) != nil {
				return
			}
			switch f.Command {
			case "aibot_subscribe":
				if ack(ws, f.Headers.RequestID, 0) != nil {
					return
				}
				if ws.WriteMessage(websocket.TextMessage, incoming()) != nil {
					return
				}
			case "ping":
				if ack(ws, f.Headers.RequestID, 0) != nil {
					return
				}
			case "aibot_respond_msg":
				replyCount.Add(1)
				var body struct {
					Kind   string `json:"msgtype"`
					Stream struct {
						ID      string `json:"id"`
						Finish  bool   `json:"finish"`
						Content string `json:"content"`
					} `json:"stream"`
				}
				_ = json.Unmarshal(f.Body, &body)
				if f.Headers.RequestID != "req-message" || body.Kind != "stream" || !body.Stream.Finish || body.Stream.Content != "actual reply" {
					t.Error("outbound protocol mismatch")
				}
				if ack(ws, f.Headers.RequestID, 0) != nil {
					return
				}
			}
		}
	}))
	defer provider.Close()
	c, err := New(testConfig(t, gateway.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	attachFixture(c, provider.URL)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer cancel()
	select {
	case msg := <-received:
		if msg.MessageID != "message-1" || msg.ReplyToID != "req-message" || msg.ExternalUserID != "allowed-user" {
			t.Fatal("inbound identity mismatch")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no ingress")
	}
	awaitStatus(t, c, "subscribed")
	bridge := httptest.NewServer(c)
	defer bridge.Close()
	sender := channel.NewWeComBotAdapter(bridge.URL)
	for range 2 {
		message := &channel.OutboundMessage{ConversationID: "allowed-user", ReplyToID: "req-message", Content: "actual reply", ContentType: "text", DeliveryID: "outbox-1"}
		if err := sender.SendReply(ctx, binding, message); err != nil {
			t.Fatal(err)
		}
	}
	if replyCount.Load() != 1 {
		t.Fatal("duplicate bridge delivery sent twice")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connector leaked after cancellation")
	}
}

func TestSpoolRestartRetainsExactEnvelopeAndRejectsConflict(t *testing.T) {
	dir := t.TempDir()
	s, err := openSpool(dir, "binding")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openSpool(dir, "binding"); !errors.Is(err, ErrLocked) {
		t.Fatal("second process could acquire lock")
	}
	now := time.Now().UTC()
	if err := s.enqueue("message-1", incoming(), now); err != nil {
		t.Fatal(err)
	}
	first := s.next()
	s.Close()
	s, err = openSpool(dir, "binding")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !bytes.Equal(first.Payload, s.next().Payload) {
		t.Fatal("pending envelope changed on restart")
	}
	if err = s.enqueue("message-1", incoming(), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Payload, s.next().Payload) {
		t.Fatal("duplicate changed receipt time")
	}
	if err = s.admit(first.Key); err != nil {
		t.Fatal(err)
	}
	if err = s.enqueue("message-1", incoming(), now.Add(time.Minute)); err != nil || s.next() != nil {
		t.Fatal("admitted duplicate re-enqueued")
	}
	if err = s.enqueue("message-1", bytes.Replace(incoming(), []byte("hello"), []byte("changed"), 1), now); !errors.Is(err, ErrProtocol) {
		t.Fatal("message identity conflict accepted")
	}
	raw, err := os.ReadFile(filepath.Join(dir, first.Key+".json"))
	if err != nil || bytes.Contains(raw, []byte("hello")) {
		t.Fatal("admitted payload not removed")
	}
}

func TestSubscriptionRejectsCredentialWithoutLeak(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		_, raw, _ := ws.ReadMessage()
		var f frame
		_ = json.Unmarshal(raw, &f)
		_ = ack(ws, f.Headers.RequestID, 853000)
		_, _, _ = ws.ReadMessage()
	}))
	defer server.Close()
	c, _ := New(testConfig(t, "http://127.0.0.1:1"), nil)
	attachFixture(c, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := c.Run(ctx)
	if !errors.Is(err, ErrSubscription) || strings.Contains(err.Error(), c.config.BotSecret) {
		t.Fatalf("subscription error: %v", err)
	}
}

func TestReplyTimeoutIsUnknownAndNotResent(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var f frame
			_ = json.Unmarshal(raw, &f)
			if f.Command == "aibot_subscribe" {
				_ = ack(ws, f.Headers.RequestID, 0)
			} else if f.Command == "aibot_respond_msg" {
				count.Add(1)
			}
		}
	}))
	defer server.Close()
	c, _ := New(testConfig(t, "http://127.0.0.1:1"), nil)
	attachFixture(c, server.URL)
	c.ackTimeout = 80 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() { cancel(); <-done }()
	awaitStatus(t, c, "subscribed")
	payload := `{"bot_id":"bot-test","request_id":"request","conversation_id":"user","content":"reply","delivery_id":"one"}`
	for _, expected := range []int{502, 409} {
		req := httptest.NewRequest("POST", "/reply", strings.NewReader(payload))
		req.Header.Set(bridgeHeader, c.config.BridgeToken)
		response := httptest.NewRecorder()
		c.ServeHTTP(response, req)
		if response.Code != expected {
			t.Fatalf("reply status=%d want %d", response.Code, expected)
		}
	}
	if count.Load() != 1 {
		t.Fatal("unknown reply was retransmitted")
	}
}

func TestGatewayRetryPreservesPayloadAndCancel(t *testing.T) {
	var first []byte
	attempts := make(chan int, 3)
	var n atomic.Int32
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		attempt := int(n.Add(1))
		if attempt == 1 {
			first = raw
			w.WriteHeader(503)
		} else {
			if !bytes.Equal(first, raw) {
				t.Error("retry payload changed")
			}
			w.WriteHeader(200)
		}
		attempts <- attempt
	}))
	defer gateway.Close()
	c, _ := New(testConfig(t, gateway.URL), nil)
	s, err := openSpool(c.config.StateDir, c.routeHash)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c.spool = s
	if err = s.enqueue("message-1", incoming(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.forwardLoop(ctx) }()
	defer cancel()
	for range 2 {
		select {
		case <-attempts:
		case <-time.After(3 * time.Second):
			t.Fatal("retry missing")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forward loop did not cancel")
	}
}

func TestConnectorAcceptsTenantBridgeTokenRange(t *testing.T) {
	for _, size := range []int{32, 256, 257, 4096} {
		config := testConfig(t, "http://127.0.0.1:1")
		config.BridgeToken = strings.Repeat("b", size)
		binding := tenant.ChannelBinding{Type: "wecom_bot", AccountID: config.BotID, Token: config.BridgeToken}
		if err := tenant.ValidateWeComBotBinding(binding); err != nil {
			t.Fatalf("tenant token length %d: %v", size, err)
		}
		if _, err := New(config, nil); err != nil {
			t.Fatalf("connector rejected admitted token length %d: %v", size, err)
		}
	}
}

func TestReplyProviderRateLimitCanRetrySameDelivery(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var f frame
			_ = json.Unmarshal(raw, &f)
			code := 0
			if f.Command == "aibot_respond_msg" && count.Add(1) == 1 {
				code = 45009 // Official API quota rejection: no message was accepted.
			}
			if ack(ws, f.Headers.RequestID, code) != nil {
				return
			}
		}
	}))
	defer server.Close()
	c, _ := New(testConfig(t, "http://127.0.0.1:1"), nil)
	attachFixture(c, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() { cancel(); <-done }()
	awaitStatus(t, c, "subscribed")
	bridge := httptest.NewServer(c)
	defer bridge.Close()
	sender := channel.NewWeComBotAdapter(bridge.URL)
	binding := &tenant.ChannelBinding{Type: "wecom_bot", AccountID: c.config.BotID, Token: c.config.BridgeToken}
	message := &channel.OutboundMessage{ConversationID: "user", ReplyToID: "request", Content: "reply", ContentType: "text", DeliveryID: "one"}
	err := sender.SendReply(ctx, binding, message)
	permanent, retryAfter := channel.DeliveryFailure(err)
	if err == nil || permanent || retryAfter <= 0 || channel.DeliveryOutcomeUnknown(err) {
		t.Fatalf("quota rejection must remain retryable with backoff: err=%v permanent=%v retry=%v", err, permanent, retryAfter)
	}
	if err := sender.SendReply(ctx, binding, message); err != nil {
		t.Fatalf("retry after quota recovery: %v", err)
	}
	if err := sender.SendReply(ctx, binding, message); err != nil || count.Load() != 2 {
		t.Fatalf("confirmed retry must be idempotent: err=%v sends=%d", err, count.Load())
	}
}

func TestConnectionReplacementStopsWithoutTakingBotBack(t *testing.T) {
	var subscriptions atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		_, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		var f frame
		_ = json.Unmarshal(raw, &f)
		subscriptions.Add(1)
		_ = ack(ws, f.Headers.RequestID, 0)
		_ = ws.WriteMessage(websocket.TextMessage, []byte(`{"cmd":"aibot_event_callback","headers":{"req_id":"event-1"},"body":{"aibotid":"bot-test","event":{"eventtype":"disconnected_event"}}}`))
		_, _, _ = ws.ReadMessage()
	}))
	defer server.Close()
	c, _ := New(testConfig(t, "http://127.0.0.1:1"), nil)
	attachFixture(c, server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Run(ctx); !errors.Is(err, ErrReplaced) || subscriptions.Load() != 1 {
		t.Fatalf("replacement must stop: err=%v subscriptions=%d", err, subscriptions.Load())
	}
	response := httptest.NewRecorder()
	c.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "replaced") {
		t.Fatalf("replacement readiness: %d %s", response.Code, response.Body.String())
	}
}

func TestSpoolDeliveryRecoveryPreservesUnknownAndSafeRetry(t *testing.T) {
	dir := t.TempDir()
	s, err := openSpool(dir, "binding")
	if err != nil {
		t.Fatal(err)
	}
	hash := dataHash([]byte("reply"))
	for _, id := range []string{"unknown", "quota", "confirmed"} {
		if status, err := s.beginDelivery(id, hash); err != nil || status != "new" {
			t.Fatalf("begin %s: %s %v", id, status, err)
		}
	}
	if err := s.finishDelivery("quota", "not_sent"); err != nil {
		t.Fatal(err)
	}
	if err := s.finishDelivery("confirmed", "sent"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openSpool(dir, "binding")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, test := range []struct{ id, want string }{{"unknown", "started"}, {"quota", "new"}, {"confirmed", "sent"}} {
		if status, err := s.beginDelivery(test.id, hash); err != nil || status != test.want {
			t.Fatalf("recover %s: status=%s want=%s err=%v", test.id, status, test.want, err)
		}
	}
	if status, err := s.beginDelivery("confirmed", dataHash([]byte("changed reply"))); err != nil || status != "conflict" {
		t.Fatalf("reused identity changed payload: %s %v", status, err)
	}
}

func TestGatewayPermanentFailureRetainsDurableIngress(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer gateway.Close()
	c, _ := New(testConfig(t, gateway.URL), nil)
	s, err := openSpool(c.config.StateDir, c.routeHash)
	if err != nil {
		t.Fatal(err)
	}
	c.spool = s
	if err := s.enqueue("message-1", incoming(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	before := s.next()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.forwardLoop(ctx); !errors.Is(err, ErrGateway) {
		t.Fatalf("permanent gateway rejection: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openSpool(c.config.StateDir, c.routeHash)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after := s.next()
	if after == nil || !bytes.Equal(before.Payload, after.Payload) {
		t.Fatal("rejected ingress was lost or changed across restart")
	}
}
