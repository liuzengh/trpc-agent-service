package wecomws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// fakePlatform is an in-process WeCom smart-bot endpoint (an httptest server
// dialed over ws://): it validates the subscribe handshake, answers pings,
// records respond frames and can push callbacks or kick the connection —
// the platform behaviors the client must survive.
type fakePlatform struct {
	t      *testing.T
	srv    *httptest.Server
	botID  string
	secret string

	// mutePongs stops the heartbeat answers, simulating a half-dead link.
	mutePongs atomic.Bool

	// holdAcks stops the subscribe handshake before the ack: the client waits
	// while the platform can push callbacks racing ahead of the ack.
	holdAcks atomic.Bool

	// badAckOnce answers the next successful subscribe with a corrupt body.
	badAckOnce atomic.Bool

	// closeOnAccept tears down every accepted connection immediately.
	closeOnAccept atomic.Bool

	accepts atomic.Int64 // websocket connections accepted (all states)

	mu        sync.Mutex
	conns     []*fakeConn
	totalSubs atomic.Int64 // subscribe frames received (all connections)
	totalAcks atomic.Int64 // successful (errcode 0) subscribe acks sent
}

type fakeConn struct {
	t  *testing.T
	ws *websocket.Conn

	push chan envelope // platform-initiated frames (callbacks, events)
	kick chan struct{} // closed to terminate the connection
	done chan struct{} // closed when the read loop exits

	wmu sync.Mutex // serializes frame writes

	written atomic.Int64 // frames successfully written on this connection

	mu       sync.Mutex
	subs     []envelope // received aibot_subscribe frames
	heldSubs []envelope // subscribe frames whose ack is withheld (holdAcks)
	responds []envelope // received aibot_respond_msg frames
	pings    int        // received pings

	pingReqIDs []string // req_id of every received heartbeat
}

func newFakePlatform(t *testing.T, botID, secret string) *fakePlatform {
	t.Helper()
	f := &fakePlatform{t: t, botID: botID, secret: secret}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakePlatform) addr() string { return "ws" + strings.TrimPrefix(f.srv.URL, "http") }

func (f *fakePlatform) serveHTTP(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	f.accepts.Add(1)
	if f.closeOnAccept.Load() {
		_ = ws.CloseNow()
		return
	}
	fc := &fakeConn{
		t:    f.t,
		ws:   ws,
		push: make(chan envelope, 16),
		kick: make(chan struct{}),
		done: make(chan struct{}),
	}
	f.mu.Lock()
	f.conns = append(f.conns, fc)
	f.mu.Unlock()
	fc.run(f.botID, f.secret, f)
}

func (f *fakePlatform) connCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

// waitConn waits for the n-th accepted connection (1-based) and returns it.
func (f *fakePlatform) waitConn(t *testing.T, n int) *fakeConn {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		conns := f.conns
		f.mu.Unlock()
		if len(conns) >= n {
			return conns[n-1]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("want %d accepted connections, got %d", n, f.connCount())
	return nil
}

// run serves one accepted connection until it dies: platform-initiated
// frames and the kick signal are forwarded by a writer goroutine, client
// frames are consumed by the read loop below.
func (c *fakeConn) run(botID, secret string, f *fakePlatform) {
	defer func() {
		_ = c.ws.CloseNow()
		close(c.done)
	}()

	write := func(env envelope) error {
		data, err := json.Marshal(env)
		if err != nil {
			return err
		}
		// writeBytes already counts the frame in written; do not add again.
		return c.writeBytes(data, websocket.MessageText)
	}

	go func() {
		for {
			select {
			case env := <-c.push:
				if write(env) != nil {
					return
				}
			case <-c.kick:
				_ = c.ws.Close(websocket.StatusGoingAway, "kicked by newer subscription")
				return
			case <-c.done:
				return
			}
		}
	}()

	for {
		typ, data, err := c.ws.Read(context.Background())
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var env envelope
		if json.Unmarshal(data, &env) != nil || env.Cmd == "" {
			continue
		}
		switch env.Cmd {
		case cmdSubscribe:
			f.totalSubs.Add(1)
			c.mu.Lock()
			c.subs = append(c.subs, env)
			held := f.holdAcks.Load()
			if held {
				c.heldSubs = append(c.heldSubs, env)
			}
			c.mu.Unlock()
			if held {
				continue // the ack is withheld until releaseAcks
			}
			var body struct {
				BotID  string `json:"bot_id"`
				Secret string `json:"secret"`
			}
			_ = json.Unmarshal(env.Body, &body)
			if body.BotID != botID || body.Secret != secret {
				_ = write(envelope{
					Headers: env.Headers,
					ErrCode: 40001,
					ErrMsg:  "invalid bot or secret",
				})
				continue
			}
			if f.badAckOnce.Swap(false) {
				// A corrupt ack body — a marshaling-valid envelope whose body
				// cannot unmarshal into the ack shape. The client must drop
				// the connection and resubscribe instead of trusting it.
				c.PushRaw(fmt.Sprintf(`{"headers":{"req_id":%q},"body":42}`, env.Headers.ReqID))
				continue
			}
			f.totalAcks.Add(1)
			_ = write(envelope{
				Headers: env.Headers,
				ErrMsg:  "ok",
			})
		case cmdPing:
			c.mu.Lock()
			c.pings++
			c.pingReqIDs = append(c.pingReqIDs, env.Headers.ReqID)
			c.mu.Unlock()
			if f.mutePongs.Load() {
				continue
			}
			// The real platform acks the heartbeat with a cmd-less frame
			// echoing the req_id; only a ping_-prefixed req_id gets one.
			_ = write(envelope{Headers: frameHeaders{ReqID: env.Headers.ReqID}, ErrMsg: "ok"})
		case cmdRespond:
			c.mu.Lock()
			c.responds = append(c.responds, env)
			c.mu.Unlock()
		}
	}
}

// Push queues one platform-initiated frame onto this connection.
func (c *fakeConn) Push(env envelope) {
	select {
	case c.push <- env:
	default:
		c.t.Errorf("push buffer full, frame dropped: %s", env.Cmd)
	}
}

// PushWait queues one platform-initiated frame, blocking until the connection
// writer has taken it — used to sequence many frames deterministically.
func (c *fakeConn) PushWait(env envelope) {
	select {
	case c.push <- env:
	case <-c.done:
		c.t.Errorf("push on dead connection dropped: %s", env.Cmd)
	}
}

// writeBytes writes one raw frame directly on the connection, bypassing the
// push channel's envelope encoder.
func (c *fakeConn) writeBytes(data []byte, typ websocket.MessageType) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.ws.Write(ctx, typ, data)
	if err == nil {
		c.written.Add(1)
	}
	return err
}

// PushRaw writes one arbitrary text frame (possibly invalid JSON) that the
// envelope encoder could not produce.
func (c *fakeConn) PushRaw(data string) {
	if err := c.writeBytes([]byte(data), websocket.MessageText); err != nil {
		c.t.Logf("push raw frame: %v", err)
	}
}

// PushBinary writes one binary frame — unrouteable per the protocol.
func (c *fakeConn) PushBinary(data []byte) {
	if err := c.writeBytes(data, websocket.MessageBinary); err != nil {
		c.t.Logf("push binary frame: %v", err)
	}
}

// releaseAcks answers every subscribe held back by the platform's holdAcks,
// counting them as successful acks.
func (c *fakeConn) releaseAcks() {
	c.mu.Lock()
	held := append([]envelope(nil), c.heldSubs...)
	c.heldSubs = nil
	c.mu.Unlock()
	for _, env := range held {
		data, err := json.Marshal(envelope{
			Headers: env.Headers,
			ErrMsg:  "ok",
		})
		if err != nil {
			return
		}
		if err := c.writeBytes(data, websocket.MessageText); err != nil {
			return
		}
	}
}

func (c *fakeConn) subFrames() []envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]envelope(nil), c.subs...)
}

func (c *fakeConn) respondFrames() []envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]envelope(nil), c.responds...)
}

func (c *fakeConn) pingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pings
}

func (c *fakeConn) pingIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.pingReqIDs...)
}

// staticRoutes serves a fixed binding list.
type staticRoutes []Binding

func (s staticRoutes) RoutesByChannel(context.Context, string) ([]Binding, error) { return s, nil }

// staticSecrets resolves every ref to one value.
type staticSecrets string

func (s staticSecrets) Resolve(context.Context, string) (string, error) { return string(s), nil }

// recordingHandler collects inbound messages and can fail the first n
// deliveries or report duplicates, driving the local retry paths.
type recordingHandler struct {
	mu    sync.Mutex
	msgs  []channels.InboundMessage
	fails int  // fail the first n Handle calls
	dups  bool // report ErrDuplicate after recording
}

func (h *recordingHandler) Handle(_ context.Context, m channels.InboundMessage) (channels.OutboundMessage, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, m)
	if h.fails > 0 {
		h.fails--
		return channels.OutboundMessage{}, errors.New("handler down")
	}
	if h.dups {
		return channels.OutboundMessage{}, channels.ErrDuplicate
	}
	return channels.OutboundMessage{}, nil
}

func (h *recordingHandler) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.msgs)
}

func (h *recordingHandler) last() channels.InboundMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.msgs[len(h.msgs)-1]
}

func (h *recordingHandler) all() []channels.InboundMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]channels.InboundMessage(nil), h.msgs...)
}

func (h *recordingHandler) heal() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fails = 0
}

func (h *recordingHandler) setDups(v bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dups = v
}

func mustBinding(t *testing.T, id, botID, secretRef string) Binding {
	t.Helper()
	raw, err := json.Marshal(bindingConfig{BotID: botID, SecretRef: secretRef})
	if err != nil {
		t.Fatal(err)
	}
	return Binding{ID: id, WebhookPath: "/wecomws/" + botID, Config: raw}
}

// newTestChannel starts a channel against the fake platform with every
// timing knob compressed to milliseconds (the resync tick is effectively
// disabled — Start's initial reconcile already ran).
func newTestChannel(t *testing.T, addr string, bnds []Binding, secretValue string, h channels.Handler, opts ...Option) *Channel {
	t.Helper()
	ch, err := New(staticSecrets(secretValue), append([]Option{WithAddr(addr), WithRoutes(staticRoutes(bnds))}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	ch.reconnectBase = 20 * time.Millisecond
	ch.reconnectCap = 200 * time.Millisecond
	ch.kickedBase = 20 * time.Millisecond
	ch.inboundRetryBase = 20 * time.Millisecond
	ch.inboundRetryCap = 200 * time.Millisecond
	ch.pingInterval = 60 * time.Millisecond
	ch.subscribeTimeout = 2 * time.Second
	ch.writeTimeout = time.Second
	ch.resyncInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = ch.Start(ctx, h) }()
	return ch
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 3s")
}

// TestSubscribeAuth: the credential handshake rides the first frame, is
// verified by the platform, and a rejection is retried on the backoff.
func TestSubscribeAuth(t *testing.T) {
	t.Run("valid credentials subscribe", func(t *testing.T) {
		f := newFakePlatform(t, "bot-1", "s3cr3t")
		newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "wecomws/bot-1/secret")}, "s3cr3t", &recordingHandler{})

		conn := f.waitConn(t, 1)
		waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

		subs := conn.subFrames()
		if len(subs) == 0 {
			t.Fatal("no subscribe frame received")
		}
		var body struct {
			BotID  string `json:"bot_id"`
			Secret string `json:"secret"`
		}
		if err := json.Unmarshal(subs[0].Body, &body); err != nil {
			t.Fatal(err)
		}
		if body.BotID != "bot-1" || body.Secret != "s3cr3t" {
			t.Fatalf("subscribe carried wrong credentials: bot=%q secret-set=%v", body.BotID, body.Secret != "")
		}
		if strings.Contains(f.srv.URL, "s3cr3t") {
			t.Fatal("the secret must ride the subscribe frame, not the URL/handshake")
		}

		// The connection stays up (heartbeat answered): no churn.
		time.Sleep(300 * time.Millisecond)
		if got := f.connCount(); got != 1 {
			t.Fatalf("subscribed connection must stay up, got %d connections", got)
		}
	})

	t.Run("rejected credentials keep retrying", func(t *testing.T) {
		f := newFakePlatform(t, "bot-1", "s3cr3t")
		newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "WRONG", &recordingHandler{})

		// Every subscribe is rejected (errcode != 0) and the client keeps
		// redialing on its backoff instead of giving up.
		waitFor(t, func() bool { return f.totalSubs.Load() >= 3 })
		if got := f.totalAcks.Load(); got != 0 {
			t.Fatalf("wrong credentials must never ack, got %d acks", got)
		}
		if f.connCount() < 2 {
			t.Fatal("a rejected client must redial")
		}
	})
}

// TestInboundNormalized: a message callback is normalized into the platform
// message shape — session key by chat type, the binding stamped, and the
// callback frame's req_id carried as the reply token.
func TestInboundNormalized(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &recordingHandler{}
	newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "s3cr3t", h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	conn.Push(envelope{
		Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "req-cb-1"},
		Body: json.RawMessage(`{"msgid":"m1","aibot_id":"bot-1","chattype":"single",
			"from":{"userid":"u1"},"msgtype":"text","text":{"content":"你好"}}`),
	})
	waitFor(t, func() bool { return h.calls() >= 1 })
	m := h.last()
	epoch, reqID := parseReplyToken(m.ReplyToken)
	if m.Channel != "wecomws" || m.MsgID != "m1" || m.UserID != "u1" || m.Text != "你好" ||
		m.SessionKey != "dm:wecomws:u1" || m.ChatID != "" || m.Type != channels.TypeText ||
		m.BindingID != "b1" || m.WebhookPath != "/wecomws/bot-1" ||
		epoch == 0 || reqID != "req-cb-1" {
		t.Fatalf("normalized direct message: %+v", m)
	}

	conn.Push(envelope{
		Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "req-cb-2"},
		Body: json.RawMessage(`{"msgid":"m2","chattype":"group","chatid":"g9",
			"from":{"userid":"u2"},"msgtype":"text","text":{"content":"hi"}}`),
	})
	waitFor(t, func() bool { return h.calls() >= 2 })
	m = h.last()
	if m.SessionKey != "group:wecomws:g9" || m.ChatID != "g9" {
		t.Fatalf("group message must key the session by chat id: %+v", m)
	}

	// Media degrades to a placeholder text instead of being dropped.
	conn.Push(envelope{
		Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "req-cb-3"},
		Body: json.RawMessage(`{"msgid":"m3","chattype":"single","from":{"userid":"u1"},"msgtype":"image"}`),
	})
	waitFor(t, func() bool { return h.calls() >= 3 })
	if m := h.last(); m.Type != channels.TypeText || !strings.Contains(m.Text, "图片") {
		t.Fatalf("media message must degrade to a placeholder: %+v", m)
	}
}

// TestHandleRetry: WS inbound has no platform redelivery, so a failing
// Handle retries locally until it lands (or the message reports duplicate);
// the retry lives on the dispatch worker, so the read loop keeps draining
// pongs and the heartbeat keeps running.
func TestHandleRetry(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &recordingHandler{fails: 2}
	newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "s3cr3t", h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	conn.Push(envelope{
		Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "req-cb-1"},
		Body: json.RawMessage(`{"msgid":"m1","chattype":"single","from":{"userid":"u1"},"msgtype":"text","text":{"content":"hi"}}`),
	})
	waitFor(t, func() bool { return h.calls() >= 3 })
	msgs := h.all()
	if len(msgs) != 3 {
		t.Fatalf("want 3 deliveries (2 failures + 1 success), got %d", len(msgs))
	}
	for i, m := range msgs {
		if m.MsgID != "m1" {
			t.Fatalf("delivery %d: message lost/changed: %+v", i, m)
		}
	}

	// The read loop kept running during the retries: pings flowed and no
	// reconnect happened.
	if got := f.connCount(); got != 1 {
		t.Fatalf("retries must not tear the connection down, got %d connections", got)
	}
	waitFor(t, func() bool { return conn.pingCount() >= 1 })

	// ErrDuplicate is a success outcome: no retry.
	h.setDups(true)
	conn.Push(envelope{
		Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "req-cb-2"},
		Body: json.RawMessage(`{"msgid":"m2","chattype":"single","from":{"userid":"u1"},"msgtype":"text","text":{"content":"hi"}}`),
	})
	waitFor(t, func() bool { return h.calls() >= 4 })
	time.Sleep(200 * time.Millisecond)
	if got := h.calls(); got != 4 {
		t.Fatalf("duplicate must not retry, deliveries=%d", got)
	}
}

// TestHandleRetryKeepsConnectionAlive: a Handler that stays down does not
// starve the read loop — the retry loop lives on the serial dispatch worker
// while pongs keep flowing, so the heartbeat does not kill a healthy socket.
// The retry count is bounded: once the cap is spent the message is dropped
// loudly instead of parking every later message on the connection behind it
// (and the bot keeps serving once the handler heals).
func TestHandleRetryKeepsConnectionAlive(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &recordingHandler{fails: 1 << 20} // down for most of the test
	newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "s3cr3t", h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	conn.Push(envelope{
		Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "req-cb-1"},
		Body: json.RawMessage(`{"msgid":"m1","chattype":"single","from":{"userid":"u1"},"msgtype":"text","text":{"content":"hi"}}`),
	})
	// The cap is spent: exactly the bounded attempts, then the drop.
	waitFor(t, func() bool { return h.calls() >= inboundRetryMax })
	time.Sleep(100 * time.Millisecond)
	if got := h.calls(); got != inboundRetryMax {
		t.Fatalf("retries must stop at the cap, got %d deliveries", got)
	}

	// The connection stayed healthy throughout: pongs drained, no reconnect.
	waitFor(t, func() bool { return conn.pingCount() >= 3 })
	if got := f.connCount(); got != 1 {
		t.Fatalf("a retrying handler must not tear down a healthy connection, got %d connections", got)
	}

	// The bot is not wedged: once the handler heals, the next message lands.
	h.heal()
	conn.Push(envelope{
		Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "req-cb-2"},
		Body: json.RawMessage(`{"msgid":"m2","chattype":"single","from":{"userid":"u1"},"msgtype":"text","text":{"content":"hi"}}`),
	})
	waitFor(t, func() bool { return h.calls() >= inboundRetryMax+1 })
	if m := h.last(); m.MsgID != "m2" {
		t.Fatalf("post-drop message lost/changed: %+v", m)
	}
	if got := f.connCount(); got != 1 {
		t.Fatalf("healing must not reconnect either, got %d connections", got)
	}
}

// TestDispatchQueueOverflowFallsBackToHeartbeatKill: when the worker is
// wedged and the bounded dispatch queue fills up, the read loop blocks on the
// enqueue and pongs starve again — the heartbeat kill stays the escape hatch
// and run() reconnects instead of babysitting the backlog forever.
func TestDispatchQueueOverflowFallsBackToHeartbeatKill(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &recordingHandler{fails: 1 << 20} // down for the whole test
	newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "s3cr3t", h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	for i := 0; i < inboundQueueMax+2; i++ {
		conn.PushWait(envelope{
			Cmd:     cmdMsgCallback,
			Headers: frameHeaders{ReqID: fmt.Sprintf("req-cb-%d", i)},
			Body: json.RawMessage(fmt.Sprintf(
				`{"msgid":"m%d","chattype":"single","from":{"userid":"u1"},"msgtype":"text","text":{"content":"hi"}}`, i)),
		})
	}
	// The queue fills, the read loop parks on the enqueue, the heartbeat
	// gives up within a couple of intervals and a fresh connection subscribes.
	f.waitConn(t, 2)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 2 })
}

// TestHeartbeatTimeout: a ping that never gets its pong kills the connection
// after one interval and the client reconnects on its own.
func TestHeartbeatTimeout(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	f.mutePongs.Store(true)
	newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "s3cr3t", &recordingHandler{})

	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	// The first unanswered ping schedules the kill on the next tick.
	waitFor(t, func() bool { return conn.pingCount() >= 1 })
	f.waitConn(t, 2)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 2 })
}

// TestDisconnectedEvent: the platform's disconnected_event means this
// connection was kicked by a newer subscription — the client reconnects and
// resubscribes, on both accepted event nestings.
func TestDisconnectedEvent(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "ref")}, "s3cr3t", &recordingHandler{})
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	conn.Push(envelope{
		Cmd: cmdEventCallback, Headers: frameHeaders{ReqID: "ev-1"},
		Body: json.RawMessage(`{"eventtype":"disconnected_event"}`),
	})
	conn2 := f.waitConn(t, 2)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 2 })

	// The nested event shape (body.event.eventtype) kicks just the same.
	conn2.Push(envelope{
		Cmd: cmdEventCallback, Headers: frameHeaders{ReqID: "ev-2"},
		Body: json.RawMessage(`{"event":{"eventtype":"disconnected_event"}}`),
	})
	conn3 := f.waitConn(t, 3)
	waitFor(t, func() bool { return conn3.subFrames() != nil && f.totalAcks.Load() >= 3 })
}

// TestSleep covers the ctx-aware wait shared with the leader loop: a done
// context returns false immediately, a short wait completes with true, and
// a cancellation mid-wait returns early.
func TestSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, time.Hour) {
		t.Error("Sleep on a canceled context must return false")
	}

	start := time.Now()
	if !Sleep(context.Background(), 10*time.Millisecond) {
		t.Error("Sleep on a live context must return true")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("10ms sleep took %v", elapsed)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	timer := time.AfterFunc(20*time.Millisecond, cancel2)
	defer timer.Stop()
	start = time.Now()
	if Sleep(ctx2, 10*time.Second) {
		t.Error("Sleep must return false when the context is canceled mid-wait")
	}
	if elapsed := time.Since(start); elapsed >= 5*time.Second {
		t.Errorf("mid-wait cancellation took %v, must return early", elapsed)
	}
}

// The platform only answers a heartbeat whose req_id follows the SDK
// convention of starting with the cmd name ("ping_..."): any other spelling
// is met with silence, and the client's own missed-pong watchdog then kills a
// perfectly healthy connection roughly every two ping intervals.
func TestHeartbeatReqIDFollowsPlatformConvention(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	newTestChannel(t, f.addr(), []Binding{mustBinding(t, "b1", "bot-1", "s3cr3t")}, "s3cr3t", &recordingHandler{})

	conn := f.waitConn(t, 1)
	// A couple of heartbeat intervals must go by without the connection
	// being torn down, and every ping must carry a ping_-prefixed req_id.
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) && conn.pingCount() < 2 {
		time.Sleep(20 * time.Millisecond)
	}
	ids := conn.pingIDs()
	if len(ids) < 1 {
		t.Fatal("no heartbeat received")
	}
	for _, id := range ids {
		if !strings.HasPrefix(id, "ping_") {
			t.Errorf("heartbeat req_id %q must start with %q", id, "ping_")
		}
	}
	if f.connCount() != 1 {
		t.Fatalf("answered heartbeats must keep the connection up, got %d connections", f.connCount())
	}
}
