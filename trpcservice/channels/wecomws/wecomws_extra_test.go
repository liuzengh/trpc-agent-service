package wecomws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// newIdleChannel builds a channel wired to addr without starting it; every
// timing knob is compressed to milliseconds. Options passed here are applied
// last, so they win over the compressed defaults.
func newIdleChannel(t *testing.T, addr string, routes RoutesProvider, secret config.SecretResolver, opts ...Option) *Channel {
	t.Helper()
	ch, err := New(secret, WithAddr(addr), WithRoutes(routes))
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
	for _, opt := range opts {
		opt(ch)
	}
	return ch
}

// startOne runs a single-binding channel against f and returns its connection
// handle plus a cancel for mid-test teardown (cleanup cancels and drains too).
func startOne(t *testing.T, f *fakePlatform, h channels.Handler, opts ...Option) (*Channel, *botConn, context.CancelFunc) {
	t.Helper()
	ch := newIdleChannel(t, f.addr(), staticRoutes{mustBinding(t, "b1", "bot-1", "ref")}, staticSecrets("s3cr3t"), opts...)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		if bc := ch.mgr.byBinding("b1"); bc != nil {
			select {
			case <-bc.done:
			case <-time.After(3 * time.Second):
				t.Error("connection run loop did not exit after cancel")
			}
		}
	})
	if err := ch.mgr.reconcile(ctx, h); err != nil {
		t.Fatal(err)
	}
	bc := ch.mgr.byBinding("b1")
	if bc == nil {
		t.Fatal("a servable binding must start a connection")
	}
	return ch, bc, cancel
}

func msgEnvelope(reqID, msgID string) envelope {
	return envelope{
		Cmd:     cmdMsgCallback,
		Headers: frameHeaders{ReqID: reqID},
		Body: json.RawMessage(fmt.Sprintf(
			`{"msgid":%q,"chattype":"single","from":{"userid":"u1"},"msgtype":"text","text":{"content":"hi"}}`, msgID)),
	}
}

// failingSecrets fails every resolve, simulating a dead secret store.
type failingSecrets struct{}

func (failingSecrets) Resolve(context.Context, string) (string, error) {
	return "", errors.New("secret store down")
}

// switchRoutes serves a mutable binding list / failure, driving the reconcile
// lifecycle.
type switchRoutes struct {
	mu    sync.Mutex
	b     []Binding
	err   error
	calls atomic.Int64
}

func (s *switchRoutes) RoutesByChannel(context.Context, string) ([]Binding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls.Add(1)
	return s.b, s.err
}

func (s *switchRoutes) set(b []Binding) {
	s.mu.Lock()
	s.b = b
	s.err = nil
	s.mu.Unlock()
}

func (s *switchRoutes) fail(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *switchRoutes) snapshots() int64 { return s.calls.Load() }

// gatedHandler blocks each Handle until released, so a test can race the
// delivery against channel cancellation deterministically.
type gatedHandler struct {
	calls   chan struct{}
	release chan error
	handled atomic.Int64
}

func (h *gatedHandler) Handle(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
	h.handled.Add(1)
	h.calls <- struct{}{}
	return channels.OutboundMessage{}, <-h.release
}

func TestOptionsApply(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("a secret resolver is required")
	}

	ch, err := New(staticSecrets("s"),
		WithAddr("ws://example.invalid/ws"),
		WithRoutes(staticRoutes{}),
		WithPingInterval(time.Second),
		WithSegmentBytes(128),
		WithResyncInterval(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if ch.pingInterval != time.Second || ch.segment != 128 || ch.resyncInterval != 2*time.Second {
		t.Fatalf("options must take effect: ping=%s segment=%d resync=%s",
			ch.pingInterval, ch.segment, ch.resyncInterval)
	}

	// Non-positive values fall back to the platform defaults.
	ch, err = New(staticSecrets("s"), WithPingInterval(0), WithSegmentBytes(-1), WithResyncInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	if ch.pingInterval != 30*time.Second || ch.segment != 2048 || ch.resyncInterval != 15*time.Second {
		t.Fatalf("non-positive options must restore defaults: ping=%s segment=%d resync=%s",
			ch.pingInterval, ch.segment, ch.resyncInterval)
	}
	if ch.Name() != "wecomws" {
		t.Fatalf("unexpected channel name %q", ch.Name())
	}

	// Callbacks arrive on the long connection: no HTTP routes to mount.
	mux := http.NewServeMux()
	ch.RegisterRoutes(mux, channels.HandlerFunc(func(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
		t.Error("no HTTP route may be mounted")
		return channels.OutboundMessage{}, nil
	}))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/wecomws/anything", strings.NewReader("{}")))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("wecomws must mount no HTTP routes (404), got %d", rec.Code)
	}
}

func TestStartRequiresConfiguration(t *testing.T) {
	ch, err := New(staticSecrets("s"), WithRoutes(staticRoutes{}))
	if err != nil {
		t.Fatal(err)
	}
	err = ch.Start(t.Context(), &recordingHandler{})
	if err == nil || !strings.Contains(err.Error(), "addr is required") {
		t.Fatalf("Start without an addr must refuse to run, got %v", err)
	}

	ch, err = New(staticSecrets("s"), WithAddr("ws://127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	err = ch.Start(t.Context(), &recordingHandler{})
	if err == nil || !strings.Contains(err.Error(), "routes provider is required") {
		t.Fatalf("Start without routes must refuse to run, got %v", err)
	}
}

// A failing binding snapshot must not abort the channel: every resync retries
// it, and ctx cancel shuts the channel down cleanly with no connection left.
func TestStartResyncFailureThenCleanShutdown(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	routes := &switchRoutes{}
	routes.fail(errors.New("routes store down"))
	ch := newIdleChannel(t, f.addr(), routes, staticSecrets("s3cr3t"), WithResyncInterval(10*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Start(ctx, &recordingHandler{}) }()

	// The initial failure is tolerated and the resync tick keeps retrying.
	waitFor(t, func() bool { return routes.snapshots() >= 2 })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start must return nil on shutdown, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Start must return after ctx cancel")
	}
	if got := f.connCount(); got != 0 {
		t.Fatalf("shutdown must leave no connection behind, got %d", got)
	}
}

// reconcile converges the live connections onto the binding snapshot: failing
// snapshots abort, broken rows are skipped, resyncs keep live connections, and
// removals are canceled and drained before reconcile returns.
func TestReconcileLifecycle(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	routes := &switchRoutes{}
	ch := newIdleChannel(t, f.addr(), routes, staticSecrets("s3cr3t"))
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		ch.mgr.stopAll()
	}()

	// A failing snapshot aborts the reconcile; nothing is dialed.
	routes.fail(errors.New("routes store down"))
	if err := ch.mgr.reconcile(ctx, &recordingHandler{}); err == nil {
		t.Fatal("a routes failure must propagate to the caller")
	}
	if got := f.connCount(); got != 0 {
		t.Fatalf("no connection may be dialed on a failed snapshot, got %d", got)
	}

	// A binding whose config cannot be parsed is skipped, not fatal.
	routes.set([]Binding{{ID: "bad", WebhookPath: "/wecomws/bad", Config: json.RawMessage(`{oops`)}})
	if err := ch.mgr.reconcile(ctx, &recordingHandler{}); err != nil {
		t.Fatal(err)
	}
	if got := ch.mgr.byBinding("bad"); got != nil {
		t.Fatalf("a malformed binding must be skipped, got %+v", got)
	}

	// A servable binding starts one connection; a resync with the same
	// snapshot must not restart it.
	routes.set([]Binding{mustBinding(t, "b1", "bot-1", "ref")})
	if err := ch.mgr.reconcile(ctx, &recordingHandler{}); err != nil {
		t.Fatal(err)
	}
	bc := ch.mgr.byBinding("b1")
	if bc == nil {
		t.Fatal("a servable binding must start a connection")
	}
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })
	if err := ch.mgr.reconcile(ctx, &recordingHandler{}); err != nil {
		t.Fatal(err)
	}
	if ch.mgr.byBinding("b1") != bc {
		t.Fatal("a resync must keep the live connection")
	}

	// A binding that disappeared is canceled and drained before reconcile
	// returns.
	routes.set(nil)
	if err := ch.mgr.reconcile(ctx, &recordingHandler{}); err != nil {
		t.Fatal(err)
	}
	if got := ch.mgr.byBinding("b1"); got != nil {
		t.Fatal("a removed binding must be dropped")
	}
	select {
	case <-bc.done:
	case <-time.After(3 * time.Second):
		t.Fatal("the removed connection must be drained before reconcile returns")
	}
	select {
	case <-conn.done:
	case <-time.After(3 * time.Second):
		t.Fatal("the platform side must observe the disconnect")
	}
}

// A bot whose secret cannot be resolved must keep retrying without ever
// dialing the platform.
func TestSecretResolveFailureKeepsRetrying(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	routes := &switchRoutes{}
	ch := newIdleChannel(t, f.addr(), routes, failingSecrets{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); ch.mgr.stopAll() })

	// The snapshot starts the connection loop, which cannot resolve the bot
	// secret: it retries on its backoff without ever dialing.
	routes.set([]Binding{mustBinding(t, "b1", "bot-1", "ref")})
	if err := ch.mgr.reconcile(ctx, &recordingHandler{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := f.accepts.Load(); n != 0 {
		t.Fatalf("a bot whose secret cannot be resolved must never dial, got %d dials", n)
	}
}

func TestDialFailureKeepsRetrying(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	addr := f.addr()
	f.srv.Close() // the platform is down from here on

	ch := newIdleChannel(t, addr, staticRoutes{mustBinding(t, "b1", "bot-1", "ref")}, staticSecrets("s3cr3t"))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); ch.mgr.stopAll() })
	if err := ch.mgr.reconcile(ctx, &recordingHandler{}); err != nil {
		t.Fatal(err)
	}

	// The client keeps redialing on its backoff; nothing can subscribe.
	time.Sleep(200 * time.Millisecond)
	if n := f.totalSubs.Load(); n != 0 {
		t.Fatalf("a down platform must never be subscribed, got %d subscribes", n)
	}
}

// A platform that drops every connection at accept leaves the client
// retrying: the subscribe write or the ack wait fails, never a successful
// handshake.
func TestCloseOnAcceptKeepsRetrying(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	f.closeOnAccept.Store(true)
	ch := newIdleChannel(t, f.addr(), staticRoutes{mustBinding(t, "b1", "bot-1", "ref")}, staticSecrets("s3cr3t"))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); ch.mgr.stopAll() })
	if err := ch.mgr.reconcile(ctx, &recordingHandler{}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return f.accepts.Load() >= 3 })
	if n := f.totalAcks.Load(); n != 0 {
		t.Fatalf("a connection dropped at accept must never ack, got %d acks", n)
	}
	if n := f.connCount(); n != 0 {
		t.Fatalf("dropped connections must not be tracked, got %d", n)
	}
}

// Callbacks racing ahead of the subscribe ack buffer (up to a cap) and
// dispatch after the ack; unrouteable pre-ack frames are ignored.
func TestPreAckBuffering(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	f.holdAcks.Store(true)
	h := &recordingHandler{}
	startOne(t, f, h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return len(conn.subFrames()) >= 1 })

	// Frames racing ahead of the ack that cannot be routed at all are skipped
	// while waiting for the ack; an unknown command is ignored likewise.
	conn.PushRaw(`not-json`)
	conn.PushWait(envelope{Cmd: "bogus_cmd", Headers: frameHeaders{ReqID: "pre-x"}})
	for i := 0; i < pendingBufferMax+2; i++ {
		conn.PushWait(msgEnvelope(fmt.Sprintf("pre-%d", i), fmt.Sprintf("m%d", i)))
	}
	// Let the platform finish writing every pre-ack frame (1 raw + 1 unknown
	// cmd + pendingBufferMax+2 callbacks) so the withheld ack lands after all
	// of them on the wire — written counts completed socket writes, so only
	// the full count proves the writer queue is drained and the ack released
	// below cannot overtake the last callback.
	waitFor(t, func() bool { return conn.written.Load() >= pendingBufferMax+4 })
	f.holdAcks.Store(false)
	conn.releaseAcks()

	waitFor(t, func() bool { return h.calls() >= pendingBufferMax })
	time.Sleep(150 * time.Millisecond)
	if got := h.calls(); got != pendingBufferMax {
		t.Fatalf("exactly the buffered %d callbacks must dispatch, got %d", pendingBufferMax, got)
	}
	if last := h.last(); last.MsgID != fmt.Sprintf("m%d", pendingBufferMax-1) {
		t.Fatalf("the newest buffered callback must dispatch last, got %q", last.MsgID)
	} else if _, reqID := parseReplyToken(last.ReplyToken); reqID != fmt.Sprintf("pre-%d", pendingBufferMax-1) {
		t.Fatalf("buffered callbacks must keep their req_id: %+v", last)
	}
}

// A disconnected_event racing ahead of the ack is still a kick: the
// connection is torn down and resubscribes.
func TestPreAckKickedEvent(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	f.holdAcks.Store(true)
	startOne(t, f, &recordingHandler{})
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return len(conn.subFrames()) >= 1 })

	conn.PushWait(envelope{
		Cmd:     cmdEventCallback,
		Headers: frameHeaders{ReqID: "ev-pre"},
		Body:    json.RawMessage(`{"eventtype":"disconnected_event"}`),
	})
	// Make sure the event is on the wire before the withheld ack.
	waitFor(t, func() bool { return conn.written.Load() >= 1 })
	f.holdAcks.Store(false)
	conn.releaseAcks()

	conn2 := f.waitConn(t, 2)
	waitFor(t, func() bool { return len(conn2.subFrames()) >= 1 })
}

// A corrupt subscribe ack body tears the connection down and the retry
// resubscribes successfully.
func TestSubscribeAckBadBody(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	f.badAckOnce.Store(true)
	startOne(t, f, &recordingHandler{})

	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return len(conn.subFrames()) >= 1 })
	conn2 := f.waitConn(t, 2)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })
	_ = conn2
}

// Frames that cannot be routed (binary, unparsable, missing cmd, unknown
// command, unparsable event body) are logged and skipped; the connection and
// the heartbeat survive all of them.
func TestPostAckFramesIgnored(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &recordingHandler{}
	startOne(t, f, h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })
	base := conn.pingCount()

	conn.PushBinary([]byte{0x00, 0x01})                      // binary frame
	conn.PushRaw(`not-json`)                                 // unparsable frame
	conn.PushRaw(`{"headers":{"req_id":"x"}}`)               // missing cmd
	conn.Push(envelope{Cmd: "mystery_cmd"})                  // unknown command
	conn.PushRaw(`{"cmd":"aibot_event_callback","body":42}`) // unparsable event body
	for _, body := range []string{
		`{"eventtype":"enter_chat"}`,
		`{"eventtype":"template_card_event"}`,
		`{"eventtype":"card_landing"}`,
	} {
		conn.Push(envelope{Cmd: cmdEventCallback, Body: json.RawMessage(body)})
	}

	// The heartbeat keeps flowing on the untouched connection.
	waitFor(t, func() bool { return conn.pingCount() > base+2 })
	if got := f.connCount(); got != 1 {
		t.Fatalf("skipped frames must not tear the connection down, got %d connections", got)
	}
	if got := h.calls(); got != 0 {
		t.Fatalf("no skipped frame may enter the pipeline, got %d deliveries", got)
	}
}

// Callbacks that cannot be normalized are dropped locally (no retry storm, no
// reconnect): WS inbound has no platform redelivery, so a broken frame is
// logged and forgotten.
func TestNormalizeDropsBadCallbacks(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &recordingHandler{}
	startOne(t, f, h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	conn.Push(envelope{Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "r1"}, Body: json.RawMessage(`42`)})
	conn.Push(envelope{Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "r2"}, Body: json.RawMessage(`{"from":{"userid":"u1"}}`)})
	conn.Push(envelope{Cmd: cmdMsgCallback, Headers: frameHeaders{ReqID: "r3"}, Body: json.RawMessage(`{"msgid":"m1"}`)})

	time.Sleep(150 * time.Millisecond)
	if got := h.calls(); got != 0 {
		t.Fatalf("bad callbacks must be dropped, got %d deliveries", got)
	}
	if got := f.connCount(); got != 1 {
		t.Fatalf("drops must not tear the connection down, got %d connections", got)
	}
}

// Media msgtypes outside the known labels degrade to a placeholder naming the
// raw msgtype instead of being dropped.
func TestUnknownMediaDegradesToRawType(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &recordingHandler{}
	startOne(t, f, h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	conn.Push(envelope{
		Cmd:     cmdMsgCallback,
		Headers: frameHeaders{ReqID: "req-media"},
		Body:    json.RawMessage(`{"msgid":"m9","chattype":"single","from":{"userid":"u1"},"msgtype":"location"}`),
	})
	waitFor(t, func() bool { return h.calls() >= 1 })
	if m := h.last(); !strings.Contains(m.Text, "[location]") {
		t.Fatalf("unknown media must degrade to its raw msgtype: %+v", m)
	}
}

// A canceled context cuts the inbound retry short: the blocked delivery
// returns instead of spinning the backoff.
func TestHandleRetryStopsOnCancel(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &gatedHandler{calls: make(chan struct{}, 4), release: make(chan error, 4)}
	_, bc, cancel := startOne(t, f, h)
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	conn.Push(msgEnvelope("req-1", "m1"))
	<-h.calls // the delivery is in flight, blocked in Handle
	cancel()  // the retry must notice the canceled context, not spin
	h.release <- errors.New("handler down")

	select {
	case <-bc.done:
	case <-time.After(3 * time.Second):
		t.Fatal("a canceled retry must return instead of sleeping the backoff")
	}
}

// A failing delivery waits out its backoff sleep; canceling the channel cuts
// the wait short.
func TestHandleRetrySleepInterrupted(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	h := &gatedHandler{calls: make(chan struct{}, 4), release: make(chan error, 4)}
	ch, bc, cancel := startOne(t, f, h)
	ch.inboundRetryBase = 5 * time.Second // park the retry in its backoff sleep
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	conn.Push(msgEnvelope("req-1", "m1"))
	<-h.calls
	h.release <- errors.New("handler down") // first delivery fails → backoff
	time.Sleep(100 * time.Millisecond)      // let the retry reach its sleep
	cancel()                                // the cancel must cut the sleep short

	select {
	case <-bc.done:
	case <-time.After(3 * time.Second):
		t.Fatal("a canceled backoff must return immediately")
	}
	if got := h.handled.Load(); got != 1 {
		t.Fatalf("a canceled retry must not redeliver, got %d deliveries", got)
	}
}

// A connection that lived past the reconnect cap was healthy: its successor's
// backoff starts fresh instead of inheriting a stale chain.
func TestHealthyConnectionResetsBackoff(t *testing.T) {
	f := newFakePlatform(t, "bot-1", "s3cr3t")
	startOne(t, f, &recordingHandler{})
	conn := f.waitConn(t, 1)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 1 })

	time.Sleep(300 * time.Millisecond) // longer than the compressed reconnect cap
	conn.Push(envelope{
		Cmd:     cmdEventCallback,
		Headers: frameHeaders{ReqID: "ev-late"},
		Body:    json.RawMessage(`{"eventtype":"disconnected_event"}`),
	})
	conn2 := f.waitConn(t, 2)
	waitFor(t, func() bool { return f.totalAcks.Load() >= 2 })
	_ = conn2
}

// Send on a binding whose connection is already gone fails loudly (the
// message stays pending in the sender's PEL instead of vanishing), and an
// unserializable frame fails before touching any connection.
func TestSendWithoutLiveConnection(t *testing.T) {
	ch, err := New(staticSecrets("s"), WithAddr("ws://127.0.0.1:1"), WithRoutes(staticRoutes{}))
	if err != nil {
		t.Fatal(err)
	}
	bc := &botConn{
		parent:  ch,
		binding: Binding{ID: "b-stale"},
		cfg:     bindingConfig{BotID: "bot-1"},
		cancel:  func() {},
		done:    make(chan struct{}),
	}
	ch.mgr.mu.Lock()
	ch.mgr.conns["b-stale"] = bc
	ch.mgr.mu.Unlock()

	err = ch.Send(context.Background(), channels.OutboundMessage{
		Channel: "wecomws", BindingID: "b-stale", MsgID: "m1", ReplyToken: "req-1", Text: "hi",
	})
	if err == nil || !strings.Contains(err.Error(), "connection is down") {
		t.Fatalf("a send on a dead connection must fail loudly, got %v", err)
	}

	err = bc.write(context.Background(), envelope{Cmd: cmdRespond, Body: json.RawMessage(`{oops`)}, 0)
	if err == nil {
		t.Fatal("an unserializable frame must fail to send")
	}
}

// The WeCom gateway matches the handshake header names case-sensitively, so
// the transport's rewrite is the difference between a 101 and a 404. The
// assertions read the raw request: a Go HTTP server canonicalizes incoming
// header names, which would hide the very thing under test.
func TestStandardCaseTransportRewritesHandshakeHeaders(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	raw := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		b := make([]byte, 4096)
		n, _ := c.Read(b)
		raw <- string(b[:n])
		// Any response will do; without one the client sees EOF.
		_, _ = c.Write([]byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"))
	}()

	req, err := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String(), nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	// Header.Set is what coder/websocket uses; Go canonicalizes the name.
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Version", "13")
	if _, ok := req.Header["Sec-Websocket-Key"]; !ok {
		t.Fatalf("precondition: Go must canonicalize to Sec-Websocket-Key, got %v", req.Header)
	}

	tr := standardCaseTransport{&http.Transport{DisableKeepAlives: true}}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	_ = resp.Body.Close()

	got := <-raw
	for _, want := range []string{"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==", "Sec-WebSocket-Version: 13"} {
		if !strings.Contains(got, want) {
			t.Errorf("request must carry %q, got:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Sec-Websocket-") {
		t.Errorf("canonical spelling must not reach the server, got:\n%s", got)
	}
}
