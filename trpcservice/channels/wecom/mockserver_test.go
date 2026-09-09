package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
)

// The values below are markers, not samples. Every one of them is something
// this package promises never to repeat — a Secret, a bot id, an external user
// or message id, a message body, the platform's own errmsg — so a test can
// take any error this package produced and assert that none of them is in it.
const (
	secretMarker = "secret-marker-4f19c7"
	botIDMarker  = "botid-marker-8d21ab"
	userIDMarker = "userid-marker-b3c05e"
	msgIDMarker  = "msgid-marker-77af10"
	bodyMarker   = "body-marker-hello-world"
	errMsgMarker = "errmsg-marker-6ce93d"
	replyMarker  = "reply-marker-answered"
)

// testTimeout bounds every wait in this package's tests. It is long enough
// that a loaded machine does not fail the suite and short enough that a
// deadlock is reported rather than waited out.
const testTimeout = 2 * time.Second

// requireRedacted asserts that an error repeats nothing this package protects.
func requireRedacted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	for _, marker := range []string{
		secretMarker, botIDMarker, userIDMarker, msgIDMarker, bodyMarker, errMsgMarker,
	} {
		require.NotContainsf(t, err.Error(), marker,
			"the error repeated a protected value: %v", err)
	}
}

func testBinding() Binding {
	return Binding{
		TenantID:   "tenant-a",
		AgentAppID: "app-a",
		BindingID:  "binding-a",
		BotID:      botIDMarker,
		SecretRef:  "env:WECOM_TEST_BOT_SECRET",
	}
}

// testAuthorizer entitles exactly this binding's reference to exactly this
// binding's tenant, through the same entitlement table the process uses.
func testAuthorizer(t *testing.T, binding Binding) *security.Entitlements {
	t.Helper()
	table, err := security.NewEntitlements(security.Grant{
		TenantID:   binding.TenantID,
		SecretRefs: []string{binding.SecretRef},
	})
	require.NoError(t, err)
	return table
}

// testConfig wires a Client to the local mock server. The timings are small
// multiples of each other so that a heartbeat, an ack timeout and a reconnect
// all happen inside one test without sleeping through real-world defaults.
func testConfig(t *testing.T, server *mockServer, binding Binding) Config {
	t.Helper()
	return Config{
		Binding:              binding,
		Authorizer:           testAuthorizer(t, binding),
		Getenv:               func(string) string { return secretMarker },
		InboundBuffer:        2,
		HeartbeatInterval:    40 * time.Millisecond,
		AckTimeout:           150 * time.Millisecond,
		ReconnectMinDelay:    time.Millisecond,
		ReconnectMaxDelay:    4 * time.Millisecond,
		MaxReconnectAttempts: ReconnectForever,
		dial:                 server.dial,
	}
}

// mockServer is a local WebSocket server standing in for the platform. It
// speaks the real protocol over a real connection: the frames, the receipts
// and the framing are the ones production uses, and only the address differs.
type mockServer struct {
	t    *testing.T
	http *httptest.Server

	conns chan *mockConn
	mu    sync.Mutex
	open  []*mockConn
}

func newMockServer(t *testing.T) *mockServer {
	t.Helper()
	server := &mockServer{t: t, conns: make(chan *mockConn, 8)}
	upgrader := websocket.Upgrader{}
	server.http = httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			conn := newMockConn(server.t, ws)
			server.mu.Lock()
			server.open = append(server.open, conn)
			server.mu.Unlock()
			select {
			case server.conns <- conn:
			default:
			}
		}))
	t.Cleanup(server.http.Close)
	t.Cleanup(func() {
		server.mu.Lock()
		defer server.mu.Unlock()
		for _, conn := range server.open {
			conn.close()
		}
	})
	return server
}

// dial is the Client's unexported test seam pointed at this server.
func (s *mockServer) dial(ctx context.Context) (wsConn, error) {
	url := "ws" + strings.TrimPrefix(s.http.URL, "http")
	ws, resp, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	return ws, nil
}

// accept waits for the client to connect.
func (s *mockServer) accept() *mockConn {
	s.t.Helper()
	select {
	case conn := <-s.conns:
		return conn
	case <-time.After(testTimeout):
		s.t.Fatal("the client did not connect")
		return nil
	}
}

// authenticate completes the subscribe exchange and returns the connection.
func (s *mockServer) authenticate() *mockConn {
	s.t.Helper()
	conn := s.accept()
	subscribe := conn.next()
	require.Equal(s.t, cmdSubscribe, subscribe.Cmd)
	conn.ack(subscribe.Headers.ReqID, 0)
	return conn
}

// idle asserts the client does not connect again within d.
func (s *mockServer) idle(d time.Duration) {
	s.t.Helper()
	select {
	case <-s.conns:
		s.t.Fatal("the client reconnected where it must not")
	case <-time.After(d):
	}
}

// mockConn is one accepted connection. Its reader answers pings on its own, so
// a test only ever sees the frames it cares about and a connection stays alive
// while the test is busy elsewhere.
type mockConn struct {
	t         *testing.T
	ws        *websocket.Conn
	writeMu   sync.Mutex
	frames    chan frame
	pings     chan struct{}
	dropPings atomic.Bool
	closed    chan struct{}
}

func newMockConn(t *testing.T, ws *websocket.Conn) *mockConn {
	conn := &mockConn{
		t:      t,
		ws:     ws,
		frames: make(chan frame, 16),
		pings:  make(chan struct{}, 64),
		closed: make(chan struct{}),
	}
	go conn.read()
	return conn
}

// read runs on a goroutine that is not the test's, so it asserts nothing.
func (c *mockConn) read() {
	defer close(c.closed)
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		in, err := decodeFrame(data)
		if err != nil {
			continue
		}
		if in.Cmd == cmdPing {
			select {
			case c.pings <- struct{}{}:
			default:
			}
			if !c.dropPings.Load() {
				c.ack(in.Headers.ReqID, 0)
			}
			continue
		}
		select {
		case c.frames <- in:
		default:
		}
	}
}

func (c *mockConn) write(payload any) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// A write to a connection the client has already closed is not a test
	// failure: several tests close from the other side on purpose.
	_ = c.ws.WriteJSON(payload)
}

// ack answers one frame. It always carries an errmsg marker, so every test
// that reads an error afterwards is also testing that provider text does not
// travel with it.
func (c *mockConn) ack(reqID string, errcode int) {
	c.write(map[string]any{
		"headers": map[string]any{"req_id": reqID},
		"errcode": errcode,
		"errmsg":  errMsgMarker,
	})
}

// ackWithoutErrcode answers with a receipt that omits errcode entirely.
func (c *mockConn) ackWithoutErrcode(reqID string) {
	c.write(map[string]any{
		"headers": map[string]any{"req_id": reqID},
		"errmsg":  errMsgMarker,
	})
}

// callback pushes one inbound message frame.
func (c *mockConn) callback(reqID string, body map[string]any) {
	c.write(map[string]any{
		"cmd":     cmdMsgCallback,
		"headers": map[string]any{"req_id": reqID},
		"body":    body,
	})
}

// event pushes one event callback frame.
func (c *mockConn) event(eventType string) {
	c.write(map[string]any{
		"cmd":     cmdEventCallback,
		"headers": map[string]any{"req_id": "event-1"},
		"body":    map[string]any{"event": map[string]any{"eventtype": eventType}},
	})
}

// next returns the next frame the client sent, pings excluded.
func (c *mockConn) next() frame {
	c.t.Helper()
	select {
	case in := <-c.frames:
		return in
	case <-time.After(testTimeout):
		c.t.Fatal("the client sent no frame")
		return frame{}
	}
}

// silent asserts the client sends nothing but heartbeats for d.
func (c *mockConn) silent(d time.Duration) {
	c.t.Helper()
	select {
	case in := <-c.frames:
		c.t.Fatalf("the client sent an unexpected %q frame", in.Cmd)
	case <-time.After(d):
	}
}

// waitPings blocks until the client has sent n heartbeats.
func (c *mockConn) waitPings(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-c.pings:
		case <-time.After(testTimeout):
			c.t.Fatalf("the client sent %d heartbeats, wanted %d", i, n)
		}
	}
}

func (c *mockConn) close() { c.ws.Close() }

// runner runs one Client in the background for the length of one test.
type runner struct {
	client *Client
	cancel context.CancelFunc
	errs   chan error
	once   sync.Once
	result error
}

func startClient(t *testing.T, cfg Config) *runner {
	t.Helper()
	client, err := New(cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{client: client, cancel: cancel, errs: make(chan error, 1)}
	go func() { r.errs <- client.Run(ctx) }()
	t.Cleanup(func() { r.stop(t) })
	return r
}

// stop cancels the run and returns what Run reported. Run only returns once
// its reader and heartbeat have been joined, so a stop that returns is also
// the assertion that this connection left no goroutine behind.
func (r *runner) stop(t *testing.T) error {
	t.Helper()
	r.once.Do(func() {
		r.cancel()
		select {
		case r.result = <-r.errs:
		case <-time.After(testTimeout):
			r.result = errors.New("Run did not return after cancellation")
		}
	})
	return r.result
}

// wait returns what Run reported without cancelling it: for the terminal
// cases, where stopping on our own would hide the reason.
func (r *runner) wait(t *testing.T) error {
	t.Helper()
	r.once.Do(func() {
		select {
		case r.result = <-r.errs:
		case <-time.After(testTimeout):
			r.result = errors.New("Run did not return on its own")
		}
	})
	return r.result
}

// receive takes the next accepted message.
func (r *runner) receive(t *testing.T) DirectText {
	t.Helper()
	select {
	case msg := <-r.client.Messages():
		return msg
	case <-time.After(testTimeout):
		t.Fatal("no message was delivered")
		return DirectText{}
	}
}

// noMessage asserts nothing is delivered within d.
func (r *runner) noMessage(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case msg := <-r.client.Messages():
		t.Fatalf("a message was delivered for session %q", msg.SessionID)
	case <-time.After(d):
	}
}

// textCallback is a well formed single-chat text body.
func textCallback() map[string]any {
	return map[string]any{
		"msgid":    msgIDMarker,
		"aibotid":  botIDMarker,
		"chattype": "single",
		"msgtype":  "text",
		"from":     map[string]any{"userid": userIDMarker},
		"text":     map[string]any{"content": bodyMarker},
	}
}

// decodeBody reads a frame body a test asserts on.
func decodeBody(t *testing.T, in frame, out any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(in.Body, out))
}
