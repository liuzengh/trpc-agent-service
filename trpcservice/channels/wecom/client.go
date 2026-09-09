package wecom

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secretref"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
)

// ReconnectForever lets a Client reconnect without a bound on attempts. The
// delay is bounded either way.
const ReconnectForever = -1

// Default timings. The heartbeat interval and the exponential backoff follow
// the official SDK; the ack timeout is this adapter's bound on how long it
// waits for any receipt, and is not a platform guarantee.
const (
	defaultInboundBuffer     = 16
	defaultHeartbeatInterval = 30 * time.Second
	defaultAckTimeout        = 10 * time.Second
	defaultReconnectMinDelay = time.Second
	defaultReconnectMaxDelay = 30 * time.Second
	defaultReconnectAttempts = 10
)

// Config builds one Client.
//
// There is no endpoint field: see defaultEndpoint. There is no logger either,
// because everything this package handles is sensitive and a logger is a
// place for it to escape; the Run error and the reply sentinels are what a
// caller observes.
type Config struct {
	// Binding is the static trust anchor. Required.
	Binding Binding

	// Authorizer entitles Binding.SecretRef to Binding.TenantID. Required, and
	// consulted before the reference is resolved: a tenant that may not name a
	// variable must not learn whether it exists.
	Authorizer security.SecretRefAuthorizer

	// Getenv resolves the entitled reference. Defaults to os.Getenv; a test
	// hands over an environment without touching the process's own.
	Getenv func(name string) string

	// InboundBuffer bounds messages read but not yet consumed. Overflow ends
	// the client with ErrInboundOverflow rather than dropping a message.
	InboundBuffer int

	HeartbeatInterval time.Duration
	AckTimeout        time.Duration
	ReconnectMinDelay time.Duration
	ReconnectMaxDelay time.Duration

	// MaxReconnectAttempts bounds consecutive failed attempts; a connection
	// that authenticates resets the count. ReconnectForever removes the bound.
	MaxReconnectAttempts int

	// dial is the test seam. It is unexported so that no caller outside this
	// package can point the subscribe frame, and the Secret in it, anywhere.
	dial dialFunc
}

// wsConn is the part of a WebSocket connection this adapter uses. It exists so
// tests can dial a local server through the same code path; the
// implementation on both sides is gorilla/websocket, so no framing, masking or
// close handshake is written here.
type wsConn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(messageType int, data []byte) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	SetReadLimit(limit int64)
	Close() error
}

type dialFunc func(ctx context.Context) (wsConn, error)

// dialWeCom dials the one endpoint this adapter knows.
func dialWeCom(handshakeTimeout time.Duration) dialFunc {
	return func(ctx context.Context) (wsConn, error) {
		dialer := *websocket.DefaultDialer
		dialer.HandshakeTimeout = handshakeTimeout
		conn, resp, err := dialer.DialContext(ctx, defaultEndpoint, nil)
		if resp != nil && resp.Body != nil {
			// Closed, never read: a rejected handshake's body is provider text
			// this package does not propagate, and an unread body leaks a
			// connection.
			resp.Body.Close()
		}
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

func (c Config) withDefaults() Config {
	if c.Getenv == nil {
		c.Getenv = os.Getenv
	}
	if c.InboundBuffer <= 0 {
		c.InboundBuffer = defaultInboundBuffer
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.AckTimeout <= 0 {
		c.AckTimeout = defaultAckTimeout
	}
	if c.ReconnectMinDelay <= 0 {
		c.ReconnectMinDelay = defaultReconnectMinDelay
	}
	if c.ReconnectMaxDelay < c.ReconnectMinDelay {
		c.ReconnectMaxDelay = maxDuration(defaultReconnectMaxDelay, c.ReconnectMinDelay)
	}
	if c.MaxReconnectAttempts == 0 {
		c.MaxReconnectAttempts = defaultReconnectAttempts
	}
	if c.dial == nil {
		c.dial = dialWeCom(c.AckTimeout)
	}
	return c
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// Client is one bot's long connection.
//
// One Binding is one connection. The platform disconnects the older connection
// when a second one subscribes with the same credential, so running two
// Clients for one bot is a takeover fight rather than redundancy — Run refuses
// to start twice, and a takeover ends the client instead of racing back.
type Client struct {
	binding Binding
	// secret is resolved once, at construction, after the entitlement check.
	// Rotating it takes a restart.
	secret  string
	cfg     Config
	inbound chan DirectText
	running atomic.Bool

	mu   sync.Mutex
	conn *connection
}

// New validates the configuration, entitles the credential reference and
// resolves it. It makes no network call.
//
// The order is the point: the reference is authorized for this tenant, by
// exact string, before anything looks it up. A caller that reversed those two
// would turn the environment into a table any tenant could probe through
// refusals.
func New(cfg Config) (*Client, error) {
	if err := cfg.Binding.Validate(); err != nil {
		return nil, err
	}
	if cfg.Authorizer == nil {
		return nil, fmt.Errorf("%w: an authorizer is required", ErrConfig)
	}
	if err := cfg.Authorizer.AuthorizeSecretRef(cfg.Binding.TenantID, cfg.Binding.SecretRef); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	// Validate already accepted the syntax; this is the same parser reading
	// the same string, so the entitled reference and the resolved variable
	// cannot be two different names.
	name, err := secretref.EnvName(cfg.Binding.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("%w: secret_ref: %w", ErrConfig, err)
	}
	secret := cfg.Getenv(name)
	if secret == "" {
		// An exported-but-empty variable is treated as unset: continuing would
		// subscribe with a blank Secret under configuration that says
		// otherwise. The variable name is named, the value never is.
		return nil, fmt.Errorf("%w: secret_ref environment variable %q is unset or empty",
			ErrConfig, name)
	}
	return &Client{
		binding: cfg.Binding,
		secret:  secret,
		cfg:     cfg,
		inbound: make(chan DirectText, cfg.InboundBuffer),
	}, nil
}

// Messages delivers accepted single-chat text. The channel is bounded and is
// never closed: Run returning is the end-of-life signal, and a closed channel
// would make an abandoned consumer read zero values forever.
func (c *Client) Messages() <-chan DirectText { return c.inbound }

// Run connects, subscribes and serves until ctx is cancelled or the client
// reaches a state it must not continue from. It always returns a non-nil
// error: context.Canceled for an orderly shutdown, ErrAuthRejected,
// ErrTakenOver, ErrInboundOverflow or ErrReconnectExhausted otherwise.
//
// Ordinary connection failures — a dial error, a read error, a lost heartbeat,
// a reply whose outcome is unknown — reconnect with exponential backoff. A
// rejected credential and a takeover do not: both would repeat, and one of
// them would fight another process for the bot.
func (c *Client) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return ErrRunning
	}
	defer c.running.Store(false)
	attempts := 0
	for {
		authenticated, err := c.runConnection(ctx)
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Shutdown wins over whatever the connection reported on its way
			// down: closing the socket produces a read error the caller did
			// not ask about.
			return ctxErr
		}
		if isTerminal(err) {
			return err
		}
		if authenticated {
			attempts = 0
		}
		attempts++
		if c.cfg.MaxReconnectAttempts != ReconnectForever && attempts > c.cfg.MaxReconnectAttempts {
			// Transport errors may carry credentials or peer-supplied text.
			// Keep them out of both the public message and the unwrap chain.
			return ErrReconnectExhausted
		}
		if err := sleepContext(ctx, c.backoff(attempts)); err != nil {
			return err
		}
	}
}

// isTerminal reports errors Run must not reconnect from.
func isTerminal(err error) bool {
	return errors.Is(err, ErrAuthRejected) ||
		errors.Is(err, ErrTakenOver) ||
		errors.Is(err, ErrInboundOverflow) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// backoff is the delay before attempt n, doubling from the minimum and capped.
func (c *Client) backoff(attempt int) time.Duration {
	delay := c.cfg.ReconnectMinDelay
	for i := 1; i < attempt && delay < c.cfg.ReconnectMaxDelay; i++ {
		delay *= 2
	}
	if delay > c.cfg.ReconnectMaxDelay {
		delay = c.cfg.ReconnectMaxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// runConnection owns exactly one connection from dial to close, and reports
// whether it ever authenticated.
//
// It starts two goroutines — one reader, one heartbeat — and joins both before
// returning, so a Run that has returned has left nothing behind.
func (c *Client) runConnection(ctx context.Context) (bool, error) {
	ws, err := c.cfg.dial(ctx)
	if err != nil {
		return false, fmt.Errorf("wecom: connect: %w", err)
	}
	generation, err := newID()
	if err != nil {
		ws.Close()
		return false, err
	}
	ws.SetReadLimit(maxFrameBytes)
	conn := newConnection(ws, generation, c.cfg.AckTimeout)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		c.readLoop(conn)
	}()
	if err := c.subscribe(ctx, conn); err != nil {
		conn.fail(err)
		workers.Wait()
		return false, conn.reason()
	}
	c.setConn(conn)
	workers.Add(1)
	go func() {
		defer workers.Done()
		c.heartbeat(ctx, conn)
	}()
	select {
	case <-ctx.Done():
		conn.fail(ctx.Err())
	case <-conn.done:
	}
	c.clearConn(conn)
	workers.Wait()
	return true, conn.reason()
}

// subscribe authenticates the connection and gates message delivery on the
// answer.
//
// The receipt has to match the req_id this frame carried and carry an explicit
// errcode of zero. Anything else — a different req_id, a missing errcode, a
// non-zero one, silence — is not authentication, and callbacks that arrive
// before it are dropped rather than served.
func (c *Client) subscribe(ctx context.Context, conn *connection) error {
	reqID, err := newReqID(cmdSubscribe)
	if err != nil {
		return err
	}
	payload, err := encodeFrame(cmdSubscribe, reqID, subscribeBody{
		BotID:  c.binding.BotID,
		Secret: c.secret,
	})
	if err != nil {
		return err
	}
	waiter, ok := conn.expectSubscribe(reqID)
	if !ok {
		return ErrNotConnected
	}
	defer conn.unregister(reqID)
	if err := conn.write(payload, c.cfg.AckTimeout); err != nil {
		return fmt.Errorf("wecom: subscribe: %w", err)
	}
	ack, err := conn.await(ctx, waiter)
	if err != nil {
		return err
	}
	switch {
	case ack.ok():
	case ack.refused():
		// The platform stands behind this: the credential is wrong, or the bot
		// is not permitted to subscribe. Redialing repeats it.
		return ErrAuthRejected
	default:
		// A receipt with no errcode is not an acknowledgement, but it is also
		// no evidence against the credential. It is a broken exchange, and a
		// broken exchange reconnects.
		return errMalformedReceipt
	}
	// The connection was marked authenticated by the reader when it delivered
	// this receipt, not here: see expectSubscribe.
	return nil
}

// readLoop is the only reader on the connection. It never blocks on a
// consumer: an accepted message is offered to the bounded channel and, if that
// fails, the connection ends. Receipts therefore keep flowing at the speed of
// the wire however slow the consumer is.
func (c *Client) readLoop(conn *connection) {
	for {
		if err := conn.ws.SetReadDeadline(time.Now().Add(c.readTimeout())); err != nil {
			conn.fail(fmt.Errorf("wecom: read deadline: %w", err))
			return
		}
		_, data, err := conn.ws.ReadMessage()
		if err != nil {
			conn.fail(fmt.Errorf("wecom: read: %w", err))
			return
		}
		in, err := decodeFrame(data)
		if err != nil {
			// One unreadable frame is not a reason to drop a working
			// connection, and there is no protocol reply to send about it.
			continue
		}
		if err := c.dispatch(conn, in); err != nil {
			conn.fail(err)
			return
		}
	}
}

// readTimeout is the backstop deadline on a silent connection: long enough
// that a healthy heartbeat always resets it, short enough that a half-open
// socket does not hold the client forever.
func (c *Client) readTimeout() time.Duration {
	return c.cfg.HeartbeatInterval*(maxMissedHeartbeats+1) + c.cfg.AckTimeout
}

// dispatch routes one inbound frame. Returning an error ends the connection;
// everything the adapter does not implement, and everything it refuses, is
// ignored instead.
func (c *Client) dispatch(conn *connection, in frame) error {
	switch in.Cmd {
	case "":
		// A frame with no command is a receipt for something this adapter
		// sent. Unmatched receipts are ignored: a late one whose waiter is
		// gone must not be handed to whoever asks next.
		conn.deliverAck(in)
		return nil
	case cmdMsgCallback:
		if !conn.authenticated() {
			return nil
		}
		msg, err := normalizeDirectText(c.binding, in, conn.generation, time.Now())
		if err != nil {
			// Refused: not delivered, not answered, connection unaffected.
			return nil
		}
		select {
		case c.inbound <- msg:
			return nil
		default:
			return ErrInboundOverflow
		}
	case cmdEventCallback:
		if isTakeover(in) {
			return ErrTakenOver
		}
		return nil
	default:
		return nil
	}
}

// heartbeat pings on an interval and requires a matching receipt for each
// ping. Consecutive misses end the connection, which then reconnects.
func (c *Client) heartbeat(ctx context.Context, conn *connection) {
	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()
	missed := 0
	for {
		select {
		case <-conn.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		ok, err := c.ping(ctx, conn)
		if err != nil {
			conn.fail(err)
			return
		}
		if ok {
			missed = 0
			continue
		}
		missed++
		if missed >= maxMissedHeartbeats {
			conn.fail(ErrHeartbeatLost)
			return
		}
	}
}

// ping sends one heartbeat and waits for its receipt. It reports whether the
// receipt arrived and was a success; an error is a fault that ends the
// connection rather than a missed beat.
func (c *Client) ping(ctx context.Context, conn *connection) (bool, error) {
	reqID, err := newReqID(cmdPing)
	if err != nil {
		return false, err
	}
	payload, err := encodeFrame(cmdPing, reqID, nil)
	if err != nil {
		return false, err
	}
	waiter, registered := conn.register(reqID)
	if !registered {
		return false, nil
	}
	defer conn.unregister(reqID)
	if err := conn.write(payload, c.cfg.AckTimeout); err != nil {
		return false, fmt.Errorf("wecom: heartbeat: %w", err)
	}
	ack, err := conn.await(ctx, waiter)
	if err != nil {
		// A timeout is a missed beat. A closed connection or a cancelled
		// context is already being handled by the loop above.
		if errors.Is(err, errAckTimeout) {
			return false, nil
		}
		return false, nil
	}
	return ack.ok(), nil
}

// SendFinalText sends the one final reply to one accepted message.
//
// The frame is aibot_respond_msg on the callback's req_id, carrying a stream
// with the message's stable id, the text, and finish set. Success is a
// receipt matching that req_id with an explicit errcode of zero, and nothing
// else is success:
//
//   - ErrReplyRejected — the platform answered with a non-zero errcode. The
//     reply did not land.
//   - ErrReplyOutcomeUnknown — written, but no receipt arrived. It may or may
//     not have landed. The connection is retired so that a receipt arriving
//     afterwards cannot be read as the answer to something else.
//   - ErrReplyTargetExpired — the target is from an earlier connection.
//   - ErrReplyAlreadySent — this message has been answered once already.
//
// Nothing is retried here. A retry across connections would need a target the
// platform still honours, and this package has no evidence that one exists;
// the caller owns that decision with the durable record this package does not
// have.
func (c *Client) SendFinalText(ctx context.Context, target ReplyTarget, text string) error {
	if err := validateReplyText(text); err != nil {
		return err
	}
	conn := c.currentConn()
	if conn == nil {
		return ErrNotConnected
	}
	if target.generation == "" || target.generation != conn.generation {
		return ErrReplyTargetExpired
	}
	payload, err := encodeFrame(cmdRespond, target.reqID, respondBody{
		MsgType: msgTypeStream,
		Stream: streamReply{
			ID:      target.streamID,
			Content: text,
			Finish:  true,
		},
	})
	if err != nil {
		return err
	}
	// Claiming before writing is what makes "already sent" true even when the
	// first attempt ended unknown: a second final stream on one req_id is a
	// second answer, not a retry.
	waiter, err := conn.claimReply(target.reqID)
	if err != nil {
		return err
	}
	defer conn.unregister(target.reqID)
	if err := conn.write(payload, c.cfg.AckTimeout); err != nil {
		// A failed write may still have put bytes on the wire.
		conn.fail(ErrReplyOutcomeUnknown)
		return ErrReplyOutcomeUnknown
	}
	ack, err := conn.await(ctx, waiter)
	if err != nil {
		conn.fail(ErrReplyOutcomeUnknown)
		return ErrReplyOutcomeUnknown
	}
	if ack.refused() {
		return ErrReplyRejected
	}
	if !ack.ok() {
		// A receipt this adapter cannot read is not a success and not a
		// refusal. The reply may have landed, and the peer is talking
		// nonsense, so the connection goes with the answer.
		conn.fail(ErrReplyOutcomeUnknown)
		return ErrReplyOutcomeUnknown
	}
	return nil
}

func (c *Client) setConn(conn *connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = conn
}

func (c *Client) clearConn(conn *connection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		c.conn = nil
	}
}

func (c *Client) currentConn() *connection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// errAckTimeout reports a receipt that did not arrive inside the ack timeout.
// It is internal: what it means depends on the frame that was waiting, and
// each caller translates it — a missed heartbeat, an unknown reply outcome, a
// failed subscribe.
var errAckTimeout = errors.New("wecom: acknowledgement timed out")

// errMalformedReceipt reports a receipt with no errcode: not an
// acknowledgement, and not a refusal either. It is an ordinary connection
// failure, so it reconnects.
var errMalformedReceipt = errors.New("wecom: receipt carries no errcode")

// connection is one live WebSocket connection and everything scoped to it: the
// generation reply targets are minted against, the receipts still outstanding,
// and the req_ids already answered.
//
// All of that is per-connection by design. A reconnect starts a new generation
// with empty maps, which is what makes a target from the previous connection
// refusable rather than silently reusable on a socket the platform correlates
// separately.
type connection struct {
	ws         wsConn
	generation string
	ackTimeout time.Duration

	// done closes exactly once, when the connection fails or is shut down.
	done chan struct{}

	// writeMu serializes writes. gorilla allows one concurrent writer, and one
	// writer is also what keeps a reply, a heartbeat and the subscribe frame
	// from interleaving on the wire.
	writeMu sync.Mutex

	failOnce sync.Once

	mu      sync.Mutex
	authed  bool
	closed  bool
	err     error
	pending map[string]chan receipt
	replied map[string]struct{}
	// authReqID is the req_id of this connection's subscribe frame, so the
	// reader can recognize the receipt that authenticates the connection.
	authReqID string
}

func newConnection(ws wsConn, generation string, ackTimeout time.Duration) *connection {
	return &connection{
		ws:         ws,
		generation: generation,
		ackTimeout: ackTimeout,
		done:       make(chan struct{}),
		pending:    make(map[string]chan receipt),
		replied:    make(map[string]struct{}),
	}
}

// fail records the first reason this connection ended and closes the socket.
//
// Closing here is what unblocks the reader: a blocked ReadMessage returns as
// soon as the underlying connection goes away, so shutdown does not wait for a
// deadline to expire. Later reasons are dropped — the first one is the cause,
// and everything after it is that cause being noticed elsewhere.
func (c *connection) fail(reason error) {
	c.failOnce.Do(func() {
		c.mu.Lock()
		c.err = reason
		c.closed = true
		c.mu.Unlock()
		close(c.done)
		c.ws.Close()
	})
}

func (c *connection) reason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		return errors.New("wecom: connection ended")
	}
	return c.err
}

func (c *connection) authenticated() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authed
}

// expectSubscribe registers the receipt waiter for the subscribe frame and
// records which req_id authenticates this connection.
//
// Authentication is then marked by the reader, in deliverAck, rather than by
// the goroutine waiting here. The platform may send a callback immediately
// behind the subscribe receipt, and the reader would otherwise dispatch that
// callback — and drop it as pre-authentication — before the waiter had been
// scheduled. Deciding it in the reader makes the order the wire order: every
// frame after the receipt is dispatched on an authenticated connection.
func (c *connection) expectSubscribe(reqID string) (chan receipt, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, false
	}
	// One critical section: a receipt cannot arrive between registering the
	// waiter and recording the req_id that authenticates on it.
	waiter := make(chan receipt, 1)
	c.pending[reqID] = waiter
	c.authReqID = reqID
	return waiter, true
}

// register starts waiting for the receipt to reqID. It reports false once the
// connection is closed, so a caller cannot wait for an answer that can no
// longer arrive.
func (c *connection) register(reqID string) (chan receipt, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, false
	}
	waiter := make(chan receipt, 1)
	c.pending[reqID] = waiter
	return waiter, true
}

// claimReply registers the receipt waiter for a final reply and, in the same
// step, records that this req_id has been answered.
//
// One step, one lock: two callers cannot both decide they are the first, and a
// caller whose reply ended with an unknown outcome cannot come back and send a
// second final stream on the same req_id.
func (c *connection) claimReply(reqID string) (chan receipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrNotConnected
	}
	if _, answered := c.replied[reqID]; answered {
		return nil, ErrReplyAlreadySent
	}
	c.replied[reqID] = struct{}{}
	waiter := make(chan receipt, 1)
	c.pending[reqID] = waiter
	return waiter, nil
}

func (c *connection) unregister(reqID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, reqID)
}

// deliverAck hands one receipt to the waiter that asked for exactly this
// req_id. A receipt nobody is waiting for is dropped.
func (c *connection) deliverAck(in frame) {
	if in.Headers.ReqID == "" {
		return
	}
	c.mu.Lock()
	waiter, waiting := c.pending[in.Headers.ReqID]
	delete(c.pending, in.Headers.ReqID)
	// A successful receipt for the subscribe frame is what makes this
	// connection authenticated, and it happens here so that the next frame the
	// reader dispatches already sees it.
	if in.Headers.ReqID == c.authReqID && (receipt{code: in.ErrCode}).ok() {
		c.authed = true
	}
	c.mu.Unlock()
	if !waiting {
		return
	}
	// The waiter is buffered and removed from the map in the same step, so
	// this never blocks the reader and never delivers twice.
	waiter <- receipt{code: in.ErrCode}
}

// await waits for one receipt. It reports whether the receipt was a success,
// or an error when none arrived: the ack timeout, the connection ending, or
// the caller giving up.
func (c *connection) await(ctx context.Context, waiter chan receipt) (receipt, error) {
	timer := time.NewTimer(c.ackTimeout)
	defer timer.Stop()
	select {
	case ack := <-waiter:
		return ack, nil
	case <-timer.C:
		return receipt{}, errAckTimeout
	case <-c.done:
		return receipt{}, c.reason()
	case <-ctx.Done():
		return receipt{}, ctx.Err()
	}
}

// write puts one frame on the wire under a bounded deadline.
func (c *connection) write(payload []byte, timeout time.Duration) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.ws.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	return c.ws.WriteMessage(websocket.TextMessage, payload)
}
