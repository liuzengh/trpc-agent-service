package wecomws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// credentialError marks a subscribe rejection (errcode != 0): a bad secret
// never heals by spinning, so its backoff grows toward a higher cap.
type credentialError struct{ errcode int }

func (e *credentialError) Error() string {
	return fmt.Sprintf("wecomws: subscribe rejected (errcode %d)", e.errcode)
}

// kickedError marks disconnected_event: the platform kicked this connection
// because a newer subscription took over (leader handover).
type kickedError struct{}

func (*kickedError) Error() string { return "wecomws: disconnected by platform" }

// staleReplyError marks a reply whose token was stamped by a connection that
// is no longer live: the platform correlates replies per connection, so the
// req_id is undeliverable on any successor. Retrying cannot heal it, and the
// reply must not be recorded as sent.
type staleReplyError struct{ reqID string }

func (e *staleReplyError) Error() string {
	return fmt.Sprintf("wecomws: stale reply token (req_id %s): the callback's connection was replaced, the platform can no longer correlate this reply", e.reqID)
}

// botConn owns one bot's connection lifecycle: dial → subscribe → heartbeat +
// read loop → jittered reconnect. run returns only when its ctx is canceled;
// every other failure reconnects internally with backoff.
type botConn struct {
	parent  *Channel
	binding Binding
	cfg     bindingConfig

	cancel func()
	done   chan struct{}

	mu      sync.Mutex
	conn    *websocket.Conn
	pingOut string // req_id of the heartbeat awaiting its pong
	epoch   uint64 // identity of the current connection; scopes reply tokens (0 before the first setConn)
}

// run owns the connection until ctx is canceled, reconnecting on every
// failure with the backoff its error class earns.
func (c *botConn) run(ctx context.Context, h channels.Handler) {
	defer close(c.done)
	wait := time.Duration(0)
	for ctx.Err() == nil {
		started := time.Now()
		err := c.serve(ctx, h)
		if ctx.Err() != nil {
			return
		}
		// A connection that lived past the reconnect cap was healthy: its
		// successor's failures start a fresh backoff chain instead of
		// inheriting one.
		if time.Since(started) > c.parent.reconnectCap {
			wait = 0
		}
		wait = c.nextWait(err, wait)
		plog.Warnf("wecomws bot %s disconnected, reconnecting in ~%s: %v", c.cfg.BotID, wait, err)
		if !sleepJitter(ctx, wait) {
			return
		}
	}
}

// nextWait advances the reconnect backoff: the first failure waits exactly
// its class base, repeats double toward the class cap, and a platform kick
// always restarts at its base (a kick is the expected, rare leader handover
// and must not inherit a stale chain).
func (c *botConn) nextWait(err error, prev time.Duration) time.Duration {
	var kicked *kickedError
	if errors.As(err, &kicked) {
		return c.parent.kickedBase
	}
	limit := c.parent.reconnectCap
	var cred *credentialError
	if errors.As(err, &cred) {
		limit = c.parent.credentialCap
	}
	if prev < c.parent.reconnectBase {
		return c.parent.reconnectBase
	}
	return min(prev*2, limit)
}

// serve runs one connection attempt to completion: resolve the secret
// (rotation takes effect on the next reconnect), dial, subscribe, then
// heartbeat + read until something fails.
func (c *botConn) serve(ctx context.Context, h channels.Handler) error {
	// connCtx is this connection attempt's lifetime. The heartbeat kill and
	// any read failure cancel it, so work parked on this connection (the
	// dispatch worker's Handle retry loop) wakes up and serve returns —
	// run() reconnects instead of babysitting a dead socket.
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	secret, err := c.parent.secret.Resolve(ctx, c.cfg.SecretRef)
	if err != nil {
		return fmt.Errorf("wecomws: resolve bot secret: %w", err)
	}
	ws, _, err := websocket.Dial(ctx, c.parent.addr,
		&websocket.DialOptions{HTTPClient: c.parent.httpClient})
	if err != nil {
		return fmt.Errorf("wecomws: dial: %w", err)
	}
	defer func() {
		c.clearConn(ws)
		_ = ws.CloseNow()
	}()

	subReqID, err := newReqID()
	if err != nil {
		return err
	}
	subFrame, err := subscribeFrame(c.cfg.BotID, secret, subReqID)
	if err != nil {
		return err
	}
	if err := c.writeTo(ctx, ws, subFrame); err != nil {
		return fmt.Errorf("wecomws: send subscribe: %w", err)
	}

	// Wait for the subscribe ack; callbacks racing ahead of it are buffered
	// and dispatched after. No ack inside the window: tear down and retry.
	subCtx, cancelSub := context.WithTimeout(connCtx, c.parent.subscribeTimeout)
	defer cancelSub()
	var pending []envelope
	acked := false
	for !acked {
		env, rerr := readEnvelope(subCtx, ws)
		if rerr != nil {
			if errors.Is(rerr, errBadFrame) {
				plog.Warnf("wecomws bot %s: %v", c.cfg.BotID, rerr)
				continue
			}
			return rerr
		}
		switch {
		case env.Headers.ReqID == subReqID || env.Cmd == cmdSubscribe:
			// The platform reports the result at the top level of the ack;
			// a body errcode (the shape some bodies use) wins when present.
			ack := errcodeBody{ErrCode: env.ErrCode, ErrMsg: env.ErrMsg}
			if len(env.Body) > 0 {
				var b errcodeBody
				if uerr := json.Unmarshal(env.Body, &b); uerr != nil {
					return fmt.Errorf("wecomws: subscribe ack body: %w", uerr)
				}
				if b.ErrCode != 0 {
					ack = b
				}
			}
			if ack.ErrCode != 0 {
				return &credentialError{errcode: ack.ErrCode}
			}
			acked = true
		case env.Cmd == cmdMsgCallback || env.Cmd == cmdEventCallback:
			if len(pending) < pendingBufferMax {
				pending = append(pending, env)
			} else {
				plog.Warnf("wecomws bot %s: dropping pre-ack callback (buffer full)", c.cfg.BotID)
			}
		default:
			plog.Warnf("wecomws bot %s: ignoring pre-ack frame cmd %q", c.cfg.BotID, env.Cmd)
		}
	}

	// Published only after the ack: until then Send sees "no live
	// connection" (the sender's PEL retries) instead of writing respond
	// frames the platform silently drops on an un-subscribed socket.
	epoch, err := newEpoch()
	if err != nil {
		return err
	}
	c.setConn(ws, epoch)
	go c.heartbeat(connCtx, ws, cancelConn)

	// Inbound frames are dispatched on a serial worker behind a bounded
	// queue so the read loop always stays free to drain pongs: a Handle
	// parked in its capped retry loop inside the read loop would starve
	// the heartbeat of pongs, which would kill the healthy connection and
	// drop the message being retried — WS inbound has no platform
	// redelivery. If the worker falls behind far enough for the queue to
	// fill, the read loop blocks on the enqueue and the heartbeat kill
	// returns as the escape hatch.
	frames := make(chan envelope, inboundQueueMax)
	fatal := make(chan error, 1)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for {
			select {
			case <-connCtx.Done():
				return
			case env := <-frames:
				if err := c.handleFrame(connCtx, h, env); err != nil {
					fatal <- err
					cancelConn() // unblock a readLoop parked in ws.Read
					return
				}
			}
		}
	}()
	defer func() {
		cancelConn()
		<-workerDone // drain before run() reconnects
	}()

	for _, env := range pending {
		if err := dispatchFrame(connCtx, frames, fatal, env); err != nil {
			return err
		}
	}
	readErr := c.readLoop(connCtx, ws, frames, fatal)
	select {
	// A worker-reported failure (e.g. the platform kick) is the real cause;
	// the read loop only saw the cancelConn that unblocked it.
	case ferr := <-fatal:
		return ferr
	default:
		return readErr
	}
}

// heartbeat pings every interval and kills the connection when the previous
// ping got no matching pong within one interval. It exits silently once its
// connection is no longer the current one (reconnected or closed).
func (c *botConn) heartbeat(ctx context.Context, ws *websocket.Conn, kill func()) {
	ticker := time.NewTicker(c.parent.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c.mu.Lock()
		if c.conn != ws {
			c.mu.Unlock()
			return
		}
		if c.pingOut != "" {
			// The previous heartbeat got no answer within one interval: the
			// connection is half-dead — kill it AND cancel connCtx so
			// anything parked on this connection wakes up and run()
			// reconnects.
			c.mu.Unlock()
			plog.Warnf("wecomws bot %s: heartbeat unanswered for %s, closing connection", c.cfg.BotID, c.parent.pingInterval)
			kill()
			_ = ws.CloseNow()
			return
		}
		reqID, err := heartbeatReqID()
		if err == nil {
			var data []byte
			data, err = json.Marshal(pingFrame(reqID))
			if err == nil {
				err = c.writeLocked(ctx, data)
				if err == nil {
					c.pingOut = reqID
				}
			}
		}
		c.mu.Unlock()
		if err != nil {
			// Without a ping on the wire the connection is unmonitored: tear
			// it down like the unanswered-ping path so run() reconnects.
			plog.Warnf("wecomws bot %s: heartbeat failed (%v), closing connection", c.cfg.BotID, err)
			kill()
			_ = ws.CloseNow()
			return
		}
	}
}

// readLoop consumes frames until a connection error (or a platform kick):
// pongs are matched against the heartbeat, callbacks and events are queued
// for the serial dispatch worker. A fatal error the worker reports (e.g. the
// disconnected event) ends the loop just like a read failure.
func (c *botConn) readLoop(ctx context.Context, ws *websocket.Conn, frames chan<- envelope, fatal <-chan error) error {
	for {
		select {
		case err := <-fatal:
			return err
		default:
		}
		env, err := readEnvelope(ctx, ws)
		if err != nil {
			if errors.Is(err, errBadFrame) {
				plog.Warnf("wecomws bot %s: %v", c.cfg.BotID, err)
				continue
			}
			return err
		}
		switch env.Cmd {
		case "":
			// An ack to one of our own requests: the heartbeat ack clears the
			// outstanding ping (its req_id echoes ours), anything else has
			// nothing to dispatch.
			c.mu.Lock()
			if env.Headers.ReqID != "" && env.Headers.ReqID == c.pingOut {
				c.pingOut = ""
			}
			c.mu.Unlock()
		case cmdMsgCallback, cmdEventCallback:
			if err := dispatchFrame(ctx, frames, fatal, env); err != nil {
				return err
			}
		default:
			plog.Warnf("wecomws bot %s: ignoring frame cmd %q", c.cfg.BotID, env.Cmd)
		}
	}
}

// dispatchFrame queues one frame for the serial worker; a fatal error the
// worker already reported, or ctx cancellation, wins over the enqueue.
func dispatchFrame(ctx context.Context, frames chan<- envelope, fatal <-chan error, env envelope) error {
	select {
	case err := <-fatal:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case frames <- env:
		return nil
	}
}

// handleFrame routes one callback frame; a kickedError propagates as a
// disconnect. Message callbacks never fail the connection: WS inbound has no
// platform redelivery, so Handle failures retry locally on a capped backoff
// (duplicates are success — the message was seen before).
//
// The retry count is bounded: the dispatch worker is serial, so an unbounded
// retry would let one poisoned message (a deterministic handler failure) or
// a long infrastructure outage park the whole connection behind it — every
// later message on the bot queues up until the queue fills and the heartbeat
// kill reconnects, which just replays the same frame. After the cap the
// message is dropped loudly instead; the user can resend, the bot stays
// alive.
func (c *botConn) handleFrame(ctx context.Context, h channels.Handler, env envelope) error {
	if env.Cmd == cmdEventCallback {
		return c.handleEvent(env)
	}
	msg, err := c.normalize(env)
	if err != nil {
		plog.Warnf("wecomws bot %s: dropping callback (req_id %s): %v", c.cfg.BotID, env.Headers.ReqID, err)
		return nil
	}
	wait := c.parent.inboundRetryBase
	for attempt := 1; ; attempt++ {
		if _, err := h.Handle(ctx, msg); err == nil || errors.Is(err, channels.ErrDuplicate) {
			return nil
		}
		switch {
		case ctx.Err() != nil:
			return nil
		case attempt >= inboundRetryMax:
			plog.Errorf("wecomws bot %s: dropping msg %s after %d failed attempts: %v",
				c.cfg.BotID, msg.MsgID, attempt, err)
			return nil
		default:
			plog.Errorf("wecomws bot %s handle msg %s: %v (attempt %d/%d, retry in %s)",
				c.cfg.BotID, msg.MsgID, err, attempt, inboundRetryMax, wait)
		}
		if !Sleep(ctx, wait) {
			return nil
		}
		wait = min(wait*2, c.parent.inboundRetryCap)
	}
}

// handleEvent maps platform events; only the disconnect is a connection
// event. enter_chat / template_card_event are out of scope.
func (c *botConn) handleEvent(env envelope) error {
	var ev eventCallback
	if len(env.Body) > 0 {
		if err := json.Unmarshal(env.Body, &ev); err != nil {
			plog.Warnf("wecomws bot %s: bad event frame: %v", c.cfg.BotID, err)
			return nil
		}
	}
	switch ev.eventType() {
	case eventDisconnected:
		return &kickedError{}
	case eventEnterChat, eventTemplateCard:
		plog.Infof("wecomws bot %s: event %s skipped (out of scope)", c.cfg.BotID, ev.eventType())
	default:
		plog.Infof("wecomws bot %s: event %q skipped", c.cfg.BotID, ev.eventType())
	}
	return nil
}

// normalize turns one message callback into the platform-normalized message,
// mirroring the wecom adapter's inbound shape. Group chats carry the chat id
// (session key becomes group:), media messages degrade to a placeholder text.
func (c *botConn) normalize(env envelope) (channels.InboundMessage, error) {
	var body msgCallback
	if err := json.Unmarshal(env.Body, &body); err != nil {
		return channels.InboundMessage{}, fmt.Errorf("parse callback body: %w", err)
	}
	if body.MsgID == "" {
		return channels.InboundMessage{}, errors.New("callback without msgid")
	}
	if body.From.UserID == "" {
		return channels.InboundMessage{}, errors.New("callback without from.userid")
	}
	chatID := ""
	if body.ChatType == "group" {
		chatID = body.ChatID
	}
	text := body.Text.Content
	if body.MsgType != "text" {
		text = mediaPlaceholder(body.MsgType)
	}
	return channels.InboundMessage{
		Channel:     c.parent.Name(),
		MsgID:       body.MsgID,
		SessionKey:  channels.SessionKey(c.parent.Name(), body.From.UserID, chatID),
		UserID:      body.From.UserID,
		ChatID:      chatID,
		Text:        text,
		Type:        channels.TypeText,
		WebhookPath: c.binding.WebhookPath,
		BindingID:   c.binding.ID,
		ReplyToken:  scopedReplyToken(c.currentEpoch(), env.Headers.ReqID),
		ReceivedAt:  time.Now(),
	}, nil
}

// currentEpoch reports the identity of the connection now serving this bot.
// Every frame this serve attempt dispatches was read on this attempt's socket
// and run() dials a successor only after serve returns, so reading it here
// stamps each callback with the connection it actually arrived on.
func (c *botConn) currentEpoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// setConn publishes the connection the read loop just dialed, under the fresh
// epoch that scopes this attempt's reply tokens.
func (c *botConn) setConn(ws *websocket.Conn, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conn = ws
	c.epoch = epoch
	c.pingOut = ""
}

// clearConn drops the connection on teardown, unless a newer one took over.
func (c *botConn) clearConn(ws *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == ws {
		c.conn = nil
		c.pingOut = ""
	}
}

// writeLocked marshals and sends one frame on the current connection; the
// caller holds c.mu, the serializer shared by heartbeat and Send.
func (c *botConn) writeLocked(ctx context.Context, data []byte) error {
	return c.writeLockedOn(ctx, c.conn, data)
}

// writeLockedOn sends one pre-marshaled frame on ws under a bounded write
// timeout; the caller holds c.mu.
func (c *botConn) writeLockedOn(ctx context.Context, ws *websocket.Conn, data []byte) error {
	if ws == nil {
		return errors.New("wecomws: connection is down")
	}
	// A bounded write keeps a hung socket from holding the serializer (and
	// with it Send and the heartbeat) forever.
	wctx, cancel := context.WithTimeout(ctx, c.parent.writeTimeout)
	defer cancel()
	return ws.Write(wctx, websocket.MessageText, data)
}

// write is the Send-facing serializer: one frame, bounded write timeout.
// wantEpoch is the connection epoch the reply token was stamped with (0 for
// a token that carries no epoch); checked under the same lock as the write, so a reconnect
// between the check and the write cannot let a dead token slip onto the
// successor connection.
func (c *botConn) write(ctx context.Context, env envelope, wantEpoch uint64) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if wantEpoch != 0 && wantEpoch != c.epoch {
		return &staleReplyError{reqID: env.Headers.ReqID}
	}
	return c.writeLocked(ctx, data)
}

// writeTo sends one frame on a specific connection under the same serializer;
// used for the subscribe frame, which precedes publication of c.conn.
func (c *botConn) writeTo(ctx context.Context, ws *websocket.Conn, env envelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeLockedOn(ctx, ws, data)
}

// pendingBufferMax bounds the pre-ack callback buffer.
const pendingBufferMax = 16

// inboundQueueMax bounds the frames buffered for the serial dispatch worker;
// it absorbs Handle stalls without blocking the read loop (see serve).
const inboundQueueMax = 64

// errBadFrame marks frames that cannot be routed (binary, unparseable, no
// cmd): logged and skipped instead of tearing the connection down.
var errBadFrame = errors.New("bad frame")

func readEnvelope(ctx context.Context, ws *websocket.Conn) (envelope, error) {
	typ, data, err := ws.Read(ctx)
	if err != nil {
		return envelope{}, err
	}
	if typ != websocket.MessageText {
		return envelope{}, fmt.Errorf("%w: binary frame", errBadFrame)
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return envelope{}, fmt.Errorf("%w: %v", errBadFrame, err)
	}
	// An ack carries no cmd: the platform answers aibot_subscribe with just
	// headers.req_id, errcode and errmsg, and matches it to the request by
	// req_id. A frame is only unroutable when it has neither.
	if env.Cmd == "" && env.Headers.ReqID == "" {
		return envelope{}, fmt.Errorf("%w: missing cmd", errBadFrame)
	}
	return env, nil
}

// Sleep waits for d exactly; false on ctx cancel.
func Sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// sleepJitter waits for d with ±25% jitter so competing reconnects do not
// fire in lockstep; false on ctx cancel.
func sleepJitter(ctx context.Context, d time.Duration) bool {
	jitter := d / 4
	if jitter > 0 {
		//nolint:gosec // G404: reconnect jitter needs no cryptographic randomness
		d += time.Duration(rand.Int64N(int64(2*jitter))) - jitter
	}
	return Sleep(ctx, d)
}
