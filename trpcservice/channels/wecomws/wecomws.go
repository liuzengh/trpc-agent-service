// Package wecomws implements the WeCom smart-bot channel over the platform's
// WebSocket long connection (wss://openws.work.weixin.qq.com). Each
// channel_binding row carries one bot (bot_id + secret_ref in the binding's
// config jsonb) and owns one connection; callbacks and replies share the
// socket and a reply must echo the callback frame's headers.req_id.
//
// Two protocol properties shape the design. The platform allows only one
// connection per bot — a newer subscription kicks the older one — so
// connections hang off a platform-wide leader (storage.LeaderLock). And
// unlike the webhook channels there is no platform redelivery for inbound
// frames: a failed Handle retries locally on a capped backoff on a
// per-connection dispatch worker, and a crash inside that window loses the
// message (accepted trade-off for protocol simplicity).
package wecomws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// ChannelName is the channel identifier, stamped on every message this
// channel serves; the sender group partition (Skip predicates) and the
// leader-lock name are keyed on it.
const ChannelName = "wecomws"

// SenderGroup is the dedicated stream:outbound consumer group the
// platform-wide leader runs for this channel's replies.
const SenderGroup = "senders-ws"

// Default knobs applied by New.
const (
	DefaultPingInterval   = 30 * time.Second
	DefaultSegmentBytes   = 2048
	DefaultResyncInterval = 15 * time.Second
)

// inboundRetryMax bounds the local retries of one inbound callback. The
// dispatch worker is serial, so retries beyond this would hold every later
// message on the connection hostage to one unhandleable frame.
const inboundRetryMax = 5

// Channel is the WeCom smart-bot WebSocket implementation of channels.Channel
// (and channels.Starter): Start owns the connections and the bindings
// reconciliation loop, Send writes replies on the connection of the
// message's binding.
type Channel struct {
	addr           string
	secret         config.SecretResolver
	routes         RoutesProvider
	pingInterval   time.Duration
	segment        int
	resyncInterval time.Duration

	// httpClient carries the handshake header fix every dial needs; see
	// standardCaseTransport.
	httpClient *http.Client

	// Connection timing knobs (reconnect 1s ×2 cap 60s; platform kick
	// restarts at 5s; subscribe rejection cap 5min; inbound retry 1s ×2
	// cap 30s).
	reconnectBase    time.Duration
	reconnectCap     time.Duration
	kickedBase       time.Duration
	credentialCap    time.Duration
	inboundRetryBase time.Duration
	inboundRetryCap  time.Duration
	subscribeTimeout time.Duration
	writeTimeout     time.Duration

	mgr *manager
}

// Option customizes the channel.
type Option func(*Channel)

// WithAddr sets the platform WebSocket endpoint (the production
// wss://openws.work.weixin.qq.com; a ws:// httptest URL in tests). Required.
func WithAddr(addr string) Option { return func(c *Channel) { c.addr = addr } }

// WithRoutes sets the source of servable bindings. Required for Start.
func WithRoutes(r RoutesProvider) Option { return func(c *Channel) { c.routes = r } }

// WithPingInterval sets the heartbeat cadence (default 30s).
func WithPingInterval(d time.Duration) Option { return func(c *Channel) { c.pingInterval = d } }

// WithSegmentBytes caps one reply frame's content (default 2048).
func WithSegmentBytes(n int) Option { return func(c *Channel) { c.segment = n } }

// WithResyncInterval sets the bindings reconciliation cadence (default 15s).
func WithResyncInterval(d time.Duration) Option {
	return func(c *Channel) { c.resyncInterval = d }
}

// wsHeaderCase maps Go's canonical spelling of the WebSocket handshake
// headers back to the spelling the platform expects.
var wsHeaderCase = map[string]string{
	"Sec-Websocket-Key":      "Sec-WebSocket-Key",
	"Sec-Websocket-Version":  "Sec-WebSocket-Version",
	"Sec-Websocket-Protocol": "Sec-WebSocket-Protocol",
}

// standardCaseTransport rewrites the WebSocket handshake headers on the way
// out. net/http canonicalizes header names, so a name set as
// "Sec-WebSocket-Key" goes on the wire as "Sec-Websocket-Key" — and the
// WeCom gateway matches these two names case-sensitively, answering the
// canonical spelling with 404 instead of the 101 upgrade. The rewrite has to
// sit in the RoundTripper: coder/websocket sets the headers with
// Header.Set inside Dial, which re-canonicalizes them.
type standardCaseTransport struct{ base http.RoundTripper }

func (t standardCaseTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	for from, to := range wsHeaderCase {
		if v, ok := r.Header[from]; ok {
			delete(r.Header, from)
			r.Header[to] = v
		}
	}
	return t.base.RoundTrip(r)
}

// New creates the channel; connections are only dialed by Start.
func New(secret config.SecretResolver, opts ...Option) (*Channel, error) {
	if secret == nil {
		return nil, errors.New("wecomws: secret resolver is required")
	}
	c := &Channel{
		secret:           secret,
		pingInterval:     DefaultPingInterval,
		segment:          DefaultSegmentBytes,
		resyncInterval:   DefaultResyncInterval,
		reconnectBase:    time.Second,
		reconnectCap:     time.Minute,
		kickedBase:       5 * time.Second,
		credentialCap:    5 * time.Minute,
		inboundRetryBase: time.Second,
		inboundRetryCap:  30 * time.Second,
		subscribeTimeout: 10 * time.Second,
		writeTimeout:     10 * time.Second,
		httpClient:       &http.Client{Transport: standardCaseTransport{http.DefaultTransport}},
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.pingInterval <= 0 {
		c.pingInterval = DefaultPingInterval
	}
	if c.segment <= 0 {
		c.segment = DefaultSegmentBytes
	}
	if c.resyncInterval <= 0 {
		c.resyncInterval = DefaultResyncInterval
	}
	c.mgr = &manager{parent: c, conns: map[string]*botConn{}}
	return c, nil
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return ChannelName }

// RegisterRoutes implements channels.Channel: callbacks arrive on the long
// connection, so there is no HTTP route to mount.
func (c *Channel) RegisterRoutes(_ *http.ServeMux, _ channels.Handler) {}

// Send implements channels.Channel: every segment goes out as one
// aibot_respond_msg frame on the binding's connection, echoing the callback
// frame's req_id the platform correlates replies by. The reply token carries
// the epoch of the connection that received the callback, and a token from a
// replaced connection is rejected as stale: the write would succeed on the
// successor socket while the platform drops the frame, so the sender would
// record the reply as sent and the user would never see it. Unknown bindings
// and missing reply tokens are errors — the message stays pending in the
// sender's PEL and redelivers instead of being dropped.
func (c *Channel) Send(ctx context.Context, msg channels.OutboundMessage) error {
	bc := c.byBinding(msg.BindingID)
	if bc == nil {
		return fmt.Errorf("wecomws: no live connection for binding %s (binding removed/disabled, or leadership held elsewhere)", msg.BindingID)
	}
	if msg.ReplyToken == "" {
		// Without the callback's req_id the platform cannot correlate the
		// reply; it would be a silent no-op, so fail loudly and leave the
		// message to the PEL instead.
		plog.Errorf("wecomws: outbound msg %s carries no reply token (req_id), cannot respond", msg.MsgID)
		return errors.New("wecomws: missing reply token (req_id)")
	}
	epoch, reqID := parseReplyToken(msg.ReplyToken)
	// One reply is one stream: segments share the id, the last one finishes
	// it. TextType is irrelevant here — the stream content carries markdown
	// as-is (channels without markdown support downgrade elsewhere).
	streamID, err := newStreamID()
	if err != nil {
		return err
	}
	segments := channels.SplitText(msg.Text, c.segment)
	for i, seg := range segments {
		frame, err := respondFrame(reqID, streamID, seg, i == len(segments)-1)
		if err != nil {
			return err
		}
		if err := bc.write(ctx, frame, epoch); err != nil {
			if i > 0 {
				return fmt.Errorf("wecomws: send segment %d/%d (partial delivery): %w", i+1, len(segments), err)
			}
			return err
		}
	}
	return nil
}

func (c *Channel) byBinding(id string) *botConn {
	return c.mgr.byBinding(id)
}

// Start implements channels.Starter: it enumerates the channel's bindings,
// runs one connection per bot and reconciles against the binding snapshot
// every resyncInterval, until ctx is canceled — then every connection is
// canceled and drained before a nil return (clean shutdown).
func (c *Channel) Start(ctx context.Context, h channels.Handler) error {
	if c.addr == "" {
		return errors.New("wecomws: addr is required")
	}
	if c.routes == nil {
		return errors.New("wecomws: routes provider is required")
	}
	if err := c.mgr.reconcile(ctx, h); err != nil {
		// The loop below retries every resyncInterval; a failing first
		// snapshot must not abort the channel.
		plog.Warnf("wecomws initial binding sync: %v", err)
	}
	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.mgr.stopAll()
			return nil
		case <-ticker.C:
			if err := c.mgr.reconcile(ctx, h); err != nil {
				plog.Warnf("wecomws binding resync: %v", err)
			}
		}
	}
}
