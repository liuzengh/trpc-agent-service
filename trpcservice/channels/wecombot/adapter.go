package wecombot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

const (
	channelType           = "wecombot"
	defaultWebSocketURL   = "wss://openws.work.weixin.qq.com"
	subscribeTimeout      = 10 * time.Second
	maxReconnectBackoff   = 30 * time.Second
	initialReconnectDelay = time.Second
	writeTimeout          = 4 * time.Second
)

// Adapter maintains one WebSocket long connection per configured route key.
type Adapter struct {
	cache     tenant.ConfigCache
	wsURL     string
	routeKeys []string

	dial       func(context.Context, string) (*botConn, error)
	heartbeat  time.Duration
	backoffMax time.Duration

	mu    sync.Mutex
	conns map[string]*botConn

	wg sync.WaitGroup
}

// botConn serializes frame writes over one WebSocket connection. The WeCom
// gateway rejects handshakes whose Sec-WebSocket-* header names are lowercased
// by Go's net/http canonicalization, so gorilla/websocket (which preserves
// the canonical RFC casing) is used instead of coder/websocket.
type botConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (c *botConn) send(frame outboundFrame) error {
	encoded, err := encodeFrame(frame)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return c.conn.WriteMessage(websocket.TextMessage, encoded)
}

func (c *botConn) read() ([]byte, error) {
	_, data, err := c.conn.ReadMessage()
	return data, err
}

// setReadDeadline bounds the next read; a zero deadline clears the limit.
func (c *botConn) setReadDeadline(deadline time.Time) {
	_ = c.conn.SetReadDeadline(deadline)
}

func (c *botConn) close() {
	_ = c.conn.Close()
}

// New constructs a WeCom AI-bot adapter.
func New(
	cache tenant.ConfigCache,
	routeKeys []string,
	wsURL string,
) (*Adapter, error) {
	if cache == nil {
		return nil, errors.New("WeCom bot config cache is required")
	}
	if strings.TrimSpace(wsURL) == "" {
		wsURL = defaultWebSocketURL
	}
	return &Adapter{
		cache:      cache,
		wsURL:      strings.TrimRight(wsURL, "/"),
		routeKeys:  append([]string(nil), routeKeys...),
		dial:       dialWebSocket,
		heartbeat:  30 * time.Second,
		backoffMax: maxReconnectBackoff,
		conns:      make(map[string]*botConn),
	}, nil
}

func dialWebSocket(ctx context.Context, url string) (*botConn, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		// Compression stays off: the gateway does not negotiate it and the
		// extra Sec-WebSocket-Extensions header could affect routing.
		EnableCompression: false,
	}
	conn, _, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	return &botConn{conn: conn}, nil
}

// Type implements channels.Adapter.
func (*Adapter) Type() string { return channelType }

// Run starts one reconnecting connection loop per route key and returns
// immediately. No HTTP routes are registered because inbound traffic arrives
// over the outbound WebSocket.
func (a *Adapter) Run(ctx context.Context, _ *http.ServeMux, sink channels.Sink) error {
	if sink == nil {
		return errors.New("WeCom bot Gateway sink is required")
	}
	for _, routeKey := range a.routeKeys {
		routeKey := routeKey
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.connectLoop(ctx, routeKey, sink)
		}()
	}
	return nil
}

// NewReplier implements channels.Adapter.
func (a *Adapter) NewReplier(snapshot tenant.Snapshot) channels.Replier {
	return &replier{adapter: a, snapshot: snapshot}
}

// Wait blocks until every connection loop has stopped after cancellation.
func (a *Adapter) Wait() {
	a.wg.Wait()
}

// connection returns the active connection for one route key.
func (a *Adapter) connection(routeKey string) *botConn {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.conns[routeKey]
}

func (a *Adapter) register(routeKey string, conn *botConn) {
	a.mu.Lock()
	a.conns[routeKey] = conn
	a.mu.Unlock()
}

func (a *Adapter) unregister(routeKey string, conn *botConn) {
	a.mu.Lock()
	if a.conns[routeKey] == conn {
		delete(a.conns, routeKey)
	}
	a.mu.Unlock()
}

// connectLoop keeps one subscribed connection alive until ctx is canceled.
func (a *Adapter) connectLoop(ctx context.Context, routeKey string, sink channels.Sink) {
	backoff := initialReconnectDelay
	for ctx.Err() == nil {
		err := a.serveOnce(ctx, routeKey, sink)
		if err == nil {
			backoff = initialReconnectDelay
			continue
		}
		if backoff > a.backoffMax {
			backoff = a.backoffMax
		}
		log.Printf("wecombot route %s connection ended: %v; retrying in %s", routeKey, err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
		backoff *= 2
	}
}

// serveOnce dials, subscribes, and pumps callbacks until the connection ends.
func (a *Adapter) serveOnce(ctx context.Context, routeKey string, sink channels.Sink) error {
	snapshot, err := a.cache.ResolveBinding(ctx, channelType, routeKey)
	if err != nil {
		return fmt.Errorf("resolve wecombot binding: %w", err)
	}
	botID := snapshot.Binding.Config["bot_id"]
	secret, err := resolveSecretConfig(snapshot.Binding.Config, "bot_secret", true)
	if err != nil {
		return err
	}
	if botID == "" {
		return errors.New("wecombot bot_id config is required")
	}

	bot, err := a.dial(ctx, a.wsURL)
	if err != nil {
		return fmt.Errorf("dial wecombot websocket: %w", err)
	}
	// Unblock the read loop when the channel context is canceled because
	// gorilla ReadMessage cannot take a context.
	closed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			bot.close()
		case <-closed:
		}
	}()
	defer close(closed)

	reqID, err := newRequestID()
	if err != nil {
		bot.close()
		return err
	}
	if err := bot.send(subscribeFrame(reqID, botID, secret)); err != nil {
		bot.close()
		return fmt.Errorf("send wecombot subscribe: %w", err)
	}
	// Frames may interleave, so read until the matching acknowledgement.
	bot.setReadDeadline(time.Now().Add(subscribeTimeout))
	subscribed := false
	for !subscribed {
		data, readErr := bot.read()
		if readErr != nil {
			bot.close()
			return fmt.Errorf("await wecombot subscribe ack: %w", readErr)
		}
		var frame inboundFrame
		if err := frame.UnmarshalJSON(data); err != nil {
			continue
		}
		if frame.ReqID != reqID {
			continue
		}
		if frame.ErrCode != 0 {
			bot.close()
			return fmt.Errorf("%w: errcode=%d errmsg=%s", errSubscribeRejected, frame.ErrCode, frame.ErrMsg)
		}
		subscribed = true
	}
	bot.setReadDeadline(time.Time{})

	a.register(routeKey, bot)
	defer a.unregister(routeKey, bot)
	log.Printf("wecombot route %s connected and subscribed (bot_id=%s)", routeKey, botID)
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.heartbeatLoop(heartbeatCtx, bot)
	}()

	for {
		data, readErr := bot.read()
		if readErr != nil {
			bot.close()
			return fmt.Errorf("read wecombot frame: %w", readErr)
		}
		var frame inboundFrame
		if err := frame.UnmarshalJSON(data); err != nil {
			continue
		}
		if frame.Cmd != cmdMsgCallback || frame.Body.MsgType != "text" {
			continue
		}
		inbound, ok := a.toInbound(routeKey, frame)
		if !ok {
			continue
		}
		if _, err := sink(ctx, inbound); err != nil {
			log.Printf("wecombot route %s gateway error: %v", routeKey, err)
		}
	}
}

func (a *Adapter) toInbound(routeKey string, frame inboundFrame) (gateway.InboundMessage, bool) {
	if frame.Body.From.UserID == "" || frame.ReqID == "" {
		return gateway.InboundMessage{}, false
	}
	messageID := frame.Body.MsgID
	if messageID == "" {
		messageID = frame.ReqID
	}
	chatType := "p2p"
	addressed := true
	if frame.Body.ChatType == "group" {
		chatType = "group"
	}
	text := frame.Body.Text.Content
	if chatType == "group" {
		text = stripGroupMention(text)
	}
	return gateway.InboundMessage{
		Channel:        channelType,
		RouteKey:       routeKey,
		MsgID:          messageID,
		ChatType:       chatType,
		SenderID:       frame.Body.From.UserID,
		GroupID:        frame.Body.ChatID,
		AddressedToBot: addressed,
		Text:           text,
		Raw:            replyTarget{RouteKey: routeKey, ReqID: frame.ReqID},
		TraceID:        frame.ReqID,
	}, true
}

// stripGroupMention removes the leading @mention token from group texts such
// as "@RobotA hello robot" because group callbacks only fire when mentioned.
func stripGroupMention(text string) string {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "@") {
		return text
	}
	if index := strings.IndexAny(trimmed, " \t\n"); index > 0 {
		return strings.TrimSpace(trimmed[index+1:])
	}
	return trimmed
}

func (a *Adapter) heartbeatLoop(ctx context.Context, bot *botConn) {
	ticker := time.NewTicker(a.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reqID, err := newRequestID()
			if err != nil {
				log.Printf("wecombot heartbeat: %v", err)
				return
			}
			if err := bot.send(pingFrame(reqID)); err != nil {
				// A failed heartbeat means the connection is dead; closing it
				// unblocks the read loop which owns reconnection.
				bot.close()
				return
			}
		}
	}
}

type replyTarget struct {
	RouteKey string
	ReqID    string
}

func resolveSecretConfig(config map[string]string, key string, required bool) (string, error) {
	reference := config[key]
	if reference == "" {
		if required {
			return "", errors.New("required secret reference is missing")
		}
		return "", nil
	}
	return tenant.ResolveSecret(reference)
}
