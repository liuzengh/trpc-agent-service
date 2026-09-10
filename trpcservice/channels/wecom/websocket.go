package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// DefaultWebSocketURL is the endpoint documented by the WeCom AI Bot
// long-connection protocol.
const DefaultWebSocketURL = "wss://openws.work.weixin.qq.com"

const (
	defaultWebSocketReconnectInitial = time.Second
	defaultWebSocketReconnectMax     = 30 * time.Second
	defaultWebSocketHeartbeat        = 30 * time.Second
	defaultWebSocketAckTimeout       = 10 * time.Second
	defaultWebSocketHandlerQueueSize = 64
	defaultWebSocketHandlerWorkers   = 2
)

var (
	errWebSocketAlreadyStarted   = errors.New("wecom websocket client is already started")
	errWebSocketClosed           = errors.New("wecom websocket client is closed")
	errWebSocketHandlerQueueFull = errors.New("wecom inbound handler queue is full")
)

// MessageHandler receives an authenticated message frame from WeCom.
type MessageHandler func(context.Context, Message) error

// ClientOption configures the official WeCom protocol client.
type ClientOption func(*clientConfig) error

type clientConfig struct {
	url              string
	dialer           *websocket.Dialer
	reconnectInitial time.Duration
	reconnectMax     time.Duration
	heartbeat        time.Duration
	ackTimeout       time.Duration
	handler          MessageHandler
	onError          func(error)
	handlerQueueSize int
}

// WithWebSocketURL is intended for a protocol-compatible test endpoint. The
// production default is the official WeCom endpoint.
func WithWebSocketURL(value string) ClientOption {
	return func(config *clientConfig) error {
		if value == "" {
			return errors.New("wecom websocket url is required")
		}
		config.url = value
		return nil
	}
}

// WithWebSocketDialer supplies a controlled dialer for tests or transport policy.
func WithWebSocketDialer(dialer *websocket.Dialer) ClientOption {
	return func(config *clientConfig) error {
		if dialer == nil {
			return errors.New("wecom websocket dialer is required")
		}
		config.dialer = dialer
		return nil
	}
}

// WithWebSocketReconnectDelay configures the bounded client reconnect backoff.
func WithWebSocketReconnectDelay(initial, maximum time.Duration) ClientOption {
	return func(config *clientConfig) error {
		if initial <= 0 || maximum < initial {
			return errors.New("wecom reconnect delay is invalid")
		}
		config.reconnectInitial = initial
		config.reconnectMax = maximum
		return nil
	}
}

// WithMessageHandler registers the inbound message handler.
func WithMessageHandler(handler MessageHandler) ClientOption {
	return func(config *clientConfig) error {
		config.handler = handler
		return nil
	}
}

// WithMessageQueueSize bounds authenticated callbacks waiting for the
// adapter. The read loop never performs provider media downloads or durable
// admission itself.
func WithMessageQueueSize(size int) ClientOption {
	return func(config *clientConfig) error {
		if size <= 0 {
			return errors.New("wecom message queue size must be positive")
		}
		config.handlerQueueSize = size
		return nil
	}
}

// WithOnError receives non-fatal handler errors. Provider payloads are not
// included in the error text by this package.
func WithOnError(handler func(error)) ClientOption {
	return func(config *clientConfig) error {
		config.onError = handler
		return nil
	}
}

// WebSocketClient implements the official WeCom AI Bot frames:
// aibot_subscribe, aibot_msg_callback/aibot_event_callback, ping, and
// aibot_send_msg. It owns only connection lifecycle and request acknowledgments.
type WebSocketClient struct {
	botID     string
	botSecret string
	config    clientConfig

	startMu sync.Mutex
	started bool
	closed  bool
	done    chan struct{}

	stateMu       sync.Mutex
	conn          *websocket.Conn
	authenticated bool
	runCancel     context.CancelFunc
	pending       map[string]chan requestResult
	writeMu       sync.Mutex
	messageQueue  chan queuedMessage
}

type queuedMessage struct {
	ctx     context.Context
	message Message
	cmd     string
	body    []byte
}

type requestResult struct {
	errCode int
	errMsg  string
	err     error
}

type protocolError struct {
	code int
	msg  string
}

func (e *protocolError) Error() string {
	if e == nil {
		return "wecom provider protocol error"
	}
	if e.code != 0 {
		return fmt.Sprintf("wecom provider returned code %d", e.code)
	}
	return "wecom provider protocol error"
}

// NewClient creates a WeCom AI Bot protocol client. Bot ID and Bot Secret are
// the only provider credentials required by the official long connection.
func NewClient(botID, botSecret string, opts ...ClientOption) (*WebSocketClient, error) {
	if botID == "" {
		return nil, errors.New("wecom bot id is required")
	}
	if botSecret == "" {
		return nil, errors.New("wecom bot secret is required")
	}
	config := clientConfig{
		url:              DefaultWebSocketURL,
		dialer:           websocket.DefaultDialer,
		reconnectInitial: defaultWebSocketReconnectInitial,
		reconnectMax:     defaultWebSocketReconnectMax,
		heartbeat:        defaultWebSocketHeartbeat,
		ackTimeout:       defaultWebSocketAckTimeout,
		handlerQueueSize: defaultWebSocketHandlerQueueSize,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&config); err != nil {
			return nil, err
		}
	}
	return &WebSocketClient{
		botID:        botID,
		botSecret:    botSecret,
		config:       config,
		done:         make(chan struct{}),
		pending:      make(map[string]chan requestResult),
		messageQueue: make(chan queuedMessage, config.handlerQueueSize),
	}, nil
}

// Run owns the client lifecycle until ctx is canceled. Connection loss is
// recovered with bounded reconnect backoff; no second job/retry system exists.
func (c *WebSocketClient) Run(ctx context.Context) error {
	if c == nil {
		return errors.New("wecom websocket client is nil")
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	c.startMu.Lock()
	if c.started {
		c.startMu.Unlock()
		return errWebSocketAlreadyStarted
	}
	if c.closed {
		c.startMu.Unlock()
		return errWebSocketClosed
	}
	c.started = true
	c.startMu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	handlerDone := make(chan struct{})
	go c.dispatchMessages(runCtx, handlerDone)
	c.stateMu.Lock()
	c.runCancel = cancel
	c.stateMu.Unlock()
	defer func() {
		cancel()
		<-handlerDone
		c.clearConnection(nil)
		c.failPending(errWebSocketClosed)
		c.stateMu.Lock()
		c.runCancel = nil
		c.stateMu.Unlock()
		close(c.done)
	}()

	delay := c.config.reconnectInitial
	for {
		if err := runCtx.Err(); err != nil {
			return err
		}
		conn, _, err := c.config.dialer.DialContext(runCtx, c.config.url, http.Header{})
		if err != nil {
			if err := waitWebSocketBackoff(runCtx, delay); err != nil {
				return err
			}
			delay = nextBackoff(delay, c.config.reconnectMax)
			continue
		}
		c.setConnection(conn)
		err = c.runConnection(runCtx, conn)
		c.clearConnection(conn)
		c.failPending(err)
		if runCtx.Err() != nil {
			return runCtx.Err()
		}
		delay = c.config.reconnectInitial
		if err := waitWebSocketBackoff(runCtx, delay); err != nil {
			return err
		}
	}
}

func (c *WebSocketClient) runConnection(ctx context.Context, conn *websocket.Conn) error {
	authReqID := uuid.NewString()
	if err := c.writeFrame(conn, "aibot_subscribe", authReqID, map[string]string{
		"bot_id": c.botID,
		"secret": c.botSecret,
	}); err != nil {
		return err
	}
	if err := conn.SetReadDeadline(time.Now().Add(c.config.ackTimeout)); err != nil {
		return err
	}
	authenticated := false
	connectionCtx, cancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go c.heartbeat(connectionCtx, conn, heartbeatDone)
	defer func() {
		cancel()
		<-heartbeatDone
	}()
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var frame protocolFrame
		if err := json.Unmarshal(payload, &frame); err != nil {
			return fmt.Errorf("decode wecom websocket frame: %w", err)
		}
		if frame.Headers.ReqID != "" {
			if frame.Headers.ReqID == authReqID {
				if frame.ErrCode != 0 {
					return &protocolError{code: frame.ErrCode, msg: frame.ErrMsg}
				}
				authenticated = true
				c.markAuthenticated(conn)
				if err := conn.SetReadDeadline(time.Time{}); err != nil {
					return err
				}
				continue
			}
			if c.resolvePending(frame) {
				continue
			}
		}
		if !authenticated {
			continue
		}
		if frame.Cmd != "aibot_msg_callback" && frame.Cmd != "aibot_event_callback" {
			continue
		}
		var message Message
		if err := json.Unmarshal(frame.Body, &message); err != nil {
			return fmt.Errorf("decode wecom message frame: %w", err)
		}
		message.ResponseRequestID = frame.Headers.ReqID
		if frame.Cmd == "aibot_event_callback" && message.MessageType == "" {
			message.MessageType = "event"
		}
		if c.config.handler != nil {
			if err := c.enqueueMessage(queuedMessage{
				ctx: ctx, message: message, cmd: frame.Cmd, body: append([]byte(nil), frame.Body...),
			}); err != nil {
				return err
			}
		}
	}
}

func (c *WebSocketClient) dispatchMessages(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	var workers sync.WaitGroup
	workers.Add(defaultWebSocketHandlerWorkers)
	for range defaultWebSocketHandlerWorkers {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item := <-c.messageQueue:
					if c.config.handler == nil {
						continue
					}
					if err := c.config.handler(item.ctx, item.message); err != nil && c.config.onError != nil {
						c.config.onError(fmt.Errorf("%w (wecom frame shape: %s)", err, safeMessageShape(item.cmd, item.body)))
					}
				}
			}
		}()
	}
	workers.Wait()
}

func (c *WebSocketClient) enqueueMessage(item queuedMessage) error {
	select {
	case c.messageQueue <- item:
		return nil
	default:
		if c.config.onError != nil {
			c.config.onError(fmt.Errorf("wecom inbound handler queue is full (wecom frame shape: %s)", safeMessageShape(item.cmd, item.body)))
		}
		return errWebSocketHandlerQueueFull
	}
}

// safeMessageShape returns protocol structure only. It intentionally omits
// all values because message bodies may contain credentials, URLs, IDs, or
// encrypted media keys. It is used to diagnose provider payload mismatches
// without turning provider traffic into application logs.
func safeMessageShape(cmd string, body []byte) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return fmt.Sprintf("cmd=%s body=invalid-json", cmd)
	}

	parts := []string{
		"cmd=" + cmd,
		"top_keys=" + sortedJSONKeys(fields),
	}
	for _, field := range []string{"from", "file", "image", "text", "mixed", "stream", "event"} {
		value, ok := fields[field]
		if !ok {
			continue
		}
		parts = append(parts, field+"="+safeJSONValueShape(value))
	}
	return strings.Join(parts, " ")
}

func safeJSONValueShape(value json.RawMessage) string {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(value, &object); err == nil && object != nil {
		return "object_keys=" + sortedJSONKeys(object)
	}
	trimmed := strings.TrimSpace(string(value))
	if len(trimmed) == 0 {
		return "empty"
	}
	switch trimmed[0] {
	case '[':
		return "array"
	case '"':
		return "string"
	case 'n':
		return "null"
	default:
		return "scalar"
	}
}

func sortedJSONKeys(fields map[string]json.RawMessage) string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func (c *WebSocketClient) heartbeat(ctx context.Context, conn *websocket.Conn, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(c.config.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = conn.Close()
			return
		case <-ticker.C:
			if err := c.writeFrame(conn, "ping", uuid.NewString(), nil); err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}

// SendMessage sends one delayed Reply Outbox message using the official
// aibot_send_msg command. The caller owns retry and attempt persistence.
func (c *WebSocketClient) SendMessage(ctx context.Context, chatID, text string) (string, error) {
	if c == nil {
		return "", errors.New("wecom websocket client is nil")
	}
	if ctx == nil {
		return "", errors.New("context is required")
	}
	if chatID == "" || text == "" {
		return "", errors.New("wecom chat id and message text are required")
	}
	requestID := uuid.NewString()
	err := c.startInBackground()
	if err != nil {
		return "", err
	}
	err = c.waitForAuthentication(ctx)
	if err != nil {
		return "", err
	}
	return c.sendRequest(ctx, "aibot_send_msg", requestID, map[string]any{
		"chatid":   chatID,
		"msgtype":  "markdown",
		"markdown": map[string]string{"content": text},
	})
}

// SendStream sends one WeCom stream frame. The first frame uses
// aibot_respond_msg; later frames use aibot_respond_update_msg and keep the
// original callback request ID so the provider can associate the response
// with the inbound message.
func (c *WebSocketClient) SendStream(
	ctx context.Context,
	destination, callbackRequestID, streamID, content string,
	finish, update bool,
) (string, error) {
	if c == nil {
		return "", errors.New("wecom websocket client is nil")
	}
	if ctx == nil {
		return "", errors.New("context is required")
	}
	if destination == "" || callbackRequestID == "" || streamID == "" || content == "" {
		return "", errors.New("wecom stream target, stream id, and content are required")
	}
	if err := c.startInBackground(); err != nil {
		return "", err
	}
	if err := c.waitForAuthentication(ctx); err != nil {
		return "", err
	}
	command := "aibot_respond_msg"
	if update {
		command = "aibot_respond_update_msg"
	}
	return c.sendRequest(ctx, command, callbackRequestID, map[string]any{
		"msgtype": "stream",
		"stream": map[string]any{
			"id":      streamID,
			"content": content,
			"finish":  finish,
		},
	})
}

// SendCard sends a provider-native WeCom template card using the delayed
// outbound command. Card updates are represented by a new durable card
// operation when a provider does not expose an update primitive.
func (c *WebSocketClient) SendCard(ctx context.Context, destination string, card map[string]any) (string, error) {
	if c == nil {
		return "", errors.New("wecom websocket client is nil")
	}
	if ctx == nil {
		return "", errors.New("context is required")
	}
	if destination == "" || card == nil {
		return "", errors.New("wecom card destination and payload are required")
	}
	if err := c.startInBackground(); err != nil {
		return "", err
	}
	requestID := uuid.NewString()
	if err := c.waitForAuthentication(ctx); err != nil {
		return "", err
	}
	return c.sendRequest(ctx, "aibot_send_msg", requestID, map[string]any{
		"chatid":        destination,
		"msgtype":       "template_card",
		"template_card": card,
	})
}

func (c *WebSocketClient) waitForAuthentication(ctx context.Context) error {
	for {
		if c.isAuthenticated() {
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *WebSocketClient) sendRequest(ctx context.Context, command, requestID string, body any) (string, error) {
	if requestID == "" {
		return "", errors.New("wecom request id is required")
	}
	resultCh := make(chan requestResult, 1)
	c.addPending(requestID, resultCh)
	if err := c.writeCurrentFrame(command, requestID, body); err != nil {
		c.removePending(requestID)
		return "", err
	}
	select {
	case <-ctx.Done():
		c.removePending(requestID)
		return "", ctx.Err()
	case result := <-resultCh:
		if result.err != nil {
			return "", result.err
		}
		if result.errCode != 0 {
			return "", &protocolError{code: result.errCode, msg: result.errMsg}
		}
		return requestID, nil
	}
}

func (c *WebSocketClient) startInBackground() error {
	c.startMu.Lock()
	closed := c.closed
	started := c.started
	c.startMu.Unlock()
	if closed {
		return errWebSocketClosed
	}
	if !started {
		go func() { _ = c.Run(context.Background()) }()
	}
	return nil
}

// Close cancels the run and waits for the connection loop to exit.
func (c *WebSocketClient) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	c.startMu.Lock()
	c.closed = true
	started := c.started
	c.startMu.Unlock()
	if !started {
		return nil
	}
	c.stateMu.Lock()
	if c.runCancel != nil {
		c.runCancel()
	}
	conn := c.conn
	c.stateMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *WebSocketClient) setConnection(conn *websocket.Conn) {
	c.stateMu.Lock()
	c.conn = conn
	c.authenticated = false
	c.stateMu.Unlock()
}

func (c *WebSocketClient) clearConnection(expected *websocket.Conn) {
	c.stateMu.Lock()
	if expected == nil || c.conn == expected {
		c.conn = nil
		c.authenticated = false
	}
	c.stateMu.Unlock()
}

func (c *WebSocketClient) markAuthenticated(conn *websocket.Conn) {
	c.stateMu.Lock()
	if c.conn == conn {
		c.authenticated = true
	}
	c.stateMu.Unlock()
}

func (c *WebSocketClient) isAuthenticated() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.authenticated && c.conn != nil
}

func (c *WebSocketClient) writeCurrentFrame(cmd, reqID string, body any) error {
	c.stateMu.Lock()
	conn := c.conn
	authenticated := c.authenticated
	c.stateMu.Unlock()
	if conn == nil || !authenticated {
		return errors.New("wecom websocket is not connected")
	}
	return c.writeFrame(conn, cmd, reqID, body)
}

func (c *WebSocketClient) writeFrame(conn *websocket.Conn, cmd, reqID string, body any) error {
	payload, err := json.Marshal(protocolFrame{Cmd: cmd, Headers: frameHeaders{ReqID: reqID}, Body: bodyBytes(body)})
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func bodyBytes(body any) json.RawMessage {
	if body == nil {
		return nil
	}
	value, err := json.Marshal(body)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return value
}

type protocolFrame struct {
	Cmd     string          `json:"cmd,omitempty"`
	Headers frameHeaders    `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode int             `json:"errcode,omitempty"`
	ErrMsg  string          `json:"errmsg,omitempty"`
}

type frameHeaders struct {
	ReqID string `json:"req_id"`
}

func (c *WebSocketClient) addPending(reqID string, result chan requestResult) {
	c.stateMu.Lock()
	c.pending[reqID] = result
	c.stateMu.Unlock()
}

func (c *WebSocketClient) removePending(reqID string) {
	c.stateMu.Lock()
	delete(c.pending, reqID)
	c.stateMu.Unlock()
}

func (c *WebSocketClient) resolvePending(frame protocolFrame) bool {
	c.stateMu.Lock()
	result, ok := c.pending[frame.Headers.ReqID]
	if ok {
		delete(c.pending, frame.Headers.ReqID)
	}
	c.stateMu.Unlock()
	if !ok {
		return false
	}
	result <- requestResult{errCode: frame.ErrCode, errMsg: frame.ErrMsg, err: nil}
	return true
}

func (c *WebSocketClient) failPending(err error) {
	c.stateMu.Lock()
	pending := c.pending
	c.pending = make(map[string]chan requestResult)
	c.stateMu.Unlock()
	for _, result := range pending {
		result <- requestResult{err: err}
	}
}

func waitWebSocketBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

var _ interface {
	Run(context.Context) error
	Close(context.Context) error
} = (*WebSocketClient)(nil)
