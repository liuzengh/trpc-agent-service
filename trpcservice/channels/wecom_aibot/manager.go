package wecom_aibot

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	defaultWSURL           = "wss://openws.work.weixin.qq.com"
	defaultReplyACKTimeout = 30 * time.Second
	defaultAuthACKTimeout  = 30 * time.Second
	maxMissedHeartbeats    = 2
)

// OutboxLeaseDuration reserves the acknowledgement deadline and a separate
// fenced receipt-commit window for durable final replies.
const OutboxLeaseDuration = 2 * defaultReplyACKTimeout

var errConnectionReplaced = errors.New("wecom ai bot connection replaced")

// Conn is the narrow WebSocket surface owned by Manager. It permits protocol
// tests to inject a fake connection while keeping one writer per socket.
type Conn interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetPongHandler(func(string) error)
	Close() error
}

// Dialer establishes the narrow WebSocket connection owned by a Manager.
type Dialer interface {
	DialContext(context.Context, string, http.Header) (Conn, error)
}

type websocketDialer struct{ dialer websocket.Dialer }

func (d websocketDialer) DialContext(ctx context.Context, url string, header http.Header) (Conn, error) {
	c, _, err := d.dialer.DialContext(ctx, url, header)
	return c, err
}

// Config wires one tenant-scoped AI Bot connection to the protocol-neutral
// Dispatcher. Secret is resolved by the owner and is never logged.
type Config struct {
	BotID             string
	Secret            string
	WSURL             string
	Target            channels.RoutingTarget
	Dispatcher        gateway.DispatchService
	Dialer            Dialer
	QueueSize         int
	HeartbeatInterval time.Duration
	ReconnectBase     time.Duration
	ReconnectMax      time.Duration
	ExecutionTimeout  time.Duration
}

// Manager owns a single long-lived connection and all of its pumps.
type Manager struct {
	botID, secret, wsURL                                     string
	target                                                   channels.RoutingTarget
	dispatcher                                               gateway.DispatchService
	dialer                                                   Dialer
	queueSize                                                int
	heartbeat, reconnectBase, reconnectMax, executionTimeout time.Duration
	replyACKTimeout                                          time.Duration
	authACKTimeout                                           time.Duration

	mu               sync.Mutex
	conn             Conn
	queue            chan outboundFrame
	pending          *pendingReply
	authReqID        string
	authDone         chan struct{}
	heartbeatReqID   string
	missedHeartbeats int
	closing          bool
	runCancel        context.CancelFunc
	runDone          chan struct{}
	ready            bool
	drains           sync.WaitGroup
}

type outboundFrame struct {
	context    context.Context
	data       []byte
	replyReqID string
	ack        chan error
	done       chan struct{}
}

type pendingReply struct {
	reqID string
	ack   chan error
	done  chan struct{}
}

// New creates a connection manager from an already trusted routing target.
func New(config Config) (*Manager, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	return &Manager{botID: config.BotID, secret: config.Secret, wsURL: config.WSURL, target: config.Target, dispatcher: config.Dispatcher, dialer: config.Dialer, queueSize: config.QueueSize, heartbeat: config.HeartbeatInterval, reconnectBase: config.ReconnectBase, reconnectMax: config.ReconnectMax, executionTimeout: config.ExecutionTimeout, replyACKTimeout: defaultReplyACKTimeout}, nil
}

func normalizeConfig(config Config) (Config, error) {
	if strings.TrimSpace(config.BotID) == "" || strings.TrimSpace(config.Secret) == "" || config.Dispatcher == nil {
		return Config{}, ErrInvalid
	}
	if err := config.Target.Validate(); err != nil || config.Target.Channel != channels.ChannelWeComAIBot {
		return Config{}, ErrInvalid
	}
	config = withDefaults(config)
	wsURL, err := normalizeWebSocketURL(config.WSURL)
	if err != nil {
		return Config{}, ErrInvalid
	}
	config.BotID = strings.TrimSpace(config.BotID)
	config.WSURL = wsURL
	return config, nil
}

func withDefaults(config Config) Config {
	if config.WSURL == "" {
		config.WSURL = defaultWSURL
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 128
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 30 * time.Second
	}
	if config.ReconnectBase <= 0 {
		config.ReconnectBase = time.Second
	}
	if config.ReconnectMax <= 0 {
		config.ReconnectMax = time.Minute
	}
	if config.ExecutionTimeout <= 0 {
		config.ExecutionTimeout = 4 * time.Minute
	}
	if config.Dialer == nil {
		config.Dialer = websocketDialer{dialer: websocket.Dialer{HandshakeTimeout: 10 * time.Second}}
	}
	return config
}

func normalizeWebSocketURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "wss" || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", ErrInvalid
	}
	return strings.TrimRight(value, "/"), nil
}

// Channel returns the fixed channel type served by this manager.
func (m *Manager) Channel() channels.Channel { return channels.ChannelWeComAIBot }

// Ready reports whether the current connection has authenticated and remains open.
func (m *Manager) Ready() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ready && !m.closing
}

// Run reconnects and serves the AI Bot connection until the context is canceled.
func (m *Manager) Run(ctx context.Context) error {
	if m == nil || ctx == nil {
		return ErrInvalid
	}
	m.mu.Lock()
	if m.runCancel != nil {
		m.mu.Unlock()
		return ErrClosed
	}
	if m.closing {
		m.mu.Unlock()
		return ErrClosed
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.runCancel, m.runDone = cancel, make(chan struct{})
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.runCancel = nil
		done := m.runDone
		m.runDone = nil
		m.ready = false
		m.mu.Unlock()
		if done != nil {
			close(done)
		}
	}()
	var attempt int
	for {
		if err := runCtx.Err(); err != nil {
			return err
		}
		conn, err := m.dialer.DialContext(runCtx, m.wsURL, nil)
		if err != nil {
			packageLog.Warn("AI Bot websocket dial failed", zap.String("error_type", "websocket_dial"), zap.String("error_detail", err.Error()))
			if !sleepBackoff(runCtx, m.reconnectBase, m.reconnectMax, attempt) {
				return runCtx.Err()
			}
			attempt++
			continue
		}
		attempt = 0
		if err := m.serveConnection(runCtx, conn); err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				packageLog.Warn("AI Bot connection stopped", zap.String("error_type", "connection_stopped"), zap.String("error_detail", err.Error()))
			}
			_ = conn.Close()
			if errors.Is(err, errConnectionReplaced) {
				return nil
			}
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			if !sleepBackoff(runCtx, m.reconnectBase, m.reconnectMax, attempt) {
				return runCtx.Err()
			}
			attempt++
		}
		if runCtx.Err() != nil {
			return runCtx.Err()
		}
	}
}

func sleepBackoff(ctx context.Context, base, max time.Duration, attempt int) bool {
	d := base * time.Duration(1<<min(attempt, 6))
	if d > max {
		d = max
	}
	jitter := randomizedJitter(d / 4)
	timer := time.NewTimer(d - d/8 + jitter)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func randomizedJitter(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(limit)+1))
	if err != nil {
		return 0
	}
	return time.Duration(value.Int64())
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (m *Manager) serveConnection(ctx context.Context, conn Conn) error {
	if conn == nil {
		return ErrClosed
	}
	m.mu.Lock()
	m.conn = conn
	m.ready = false
	m.authDone = make(chan struct{})
	m.heartbeatReqID = ""
	m.missedHeartbeats = 0
	authDone := m.authDone
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if m.conn == conn {
			m.conn = nil
			m.ready = false
			m.authDone = nil
			m.heartbeatReqID = ""
			m.missedHeartbeats = 0
		}
		m.mu.Unlock()
	}()
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan outboundFrame, m.queueSize)
	m.mu.Lock()
	m.queue = queue
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if m.queue == queue {
			m.queue = nil
		}
		m.mu.Unlock()
	}()
	writeErr := make(chan error, 1)
	go m.writePump(connCtx, conn, queue, writeErr)
	if err := m.enqueueAuth(connCtx, queue); err != nil {
		return err
	}
	readErr := make(chan error, 1)
	go func() { readErr <- m.readPump(connCtx, conn, queue) }()
	authTimer := time.NewTimer(m.authenticationTimeout())
	defer authTimer.Stop()
	select {
	case <-authDone:
	case err := <-readErr:
		return err
	case err := <-writeErr:
		return err
	case <-authTimer.C:
		packageLog.Warn("AI Bot authentication timed out", zap.String("bot_id", m.botID), zap.String("error_type", "authentication_timeout"))
		return ErrAuthentication
	case <-connCtx.Done():
		return connCtx.Err()
	}
	select {
	case err := <-readErr:
		return err
	case err := <-writeErr:
		return err
	case <-connCtx.Done():
		return connCtx.Err()
	}
}

func (m *Manager) sendReply(ctx context.Context, reqID string, body StreamReply) error {
	if ctx == nil || strings.TrimSpace(reqID) == "" {
		return ErrInvalid
	}
	data, err := encodeFrame(Frame{Cmd: cmdRespond, Headers: Headers{ReqID: reqID}, Body: mustJSON(body)})
	if err != nil {
		return err
	}
	replyCtx, cancel := context.WithTimeout(ctx, m.replyAcknowledgementTimeout())
	defer cancel()
	m.mu.Lock()
	queue, closing := m.queue, m.closing
	if closing || queue == nil {
		m.mu.Unlock()
		return ErrNotReady
	}
	ack := make(chan error, 1)
	done := make(chan struct{})
	select {
	case queue <- outboundFrame{context: replyCtx, data: data, replyReqID: reqID, ack: ack, done: done}:
		m.mu.Unlock()
		select {
		case err := <-ack:
			return err
		case <-replyCtx.Done():
			m.clearPendingReplyFor(reqID, ErrAcknowledgementTimeout)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrAcknowledgementTimeout
		}
	case <-ctx.Done():
		m.mu.Unlock()
		return ctx.Err()
	default:
		m.mu.Unlock()
		return ErrQueueFull
	}
}

func (m *Manager) replyAcknowledgementTimeout() time.Duration {
	if m.replyACKTimeout <= 0 {
		return defaultReplyACKTimeout
	}
	return m.replyACKTimeout
}

func (m *Manager) authenticationTimeout() time.Duration {
	if m.authACKTimeout <= 0 {
		return defaultAuthACKTimeout
	}
	return m.authACKTimeout
}

func (m *Manager) writePump(ctx context.Context, conn Conn, queue <-chan outboundFrame, errs chan<- error) {
	defer func() {
		m.clearPendingReply(ErrClosed)
		drainOutboundReplies(queue, ErrClosed)
	}()
	for {
		select {
		case <-ctx.Done():
			errs <- ErrClosed
			return
		case outbound := <-queue:
			if outbound.data == nil {
				errs <- nil
				return
			}
			if outbound.context != nil && outbound.context.Err() != nil {
				if outbound.ack != nil {
					outbound.ack <- ErrAcknowledgementTimeout
				}
				continue
			}
			if outbound.replyReqID != "" {
				if !m.setPendingReply(outbound.replyReqID, outbound.ack, outbound.done) {
					outbound.ack <- ErrClosed
					continue
				}
			}
			if outbound.context != nil && outbound.context.Err() != nil {
				m.clearPendingReplyFor(outbound.replyReqID, ErrAcknowledgementTimeout)
				continue
			}
			if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				m.clearPendingReply(ErrClosed)
				errs <- ErrClosed
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, outbound.data); err != nil {
				_ = conn.Close()
				m.clearPendingReply(ErrClosed)
				errs <- ErrClosed
				return
			}
			if outbound.replyReqID != "" {
				select {
				case <-ctx.Done():
					m.clearPendingReply(ErrClosed)
					errs <- ErrClosed
					return
				case <-outbound.done:
				}
			}
		}
	}
}

func drainOutboundReplies(queue <-chan outboundFrame, err error) {
	for {
		select {
		case outbound := <-queue:
			if outbound.ack != nil {
				outbound.ack <- err
			}
		default:
			return
		}
	}
}

func (m *Manager) enqueueAuth(ctx context.Context, queue chan<- outboundFrame) error {
	reqID := cmdSubscribe + "-" + uuid.NewString()
	data, err := encodeFrame(Frame{Cmd: cmdSubscribe, Headers: Headers{ReqID: reqID}, Body: mustJSON(struct {
		BotID  string `json:"bot_id"`
		Secret string `json:"secret"`
	}{m.botID, m.secret})})
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.authReqID = reqID
	m.mu.Unlock()
	select {
	case queue <- outboundFrame{data: data}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) readPump(ctx context.Context, conn Conn, queue chan<- outboundFrame) error {
	reads := make(chan inboundReadResult, 1)
	go readInboundFrames(ctx, conn, reads)
	heartbeats := time.NewTicker(m.heartbeat)
	defer heartbeats.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeats.C:
			if err := m.scheduleHeartbeat(conn, queue); err != nil {
				return err
			}
		case result := <-reads:
			if result.err != nil {
				return ErrClosed
			}
			frame, err := decodeFrame(result.data)
			if err != nil {
				continue
			}
			if err := m.handleInboundFrame(ctx, frame); err != nil {
				return err
			}
		}
	}
}

type inboundReadResult struct {
	data []byte
	err  error
}

func readInboundFrames(ctx context.Context, conn Conn, reads chan<- inboundReadResult) {
	for {
		_, data, err := conn.ReadMessage()
		select {
		case reads <- inboundReadResult{data: data, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (m *Manager) scheduleHeartbeat(conn Conn, queue chan<- outboundFrame) error {
	reqID := cmdPing + "-" + uuid.NewString()
	m.mu.Lock()
	if m.conn != conn || !m.ready {
		m.mu.Unlock()
		return nil
	}
	if m.heartbeatReqID != "" {
		m.missedHeartbeats++
		if m.missedHeartbeats >= maxMissedHeartbeats {
			m.ready = false
			m.heartbeatReqID = ""
			m.missedHeartbeats = 0
			m.mu.Unlock()
			_ = conn.Close()
			return ErrClosed
		}
	}
	m.heartbeatReqID = reqID
	m.mu.Unlock()
	if err := sendHeartbeat(queue, reqID); err != nil {
		m.clearHeartbeat(reqID)
		return err
	}
	return nil
}

func sendHeartbeat(queue chan<- outboundFrame, reqID string) error {
	ping, err := encodeFrame(Frame{Cmd: cmdPing, Headers: Headers{ReqID: reqID}})
	if err != nil {
		return err
	}
	select {
	case queue <- outboundFrame{data: ping}:
		return nil
	default:
		return ErrQueueFull
	}
}

func (m *Manager) handleInboundFrame(ctx context.Context, frame Frame) error {
	// The AI Bot protocol's subscribe acknowledgement is a response frame. In
	// deployments it may echo the request command or omit it; the request ID is
	// the authoritative correlation key. Handle it before command dispatch so
	// either wire representation can complete authentication.
	if m.isAuthResponse(frame.Headers.ReqID) {
		return m.acceptAuthentication(frame)
	}
	if frame.Cmd == "" && m.acknowledgeHeartbeat(frame) {
		return nil
	}
	switch frame.Cmd {
	case cmdCallback:
		m.dispatchCallback(ctx, frame)
	case cmdEventCallback:
		return m.handleEventCallback(frame)
	case "":
		m.acknowledgeReply(frame)
	}
	return nil
}

func (m *Manager) handleEventCallback(frame Frame) error {
	var event Event
	if unmarshalBody(frame.Body, &event) != nil || event.AIBotID != m.botID || event.Event.EventType != "disconnected_event" {
		return nil
	}
	m.mu.Lock()
	m.ready = false
	m.mu.Unlock()
	m.clearPendingReply(ErrClosed)
	return errConnectionReplaced
}

func (m *Manager) acceptAuthentication(frame Frame) error {
	if frame.ErrCode == nil || *frame.ErrCode != 0 {
		fields := []zap.Field{zap.String("bot_id", m.botID), zap.String("error_type", "authentication_rejected")}
		if frame.ErrCode != nil {
			fields = append(fields, zap.Int("remote_code", *frame.ErrCode))
		}
		if strings.TrimSpace(frame.ErrMsg) != "" {
			fields = append(fields, zap.String("remote_message", frame.ErrMsg))
		}
		packageLog.Warn("AI Bot authentication rejected", fields...)
		return ErrAuthentication
	}
	m.mu.Lock()
	m.ready = true
	m.heartbeatReqID = ""
	m.missedHeartbeats = 0
	done := m.authDone
	m.authDone = nil
	m.mu.Unlock()
	if done != nil {
		close(done)
	}
	return nil
}

func (m *Manager) dispatchCallback(ctx context.Context, frame Frame) {
	if !m.Ready() || !m.beginDrain() {
		return
	}
	go func() {
		defer m.drains.Done()
		m.handleCallback(ctx, frame)
	}()
}

func (m *Manager) acknowledgeReply(frame Frame) {
	if frame.ErrCode != nil && *frame.ErrCode != 0 {
		m.completeReplyAck(frame.Headers.ReqID, ErrClosed)
		return
	}
	m.completeReplyAck(frame.Headers.ReqID, nil)
}

func (m *Manager) acknowledgeHeartbeat(frame Frame) bool {
	m.mu.Lock()
	if frame.Headers.ReqID == "" || frame.Headers.ReqID != m.heartbeatReqID {
		m.mu.Unlock()
		return false
	}
	m.heartbeatReqID = ""
	if frame.ErrCode == nil || *frame.ErrCode == 0 {
		m.missedHeartbeats = 0
		m.mu.Unlock()
		return true
	}
	m.ready = false
	m.missedHeartbeats = 0
	conn := m.conn
	m.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	return true
}

func (m *Manager) clearHeartbeat(reqID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.heartbeatReqID == reqID {
		m.heartbeatReqID = ""
	}
}

func (m *Manager) isAuthResponse(reqID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return reqID != "" && reqID == m.authReqID
}

func (m *Manager) handleCallback(parent context.Context, frame Frame) {
	var message Message
	if jsonErr := unmarshalBody(frame.Body, &message); jsonErr != nil || message.MsgID == "" || message.AIBotID != m.botID || message.MsgType != "text" || strings.TrimSpace(message.From.UserID) == "" || strings.TrimSpace(message.Text.Content) == "" {
		return
	}
	inbound := gateway.InboundMessage{Content: message.Text.Content, ContentType: gateway.ContentTypeText, ExternalMessageID: message.MsgID, ExternalUserID: message.From.UserID, ConversationKind: channels.ConversationDirect, ExternalPeerID: message.From.UserID}
	if message.ChatID != "" {
		inbound.ConversationKind = channels.ConversationGroup
		inbound.ExternalChatID = message.ChatID
		inbound.ExternalPeerID = ""
	}
	ctx, cancel := context.WithTimeout(parent, m.executionTimeout)
	defer cancel()
	stream, err := m.dispatcher.Dispatch(ctx, gateway.DispatchRequest{Principal: mustPrincipal(m.target), RequestID: frame.Headers.ReqID, Message: inbound})
	if err != nil {
		logCallbackFailure("callback dispatch failed", frame.Headers.ReqID, "dispatch_failed", err)
		return
	}
	if stream == nil {
		logCallbackNilStream(frame.Headers.ReqID)
		return
	}
	for event := range stream {
		if event.Type != gateway.DispatchEventMessage || strings.TrimSpace(event.Text) == "" {
			continue
		}
		body := StreamReply{MsgType: "stream", Stream: struct {
			ID      string `json:"id"`
			Finish  bool   `json:"finish"`
			Content string `json:"content,omitempty"`
		}{ID: frame.Headers.ReqID, Content: event.Text, Finish: false}}
		if err := m.sendReply(ctx, frame.Headers.ReqID, body); err != nil {
			logCallbackFailure("callback reply failed", frame.Headers.ReqID, "reply_failed", err)
			return
		}
	}
}

func (m *Manager) beginDrain() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return false
	}
	m.drains.Add(1)
	return true
}

func (m *Manager) setPendingReply(reqID string, ack chan error, done chan struct{}) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.pending != nil {
		return false
	}
	m.pending = &pendingReply{reqID: reqID, ack: ack, done: done}
	return true
}

func (m *Manager) completeReplyAck(reqID string, err error) {
	m.mu.Lock()
	pending := m.pending
	if pending == nil || pending.reqID != reqID {
		m.mu.Unlock()
		return
	}
	m.pending = nil
	m.mu.Unlock()
	if pending != nil {
		pending.ack <- err
		close(pending.done)
	}
}

func (m *Manager) clearPendingReply(err error) {
	m.mu.Lock()
	pending := m.pending
	m.pending = nil
	m.mu.Unlock()
	if pending != nil {
		pending.ack <- err
		close(pending.done)
	}
}

func (m *Manager) clearPendingReplyFor(reqID string, err error) {
	m.mu.Lock()
	pending := m.pending
	if pending == nil || pending.reqID != reqID {
		m.mu.Unlock()
		return
	}
	m.pending = nil
	m.mu.Unlock()
	pending.ack <- err
	close(pending.done)
}

func mustJSON(v any) []byte { data, _ := json.Marshal(v); return data }
func unmarshalBody(data []byte, out any) error {
	if len(data) == 0 {
		return ErrMalformedFrame
	}
	return json.Unmarshal(data, out)
}
func mustPrincipal(target channels.RoutingTarget) gateway.Principal {
	p, _ := gateway.NewChannelPrincipal(target)
	return p
}

// BeginShutdown prevents new callbacks and interrupts the active connection.
func (m *Manager) BeginShutdown() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closing = true
	cancel := m.runCancel
	conn := m.conn
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close()
	}
	m.clearPendingReply(ErrClosed)
}

// Close waits for the serving connection and dispatched callbacks to finish.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.BeginShutdown()
	m.mu.Lock()
	done := m.runDone
	m.mu.Unlock()
	if done != nil {
		<-done
	}
	m.drains.Wait()
	return nil
}

var _ channels.PollingAdapter = (*Manager)(nil)
