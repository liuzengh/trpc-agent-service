package channels

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

const defaultWeComEndpoint = "wss://openws.work.weixin.qq.com"

type wecomHeaders struct {
	ReqID string `json:"req_id,omitempty"`
}

type wecomEnvelope struct {
	Cmd     string          `json:"cmd"`
	Headers wecomHeaders    `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode *int            `json:"errcode,omitempty"`
	ErrMsg  string          `json:"errmsg,omitempty"`
}

type WeComAdapter struct {
	bindingID string
	accountID string
	botID     string
	secret    string
	endpoint  string

	mu      sync.RWMutex
	conn    *websocket.Conn
	started bool
	ready   bool
	cancel  context.CancelFunc
	writeMu sync.Mutex

	responseMu sync.Mutex
	responses  map[string]chan wecomEnvelope

	heartbeatInterval time.Duration
	readPollInterval  time.Duration
	reconnectBackoff  time.Duration
	maxBackoff        time.Duration
	subscribeTimeout  time.Duration
}

func NewWeComAdapter(bindingID, accountID, botID, secret, endpoint string) (*WeComAdapter, error) {
	if strings.TrimSpace(bindingID) == "" || strings.TrimSpace(accountID) == "" || strings.TrimSpace(botID) == "" || strings.TrimSpace(secret) == "" {
		return nil, errors.New("wecom binding, account, bot ID and secret are required")
	}
	if strings.TrimSpace(endpoint) == "" {
		endpoint = defaultWeComEndpoint
	}
	return &WeComAdapter{
		bindingID: bindingID, accountID: accountID, botID: botID, secret: secret, endpoint: endpoint,
		responses:         make(map[string]chan wecomEnvelope),
		heartbeatInterval: 30 * time.Second, readPollInterval: time.Second,
		reconnectBackoff: time.Second, maxBackoff: 30 * time.Second, subscribeTimeout: 10 * time.Second,
	}, nil
}

func (a *WeComAdapter) Name() string { return a.bindingID }

func (a *WeComAdapter) Ready(context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.started || !a.ready || a.conn == nil {
		return errors.New("wecom adapter is not ready")
	}
	return nil
}

func (a *WeComAdapter) Start(ctx context.Context, sink IngressSink) error {
	if sink == nil {
		return errors.New("wecom ingress sink is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		cancel()
		return errors.New("wecom adapter is already running")
	}
	a.started, a.cancel = true, cancel
	a.mu.Unlock()
	defer func() {
		cancel()
		a.disconnect()
		a.mu.Lock()
		a.started, a.ready, a.cancel = false, false, nil
		a.mu.Unlock()
	}()
	backoff := a.reconnectBackoff
	for {
		if runCtx.Err() != nil {
			return nil
		}
		if err := a.connect(runCtx); err != nil {
			if runCtx.Err() != nil {
				return nil
			}
			a.disconnect()
			if !waitWeComRetry(runCtx, backoff) {
				return nil
			}
			backoff = nextWeComBackoff(backoff, a.maxBackoff)
			continue
		}
		backoff = a.reconnectBackoff
		_ = a.readLoop(runCtx, sink)
		a.disconnect()
		if runCtx.Err() == nil && !waitWeComRetry(runCtx, backoff) {
			return nil
		}
	}
}

func (a *WeComAdapter) connect(ctx context.Context) error {
	config, err := websocket.NewConfig(a.endpoint, "http://localhost/")
	if err != nil {
		return err
	}
	conn, err := config.DialContext(ctx)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.conn = conn
	a.mu.Unlock()
	stopCancellation := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopCancellation:
		}
	}()
	defer close(stopCancellation)
	_ = conn.SetDeadline(time.Now().Add(a.subscribeTimeout))
	reqID := fmt.Sprintf("subscribe-%d", time.Now().UnixNano())
	if err := a.write(wecomEnvelope{Cmd: "aibot_subscribe", Headers: wecomHeaders{ReqID: reqID}, Body: mustJSON(map[string]string{"bot_id": a.botID, "secret": a.secret})}); err != nil {
		_ = conn.Close()
		return err
	}
	var response wecomEnvelope
	if err := websocket.JSON.Receive(conn, &response); err != nil {
		_ = conn.Close()
		return err
	}
	if response.Headers.ReqID != reqID {
		_ = conn.Close()
		return errors.New("wecom subscription response mismatch")
	}
	errCode, validResponse := response.ErrCode, response.ErrCode != nil
	if response.Cmd != "" {
		if response.Cmd != "aibot_subscribe" {
			_ = conn.Close()
			return errors.New("wecom subscription response mismatch")
		}
		var result struct {
			ErrCode *int `json:"errcode"`
		}
		if len(response.Body) > 0 && json.Unmarshal(response.Body, &result) == nil && result.ErrCode != nil {
			errCode, validResponse = result.ErrCode, true
		}
	}
	if !validResponse || *errCode != 0 {
		_ = conn.Close()
		return errors.New("wecom subscription rejected")
	}
	_ = conn.SetDeadline(time.Time{})
	a.markReady(true)
	return nil
}

func (a *WeComAdapter) readLoop(ctx context.Context, sink IngressSink) error {
	a.mu.RLock()
	conn := a.conn
	a.mu.RUnlock()
	if conn == nil {
		return errors.New("wecom connection is missing")
	}
	heartbeat := time.NewTicker(a.heartbeatInterval)
	defer heartbeat.Stop()
	for {
		// Reads use a short deadline so heartbeat and cancellation are observed
		// without a second reader goroutine touching the WebSocket.
		_ = conn.SetReadDeadline(time.Now().Add(a.readPollInterval))
		var incoming wecomEnvelope
		err := websocket.JSON.Receive(conn, &incoming)
		if err == nil {
			if incoming.Cmd == "aibot_msg_callback" {
				if err := a.handleMessage(ctx, sink, incoming); err != nil {
					return err
				}
			} else if incoming.Cmd == "aibot_event_callback" && isWeComDisconnected(incoming.Body) {
				return errors.New("wecom disconnected event")
			} else {
				a.deliverResponse(incoming)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-heartbeat.C:
			if err := a.write(wecomEnvelope{Cmd: "ping", Headers: wecomHeaders{ReqID: fmt.Sprintf("ping-%d", time.Now().UnixNano())}}); err != nil {
				return err
			}
		default:
			// A deadline timeout is expected; any other error means the socket
			// is closed or the server sent malformed data.
			if !isTimeout(err) {
				return err
			}
		}
	}
}

func (a *WeComAdapter) handleMessage(ctx context.Context, sink IngressSink, incoming wecomEnvelope) error {
	var body struct {
		MsgID    string `json:"msgid"`
		AIBotID  string `json:"aibotid"`
		ChatType string `json:"chattype"`
		ChatID   string `json:"chatid"`
		From     struct {
			UserID string `json:"userid"`
		} `json:"from"`
		Text struct {
			Content string `json:"content"`
		} `json:"text"`
	}
	if err := json.Unmarshal(incoming.Body, &body); err != nil || body.MsgID == "" || body.From.UserID == "" || strings.TrimSpace(body.Text.Content) == "" {
		return nil
	}
	if body.AIBotID != "" && body.AIBotID != a.accountID && body.AIBotID != a.botID {
		return nil
	}
	conversationType := message.ConversationDirect
	conversationID := body.From.UserID
	if body.ChatType == "group" {
		conversationType = message.ConversationGroup
		conversationID = body.ChatID
	}
	if conversationID == "" {
		return nil
	}
	_, err := sink.Accept(ctx, message.InboundMessage{
		Channel: "wecom_aibot", BindingID: a.bindingID, ExternalAccountID: a.accountID,
		PlatformMessageID: body.MsgID, ActorUserID: body.From.UserID, ConversationID: conversationID,
		ConversationType: conversationType, Text: body.Text.Content, PlatformRequestID: incoming.Headers.ReqID,
		ReceivedAt: time.Now().UTC(),
	})
	return err
}

func (a *WeComAdapter) Send(ctx context.Context, outbound message.OutboundMessage) error {
	if strings.TrimSpace(outbound.Text) == "" || outbound.ConversationID == "" || outbound.PlatformRequestID == "" {
		return errors.New("wecom outbound target is incomplete")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	response, unregister, err := a.registerResponse(outbound.PlatformRequestID)
	if err != nil {
		return err
	}
	defer unregister()
	body := map[string]any{
		"msgtype": "stream",
		"stream": map[string]any{
			"id":      wecomStreamID(outbound.PlatformRequestID),
			"finish":  true,
			"content": outbound.Text,
		},
	}
	if err := a.write(wecomEnvelope{Cmd: "aibot_respond_msg", Headers: wecomHeaders{ReqID: outbound.PlatformRequestID}, Body: mustJSON(body)}); err != nil {
		return err
	}
	select {
	case result := <-response:
		if result.ErrCode == nil {
			return errors.New("wecom response status is missing")
		}
		if *result.ErrCode != 0 {
			return fmt.Errorf("wecom response rejected with errcode %d", *result.ErrCode)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for wecom response: %w", ctx.Err())
	}
}

func (a *WeComAdapter) registerResponse(reqID string) (<-chan wecomEnvelope, func(), error) {
	a.responseMu.Lock()
	defer a.responseMu.Unlock()
	if a.responses == nil {
		a.responses = make(map[string]chan wecomEnvelope)
	}
	if _, exists := a.responses[reqID]; exists {
		return nil, nil, errors.New("wecom response is already pending")
	}
	response := make(chan wecomEnvelope, 1)
	a.responses[reqID] = response
	return response, func() {
		a.responseMu.Lock()
		delete(a.responses, reqID)
		a.responseMu.Unlock()
	}, nil
}

func (a *WeComAdapter) deliverResponse(incoming wecomEnvelope) {
	if incoming.Headers.ReqID == "" || incoming.ErrCode == nil {
		return
	}
	a.responseMu.Lock()
	response := a.responses[incoming.Headers.ReqID]
	a.responseMu.Unlock()
	if response == nil {
		return
	}
	select {
	case response <- incoming:
	default:
	}
}

func wecomStreamID(reqID string) string {
	digest := sha256.Sum256([]byte(reqID))
	return fmt.Sprintf("stream-%x", digest[:16])
}

func (a *WeComAdapter) write(envelope wecomEnvelope) error {
	a.mu.RLock()
	conn := a.conn
	a.mu.RUnlock()
	if conn == nil {
		return errors.New("wecom connection is not ready")
	}
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	return websocket.JSON.Send(conn, envelope)
}

func (a *WeComAdapter) markReady(value bool) {
	a.mu.Lock()
	a.ready = value
	a.mu.Unlock()
}

func (a *WeComAdapter) disconnect() {
	a.mu.Lock()
	conn := a.conn
	a.conn = nil
	a.ready = false
	a.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (a *WeComAdapter) Close() error {
	a.mu.Lock()
	cancel := a.cancel
	conn := a.conn
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func mustJSON(value any) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}

func isTimeout(err error) bool {
	if netErr, ok := err.(interface{ Timeout() bool }); ok {
		return netErr.Timeout()
	}
	return strings.Contains(strings.ToLower(err.Error()), "i/o timeout")
}

func isWeComDisconnected(body json.RawMessage) bool {
	var value struct {
		Event struct {
			EventType string `json:"eventtype"`
		} `json:"event"`
	}
	return json.Unmarshal(body, &value) == nil && value.Event.EventType == "disconnected_event"
}

func waitWeComRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextWeComBackoff(current, maximum time.Duration) time.Duration {
	current *= 2
	if current > maximum {
		return maximum
	}
	return current
}
