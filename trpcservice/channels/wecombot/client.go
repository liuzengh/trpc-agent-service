package wecombot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
	"golang.org/x/net/websocket"
)

// MessageHandler processes one normalized WeCom Smart Bot message.
type MessageHandler = channels.InboundHandler

// Client manages the WebSocket connection lifecycle to the WeCom Smart Bot gateway.
type Client struct {
	config    Config
	mu        sync.RWMutex
	writeMu   sync.Mutex
	conn      *websocket.Conn
	sessionID string
	done      chan struct{}
	pending   map[string]chan WireFrame
	running   bool
}

const (
	cardActionWorkers = 8
	cardActionQueue   = 32
)

var ErrCardActionBackpressure = errors.New("wecombot card action executor is saturated")

type cardActionJob struct {
	ctx     context.Context
	inbound channels.InboundMessage
	handler MessageHandler
}

type cardActionExecutor struct {
	queue  chan cardActionJob
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newCardActionExecutor(ctx context.Context) *cardActionExecutor {
	workerCtx, cancel := context.WithCancel(ctx)
	executor := &cardActionExecutor{queue: make(chan cardActionJob, cardActionQueue), cancel: cancel}
	executor.wg.Add(cardActionWorkers)
	for range cardActionWorkers {
		safego.Go("wecom card action worker", func() {
			defer executor.wg.Done()
			for {
				select {
				case job := <-executor.queue:
					if job.ctx.Err() != nil {
						continue
					}
					var handlerErr error
					if panicErr := safego.Run("wecom card action handler", func() {
						handlerErr = job.handler(job.ctx, job.inbound)
					}); panicErr != nil {
						continue
					}
					if handlerErr != nil {
						slog.Error("wecombot: card action handler error", "message_id", job.inbound.MessageID, "error", handlerErr)
					}
				case <-workerCtx.Done():
					return
				}
			}
		})
	}
	return executor
}

func (e *cardActionExecutor) submit(ctx context.Context, inbound channels.InboundMessage, handler MessageHandler) bool {
	if e == nil || handler == nil {
		return false
	}
	select {
	case e.queue <- cardActionJob{ctx: ctx, inbound: inbound, handler: handler}:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func (e *cardActionExecutor) close() {
	if e != nil {
		e.cancel()
		e.wg.Wait()
	}
}

// NewClient constructs a WeComBot client.
func NewClient(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.BotID) == "" || strings.TrimSpace(cfg.Secret) == "" {
		return nil, errors.New("wecombot: bot_id and secret are required")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = DefaultEndpoint
	}
	if cfg.Origin == "" {
		cfg.Origin = DefaultOrigin
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.HeartbeatTimer <= 0 {
		cfg.HeartbeatTimer = DefaultHeartbeatTimer
	}
	return &Client{
		config:  cfg,
		pending: make(map[string]chan WireFrame),
	}, nil
}

// Run connects to the WeCom WebSocket gateway and continuously handles frames until ctx is cancelled.
// It automatically reconnects with exponential backoff on connection loss.
func (c *Client) Run(ctx context.Context, handler MessageHandler) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return errors.New("wecombot: client is already running")
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	cardActions := newCardActionExecutor(ctx)
	defer cardActions.close()
	attempt := 0
	for ctx.Err() == nil {
		authenticated, err := c.connectAndLoop(ctx, handler, cardActions)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrConnectionReplaced) {
			// WeCom sends disconnected_event when another connection for the
			// same bot has taken ownership. Reconnecting the superseded process
			// would make the two instances continuously replace each other.
			return err
		}
		if authenticated {
			// A successfully authenticated session starts a fresh reconnect
			// sequence. Otherwise repeated historical disconnects would leave a
			// healthy bot permanently stuck at the maximum delay.
			attempt = 0
		}
		if c.config.OnDisconnected != nil {
			c.config.OnDisconnected(err)
		}
		if err != nil {
			slog.Warn("wecombot: connection terminated, will reconnect", "error", err, "attempt", attempt)
		}
		attempt++
		backoff := reconnectBackoff(attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return ctx.Err()
}

func reconnectBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return time.Second
	}
	delay := time.Second * time.Duration(1<<min(attempt-1, 5))
	return min(delay, 30*time.Second)
}

func (c *Client) connectAndLoop(ctx context.Context, handler MessageHandler, cardActions *cardActionExecutor) (bool, error) {
	wsCfg, err := websocket.NewConfig(c.config.Endpoint, c.config.Origin)
	if err != nil {
		return false, fmt.Errorf("wecombot: invalid ws config: %w", err)
	}

	conn, err := wsCfg.DialContext(ctx)
	if err != nil {
		return false, fmt.Errorf("wecombot: dial failed: %w", err)
	}
	defer conn.Close()
	closeOnCancelDone := make(chan struct{})
	safego.Go("wecom connection cancellation", func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-closeOnCancelDone:
		}
	})
	defer close(closeOnCancelDone)

	sessionDone := make(chan struct{})
	defer close(sessionDone)
	sessionCtx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()

	c.mu.Lock()
	c.conn = conn
	c.sessionID = uuid.NewString()
	c.done = sessionDone
	c.pending = make(map[string]chan WireFrame)
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
			c.sessionID = ""
			c.done = nil
			c.pending = nil
		}
		c.mu.Unlock()
	}()

	// 1. Send aibot_subscribe handshake
	subscribeID := uuid.NewString()
	subBody, _ := json.Marshal(map[string]string{
		"bot_id": c.config.BotID,
		"secret": c.config.Secret,
	})
	if err := c.sendFrame(conn, WireFrame{
		Command: "aibot_subscribe",
		Headers: Headers{RequestID: subscribeID},
		Body:    subBody,
	}); err != nil {
		return false, fmt.Errorf("wecombot: send subscribe failed: %w", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(c.config.RequestTimeout))
	var subResp WireFrame
	if err := websocket.JSON.Receive(conn, &subResp); err != nil {
		return false, fmt.Errorf("wecombot: receive subscribe response failed: %w", err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	if subResp.Headers.RequestID != subscribeID || subResp.ErrorCode == nil || *subResp.ErrorCode != 0 {
		errMsg := subResp.ErrorMessage
		if errMsg == "" {
			errMsg = "handshake rejected"
		}
		return false, fmt.Errorf("%w: %s", ErrAuthFailed, errMsg)
	}

	slog.Info("wecombot: successfully subscribed to gateway", "bot_id", c.config.BotID)
	if c.config.OnReady != nil {
		c.config.OnReady()
	}

	// 2. Start heartbeat
	hbStop := make(chan struct{})
	defer close(hbStop)
	safego.Go("wecom heartbeat", func() { c.heartbeatLoop(ctx, conn, sessionDone, hbStop) })

	// Provider command acknowledgments arrive on the same WebSocket as inbound
	// callbacks. Never execute the application handler on the receive loop: the
	// handler can legitimately send a callback reply and wait for its ACK.
	deliveries := make(chan func(), 256)
	safego.Go("wecom inbound delivery worker", func() {
		for {
			select {
			case <-sessionCtx.Done():
				return
			case deliver := <-deliveries:
				if deliver != nil {
					_ = safego.Run("wecom inbound delivery", deliver)
				}
			}
		}
	})
	queueDelivery := func(deliver func()) bool {
		select {
		case deliveries <- deliver:
			return true
		case <-sessionCtx.Done():
			return false
		}
	}

	// 3. Receive message loop
	for ctx.Err() == nil {
		var frame WireFrame
		if err := websocket.JSON.Receive(conn, &frame); err != nil {
			return true, err
		}

		// Handle request-response replies (e.g. from ping or aibot_respond_msg)
		if frame.Command == "" && frame.ErrorCode != nil {
			c.dispatchPending(frame)
			continue
		}

		// Handle provider-native events. Card actions are normalized into the
		// same ingress callback as messages so platform controls can consume them
		// before Kafka/LLM execution.
		if frame.Command == "aibot_event_callback" {
			var evt EventCallbackBody
			if err := json.Unmarshal(frame.Body, &evt); err != nil {
				slog.Warn("wecombot: unmarshal event callback failed", "error", err)
				continue
			}
			if evt.Event.Type == "disconnected_event" {
				return true, ErrConnectionReplaced
			}
			if evt.Event.Type == "template_card_event" && handler != nil {
				inbound, ok := normalizeCardAction(frame, evt)
				if ok {
					slog.Info("wecombot: template card action received", "message_id", inbound.MessageID, "req_id", frame.Headers.RequestID)
					// Template-card callbacks have a provider-enforced 5s response
					// window. Do not queue them behind ordinary message delivery: a
					// slow/retrying message handler could otherwise make an otherwise
					// valid aibot_respond_update_msg arrive too late.
					if !cardActions.submit(sessionCtx, inbound, handler) {
						if err := sessionCtx.Err(); err != nil {
							return true, err
						}
						return true, ErrCardActionBackpressure
					}
				} else {
					slog.Warn("wecombot: ignored malformed template card event", "req_id", frame.Headers.RequestID)
				}
			}
			continue
		}

		// Handle inbound user message
		if frame.Command == "aibot_msg_callback" {
			var body InboundMsgBody
			if err := json.Unmarshal(frame.Body, &body); err != nil {
				slog.Warn("wecombot: unmarshal message callback failed", "error", err)
				continue
			}
			if handler == nil {
				continue
			}
			messageFrame, messageBody := frame, body
			queueDelivery(func() {
				inbound, deliver, err := c.normalizeInbound(sessionCtx, messageFrame, messageBody)
				if err != nil {
					slog.Error("wecombot: normalize message failed", "msg_type", messageBody.MsgType, "error", err)
					return
				}
				if !deliver {
					return
				}
				if err := handler(sessionCtx, inbound); err != nil {
					slog.Error("wecombot: handler error", "message_id", inbound.MessageID, "error", err)
				}
			})
		}
	}
	return true, ctx.Err()
}

// Close interrupts the active WebSocket receive loop. Run may reconnect later
// unless its context is cancelled, so callers normally cancel first and then
// call Close to make shutdown immediate.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (c *Client) heartbeatLoop(ctx context.Context, conn *websocket.Conn, sessionDone, hbStop <-chan struct{}) {
	ticker := time.NewTicker(c.config.HeartbeatTimer)
	defer ticker.Stop()
	missedPongs := 0

	for {
		select {
		case <-ticker.C:
			pingID := uuid.NewString()
			if err := c.Request(ctx, "ping", pingID, nil); err != nil {
				missedPongs++
				slog.Warn("wecombot: heartbeat ack missed", "error", err, "missed", missedPongs)
				if missedPongs >= DefaultMaxMissedPongs {
					_ = conn.Close()
					return
				}
				continue
			}
			missedPongs = 0
		case <-sessionDone:
			return
		case <-hbStop:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Request sends a command with a request ID and waits synchronously for its acknowledgment.
func (c *Client) Request(ctx context.Context, cmd string, reqID string, body any) error {
	_, err := c.RequestFrame(ctx, cmd, reqID, body)
	return err
}

// RequestFrame is the low-level request primitive used by protocol features
// whose acknowledgments contain data, such as WeCom media upload.
func (c *Client) RequestFrame(ctx context.Context, cmd string, reqID string, body any) (WireFrame, error) {
	c.mu.Lock()
	conn := c.conn
	done := c.done
	if conn == nil || done == nil {
		c.mu.Unlock()
		return WireFrame{}, ErrNotConnected
	}
	if _, exists := c.pending[reqID]; exists {
		c.mu.Unlock()
		return WireFrame{}, errors.New("wecombot: duplicate request ID in flight")
	}
	respCh := make(chan WireFrame, 1)
	c.pending[reqID] = respCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		if c.pending != nil {
			delete(c.pending, reqID)
		}
		c.mu.Unlock()
	}()

	var rawBody json.RawMessage
	if body != nil {
		var err error
		rawBody, err = json.Marshal(body)
		if err != nil {
			return WireFrame{}, fmt.Errorf("wecombot: marshal request body: %w", err)
		}
	}
	frame := WireFrame{Command: cmd, Headers: Headers{RequestID: reqID}, Body: rawBody}
	if err := c.sendFrame(conn, frame); err != nil {
		return WireFrame{}, err
	}

	timer := time.NewTimer(c.config.RequestTimeout)
	defer timer.Stop()
	select {
	case resp := <-respCh:
		if resp.ErrorCode != nil && *resp.ErrorCode != 0 {
			return WireFrame{}, fmt.Errorf("%w: errcode=%d errmsg=%s", ErrRequestRejected, *resp.ErrorCode, resp.ErrorMessage)
		}
		return resp, nil
	case <-done:
		return WireFrame{}, ErrNotConnected
	case <-ctx.Done():
		return WireFrame{}, ctx.Err()
	case <-timer.C:
		return WireFrame{}, ErrRequestTimeout
	}
}

func (c *Client) sendFrame(conn *websocket.Conn, frame WireFrame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return websocket.JSON.Send(conn, frame)
}

func (c *Client) dispatchPending(frame WireFrame) {
	c.mu.RLock()
	ch := c.pending[frame.Headers.RequestID]
	c.mu.RUnlock()
	if ch != nil {
		select {
		case ch <- frame:
		default:
		}
	}
}
