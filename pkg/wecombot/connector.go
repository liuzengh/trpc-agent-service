// Package wecombot owns one enterprise WeChat intelligent-bot connection and
// bridges authenticated text callbacks into the existing durable Gateway.
package wecombot

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

const officialEndpoint = "wss://openws.work.weixin.qq.com"
const bridgeHeader = "X-WeCom-Bot-Bridge-Token"
const maxFrameBytes = 128 << 10
const maxReplyBytes = 20480

var (
	ErrConfiguration = errors.New("wecom bot connector configuration invalid")
	ErrDisconnected  = errors.New("wecom bot connection unavailable")
	ErrReplaced      = errors.New("wecom bot connection replaced by another client; operator action required")
	ErrProtocol      = errors.New("wecom bot protocol invalid")
	ErrSubscription  = errors.New("wecom bot subscription rejected")
	ErrSpool         = errors.New("wecom bot durable spool unavailable")
	ErrSpoolFull     = errors.New("wecom bot durable spool capacity reached; pending work retained")
	ErrGateway       = errors.New("wecom bot gateway rejected callback; pending work retained")
	ErrLocked        = errors.New("wecom bot state is locked by another process")
)

var routePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type Config struct {
	BotID          string
	BotSecret      string
	BridgeToken    string
	GatewayBaseURL string
	RouteKey       string
	StateDir       string
}

type Event struct {
	Operation string
	Outcome   string
	Code      int
}

type frame struct {
	Command string `json:"cmd,omitempty"`
	Headers struct {
		RequestID string `json:"req_id"`
	} `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode *int            `json:"errcode,omitempty"`
}

type connection struct {
	ws      *websocket.Conn
	writeMu sync.Mutex
	mu      sync.Mutex
	waiters map[string]chan *int
	ready   bool
	done    chan struct{}
	once    sync.Once
}

type Connector struct {
	config                                  Config
	gatewayURL, routeHash                   string
	dialer                                  *websocket.Dialer
	dial                                    func(context.Context) (*websocket.Conn, *http.Response, error)
	client                                  *http.Client
	notify                                  func(Event)
	mu                                      sync.RWMutex
	active                                  *connection
	running                                 bool
	status                                  string
	spool                                   *spool
	wake                                    chan struct{}
	ackTimeout, heartbeat, reconnectMaximum time.Duration
}

func New(config Config, notify func(Event)) (*Connector, error) {
	if !validIdentity(config.BotID, 128) || !validIdentity(config.BotSecret, 4096) ||
		!tenant.IsValidWeComBotBridgeToken(config.BridgeToken) || !routePattern.MatchString(config.RouteKey) || config.StateDir == "" {
		return nil, ErrConfiguration
	}
	u, err := url.Parse(config.GatewayBaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" ||
		strings.ContainsAny(u.Host, "\\\r\n\x00") || strings.HasSuffix(u.Host, ":") {
		return nil, ErrConfiguration
	}
	u.Path = "/webhook"
	u.RawQuery = url.Values{"token": []string{config.RouteKey}}.Encode()
	config.StateDir, err = filepath.Abs(config.StateDir)
	if err != nil {
		return nil, ErrConfiguration
	}
	digest := sha256.Sum256([]byte(config.BotID + "\x00" + u.String()))
	dialer := &websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: 15 * time.Second}
	c := &Connector{
		config: config, gatewayURL: u.String(), routeHash: hex.EncodeToString(digest[:]), dialer: dialer,
		client: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		notify: notify, status: "starting", wake: make(chan struct{}, 1),
		ackTimeout: 8 * time.Second, heartbeat: 30 * time.Second, reconnectMaximum: 30 * time.Second,
	}
	c.dial = func(ctx context.Context) (*websocket.Conn, *http.Response, error) {
		return dialer.DialContext(ctx, officialEndpoint, nil)
	}
	return c, nil
}

// Run holds the process lock, reconnects transient transport failures, and
// stops on rejected subscription, kicked connections or durable ingress errors.
func (c *Connector) Run(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrConfiguration
	}
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return ErrLocked
	}
	c.running = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.running = false; c.mu.Unlock() }()
	store, err := openSpool(c.config.StateDir, c.routeHash)
	if err != nil {
		c.setStatus("spool_error")
		return err
	}
	defer store.Close()
	c.mu.Lock()
	c.spool = store
	c.mu.Unlock()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	forwardDone := make(chan error, 1)
	go func() { forwardDone <- c.forwardLoop(runCtx) }()
	defer func() { cancel(); <-forwardDone }()
	for attempt := 0; ; attempt++ {
		select {
		case err := <-forwardDone:
			forwardDone <- err
			c.setStatus("gateway_error")
			return err
		default:
		}
		if runCtx.Err() != nil {
			c.setStatus("stopped")
			return runCtx.Err()
		}
		c.setStatus("connecting")
		ws, response, dialErr := c.dial(runCtx)
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		if dialErr != nil {
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			c.emit("connection", "retry", 0)
			if err := c.pause(runCtx, attempt, forwardDone); err != nil {
				return err
			}
			continue
		}
		conn := &connection{ws: ws, waiters: map[string]chan *int{}, done: make(chan struct{})}
		c.mu.Lock()
		c.active = conn
		c.mu.Unlock()
		readDone := make(chan error, 1)
		go func() { readDone <- c.readLoop(conn) }()
		err = c.subscribe(runCtx, conn)
		if err == nil {
			conn.mu.Lock()
			conn.ready = true
			conn.mu.Unlock()
			c.setStatus("subscribed")
			c.emit("connection", "subscribed", 0)
			started := time.Now()
			err = c.connectedLoop(runCtx, conn, readDone, forwardDone)
			if time.Since(started) > time.Minute {
				attempt = 0
			}
		}
		conn.shutdown()
		// readLoop is always joined before dialing a replacement connection.
		readErr := <-readDone
		c.mu.Lock()
		if c.active == conn {
			c.active = nil
		}
		c.mu.Unlock()
		if runCtx.Err() != nil {
			c.setStatus("stopped")
			return runCtx.Err()
		}
		if errors.Is(readErr, ErrReplaced) || errors.Is(err, ErrReplaced) {
			c.setStatus("replaced")
			return ErrReplaced
		}
		if errors.Is(err, ErrSubscription) {
			c.setStatus("subscription_rejected")
			return err
		}
		if errors.Is(readErr, ErrSpool) || errors.Is(readErr, ErrSpoolFull) || errors.Is(readErr, ErrProtocol) {
			c.setStatus("ingress_error")
			return readErr
		}
		if errors.Is(err, ErrGateway) || errors.Is(err, ErrSpool) {
			c.setStatus("gateway_error")
			return err
		}
		c.setStatus("disconnected")
		c.emit("connection", "retry", 0)
		if err := c.pause(runCtx, attempt, forwardDone); err != nil {
			return err
		}
	}
}

func (c *Connector) connectedLoop(ctx context.Context, conn *connection, readDone, forwardDone chan error) error {
	ticker := time.NewTicker(c.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readDone:
			readDone <- err
			return err
		case err := <-forwardDone:
			forwardDone <- err
			return err
		case <-ticker.C:
			id := requestID("ping")
			code, sent, err := conn.exchange(ctx, id, command("ping", id, nil), c.ackTimeout, false)
			if err != nil || !sent || code != 0 {
				return ErrDisconnected
			}
		}
	}
}

func (c *Connector) subscribe(ctx context.Context, conn *connection) error {
	id := requestID("subscribe")
	body := map[string]string{"bot_id": c.config.BotID, "secret": c.config.BotSecret}
	code, _, err := conn.exchange(ctx, id, command("aibot_subscribe", id, body), c.ackTimeout, false)
	if err != nil {
		return ErrDisconnected
	}
	if code != 0 {
		return fmt.Errorf("%w (code %d)", ErrSubscription, code)
	}
	return nil
}

func (c *Connector) readLoop(conn *connection) error {
	defer conn.shutdown()
	conn.ws.SetReadLimit(maxFrameBytes)
	for {
		kind, raw, err := conn.ws.ReadMessage()
		if err != nil {
			return ErrDisconnected
		}
		if kind != websocket.TextMessage || !utf8.Valid(raw) {
			return ErrProtocol
		}
		var input frame
		if json.Unmarshal(raw, &input) != nil {
			return ErrProtocol
		}
		if input.Command == "" && input.ErrCode != nil {
			conn.mu.Lock()
			pending := conn.waiters[input.Headers.RequestID]
			conn.mu.Unlock()
			if pending != nil {
				select {
				case pending <- input.ErrCode:
				default:
				}
			}
			continue
		}
		if input.Command == "aibot_event_callback" {
			var event struct {
				Event struct {
					Type string `json:"eventtype"`
				} `json:"event"`
			}
			if json.Unmarshal(input.Body, &event) != nil {
				return ErrProtocol
			}
			if event.Event.Type == "disconnected_event" {
				return ErrReplaced
			}
			c.emit("callback", "event_ignored", 0)
			continue
		}
		if input.Command != "aibot_msg_callback" {
			return ErrProtocol
		}
		var body struct {
			MsgID    string `json:"msgid"`
			BotID    string `json:"aibotid"`
			Kind     string `json:"msgtype"`
			ChatType string `json:"chattype"`
			From     struct {
				UserID string `json:"userid"`
			} `json:"from"`
			Text struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if json.Unmarshal(input.Body, &body) != nil || body.BotID != c.config.BotID {
			return ErrProtocol
		}
		if body.Kind != "text" || body.ChatType != "single" {
			c.emit("callback", "unsupported_ignored", 0)
			continue
		}
		if !validIdentity(body.MsgID, 256) || !validIdentity(input.Headers.RequestID, 256) || !validIdentity(body.From.UserID, 256) || body.Text.Content == "" || len(body.Text.Content) > maxReplyBytes {
			return ErrProtocol
		}
		if err := c.spool.enqueue(body.MsgID, raw, time.Now().UTC()); err != nil {
			return err
		}
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
}

func (conn *connection) exchange(ctx context.Context, id string, payload []byte, timeout time.Duration, requireReady bool) (int, bool, error) {
	waiter := make(chan *int, 1)
	conn.mu.Lock()
	if (requireReady && !conn.ready) || len(conn.waiters) >= 64 {
		conn.mu.Unlock()
		return 0, false, ErrDisconnected
	}
	select {
	case <-conn.done:
		conn.mu.Unlock()
		return 0, false, ErrDisconnected
	default:
	}
	if _, exists := conn.waiters[id]; exists {
		conn.mu.Unlock()
		return 0, false, ErrDisconnected
	}
	conn.waiters[id] = waiter
	conn.mu.Unlock()
	defer func() { conn.mu.Lock(); delete(conn.waiters, id); conn.mu.Unlock() }()
	conn.writeMu.Lock()
	if ctx.Err() != nil {
		conn.writeMu.Unlock()
		return 0, false, ctx.Err()
	}
	select {
	case <-conn.done:
		conn.writeMu.Unlock()
		return 0, false, ErrDisconnected
	default:
	}
	_ = conn.ws.SetWriteDeadline(time.Now().Add(3 * time.Second))
	err := conn.ws.WriteMessage(websocket.TextMessage, payload)
	conn.writeMu.Unlock()
	if err != nil {
		conn.shutdown()
		return 0, true, ErrDisconnected
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case code := <-waiter:
		return *code, true, nil
	case <-ctx.Done():
		return 0, true, ctx.Err()
	case <-conn.done:
		return 0, true, ErrDisconnected
	case <-timer.C:
		return 0, true, ErrDisconnected
	}
}

func (conn *connection) shutdown() {
	conn.once.Do(func() { conn.mu.Lock(); conn.ready = false; conn.mu.Unlock(); close(conn.done); conn.ws.Close() })
}
func (c *Connector) setStatus(status string) { c.mu.Lock(); c.status = status; c.mu.Unlock() }
func (c *Connector) emit(operation, outcome string, code int) {
	if c.notify != nil {
		c.notify(Event{operation, outcome, code})
	}
}
func (c *Connector) pause(ctx context.Context, attempt int, forwardDone chan error) error {
	if attempt > 5 {
		attempt = 5
	}
	delay := time.Second << attempt
	if delay > c.reconnectMaximum {
		delay = c.reconnectMaximum
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-forwardDone:
		forwardDone <- err
		return err
	case <-timer.C:
		return nil
	}
}
func command(name, id string, body any) []byte {
	value := map[string]any{"cmd": name, "headers": map[string]string{"req_id": id}}
	if body != nil {
		value["body"] = body
	}
	data, _ := json.Marshal(value)
	return data
}
func requestID(prefix string) string {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return prefix + fmt.Sprint(time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(random[:])
}
func validIdentity(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}
