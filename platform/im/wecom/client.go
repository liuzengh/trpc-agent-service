package wecom

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type Client struct {
	cfg      Config
	opts     options
	mu       sync.Mutex
	started  bool
	closed   bool
	terminal bool
	drainErr error // sticky aggregate result; never turns an incomplete drain into success
	cancel   context.CancelFunc
	done     chan struct{}
	current  *session
	snapshot StateSnapshot
	authAck  *CommandAck // matched ACK for this generation, independent of socket liveness
	states   chan StateSnapshot
	events   chan queuedEvent
	notices  chan queuedEvent
}
type queuedEvent struct {
	ctx   context.Context
	event Event
}
type session struct {
	client       *Client
	conn         *websocket.Conn
	ctx          context.Context
	cancel       context.CancelFunc
	gen          uint64
	once         sync.Once
	err          error // protected by client.mu
	pending      map[string]*pending
	replyPending int
	used         map[string]ErrorCode
	writer       chan struct{}
	readDone     chan struct{}
}
type pending struct {
	id        string
	kind      string
	attempted bool
	result    commandResult
	completed bool
	done      chan struct{}
}
type commandResult struct {
	ack CommandAck
	err error
}

func NewClient(cfg Config, opts ...Option) (*Client, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	var options options
	for _, opt := range opts {
		if opt == nil {
			return nil, ErrInvalidConfig
		}
		if err := opt(&options); err != nil {
			return nil, ErrInvalidConfig
		}
	}
	options.httpClient = handshakeClient(options.httpClient)
	c := &Client{cfg: cfg, opts: options, done: make(chan struct{}), states: make(chan StateSnapshot, cfg.StateBuffer), events: make(chan queuedEvent, cfg.EventBuffer), notices: make(chan queuedEvent, 1)}
	c.setStateLocked(StateIdle, 0, "")
	return c, nil
}
func (c *Client) State() StateSnapshot         { c.mu.Lock(); defer c.mu.Unlock(); return c.snapshot }
func (c *Client) States() <-chan StateSnapshot { return c.states }
func (c *Client) setStateLocked(state State, gen uint64, reason ErrorCode) {
	if c.terminal {
		return
	}
	c.snapshot = StateSnapshot{State: state, Generation: gen, Sequence: c.snapshot.Sequence + 1, Reason: reason}
	select {
	case c.states <- c.snapshot:
	default:
		select {
		case <-c.states:
		default:
		}
		select {
		case c.states <- c.snapshot:
		default:
		}
	}
}

// Run performs one Client lifecycle, including at most MaxReconnects transport
// recoveries. Handler calls are ordered and never run on the protocol reader.
// A handler must return after its context is canceled; Go cannot forcibly stop it.
func (c *Client) Run(ctx context.Context, handler Handler) (result error) {
	if ctx == nil || handler == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.started {
		c.mu.Unlock()
		return ErrAlreadyRun
	}
	c.started = true
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.mu.Unlock()
	workerDone := make(chan struct{})
	go c.dispatch(runCtx, handler, workerDone)
	defer func() {
		cancel()
		c.mu.Lock()
		s := c.current
		c.mu.Unlock()
		if s != nil {
			s.fail(ErrCanceled)
		}
		timer := time.NewTimer(c.cfg.CloseTimeout)
		defer timer.Stop()
		select {
		case <-workerDone:
		case <-timer.C:
			result = ErrDrainTimeout
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		state := StateFailed
		code := codeFor(result)
		if result == nil {
			state = StateStopped
		}
		if errors.Is(result, ErrCanceled) {
			state = StateStopped
		}
		if errors.Is(result, ErrReplaced) {
			state = StateReplaced
		}
		if c.closed {
			state = StateClosed
			if !errors.Is(result, ErrDrainTimeout) {
				result = nil
				code = CodeClosed
			}
		}
		c.setStateLocked(state, c.snapshot.Generation, code)
		if errors.Is(result, ErrDrainTimeout) {
			c.drainErr = ErrDrainTimeout
		}
		c.terminal = true
		close(c.states)
		close(c.done)
	}()
	for attempt := 0; ; attempt++ {
		if runCtx.Err() != nil {
			return ErrCanceled
		}
		gen := uint64(attempt + 1)
		c.mu.Lock()
		c.setStateLocked(StateConnecting, gen, "")
		c.mu.Unlock()
		dialCtx, dialCancel := context.WithTimeout(runCtx, c.cfg.DialTimeout)
		conn, _, err := websocket.Dial(dialCtx, c.cfg.URL, &websocket.DialOptions{HTTPClient: c.opts.httpClient})
		dialCancel()
		if err != nil {
			result = ErrDial
		} else {
			conn.SetReadLimit(c.cfg.ReadLimit)
			sessionCtx, sessionCancel := context.WithCancel(runCtx)
			s := &session{client: c, conn: conn, ctx: sessionCtx, cancel: sessionCancel, gen: gen, pending: make(map[string]*pending), used: make(map[string]ErrorCode), writer: make(chan struct{}, 1), readDone: make(chan struct{})}
			c.mu.Lock()
			c.current = s
			c.authAck = nil
			c.setStateLocked(StateAuthenticating, gen, "")
			c.mu.Unlock()
			go s.read()
			authAck, authErr := c.command(runCtx, s, requestID("aibot_subscribe"), "auth", map[string]any{"cmd": "aibot_subscribe", "body": map[string]string{"bot_id": c.cfg.BotID, "secret": c.cfg.Secret}})
			if authErr != nil {
				failure := &AuthenticationError{Certainty: Unknown, Code: CodeAckTimeout, ProviderCode: authAck.ErrCode}
				var commandFailure *CommandError
				if errors.As(authErr, &commandFailure) {
					failure.Certainty, failure.Code = commandFailure.Certainty, commandFailure.Code
				}
				s.fail(failure)
			}
			heartbeatDone := make(chan struct{})
			if authErr == nil {
				go s.heartbeat(heartbeatDone)
			} else {
				close(heartbeatDone)
			}
			<-s.ctx.Done()
			s.fail(ErrCanceled)
			<-s.readDone
			<-heartbeatDone
			c.mu.Lock()
			result = s.err
			c.current = nil
			// An explicit rejection is terminal authentication evidence. A later
			// socket close must not turn it into a retryable disconnect.
			if c.authAck != nil && c.authAck.ErrCode != 0 {
				result = &AuthenticationError{Certainty: Rejected, Code: CodeRejected, ProviderCode: c.authAck.ErrCode}
			}
			c.mu.Unlock()
		}
		if runCtx.Err() != nil {
			return ErrCanceled
		}
		if !retryable(result) || attempt >= c.cfg.MaxReconnects {
			return result
		}
		c.mu.Lock()
		c.setStateLocked(StateBackoff, gen, codeFor(result))
		c.mu.Unlock()
		timer := time.NewTimer(c.cfg.ReconnectBackoff)
		select {
		case <-runCtx.Done():
			timer.Stop()
			return ErrCanceled
		case <-timer.C:
		}
	}
}
func (c *Client) dispatch(ctx context.Context, handler Handler, done chan struct{}) {
	defer close(done)
	for {
		var item queuedEvent
		select {
		case item = <-c.notices:
		default:
			select {
			case <-ctx.Done():
				select {
				case item = <-c.notices:
				default:
					return
				}
			case item = <-c.notices:
			case item = <-c.events:
			}
		}
		// Replacement notification is still delivered with its canceled session
		// context. Other stale queued callbacks must not run on a new connection.
		if item.ctx.Err() != nil && item.event.EventType != "disconnected_event" {
			continue
		}
		if err := handler(item.ctx, item.event); err != nil {
			c.mu.Lock()
			s := c.current
			c.mu.Unlock()
			if item.ctx.Err() != nil || s == nil || s.gen != item.event.Generation {
				// A canceled old callback cannot retire the dispatcher serving a
				// new generation. Session cancellation is not a handler failure.
				continue
			}
			s.fail(ErrHandler)
			return
		}
	}
}
func (s *session) heartbeat(done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.client.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			_, err := s.client.command(s.ctx, s, requestID("ping"), "ping", map[string]any{"cmd": "ping"})
			if err != nil {
				s.fail(ErrHeartbeat)
				return
			}
		}
	}
}
func (s *session) fail(err error) { s.stop(err, nil) }
func (s *session) stop(err error, notice *Event) {
	s.once.Do(func() {
		c := s.client
		c.mu.Lock()
		s.err = err
		state := StateDisconnected
		if errors.Is(err, ErrReplaced) {
			state = StateReplaced
		}
		c.setStateLocked(state, s.gen, codeFor(err))
		for _, p := range s.pending {
			certainty := NotSent
			if p.attempted {
				certainty = Unknown
			}
			c.completeLocked(s, p, commandResult{err: commandError(certainty, codeFor(err))})
		}
		c.mu.Unlock()
		if notice != nil {
			// The terminal reason and non-ready state are installed before
			// the handler sees the replacement; cancellation follows enqueue.
			select {
			case c.notices <- queuedEvent{ctx: s.ctx, event: *notice}:
			default:
			}
		}
		s.cancel()
		_ = s.conn.CloseNow()
	})
}
func (c *Client) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	c.closed = true
	cancel := c.cancel
	started := c.started
	if !started && !c.terminal {
		c.setStateLocked(StateClosed, 0, CodeClosed)
		c.terminal = true
		close(c.states)
		close(c.done)
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if !started {
		return nil
	}
	cleanup, cancelCleanup := context.WithTimeout(ctx, c.cfg.CloseTimeout)
	defer cancelCleanup()
	select {
	case <-c.done:
		c.mu.Lock()
		err := c.drainErr
		c.mu.Unlock()
		return err
	case <-cleanup.Done():
		return ErrDrainTimeout
	}
}
func requestID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("wecom: random source failed")
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}
func retryable(err error) bool {
	return errors.Is(err, ErrDial) || errors.Is(err, ErrDisconnected) || errors.Is(err, ErrHeartbeat)
}
func codeFor(err error) ErrorCode {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrCanceled):
		return CodeCanceled
	case errors.Is(err, ErrReplaced):
		return CodeReplaced
	case errors.Is(err, ErrAuth):
		return CodeRejected
	case errors.Is(err, ErrProtocol):
		return CodeProtocol
	case errors.Is(err, ErrEventOverflow), errors.Is(err, ErrRequestCapacity):
		return CodeCapacity
	case errors.Is(err, ErrHandler):
		return CodeHandler
	case errors.Is(err, ErrDrainTimeout):
		return CodeDrainTimeout
	default:
		return CodeDisconnected
	}
}
