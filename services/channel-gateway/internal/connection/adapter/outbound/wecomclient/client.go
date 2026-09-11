// Package wecomclient adapts a public protocol Client to Connection lifecycle
// ports. Admission classifies its own failures through injected predicates.
package wecomclient

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	connection "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
)

type ErrorPolicy struct{ Temporary, Rejected func(error) bool }

func NewClient(client *wecom.Client, handler wecom.Handler, policy ErrorPolicy, logger *slog.Logger) (*Client, error) {
	if client == nil || handler == nil || policy.Temporary == nil || policy.Rejected == nil || logger == nil {
		return nil, errors.New("invalid managed WeCom client dependencies")
	}
	zero := make(chan struct{})
	close(zero)
	return &Client{client: client, handler: handler, accepting: true, zero: zero, policy: policy, logger: logger}, nil
}

type Client struct {
	client           *wecom.Client
	handler          wecom.Handler
	mu               sync.Mutex
	accepting        bool
	active           int
	reservations     int
	zero             chan struct{}
	replaced         atomic.Bool
	retryable        atomic.Bool
	handlerTemporary atomic.Bool
	started          atomic.Bool
	complete         atomic.Bool
	policy           ErrorPolicy
	logger           *slog.Logger
}

func (c *Client) Run(ctx context.Context) error {
	if ctx == nil {
		return wecom.ErrInvalidConfig
	}
	if !c.started.CompareAndSwap(false, true) {
		return wecom.ErrAlreadyRun
	}
	result := c.client.Run(ctx, func(ctx context.Context, e wecom.Event) error {
		if e.EventType == "disconnected_event" {
			c.replaced.Store(true)
			return nil
		}
		c.mu.Lock()
		if !c.accepting {
			c.mu.Unlock()
			return nil
		}
		if c.active == 0 {
			c.zero = make(chan struct{})
		}
		c.active++
		c.mu.Unlock()
		defer func() {
			c.mu.Lock()
			c.active--
			if c.active == 0 {
				close(c.zero)
			}
			c.mu.Unlock()
		}()
		err := c.handler(ctx, e)
		// Deterministic event rejection does not corrupt the authenticated socket.
		// There is no remote durable-consume ACK implied by a nil handler return.
		if c.policy.Rejected(err) {
			c.logger.Warn("WeCom inbound rejected", "reason", "invalid_or_conflicting_event")
			return nil
		}
		if err != nil && ctx.Err() == nil && c.policy.Temporary(err) {
			c.handlerTemporary.Store(true)
		}
		return err
	})
	// Event-buffer overflow is bounded input pressure, not a credential change.
	// Recovery is rate-limited by Supervisor, never by unlimited SDK recreation.
	retryable := errors.Is(result, wecom.ErrEventOverflow) || (errors.Is(result, wecom.ErrHandler) && c.handlerTemporary.Load())
	c.retryable.Store(retryable)
	// Publish the final classification before exposing a normal terminal state.
	// Protocol/auth/drain failures cannot inherit a stale handler candidate.
	c.complete.Store(true)
	if result != nil && ctx.Err() == nil {
		c.logger.Warn("WeCom client stopped", "retryable", c.retryable.Load(), "reason", string(c.client.State().Reason))
	}
	return result
}
func (c *Client) Quiesce() { c.mu.Lock(); c.accepting = false; c.mu.Unlock() }
func (c *Client) Drain(ctx context.Context) error {
	if ctx == nil {
		return wecom.ErrInvalidConfig
	}
	c.mu.Lock()
	zero := c.zero
	c.mu.Unlock()
	select {
	case <-zero:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *Client) Close(ctx context.Context) error { c.Quiesce(); return c.client.Close(ctx) }
func (c *Client) Status() connection.ClientStatus {
	s := c.client.State()
	replaced := c.replaced.Load() || s.State == wecom.StateReplaced
	terminal := c.complete.Load() || replaced
	return connection.ClientStatus{Ready: s.State == wecom.StateReady, Replaced: replaced, Terminal: terminal, Retryable: terminal && !replaced && c.retryable.Load()}
}
