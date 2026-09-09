//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ErrShuttingDown is returned to callers that arrive after shutdown has begun.
var ErrShuttingDown = errors.New("server is shutting down")

// ErrShutdownHookPanic identifies a cleanup hook that panicked. The panic
// value is intentionally not retained because it may contain credentials or
// provider connection material.
var ErrShutdownHookPanic = errors.New("shutdown hook panicked")

// Coordinator sequences a graceful shutdown: it flips readiness to failing so
// load balancers stop sending new traffic, waits for in-flight work to finish,
// then runs cleanup hooks in reverse registration order.
//
// http.Server.Shutdown alone is not sufficient. It stops the listener and waits
// for handlers, but readiness keeps reporting healthy for the whole drain, so a
// load balancer continues routing new requests to a process that is closing, and
// nothing orders the release of leases or the flush of telemetry.
type Coordinator struct {
	draining atomic.Bool

	// mu guards every field below. inFlight and draining must be consulted and
	// mutated together: sync.WaitGroup cannot be used here because Add races
	// with a concurrent Wait by definition, and an atomic flag checked before
	// Add does not close that window.
	mu       sync.Mutex
	inFlight int
	// idle is closed and replaced whenever inFlight reaches zero, letting
	// waiters block without polling.
	idle  chan struct{}
	hooks []namedHook
	done  bool
	// shutdownDone is closed only after the owner has finished draining and all
	// cleanup hooks. Concurrent Shutdown callers wait on it and receive the
	// owner's exact result instead of observing an incomplete shutdown as nil.
	shutdownDone chan struct{}
	shutdownErr  error
}

type namedHook struct {
	name string
	fn   func(context.Context) error
}

// NewCoordinator creates a shutdown coordinator that is initially serving.
func NewCoordinator() *Coordinator {
	idle := make(chan struct{})
	close(idle) // no work in flight yet
	return &Coordinator{idle: idle}
}

// Draining reports whether shutdown has started. Readiness probes must fail once
// this is true.
func (c *Coordinator) Draining() bool { return c.draining.Load() }

// BeginDrain fails readiness and prevents new tracked work without running
// cleanup hooks. ServeUntilSignal uses it before closing the HTTP listener so
// orchestrators can observe the transition while existing handlers drain.
func (c *Coordinator) BeginDrain() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.draining.Store(true)
}

// OnShutdown registers a cleanup hook. Hooks run in reverse registration order
// after in-flight work drains, so dependencies registered first are torn down
// last.
func (c *Coordinator) OnShutdown(name string, fn func(context.Context) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hooks = append(c.hooks, namedHook{name: name, fn: fn})
}

// Begin marks the start of a unit of in-flight work. It returns ErrShuttingDown
// once draining has started, so new work is refused rather than accepted into a
// closing process. The returned function must be called when the work completes.
func (c *Coordinator) Begin() (func(), error) {
	c.mu.Lock()
	if c.draining.Load() {
		c.mu.Unlock()
		return nil, ErrShuttingDown
	}
	if c.inFlight == 0 {
		// Leaving the idle state: install a fresh channel for waiters to block on.
		c.idle = make(chan struct{})
	}
	c.inFlight++
	c.mu.Unlock()

	var once sync.Once
	return func() { once.Do(c.finish) }, nil
}

// finish records completion of one unit of work, releasing waiters once the last
// one drains.
func (c *Coordinator) finish() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight--
	if c.inFlight == 0 {
		close(c.idle)
	}
}

// Shutdown flips readiness, waits for in-flight work up to the context deadline,
// then runs cleanup hooks. Hook errors are collected rather than aborting the
// sequence, so one failing dependency cannot strand the others.
func (c *Coordinator) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.done {
		done := c.shutdownDone
		c.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		c.mu.Lock()
		err := c.shutdownErr
		c.mu.Unlock()
		return err
	}
	c.done = true
	// Publish the drain transition while holding the same mutex Begin uses for
	// admission. There is no window for new work after shutdown ownership is
	// claimed but before admission is closed.
	c.draining.Store(true)
	c.shutdownDone = make(chan struct{})
	hooks := make([]namedHook, len(c.hooks))
	copy(hooks, c.hooks)
	c.mu.Unlock()

	drainErr := c.waitForDrain(ctx)

	var errs []error
	if drainErr != nil {
		errs = append(errs, drainErr)
	}

	for i := len(hooks) - 1; i >= 0; i-- {
		h := hooks[i]
		if err := invokeShutdownHook(h, ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown hook %q: %w", h.name, err))
		}
	}

	shutdownErr := errors.Join(errs...)
	c.mu.Lock()
	c.shutdownErr = shutdownErr
	close(c.shutdownDone)
	c.mu.Unlock()
	return shutdownErr
}

// invokeShutdownHook isolates an individual cleanup implementation. Cleanup
// is best-effort and ordered; a panic from one provider must not strand the
// remaining hooks or leave concurrent Shutdown callers waiting forever.
func invokeShutdownHook(h namedHook, ctx context.Context) (err error) {
	if h.fn == nil {
		return errors.New("shutdown hook is nil")
	}
	defer func() {
		if recover() != nil {
			err = ErrShutdownHookPanic
		}
	}()
	return h.fn(ctx)
}

// waitForDrain blocks until in-flight work finishes or the context expires.
func (c *Coordinator) waitForDrain(ctx context.Context) error {
	c.mu.Lock()
	drained := c.idle
	c.mu.Unlock()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("in-flight work did not drain: %w", ctx.Err())
	}
}

// WaitForDrain exposes drain-only waiting for callers that sequence the HTTP
// server shutdown themselves.
func (c *Coordinator) WaitForDrain(ctx context.Context) error {
	return c.waitForDrain(ctx)
}

// Middleware refuses new requests once draining and otherwise tracks the request
// as in-flight, so Shutdown waits for it.
func (c *Coordinator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done, err := c.Begin()
		if err != nil {
			// 503 with Connection: close tells clients and proxies to retry
			// elsewhere rather than reuse this connection.
			w.Header().Set("Connection", "close")
			w.Header().Set("Retry-After", "1")
			http.Error(w, "server is shutting down", http.StatusServiceUnavailable)
			return
		}
		defer done()
		next.ServeHTTP(w, r)
	})
}

// DrainGracePeriod is the default time allowed for in-flight work to finish.
const DrainGracePeriod = 30 * time.Second
