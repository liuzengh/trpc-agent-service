// Package dispatcher provides per-session ordered asynchronous dispatch.
package dispatcher

import (
	"context"
	"errors"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// Handler processes one durable request.
type Handler func(context.Context, gateway.RunRequest) error

// ErrorHandler schedules or reports a failed durable request.
type ErrorHandler func(context.Context, gateway.RunRequest, error)

type sessionQueue struct {
	requests []gateway.RunRequest
	running  bool
}

// Dispatcher serializes one session while allowing different sessions in parallel.
type Dispatcher struct {
	ctx     context.Context
	cancel  context.CancelFunc
	handler Handler
	onError ErrorHandler
	mu      sync.Mutex
	queues  map[gateway.SessionKey]*sessionQueue
	wg      sync.WaitGroup
	closed  bool
}

// New creates a Dispatcher.
func New(parent context.Context, handler Handler) (*Dispatcher, error) {
	return NewWithErrorHandler(parent, handler, nil)
}

// NewWithErrorHandler creates a Dispatcher with explicit retry/error handling.
func NewWithErrorHandler(parent context.Context, handler Handler, onError ErrorHandler) (*Dispatcher, error) {
	if parent == nil || handler == nil {
		return nil, errors.New("dispatcher: context and handler are required")
	}
	ctx, cancel := context.WithCancel(parent)
	return &Dispatcher{ctx: ctx, cancel: cancel, handler: handler, onError: onError, queues: make(map[gateway.SessionKey]*sessionQueue)}, nil
}

// Submit enqueues without waiting for Agent execution.
func (dispatcher *Dispatcher) Submit(request gateway.RunRequest) error {
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.closed {
		return errors.New("dispatcher: closed")
	}
	key := request.Key()
	queue := dispatcher.queues[key]
	if queue == nil {
		queue = &sessionQueue{}
		dispatcher.queues[key] = queue
	}
	queue.requests = append(queue.requests, request)
	if !queue.running {
		queue.running = true
		dispatcher.wg.Add(1)
		go dispatcher.run(key)
	}
	return nil
}

func (dispatcher *Dispatcher) run(key gateway.SessionKey) {
	defer dispatcher.wg.Done()
	for {
		dispatcher.mu.Lock()
		queue := dispatcher.queues[key]
		if len(queue.requests) == 0 {
			delete(dispatcher.queues, key)
			dispatcher.mu.Unlock()
			return
		}
		request := queue.requests[0]
		queue.requests = queue.requests[1:]
		dispatcher.mu.Unlock()
		if err := dispatcher.handler(dispatcher.ctx, request); err != nil && dispatcher.onError != nil {
			dispatcher.onError(dispatcher.ctx, request, err)
		}
	}
}

// Close stops accepting work and waits for queued handlers within ctx.
func (dispatcher *Dispatcher) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("dispatcher: nil close context")
	}
	dispatcher.mu.Lock()
	if !dispatcher.closed {
		dispatcher.closed = true
		dispatcher.cancel()
	}
	dispatcher.mu.Unlock()
	done := make(chan struct{})
	go func() { dispatcher.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
