package leaseheartbeat

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
)

// Heartbeat keeps one leased value alive until it is stopped, its parent is
// cancelled, or renewal fails. A successful renewal may replace the value
// (for example when a backend returns an updated lease deadline).
type Heartbeat[T any] struct {
	context context.Context
	cancel  context.CancelFunc
	done    chan struct{}

	mu    sync.Mutex
	value T
	err   error
}

// Start begins a lease-renewal loop. renewTimeout <= 0 means renew uses the
// heartbeat context directly; otherwise each renewal gets an independent
// bounded context so finalization does not inherit an almost-expired request.
func Start[T any](
	parent context.Context,
	component string,
	initial T,
	interval time.Duration,
	renewTimeout time.Duration,
	renew func(context.Context, T) (T, error),
) *Heartbeat[T] {
	if parent == nil {
		parent = context.Background()
	}
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ctx, cancel := context.WithCancel(parent)
	heartbeat := &Heartbeat[T]{context: ctx, cancel: cancel, done: make(chan struct{}), value: initial}
	go func() {
		defer close(heartbeat.done)
		if panicErr := safego.Run(component, func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if renew == nil {
						heartbeat.fail(errors.New("lease renew function is required"))
						return
					}
					current := heartbeat.Value()
					renewCtx := ctx
					cancelRenew := func() {}
					if renewTimeout > 0 {
						renewCtx, cancelRenew = context.WithTimeout(context.Background(), renewTimeout)
					}
					renewed, err := renew(renewCtx, current)
					cancelRenew()
					if err != nil {
						heartbeat.fail(err)
						return
					}
					heartbeat.mu.Lock()
					heartbeat.value = renewed
					heartbeat.mu.Unlock()
				}
			}
		}); panicErr != nil {
			heartbeat.fail(panicErr)
		}
	}()
	return heartbeat
}

func (h *Heartbeat[T]) fail(err error) {
	if h == nil || err == nil {
		return
	}
	h.mu.Lock()
	if h.err == nil {
		h.err = err
	}
	h.mu.Unlock()
	h.cancel()
}

func (h *Heartbeat[T]) Context() context.Context {
	if h == nil || h.context == nil {
		return context.Background()
	}
	return h.context
}

func (h *Heartbeat[T]) Done() <-chan struct{} {
	if h == nil || h.done == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return h.done
}

func (h *Heartbeat[T]) Err() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *Heartbeat[T]) Value() T {
	if h == nil {
		var zero T
		return zero
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.value
}

func (h *Heartbeat[T]) Stop() {
	if h == nil {
		return
	}
	h.cancel()
	<-h.done
}
