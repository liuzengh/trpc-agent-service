package agent

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type idempotencyLeaseHeartbeat struct {
	context context.Context
	cancel  context.CancelFunc
	done    chan struct{}

	mu  sync.Mutex
	err error
}

func startIdempotencyLeaseHeartbeat(parent context.Context, store storage.IdempotencyStore, lease storage.Lease, ttl time.Duration) *idempotencyLeaseHeartbeat {
	interval := ttl / 3
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(parent)
	heartbeat := &idempotencyLeaseHeartbeat{context: ctx, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(heartbeat.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(context.Background(), finalizationTimeout)
				err := store.Renew(renewCtx, lease, ttl)
				renewCancel()
				if err != nil {
					heartbeat.mu.Lock()
					if heartbeat.err == nil {
						heartbeat.err = err
					}
					heartbeat.mu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	return heartbeat
}

func (h *idempotencyLeaseHeartbeat) Err() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *idempotencyLeaseHeartbeat) Stop() {
	if h == nil {
		return
	}
	h.cancel()
	<-h.done
}
