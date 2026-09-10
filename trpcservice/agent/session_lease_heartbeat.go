package agent

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type sessionLeaseHeartbeat struct {
	context context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	store   storage.SessionExecutionLeaser

	mu    sync.Mutex
	lease storage.SessionExecutionLease
	err   error
}

func startSessionLeaseHeartbeat(parent context.Context, store storage.SessionExecutionLeaser, lease storage.SessionExecutionLease, ttl time.Duration) *sessionLeaseHeartbeat {
	interval := ttl / 3
	if interval < time.Second {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	heartbeat := &sessionLeaseHeartbeat{
		context: ctx, cancel: cancel, done: make(chan struct{}), store: store, lease: lease,
	}
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
				heartbeat.mu.Lock()
				current := heartbeat.lease
				heartbeat.mu.Unlock()
				renewed, err := store.RenewSessionExecutionLease(renewCtx, current, ttl)
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
				heartbeat.mu.Lock()
				heartbeat.lease = renewed
				heartbeat.mu.Unlock()
			}
		}
	}()
	return heartbeat
}

func (h *sessionLeaseHeartbeat) Err() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *sessionLeaseHeartbeat) Lease() storage.SessionExecutionLease {
	if h == nil {
		return storage.SessionExecutionLease{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lease
}

func (h *sessionLeaseHeartbeat) Stop() {
	if h == nil {
		return
	}
	h.cancel()
	<-h.done
}
