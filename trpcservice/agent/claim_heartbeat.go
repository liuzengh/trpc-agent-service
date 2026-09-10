package agent

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// startClaimHeartbeat keeps a long-running execution claim fresh. Completion
// still carries a trace fence, so a lost claim cannot be committed by its old
// owner even if a renewal races with takeover.
type claimHeartbeat struct {
	context context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	err     error
}

func startClaimHeartbeat(parent context.Context, store storage.ExecutionDedupStore, tenantID, channel, bindingID, messageID, traceID string, ttl time.Duration) *claimHeartbeat {
	interval := ttl / 3
	if interval < time.Second {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	heartbeat := &claimHeartbeat{context: ctx, cancel: cancel, done: make(chan struct{})}
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
				err := store.Renew(renewCtx, tenantID, channel, bindingID, messageID, traceID)
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

func (h *claimHeartbeat) Err() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *claimHeartbeat) Stop() {
	if h == nil {
		return
	}
	h.cancel()
	<-h.done
}
