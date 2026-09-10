package agent

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
)

type usageReservationHeartbeat struct {
	context context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	err     error
}

func startUsageReservationHeartbeat(parent context.Context, governor governance.UsageGovernor, reservation governance.UsageReservation, ttl time.Duration) *usageReservationHeartbeat {
	interval := ttl / 3
	if interval < time.Second {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	heartbeat := &usageReservationHeartbeat{context: ctx, cancel: cancel, done: make(chan struct{})}
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
				err := governor.Renew(renewCtx, reservation, ttl)
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

func (h *usageReservationHeartbeat) Err() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *usageReservationHeartbeat) Stop() {
	if h == nil {
		return
	}
	h.cancel()
	<-h.done
}
