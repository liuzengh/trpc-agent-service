package reply

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

func (s *Sender) protectOutbound(parent context.Context, item gateway.OutboundItem) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := max(time.Millisecond, s.opts.ClaimLease/3)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renew, stop := context.WithTimeout(ctx, min(2*time.Second, interval))
				err := s.journal.RenewOutbound(renew, item, s.opts.WorkerID, s.opts.ClaimLease)
				stop()
				if err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	return ctx, func() { cancel(context.Canceled); <-done }
}
