package delivery

import (
	"context"
	"errors"
	"sync"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type EventDeliverer interface {
	Deliver(context.Context, channel.ReplyEvent) error
}

// Consumer owns one tenant/binding/account Reply Stream. Provider errors leave
// the Redis entry pending; the durable Delivery Ledger decides whether a
// reclaimed delivery is sent, deferred, reconciled, or ACKed as already done.
type Consumer struct {
	Queue           channel.ReplyQueue
	Deliverer       EventDeliverer
	Destination     channel.ReplyDestination
	ConsumerID      string
	ReclaimInterval time.Duration
	ReclaimLimit    int
	OnDeliveryError func(channel.ReplyDelivery, error)
	Telemetry       telemetry.Provider
}

func (c Consumer) Run(ctx context.Context) error {
	if err := c.validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		errCh <- c.Queue.ConsumeReplies(runCtx, c.Destination, channel.ReplyConsumerOptions{ConsumerID: c.ConsumerID}, c.process)
	}()
	go func() {
		defer wg.Done()
		errCh <- c.reclaimLoop(runCtx)
	}()
	err := <-errCh
	cancel()
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (c Consumer) ReclaimOnce(ctx context.Context) (int, error) {
	if err := c.validate(); err != nil {
		return 0, err
	}
	deliveries, err := c.Queue.ReclaimReplies(ctx, c.Destination, channel.ReplyConsumerOptions{ConsumerID: c.ConsumerID, Limit: c.ReclaimLimit})
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, delivery := range deliveries {
		if err := c.process(ctx, delivery); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (c Consumer) reclaimLoop(ctx context.Context) error {
	interval := c.ReclaimInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := c.ReclaimOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func (c Consumer) process(ctx context.Context, delivery channel.ReplyDelivery) (resultErr error) {
	ctx, finish := telemetry.StartOperation(ctx, c.Telemetry, delivery.Event.TraceParent, telemetry.OperationChannelDeliver,
		telemetry.ComponentAttribute(telemetry.ComponentChannelDelivery))
	defer func() { finish(resultErr) }()
	if delivery.Destination != c.Destination {
		return runtime.ErrTenantScope
	}
	if err := c.Deliverer.Deliver(ctx, delivery.Event); err != nil {
		if c.OnDeliveryError != nil {
			c.OnDeliveryError(delivery, err)
		}
		var terminal TerminalError
		if errors.As(err, &terminal) {
			return c.Queue.AckReply(ctx, c.Destination, delivery)
		}
		// Retryable/ambiguous failures are durable state. Leave the entry
		// pending and continue consuming unrelated account replies.
		return nil
	}
	return c.Queue.AckReply(ctx, c.Destination, delivery)
}

func (c Consumer) validate() error {
	if c.Queue == nil || c.Deliverer == nil || c.ConsumerID == "" || c.Destination.TenantID == "" ||
		c.Destination.Channel == "" || c.Destination.ChannelBindingID == "" || c.Destination.ExternalAccountID == "" {
		return runtime.ErrInvariantViolation
	}
	return nil
}
