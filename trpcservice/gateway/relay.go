package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"

	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

// RelayOptions controls queue-outbox claiming and retry.
type RelayOptions struct {
	WorkerID     string
	BatchSize    int
	ClaimLease   time.Duration
	PollInterval time.Duration
	RetryDelay   time.Duration
}

// OutboxRelay publishes committed PostgreSQL outbox records to the work queue.
type OutboxRelay struct {
	journal Journal
	queue   workqueue.Queue
	opts    RelayOptions
}

func NewOutboxRelay(
	journal Journal,
	queue workqueue.Queue,
	opts RelayOptions,
) (*OutboxRelay, error) {
	if journal == nil || queue == nil {
		return nil, fmt.Errorf("outbox relay journal and queue are required")
	}
	if opts.WorkerID == "" || opts.BatchSize <= 0 || opts.ClaimLease <= 0 ||
		opts.PollInterval <= 0 || opts.RetryDelay <= 0 {
		return nil, fmt.Errorf("outbox relay options are invalid")
	}
	return &OutboxRelay{journal: journal, queue: queue, opts: opts}, nil
}

// RelayOnce claims and publishes one batch. Publishing happens before marking
// a row complete, so a crash can duplicate a task but cannot lose one.
func (r *OutboxRelay) RelayOnce(ctx context.Context) (int, error) {
	items, err := r.journal.ClaimQueueOutbox(
		ctx,
		r.opts.WorkerID,
		r.opts.BatchSize,
		r.opts.ClaimLease,
	)
	if err != nil {
		return 0, err
	}
	published := 0
	var relayErr error
	for _, item := range items {
		if err := r.publish(ctx, item); err != nil {
			markErr := r.journal.MarkQueueOutboxFailed(
				ctx,
				item.ID,
				r.opts.WorkerID,
				time.Now().Add(r.opts.RetryDelay),
				err,
			)
			relayErr = errors.Join(relayErr, err, markErr)
			continue
		}
		if err := r.journal.MarkQueueOutboxPublished(
			ctx,
			item.ID,
			r.opts.WorkerID,
		); err != nil {
			relayErr = errors.Join(relayErr, err)
			continue
		}
		published++
	}
	return published, relayErr
}

func (r *OutboxRelay) publish(ctx context.Context, item QueueOutboxItem) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{
		"traceparent": item.Task.TraceParent, "tracestate": item.Task.TraceState,
	})
	ctx, span := otel.Tracer("trpc-agent-service/queue").Start(ctx, "queue.publish")
	defer span.End()
	span.SetAttributes(attribute.String("tenant.id", item.Task.Scope.TenantID), attribute.String("gen_ai.request.id", item.Task.RequestID))
	task := item.Task
	if admission, ok := r.journal.(RunAdmission); ok {
		skip, err := admission.SkipDelivery(ctx, task)
		if err != nil {
			return err
		}
		if skip {
			return nil
		}
	}
	task.TraceParent, task.TraceState = outboundTraceHeaders(ctx)
	return r.queue.Publish(ctx, task)
}

// Run continuously relays committed outbox records until ctx is cancelled.
func (r *OutboxRelay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.opts.PollInterval)
	defer ticker.Stop()
	for {
		// Keep the relay alive after a batch error; failed rows have a retry
		// timestamp and dependency failures are exposed through readiness.
		_, _ = r.RelayOnce(ctx)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}
