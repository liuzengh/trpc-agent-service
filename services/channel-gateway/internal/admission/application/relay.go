package application

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type OutboxMessage struct {
	Carrier                      tracecontext.Carrier
	EventID, Subject, ClaimToken string
	Payload                      []byte
}
type Outbox interface {
	Claim(context.Context) (OutboxMessage, bool, error)
	Published(context.Context, OutboxMessage) error
	Retry(context.Context, OutboxMessage) error
}
type Publisher interface {
	PublishMessage(context.Context, string, string, []byte, tracecontext.Carrier) error
}

// Relay has at-least-once semantics. Stable event IDs remain unchanged on retries.
type Relay struct {
	Tracer    trace.Tracer
	ledger    Outbox
	publisher Publisher
}

func NewRelay(ledger Outbox, publisher Publisher) *Relay {
	return &Relay{ledger: ledger, publisher: publisher}
}
func (r *Relay) PublishNext(ctx context.Context) (published bool, err error) {
	ctx, cancelOperation := context.WithTimeout(ctx, 10*time.Second)
	defer cancelOperation()
	msg, found, err := r.ledger.Claim(ctx)
	if err != nil || !found {
		return false, err
	}
	parent := msg.Carrier.Restore(ctx)
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("messaging.system", "nats"), attribute.String("messaging.destination.name", msg.Subject), attribute.String("messaging.operation.name", "publish"), attribute.String("messaging.message.id", msg.EventID))}
	if sc := trace.SpanContextFromContext(parent); sc.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
	}
	callCtx, span := telemetrytrace.Start(r.Tracer, parent, "publish execution.run-requested.v1", opts...)
	defer func() { telemetrytrace.End(span, err) }()
	callCtx, cancel := context.WithTimeout(callCtx, 5*time.Second)
	defer cancel()
	if err = r.publisher.PublishMessage(callCtx, msg.Subject, msg.EventID, msg.Payload, msg.Carrier); err != nil {
		// If retry bookkeeping fails the claim lease still expires for crash recovery.
		retryErr := r.ledger.Retry(ctx, msg)
		return true, errors.Join(err, retryErr)
	}
	return true, r.ledger.Published(ctx, msg)
}
func (r *Relay) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		found, err := r.PublishNext(ctx)
		if err == nil && found {
			continue
		}
		delay := 100 * time.Millisecond
		if err != nil {
			delay = time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
