// Package natsadapter owns the durable ReplyIntent handoff, not Provider sending.
package natsadapter

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const StreamName = "REPLY_INTENTS_V1"
const DurableName = "channel-gateway-replies-v1"

type MessageConsumer interface {
	Next(...jetstream.FetchOpt) (jetstream.Msg, error)
}
type StreamInspector interface {
	Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error)
}
type Handler interface {
	Handle(context.Context, []byte) (d.Receipt, error)
}
type ReceiptStore interface {
	FindTransportReceipt(context.Context, d.TransportPosition) (d.TransportReceipt, bool, error)
	RecordTransportReceipt(context.Context, d.TransportReceipt) error
}
type Consumer struct {
	Tracer   trace.Tracer
	consumer MessageConsumer
	stream   StreamInspector
	handler  Handler
	receipts ReceiptStore
}

func New(consumer MessageConsumer, stream StreamInspector, handler Handler, receipts ReceiptStore) (*Consumer, error) {
	if consumer == nil || stream == nil || handler == nil || receipts == nil {
		return nil, d.ErrInvalid
	}
	return &Consumer{consumer: consumer, stream: stream, handler: handler, receipts: receipts}, nil
}
func (c *Consumer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		msg, err := c.consumer.Next(jetstream.FetchMaxWait(time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if !wait(ctx) {
				return ctx.Err()
			}
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		err = c.process(callCtx, msg)
		cancel()
		if err != nil {
			_ = msg.NakWithDelay(time.Second)
		}
	}
	return ctx.Err()
}

// process ACKs only after a durable terminal receipt. It verifies broker metadata
// and stream incarnation on every message, including replay and malformed JSON.
func (c *Consumer) process(ctx context.Context, msg jetstream.Msg) (resultErr error) {
	metadata, err := msg.Metadata()
	if err != nil || metadata == nil {
		return d.ErrUnavailable
	}
	info, err := c.stream.Info(ctx)
	if err != nil {
		return d.ErrUnavailable
	}
	if info == nil || info.Config.Name != StreamName || len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != wire.ReplyIntentSubject || info.Created.IsZero() || metadata.Timestamp.Before(info.Created) || metadata.Stream != StreamName || metadata.Consumer != DurableName || msg.Subject() != wire.ReplyIntentSubject || metadata.Sequence.Stream == 0 || metadata.Sequence.Stream > info.State.LastSeq {
		return d.ErrUnavailable
	}
	carrier := tracecontext.FromHeaders(msg.Headers())
	parent := trace.SpanContextFromContext(carrier.Restore(context.Background()))
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer)}
	if parent.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: parent}))
	}
	ctx, span := telemetrytrace.Resume(c.Tracer, ctx, carrier, "process execution.reply-intent.v1", opts...)
	defer func() { telemetrytrace.End(span, resultErr) }()
	p := d.TransportPosition{StreamName: StreamName, StreamID: info.Created.UTC().Format(time.RFC3339Nano), Sequence: metadata.Sequence.Stream, RawDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(msg.Data()))}
	if receipt, found, err := c.receipts.FindTransportReceipt(ctx, p); err != nil {
		return err
	} else if found {
		recordReceiptSpan(span, receipt)
		return msg.DoubleAck(ctx)
	}
	result, err := c.handler.Handle(ctx, msg.Data())
	r := d.TransportReceipt{Position: p, Outcome: "ACCEPTED", IntentID: result.IntentID, RunID: result.RunID}
	if err != nil {
		reason := permanentReason(err)
		if reason == "" {
			return err
		}
		r.Outcome = "REJECTED"
		r.Reason = reason
	}
	if err = c.receipts.RecordTransportReceipt(ctx, r); err != nil {
		return err
	}
	recordReceiptSpan(span, r)
	return msg.DoubleAck(ctx)
}
func recordReceiptSpan(span trace.Span, receipt d.TransportReceipt) {
	outcome := "failed"
	if receipt.Outcome == "ACCEPTED" {
		outcome = "ACCEPTED"
	}
	span.SetAttributes(attribute.String("app.run.id", receipt.RunID), attribute.String("app.intent.id", receipt.IntentID), attribute.String("app.outcome", outcome))
}
func permanentReason(err error) string {
	switch {
	case errors.Is(err, d.ErrInvalid):
		return "INVALID_WIRE"
	case errors.Is(err, d.ErrConflict):
		return "CONFLICT"
	case errors.Is(err, d.ErrUnauthorized):
		return "UNAUTHORIZED"
	case errors.Is(err, d.ErrExpired):
		return "EXPIRED"
	case errors.Is(err, d.ErrUnsupported):
		return "UNSUPPORTED"
	default:
		return ""
	}
}
func wait(ctx context.Context) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
