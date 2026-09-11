package natsadapter

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	app "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/trace"

	codec "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"github.com/nats-io/nats.go/jetstream"
)

type ReplyOutbox interface {
	PendingTracedReplies(context.Context, int) ([]app.TracedReply, error)
	MarkReplyPublished(context.Context, string, string) error
}
type Publisher interface {
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}
type ReplyRelay struct {
	Tracer    trace.Tracer
	source    ReplyOutbox
	publisher Publisher
	batch     int
}

func NewReplyRelay(source ReplyOutbox, publisher Publisher, batch int) (*ReplyRelay, error) {
	if source == nil || publisher == nil || batch < 1 || batch > 1000 {
		return nil, domain.ErrInvalid
	}
	return &ReplyRelay{source: source, publisher: publisher, batch: batch}, nil
}

// Tick publishes the exact immutable payload with stable IntentID. A crash after
// PubAck but before marking retries the same bytes and id, never executes Agent.
func (r *ReplyRelay) Tick(ctx context.Context) (int, error) {
	rows, err := r.source.PendingTracedReplies(ctx, r.batch)
	if err != nil {
		return 0, err
	}
	if len(rows) > r.batch {
		return 0, ErrIntegrity
	}
	published := 0
	for _, item := range rows {
		event, err := codec.DecodeReplyIntent(item.Payload)
		if err != nil {
			return published, ErrIntegrity
		}
		digest, err := codec.ReplyIntentDigest(event)
		if err != nil || digest != item.Digest || event.IntentID != item.IntentID {
			return published, ErrIntegrity
		}
		if err := r.publish(ctx, item); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}
func (r *ReplyRelay) publish(ctx context.Context, item app.TracedReply) (resultErr error) {
	parent := trace.SpanContextFromContext(item.Carrier.Restore(context.Background()))
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindClient)}
	if parent.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: parent}))
	}
	ctx, span := telemetrytrace.Resume(r.Tracer, ctx, item.Carrier, "publish execution.reply-intent.v1", opts...)
	defer func() { telemetrytrace.End(span, resultErr) }()
	msg := &nats.Msg{Subject: ReplySubject, Data: item.Payload, Header: nats.Header{}}
	item.Carrier.Inject(msg.Header)
	ack, err := r.publisher.PublishMsg(ctx, msg, jetstream.WithMsgID(item.IntentID))
	if err != nil {
		return ErrUnavailable
	}
	if ack == nil || ack.Stream != ReplyStream {
		return ErrUnavailable
	}
	if err = r.source.MarkReplyPublished(ctx, item.IntentID, item.Digest); err != nil {
		return err
	}
	return nil
}
