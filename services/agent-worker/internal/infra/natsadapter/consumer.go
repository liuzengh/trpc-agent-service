// Package natsadapter implements bounded Worker broker operations. Runtime only
// binds existing declared durables; topology creation belongs to the reconciler.
package natsadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	controlwire "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	executionwire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/telemetrytrace"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	runwire "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/wire"
	execution "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	manifestwire "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/adapter/inbound/wire"
	manifest "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	RunStream       = "RUN_REQUESTS_V1"
	RunSubject      = "execution.run-requested.v1"
	RunDurable      = "agent-worker-runs-v1"
	ManifestStream  = "RUNTIME_MANIFESTS_V1"
	ManifestSubject = "control.runtime-manifest.published.v1"
	ManifestDurable = "worker-manifests-v1"
	ReplyStream     = "REPLY_INTENTS_V1"
	ReplySubject    = "execution.reply-intent.v1"
)

var ErrUnavailable = errors.New("Worker broker operation unavailable")
var ErrTopology = errors.New("Worker broker topology differs from V1 declaration")
var ErrIntegrity = errors.New("Worker outbox integrity failed")

type RunAcceptor interface {
	Accept(context.Context, execution.Requested) (execution.Receipt, error)
}
type ManifestApplier interface {
	Apply(context.Context, manifest.Publication, int) error
}
type Rejector interface {
	Reject(context.Context, string, string, string) error
}
type MessageConsumer interface {
	Next(...jetstream.FetchOpt) (jetstream.Msg, error)
}
type StreamInspector interface {
	Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error)
}
type Consumer struct {
	Tracer                       trace.Tracer
	consumer                     MessageConsumer
	stream                       StreamInspector
	intake                       RunAcceptor
	projection                   ManifestApplier
	rejector                     Rejector
	capacity                     int
	streamName, subject, durable string
}

func NewRun(consumer MessageConsumer, stream StreamInspector, intake RunAcceptor, rejector Rejector) (*Consumer, error) {
	if consumer == nil || stream == nil || intake == nil || rejector == nil {
		return nil, execution.ErrInvalid
	}
	return &Consumer{consumer: consumer, stream: stream, intake: intake, rejector: rejector, streamName: RunStream, subject: RunSubject, durable: RunDurable}, nil
}
func NewManifest(consumer MessageConsumer, stream StreamInspector, projection ManifestApplier, rejector Rejector, capacity int) (*Consumer, error) {
	if consumer == nil || stream == nil || projection == nil || rejector == nil || capacity < 1 {
		return nil, execution.ErrInvalid
	}
	return &Consumer{consumer: consumer, stream: stream, projection: projection, rejector: rejector, capacity: capacity, streamName: ManifestStream, subject: ManifestSubject, durable: ManifestDurable}, nil
}
func BindRun(ctx context.Context, js jetstream.JetStream, intake RunAcceptor, rejector Rejector) (*Consumer, error) {
	stream, consumer, err := bind(ctx, js, RunStream, RunSubject, RunDurable, jetstream.WorkQueuePolicy, executionwire.MaxRunRequestedBytes)
	if err != nil {
		return nil, err
	}
	return NewRun(consumer, stream, intake, rejector)
}
func BindManifest(ctx context.Context, js jetstream.JetStream, projection ManifestApplier, rejector Rejector, capacity int) (*Consumer, error) {
	stream, consumer, err := bind(ctx, js, ManifestStream, ManifestSubject, ManifestDurable, jetstream.LimitsPolicy, controlwire.MaxManifestEventBytes)
	if err != nil {
		return nil, err
	}
	return NewManifest(consumer, stream, projection, rejector, capacity)
}
func bind(ctx context.Context, js jetstream.JetStream, name, subject, durable string, retention jetstream.RetentionPolicy, maxMessage int) (jetstream.Stream, jetstream.Consumer, error) {
	if js == nil {
		return nil, nil, execution.ErrInvalid
	}
	stream, err := js.Stream(ctx, name)
	if err != nil {
		return nil, nil, ErrUnavailable
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, nil, ErrUnavailable
	}
	consumer, err := stream.Consumer(ctx, durable)
	if err != nil {
		return nil, nil, ErrUnavailable
	}
	ci, err := consumer.Info(ctx)
	if err != nil {
		return nil, nil, ErrUnavailable
	}
	if err := validateSource(info, ci, name, subject, durable, retention, maxMessage); err != nil {
		return nil, nil, err
	}
	return stream, consumer, nil
}

// ValidateRunSource and ValidateManifestSource share startup invariants with
// periodic readiness checks. Pure validation performs no broker API writes.
func ValidateRunSource(info *jetstream.StreamInfo, consumer *jetstream.ConsumerInfo) error {
	return validateSource(info, consumer, RunStream, RunSubject, RunDurable, jetstream.WorkQueuePolicy, executionwire.MaxRunRequestedBytes)
}
func ValidateManifestSource(info *jetstream.StreamInfo, consumer *jetstream.ConsumerInfo) error {
	return validateSource(info, consumer, ManifestStream, ManifestSubject, ManifestDurable, jetstream.LimitsPolicy, controlwire.MaxManifestEventBytes)
}
func validateSource(info *jetstream.StreamInfo, ci *jetstream.ConsumerInfo, name, subject, durable string, retention jetstream.RetentionPolicy, maxMessage int) error {
	if info == nil || ci == nil {
		return ErrTopology
	}
	s := info.Config
	if s.Name != name || len(s.Subjects) != 1 || s.Subjects[0] != subject || s.Retention != retention || s.Storage != jetstream.FileStorage || s.Discard != jetstream.DiscardNew || s.MaxBytes < 1 || (s.Replicas != 1 && s.Replicas != 3 && s.Replicas != 5) || s.Duplicates != 2*time.Minute || s.MaxMsgSize < int32(maxMessage) || s.MaxAge != 0 || s.MaxMsgs > 0 || s.MaxMsgsPerSubject > 0 || !s.DenyDelete || !s.DenyPurge || s.NoAck || s.Sealed || s.AllowRollup || s.AllowMsgTTL || s.SubjectTransform != nil || s.RePublish != nil || s.Mirror != nil || len(s.Sources) > 0 || s.DiscardNewPerSubject {
		return ErrTopology
	}
	c := ci.Config
	if c.Durable != durable || c.AckPolicy != jetstream.AckExplicitPolicy || c.DeliverPolicy != jetstream.DeliverAllPolicy || c.FilterSubject != subject || len(c.FilterSubjects) > 0 || c.DeliverSubject != "" || c.MaxDeliver != -1 || c.MaxAckPending != 64 || c.AckWait != 30*time.Second || c.ReplayPolicy != jetstream.ReplayInstantPolicy || len(c.BackOff) > 0 || c.InactiveThreshold != 0 || c.HeadersOnly || c.PauseUntil != nil || ci.Paused {
		return ErrTopology
	}
	return nil
}

// Poll performs at most one fetch and one durable handoff. No model call occurs.
// Root bootstrap owns retry scheduling; an unavailable handoff delays NAK first.
func (c *Consumer) Poll(ctx context.Context) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	msg, err := c.consumer.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			return false, nil
		}
		return false, ErrUnavailable
	}
	err = c.Handle(ctx, msg)
	if err != nil {
		if nakErr := msg.NakWithDelay(time.Second); nakErr != nil {
			return true, errors.Join(err, ErrUnavailable)
		}
	}
	return true, err
}
func (c *Consumer) Handle(ctx context.Context, msg jetstream.Msg) (err error) {
	if ctx == nil || msg == nil {
		return execution.ErrInvalid
	}
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return ErrUnavailable
	}
	info, err := c.stream.Info(ctx)
	if err != nil {
		return ErrUnavailable
	}
	if info == nil || info.Config.Name != c.streamName || len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != c.subject || info.Created.IsZero() || meta.Stream != c.streamName || meta.Consumer != c.durable || msg.Subject() != c.subject || meta.Sequence.Stream == 0 || meta.Sequence.Stream > info.State.LastSeq || meta.Timestamp.Before(info.Created) {
		return ErrTopology
	}
	source := fmt.Sprintf("%s@%s#%d", c.streamName, info.Created.UTC().Format(time.RFC3339Nano), meta.Sequence.Stream)
	if c.intake != nil {
		carrier := tracecontext.FromHeaders(msg.Headers())
		parent := carrier.Restore(context.Background())
		opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attribute.String("messaging.system", "nats"), attribute.String("messaging.destination.name", c.subject), attribute.String("messaging.operation.name", "process"))}
		if sc := trace.SpanContextFromContext(parent); sc.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
		}
		var span trace.Span
		ctx, span = telemetrytrace.Resume(c.Tracer, ctx, carrier, "process execution.run-requested.v1", opts...)
		defer func() { telemetrytrace.End(span, err) }()
		r, err := runwire.Decode(msg.Data())
		if err != nil {
			if errors.Is(err, executionwire.ErrInvalidRunRequested) || errors.Is(err, execution.ErrInvalid) {
				return c.reject(ctx, msg, source, "INVALID_RUN_WIRE")
			}
			return err
		}
		span.SetAttributes(attribute.String("app.run.id", r.RunID), attribute.String("messaging.message.id", r.EventID))
		if _, err = c.intake.Accept(ctx, r); err != nil && !errors.Is(err, execution.ErrConflict) {
			return err
		}
	} else {
		m, err := manifestwire.Decode(msg.Data())
		if err != nil {
			if errors.Is(err, controlwire.ErrInvalidManifestEvent) {
				return c.reject(ctx, msg, source, "INVALID_MANIFEST_WIRE")
			}
			return err
		}
		if err = c.projection.Apply(ctx, m, c.capacity); err != nil && !errors.Is(err, manifest.ErrConflict) {
			return err
		}
	}
	// ErrConflict from these ports means their own conflict transaction committed.
	return msg.DoubleAck(ctx)
}
func (c *Consumer) reject(ctx context.Context, msg jetstream.Msg, source, reason string) error {
	if err := c.rejector.Reject(ctx, source, execution.Digest(msg.Data()), reason); err != nil {
		return err
	}
	return msg.DoubleAck(ctx)
}
