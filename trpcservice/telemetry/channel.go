package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// The two instruments. They are the same pair for every stage, so one query
// answers "how much of this happened" and "how long did it take" for all of
// them, and adding a stage adds no instrument.
const (
	stageCountMetric    = "trpc.channel.stage.count"
	stageDurationMetric = "trpc.channel.stage.duration"
)

// spanPrefix names the stage spans, which are the only spans this package
// creates.
const spanPrefix = "channel."

// The attribute keys. This list is the whitelist: nothing outside it is ever
// set, and the identifiers in it are internal ones this platform minted.
const (
	attrTenant   = "trpc.tenant_id"
	attrApp      = "trpc.app_id"
	attrBinding  = "trpc.binding_id"
	attrChannel  = "trpc.channel"
	attrStage    = "trpc.stage"
	attrOutcome  = "trpc.outcome"
	attrError    = "trpc.error_type"
	attrRequest  = "trpc.request_id"
	attrRun      = "trpc.run_id"
	attrOutbox   = "trpc.outbox_id"
	attrRevision = "trpc.revision_id"
	attrAttempt  = "trpc.attempt"
	attrEvents   = "trpc.event_count"
)

// errorTypeNone is what a stage that did not fail records, so that every
// measurement of one instrument carries the same set of labels.
const errorTypeNone = "none"

// Stage names one step of the channel pipeline. The three are separate records
// correlated by the durable request id, not parent and child of one trace:
// nothing is propagated through the Store, and a claim executed after a restart
// has no caller to be a child of.
type Stage string

const (
	// StageAccept is one inbound message being written to the Store.
	StageAccept Stage = "accept"
	// StageExecute is one claimed Run being answered.
	StageExecute Stage = "execute"
	// StageDeliver is one attempt to put one answer on the wire.
	StageDeliver Stage = "deliver"

	// stageUnknown is what an unrecognised stage records as. It exists so the
	// vocabulary stays closed even if a caller computes a stage name.
	stageUnknown Stage = "unknown"
)

// checked keeps the stage inside the closed vocabulary.
func (s Stage) checked() Stage {
	switch s {
	case StageAccept, StageExecute, StageDeliver:
		return s
	default:
		return stageUnknown
	}
}

// Outcome is what one stage ended with. It is a closed vocabulary because it is
// a metric label: an outcome derived from an error message would put whatever
// the model, the database or the platform said into a label set.
type Outcome string

const (
	// OutcomeSucceeded means the stage did what it exists to do: the message was
	// stored, the Run was answered, the reply was accepted by the platform.
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeDuplicate means the external event was already stored, so this
	// delivery created nothing.
	OutcomeDuplicate Outcome = "duplicate"
	// OutcomeYielded means a claim was returned to the queue unused.
	OutcomeYielded Outcome = "yielded"
	// OutcomeInterrupted means an earlier attempt had already reached the
	// Runner, so this one refused to run it again.
	OutcomeInterrupted Outcome = "interrupted"
	// OutcomeFailed is a definite negative outcome the stage recorded.
	OutcomeFailed Outcome = "failed"
	// OutcomeRejected means the platform definitely refused the reply.
	OutcomeRejected Outcome = "rejected"
	// OutcomeStaleTarget means the reply address no longer belongs to a live
	// connection, which is not a refusal of the answer.
	OutcomeStaleTarget Outcome = "stale_target"
	// OutcomeSkipped means the stage decided nothing and attempted nothing: a
	// lost claim, a shutdown, or an answer already at risk of having been sent.
	OutcomeSkipped Outcome = "skipped"
	// OutcomeUnknown means the stage neither confirmed nor definitely failed. It
	// is also the fallback for a value outside this vocabulary.
	OutcomeUnknown Outcome = "unknown"
)

// checked keeps the outcome inside the closed vocabulary.
func (o Outcome) checked() Outcome {
	switch o {
	case OutcomeSucceeded, OutcomeDuplicate, OutcomeYielded, OutcomeInterrupted,
		OutcomeFailed, OutcomeRejected, OutcomeStaleTarget, OutcomeSkipped:
		return o
	default:
		return OutcomeUnknown
	}
}

// failed reports whether the span should carry an error status. The status
// carries no description; the outcome above is the whole of what is said.
func (o Outcome) failed() bool {
	switch o {
	case OutcomeFailed, OutcomeRejected, OutcomeStaleTarget,
		OutcomeInterrupted, OutcomeUnknown:
		return true
	default:
		return false
	}
}

// Binding is the static identity every record of one consumer carries, and the
// whole of what a record says about whose message this was. There is no field
// for a principal, a session or an external account.
type Binding struct {
	TenantID  string
	AppID     string
	BindingID string
	Channel   channels.ChannelType
}

// Validate rejects a binding that could not identify a consumer.
func (b Binding) Validate() error {
	if b.TenantID == "" || b.AppID == "" || b.BindingID == "" {
		return fmt.Errorf(
			"%w: a telemetry binding needs a tenant, an app and a channel binding id",
			ErrConfig)
	}
	return b.Channel.Validate()
}

// Result is what one stage ended with.
//
// Every field is either a value from a closed vocabulary or an identifier this
// platform minted. There is deliberately nowhere to put a message, a target, an
// external account or an error.
type Result struct {
	Outcome   Outcome
	ErrorType channels.ErrorType
	// RequestID is what correlates the three stages of one message. On the
	// duplicate path it is the id of the request the *first* delivery created.
	RequestID string
	RunID     string
	OutboxID  string
	// RevisionID is the revision that actually answered, once the pin has been
	// resolved.
	RevisionID string
	// Attempt is the attempt this stage acted on: the persistence attempt that
	// stored an accepted message, the Run's claim generation, or the send
	// attempt of one answer.
	Attempt int32
	// Events counts what one execution produced. It is a count and never a body.
	Events int32
}

// ChannelRecorder records the stages of one channel consumer.
//
// A nil *ChannelRecorder is a consumer without telemetry, and every method here
// accepts one.
type ChannelRecorder struct {
	telemetry *Telemetry
	// spanIdentity is on every span; metricIdentity is the part of it a metric
	// label set may carry.
	spanIdentity   []attribute.KeyValue
	metricIdentity []attribute.KeyValue
}

// ChannelRecorder builds the recorder for one binding. A nil Telemetry — the
// disabled process — returns a nil recorder and no error.
func (t *Telemetry) ChannelRecorder(binding Binding) (*ChannelRecorder, error) {
	if t == nil {
		return nil, nil
	}
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	return &ChannelRecorder{
		telemetry: t,
		spanIdentity: []attribute.KeyValue{
			attribute.String(attrTenant, binding.TenantID),
			attribute.String(attrApp, binding.AppID),
			attribute.String(attrBinding, binding.BindingID),
			attribute.String(attrChannel, string(binding.Channel)),
		},
		// The binding is left off the measurements: one process serves one
		// binding, so it would add a label without adding an answer.
		metricIdentity: []attribute.KeyValue{
			attribute.String(attrTenant, binding.TenantID),
			attribute.String(attrApp, binding.AppID),
			attribute.String(attrChannel, string(binding.Channel)),
		},
	}, nil
}

// Span is one stage record in flight. A nil *Span records nothing, which is
// what Start returns when there is no recorder.
type Span struct {
	recorder *ChannelRecorder
	stage    Stage
	span     trace.Span
	started  time.Time
	ended    bool
}

// Start begins one stage record and returns the context it belongs to.
//
// The context is returned so the calls the stage makes run inside its span. It
// is not a propagation scheme: nothing writes a trace context to the Store or
// puts one on the wire.
func (r *ChannelRecorder) Start(ctx context.Context, stage Stage) (context.Context, *Span) {
	if r == nil {
		return ctx, nil
	}
	stage = stage.checked()
	ctx, span := r.telemetry.tracer.Start(ctx, spanPrefix+string(stage))
	return ctx, &Span{recorder: r, stage: stage, span: span, started: time.Now()}
}

// End closes one stage record. A second call records nothing: one pass through
// a stage is one measurement, whatever the caller's control flow does.
func (s *Span) End(result Result) {
	if s == nil || s.ended {
		return
	}
	s.ended = true
	elapsed := time.Since(s.started)
	outcome := result.Outcome.checked()
	errorType := checkedErrorType(result.ErrorType)
	classification := []attribute.KeyValue{
		attribute.String(attrStage, string(s.stage)),
		attribute.String(attrOutcome, string(outcome)),
		attribute.String(attrError, errorType),
	}

	attributes := make([]attribute.KeyValue, 0, len(s.recorder.spanIdentity)+9)
	attributes = append(attributes, s.recorder.spanIdentity...)
	attributes = append(attributes, classification...)
	// Identifiers go on the span and never on a measurement: they are how one
	// request is followed across three stages, and they are exactly the
	// cardinality a metric label must not have.
	for _, identifier := range []struct{ key, value string }{
		{attrRequest, result.RequestID},
		{attrRun, result.RunID},
		{attrOutbox, result.OutboxID},
		{attrRevision, result.RevisionID},
	} {
		if identifier.value != "" {
			attributes = append(attributes, attribute.String(identifier.key, identifier.value))
		}
	}
	if result.Attempt > 0 {
		attributes = append(attributes, attribute.Int(attrAttempt, int(result.Attempt)))
	}
	if result.Events > 0 {
		attributes = append(attributes, attribute.Int(attrEvents, int(result.Events)))
	}
	s.span.SetAttributes(attributes...)
	if outcome.failed() {
		s.span.SetStatus(codes.Error, "")
	}
	s.span.End()

	measured := metric.WithAttributes(
		append(append([]attribute.KeyValue{}, s.recorder.metricIdentity...), classification...)...)
	// Background, not the stage's context: a context carrying this span would
	// attach an exemplar, and an exemplar carries the trace and span ids of the
	// request into the metric stream.
	ctx := context.Background()
	s.recorder.telemetry.stages.Add(ctx, 1, measured)
	s.recorder.telemetry.duration.Record(ctx, float64(elapsed.Microseconds())/1000, measured)
}

// checkedErrorType keeps the error class inside the Store's closed vocabulary,
// so a class that is not one of them is recorded as the platform fault it would
// have to be.
func checkedErrorType(errorType channels.ErrorType) string {
	switch {
	case errorType == channels.ErrorNone:
		return errorTypeNone
	case errorType.Validate() != nil:
		return string(channels.ErrorInternal)
	default:
		return string(errorType)
	}
}
