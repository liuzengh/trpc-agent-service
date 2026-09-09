package telemetry

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ContextState marks what the carrier extraction found. It is a low
// cardinality metric attribute and never a business fact.
type ContextState string

const (
	ContextStatePresent ContextState = "present"
	ContextStateMissing ContextState = "missing"
	ContextStateInvalid ContextState = "invalid"
)

// TraceCarrier is the optional, additive, W3C trace-context field carried
// through durable boundaries (queue job payloads and reply Outbox payloads).
// It holds only a traceparent; baggage is never propagated and a caller never
// supplies one.
type TraceCarrier struct {
	Traceparent string `json:"traceparent,omitempty"`
}

// InjectCarrier captures the current span context as a W3C traceparent. It
// returns nil when the context carries no valid remote-eligible span so old
// payloads stay byte-identical.
func InjectCarrier(ctx context.Context) *TraceCarrier {
	span := trace.SpanContextFromContext(ctx)
	if !span.IsValid() || !span.HasTraceID() {
		return nil
	}
	carrier := &TraceCarrier{}
	propagator.Inject(ctx, carrierWriter{carrier})
	if carrier.Traceparent == "" {
		return nil
	}
	return carrier
}

// Extract decorates the context with the carrier's span context for LINK
// purposes only: the extracted context is never used for authentication,
// tenant resolution, dedup, lease or fencing. Missing or invalid carriers
// yield (original context, ContextStateMissing/Invalid) and the caller starts
// a fresh server span.
func Extract(ctx context.Context, carrier *TraceCarrier) (context.Context, ContextState) {
	if carrier == nil || carrier.Traceparent == "" {
		return ctx, ContextStateMissing
	}
	header := http.Header{}
	header.Set("traceparent", carrier.Traceparent)
	extracted := propagator.Extract(ctx, propagation.HeaderCarrier(header))
	span := trace.SpanContextFromContext(extracted)
	if !span.IsValid() || !span.HasTraceID() {
		return ctx, ContextStateInvalid
	}
	return extracted, ContextStatePresent
}

var propagator = newPropagator()

func newPropagator() propagation.TextMapPropagator {
	return propagation.TraceContext{}
}

type carrierWriter struct{ carrier *TraceCarrier }

func (w carrierWriter) Get(string) string { return "" }

func (w carrierWriter) Set(key, value string) {
	if key == "traceparent" {
		w.carrier.Traceparent = value
	}
}

func (w carrierWriter) Keys() []string { return []string{"traceparent"} }
