// Package tracecontext transports bounded W3C observation metadata. A Carrier
// never authorizes a request or participates in a business payload's digest.
package tracecontext

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const MaxFieldBytes = 512

type Carrier struct {
	Traceparent string
	Tracestate  string
}

// Normalize drops invalid observation data, never returning a business error.
// Only trusted internal carriers should reach this function; public IM ingress
// starts a local root instead of inheriting arbitrary external tracing state.
func (c Carrier) Normalize() Carrier {
	if len(c.Traceparent) > MaxFieldBytes || strings.ContainsAny(c.Traceparent, "\r\n") {
		return Carrier{}
	}
	if len(c.Tracestate) > MaxFieldBytes || strings.ContainsAny(c.Tracestate, "\r\n") {
		c.Tracestate = ""
	}
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": c.Traceparent, "tracestate": c.Tracestate}))
	if !sc.IsValid() {
		return Carrier{}
	}
	out := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(trace.ContextWithSpanContext(context.Background(), sc), out)
	return Carrier{out.Get("traceparent"), out.Get("tracestate")}
}

func Capture(ctx context.Context) Carrier {
	h := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, h)
	return (Carrier{h.Get("traceparent"), h.Get("tracestate")}).Normalize()
}

// Restore preserves the current task's cancellation/deadline and replaces only
// its SpanContext. No old HTTP context or SDK Span object is persisted.
func (c Carrier) Restore(ctx context.Context) context.Context {
	c = c.Normalize()
	if c.Traceparent == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx,
		propagation.MapCarrier{"traceparent": c.Traceparent, "tracestate": c.Tracestate})
}

// FromHeaders supports HTTP and NATS header maps, including mixed-case keys.
// Ambiguous traceparent is discarded. Multiple tracestate lines form one list.
func FromHeaders(headers map[string][]string) Carrier {
	var c Carrier
	parents := 0
	stateInvalid := false
	for key, values := range headers {
		switch {
		case strings.EqualFold(key, "traceparent"):
			for _, value := range values {
				parents++
				if len(value) > MaxFieldBytes {
					return Carrier{}
				}
				c.Traceparent = value
			}
		case strings.EqualFold(key, "tracestate"):
			for _, value := range values {
				extra := len(value)
				if c.Tracestate != "" {
					extra++
				}
				if len(c.Tracestate)+extra > MaxFieldBytes {
					stateInvalid = true
					continue
				}
				if c.Tracestate != "" {
					c.Tracestate += ","
				}
				c.Tracestate += value
			}
		}
	}
	if parents != 1 {
		return Carrier{}
	}
	if stateInvalid {
		c.Tracestate = ""
	}
	return c.Normalize()
}

// Inject replaces only trace headers, retaining broker IDs and other metadata.
func (c Carrier) Inject(headers map[string][]string) {
	if headers == nil {
		return
	}
	for key := range headers {
		if strings.EqualFold(key, "traceparent") || strings.EqualFold(key, "tracestate") {
			delete(headers, key)
		}
	}
	c = c.Normalize()
	if c.Traceparent == "" {
		return
	}
	headers["traceparent"] = []string{c.Traceparent}
	if c.Tracestate != "" {
		headers["tracestate"] = []string{c.Tracestate}
	}
}
