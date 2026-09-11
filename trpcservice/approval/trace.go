package approval

import (
	"context"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func originTraceParent(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

func originSpanContext(value string) trace.SpanContext {
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": value})
	return trace.SpanContextFromContext(ctx)
}
