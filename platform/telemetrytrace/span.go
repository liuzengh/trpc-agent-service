package telemetrytrace

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Start preserves propagation even when this process has no exporter.
func Start(t trace.Tracer, ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if t == nil {
		t = noop.NewTracerProvider().Tracer("application")
	}
	return t.Start(ctx, name, opts...)
}

// End never serializes an underlying dependency error into telemetry.
func End(span trace.Span, err error) {
	if err != nil {
		kind := "failed"
		if errors.Is(err, context.Canceled) {
			kind = "cancelled"
		} else if errors.Is(err, context.DeadlineExceeded) {
			kind = "deadline"
		}
		span.SetStatus(codes.Error, "")
		span.SetAttributes(attribute.String("error.type", kind))
	}
	span.End()
}
