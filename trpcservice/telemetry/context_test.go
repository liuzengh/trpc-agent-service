package telemetry

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"testing"
)

func TestTraceParentRoundTrip(t *testing.T) {
	old := otel.GetTextMapPropagator()
	defer otel.SetTextMapPropagator(old)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer tp.Shutdown(context.Background())
	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	value := InjectTraceParent(ctx)
	span.End()
	if value == "" {
		t.Fatal("traceparent was not injected")
	}
	extracted := ExtractTraceParent(context.Background(), value)
	_, child := tp.Tracer("test").Start(extracted, "child")
	defer child.End()
	if child.SpanContext().TraceID() != trace.SpanContextFromContext(ctx).TraceID() {
		t.Fatal("trace ID did not propagate")
	}
}
