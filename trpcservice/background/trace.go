package background

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

func ContextWithTraceParent(ctx context.Context, traceParent string) context.Context {
	carrier := propagation.MapCarrier{}
	if traceParent != "" {
		carrier.Set("traceparent", traceParent)
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
