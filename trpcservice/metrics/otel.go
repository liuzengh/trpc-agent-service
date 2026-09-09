// Tracing init and traceparent propagation helpers.
package metrics

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	atrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

// InitTracing initializes the global tracer provider and the W3C propagator.
// The returned shutdown flushes pending spans; call it before process exit.
func InitTracing(ctx context.Context) (shutdown func() error, err error) {
	clean, err := atrace.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start tracing: %w", err)
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return clean, nil
}

// InjectTraceparent writes the current span context from ctx into carrier.
func InjectTraceparent(ctx context.Context, carrier propagation.MapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// ExtractTraceparent restores the remote span context from carrier into ctx.
func ExtractTraceparent(ctx context.Context, carrier propagation.MapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}
