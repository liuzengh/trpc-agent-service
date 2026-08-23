// Package telemetry configures OpenTelemetry and preserves IM trace identifiers.
package telemetry

import (
	"context"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// ContextWithTraceID converts a UUID-shaped platform trace ID into an OTel
// remote parent, so channel, worker, tRPC Runner and reply spans share a trace.
func ContextWithTraceID(ctx context.Context, id string) context.Context {
	hexID := strings.ReplaceAll(id, "-", "")
	if len(hexID) != 32 {
		return ctx
	}
	traceID, err := trace.TraceIDFromHex(hexID)
	if err != nil {
		return ctx
	}
	spanID, err := trace.SpanIDFromHex(hexID[:16])
	if err != nil {
		return ctx
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	return trace.ContextWithRemoteSpanContext(ctx, spanContext)
}
