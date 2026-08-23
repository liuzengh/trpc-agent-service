package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestContextWithTraceID(t *testing.T) {
	original := context.Background()
	if got := ContextWithTraceID(original, "invalid"); trace.SpanContextFromContext(got).IsValid() {
		t.Fatal("invalid trace id should be ignored")
	}
	id := "123e4567-e89b-12d3-a456-426614174000"
	span := trace.SpanContextFromContext(ContextWithTraceID(original, id))
	if !span.IsValid() || !span.IsRemote() || !span.IsSampled() || span.TraceID().String() != "123e4567e89b12d3a456426614174000" {
		t.Fatalf("span context=%+v", span)
	}
}

func TestInitWithoutExporter(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	shutdown, err := Init(context.Background(), "test")
	if err != nil || shutdown == nil {
		t.Fatalf("missing shutdown function or init error: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
