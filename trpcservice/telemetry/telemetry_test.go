package telemetry

import (
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func TestTelemetryResourceMergesWithDefaultSchema(t *testing.T) {
	res, err := telemetryResource("resource-test")
	if err != nil {
		t.Fatalf("telemetry resource: %v", err)
	}
	value, ok := res.Set().Value(semconv.ServiceNameKey)
	if !ok || value.AsString() != "resource-test" {
		t.Fatalf("service.name = %q, ok=%t", value.AsString(), ok)
	}
}
