package metrics

import (
	"context"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"testing"
)

func TestConfigureOTLPDisabledAndSampleRatioValidation(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	providers, err := ConfigureOTLP(context.Background(), "service", "node")
	if err != nil {
		t.Fatal(err)
	}
	if err := providers.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Setenv(traceSampleRatioEnv, "1.1")
	if _, err := traceSampleRatio(); err == nil {
		t.Fatal("invalid sampling ratio accepted")
	}
	t.Setenv(traceSampleRatioEnv, "0.25")
	ratio, err := traceSampleRatio()
	if err != nil || ratio != 0.25 {
		t.Fatalf("ratio=%v err=%v", ratio, err)
	}
}

func TestServiceResourceMergesWithDefaultSchema(t *testing.T) {
	merged, err := serviceResource("trpc-agent-service", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	value, ok := merged.Set().Value(semconv.ServiceNameKey)
	if !ok || value.AsString() != "trpc-agent-service" {
		t.Fatalf("service.name=%q found=%v", value.AsString(), ok)
	}
}
