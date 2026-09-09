//go:build integration

package telemetry

import (
	"context"
	"os"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
)

// TestSetupTracingExportsToTLSCollector exercises the same environment-driven
// seam used by every production process. The surrounding lab starts a real
// TLS-enabled OpenTelemetry Collector and separately checks its received span
// log, so a successful shutdown is evidence that the batch was flushed rather
// than merely queued in-process.
func TestSetupTracingExportsToTLSCollector(t *testing.T) {
	if os.Getenv("OTEL_TLS_INTEGRATION") != "1" {
		t.Skip("set OTEL_TLS_INTEGRATION=1 to run against a TLS collector")
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		t.Fatal("TLS collector acceptance requires OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	// Acceptance must export its probe, independently of production sampling.
	t.Setenv("OTEL_TRACE_SAMPLE_RATIO", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "false")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdown, err := SetupTracing(ctx, "trpc-agent-v13-otlp-tls-integration")
	if err != nil {
		t.Fatalf("setup TLS tracing: %v", err)
	}

	_, span := otel.Tracer("enterprise/integration").Start(context.Background(), "integration.otlp_tls")
	if !span.IsRecording() || !span.SpanContext().IsSampled() {
		span.End()
		_ = shutdown(ctx)
		t.Fatal("TLS collector probe must be recorded and sampled")
	}
	span.End()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("flush TLS trace export: %v", err)
	}
}
