package observability

import (
	"context"
	"errors"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestStorageSpanUsesOnlyRedactedLowCardinalityAttributes(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})
	ctx, finish := StartStorage(context.Background(), "session.commit", "postgres", "tenant-secret-value", "revision-1")
	if ctx == nil {
		t.Fatal("storage span did not return context")
	}
	finish(errors.New("provider response contains secret-value"))
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	for _, attr := range spans[0].Attributes() {
		if strings.Contains(attr.Value.AsString(), "tenant-secret-value") || strings.Contains(attr.Value.AsString(), "provider response") {
			t.Fatalf("sensitive value appeared in span attribute %s=%s", attr.Key, attr.Value.AsString())
		}
	}
}

func TestStorageMetricsErrorClassificationAndSingleObservation(t *testing.T) {
	exporter := metrics.NewMetrics()
	restore := SetStorageMetrics(exporter)
	defer restore()
	_, finish := StartStorage(context.Background(), "session.get", "redis", "tenant-a", "version-unique")
	finish(context.DeadlineExceeded)
	finish(nil)
	out := httptest.NewRecorder()
	exporter.ServeHTTP(out, httptest.NewRequest("GET", "/metrics", nil))
	body := out.Body.String()
	if !strings.Contains(body, `# TYPE storage_operation_duration_seconds histogram`) || !strings.Contains(body, `result="timeout"`) || !strings.Contains(body, `le="+Inf"} 1`) {
		t.Fatalf("missing storage histogram: %s", body)
	}
	if strings.Contains(body, "version-unique") || strings.Contains(body, `tenant="tenant-a"`) {
		t.Fatal("unbounded/plain tenant labels")
	}
}
