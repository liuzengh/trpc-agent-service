package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestOTelObserverRecordsExecutionTelemetry verifies that a handled execution
// increments the counter, records a duration, and exports a span carrying only
// routing attributes — never message content.
func TestOTelObserverRecordsExecutionTelemetry(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	exporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracer := tracerProvider.Tracer("platform-tests")

	observer, err := NewOTelObserver(tracer, meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatalf("NewOTelObserver() error = %v", err)
	}

	_, finish := observer.StartExecution(context.Background(), ExecutionAttributes{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", ProviderRequestID: "tg-update-42", ConfigVersion: 3,
	})
	finish(nil)

	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	var total, failures int64
	var durations int
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, metricData := range scopeMetrics.Metrics {
			switch metricData.Name {
			case "agent.execution.total":
				total = metricData.Data.(metricdata.Sum[int64]).DataPoints[0].Value
			case "agent.execution.failures":
				failures = metricData.Data.(metricdata.Sum[int64]).DataPoints[0].Value
			case "agent.execution.duration.ms":
				durations = len(metricData.Data.(metricdata.Histogram[float64]).DataPoints)
			}
		}
	}
	if total != 1 {
		t.Fatalf("agent.execution.total = %d, want 1", total)
	}
	if failures != 0 {
		t.Fatalf("agent.execution.failures = %d, want 0", failures)
	}
	if durations != 1 {
		t.Fatalf("duration data points = %d, want 1", durations)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(spans))
	}
	if spans[0].Name != "agent.runtime.handle" {
		t.Fatalf("span name = %q, want agent.runtime.handle", spans[0].Name)
	}
	attributes := spans[0].Attributes
	if got := attributeValue(attributes, "tenant.id"); got != "tenant-a" {
		t.Fatalf("span tenant.id = %q, want tenant-a", got)
	}
	if got := attributeValue(attributes, "config.version"); got != "3" {
		t.Fatalf("span config.version = %q, want 3", got)
	}
	if got := attributeValue(attributes, "external.request.id"); got != "tg-update-42" {
		t.Fatalf("span external.request.id = %q, want tg-update-42", got)
	}
}

// TestOTelObserverRecordsFailures verifies error executions increment the
// failure counter and mark the span as errored.
func TestOTelObserverRecordsFailures(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	exporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	observer, err := NewOTelObserver(tracerProvider.Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatalf("NewOTelObserver() error = %v", err)
	}

	_, finish := observer.StartExecution(context.Background(), ExecutionAttributes{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", ConfigVersion: 1,
	})
	finish(errors.New("send failed"))

	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	var failures int64
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, metricData := range scopeMetrics.Metrics {
			if metricData.Name == "agent.execution.failures" {
				failures = metricData.Data.(metricdata.Sum[int64]).DataPoints[0].Value
			}
		}
	}
	if failures != 1 {
		t.Fatalf("agent.execution.failures = %d, want 1", failures)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(spans))
	}
	if spans[0].Status.Code.String() == "Unset" {
		t.Fatal("failed execution span status = Unset, want Error")
	}
}

func TestOTelObserverRecordsCachedPromptTokensWithoutRequestLabels(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	observer, err := NewOTelObserver(sdktrace.NewTracerProvider().Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatal(err)
	}
	observer.RecordModelUsage(context.Background(), ModelUsageAttributes{
		TenantID: "tenant-a", AppCode: "support", ProviderID: "primary", ModelName: "model-a",
		PromptTokens: 100, CachedPromptTokens: 70, CompletionTokens: 20, TotalTokens: 120, CostMicros: 9,
	})

	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		t.Fatal(err)
	}
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, metricData := range scopeMetrics.Metrics {
			if metricData.Name != "model.tokens.prompt.cached" {
				continue
			}
			point := metricData.Data.(metricdata.Sum[int64]).DataPoints[0]
			if point.Value != 70 {
				t.Fatalf("cached prompt tokens = %d, want 70", point.Value)
			}
			for _, label := range point.Attributes.ToSlice() {
				if string(label.Key) == "external.request.id" {
					t.Fatal("request ID must not be a metric label")
				}
			}
			return
		}
	}
	t.Fatal("model.tokens.prompt.cached metric not found")
}

func attributeValue(attributes []attribute.KeyValue, key string) string {
	for _, entry := range attributes {
		if string(entry.Key) == key {
			if entry.Value.Type() == attribute.STRING {
				return entry.Value.AsString()
			}
			return fmt.Sprintf("%d", entry.Value.AsInt64())
		}
	}
	return ""
}

func TestOTelObserverWrapsHTTPWithoutRecordingRequestContent(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	exporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	observer, err := NewOTelObserver(tracerProvider.Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatalf("NewOTelObserver() error = %v", err)
	}
	handler := observer.WrapHTTP(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusAccepted)
	}))
	request := httptest.NewRequest(http.MethodPost, "/api/v1/chat/async", nil)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	request.Header.Set("X-Request-ID", "feishu-request-42")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("HTTP status = %d, want %d", response.Code, http.StatusAccepted)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(spans))
	}
	if got := attributeValue(spans[0].Attributes, "http.route"); got != "/api/v1/{resource}" {
		t.Fatalf("http.route = %q", got)
	}
	if got := attributeValue(spans[0].Attributes, "external.request.id"); got != "feishu-request-42" {
		t.Fatalf("HTTP span external.request.id = %q", got)
	}
	for _, attribute := range spans[0].Attributes {
		if string(attribute.Key) == "url.path" || string(attribute.Key) == "http.target" || string(attribute.Key) == "http.request.body" {
			t.Fatalf("HTTP span contains unsafe request attribute %q", attribute.Key)
		}
	}
}
