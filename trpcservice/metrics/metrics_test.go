package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

func TestMetricLabelsCannotCarryUserSessionOrMessageIDs(t *testing.T) {
	attributes := (Labels{TenantID: "t", AppID: "a", Channel: "http", Operation: "runner", Status: "ok"}).attributes()
	for _, item := range attributes {
		switch string(item.Key) {
		case "user.id", "session.id", "message.id", "request.id":
			t.Fatalf("high-cardinality metric key %q", item.Key)
		}
	}
}

func TestStorageMetricLabelsCannotCarryKeysPayloadsOrCredentials(t *testing.T) {
	attributes := (StorageLabels{TenantID: "t", AppID: "a", Domain: "session", Backend: "postgres", Operation: "get", Status: "success"}).attributes()
	for _, item := range attributes {
		switch string(item.Key) {
		case "user.id", "session.id", "message.id", "request.id", "content", "error", "endpoint", "credential":
			t.Fatalf("unsafe storage metric key %q", item.Key)
		}
	}
}

func TestStorageOperationEmitsDurationAndFailureCounter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer func() {
		_ = provider.Shutdown(context.Background())
		otel.SetMeterProvider(old)
	}()
	telemetry, err := New("storage-metric-test")
	if err != nil {
		t.Fatal(err)
	}
	telemetry.StorageOperation(context.Background(), StorageLabels{TenantID: "tenant", AppID: "app", Domain: "memory", Backend: "external", Operation: "search", Status: "failed"}, time.Millisecond)
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	foundDuration, foundErrors := false, false
	for _, scope := range collected.ScopeMetrics {
		for _, item := range scope.Metrics {
			switch item.Name {
			case "agent.storage.operation.duration":
				value, ok := item.Data.(metricdata.Histogram[float64])
				foundDuration = ok && len(value.DataPoints) == 1 && value.DataPoints[0].Count == 1
			case "agent.storage.operation.errors":
				value, ok := item.Data.(metricdata.Sum[int64])
				foundErrors = ok && len(value.DataPoints) == 1 && value.DataPoints[0].Value == 1
			}
		}
	}
	if !foundDuration || !foundErrors {
		t.Fatalf("storage metrics duration=%v errors=%v", foundDuration, foundErrors)
	}
}

func TestCostAndMissingUsageMetricsUseBoundedLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer func() {
		_ = provider.Shutdown(context.Background())
		otel.SetMeterProvider(old)
	}()
	telemetry, err := New("cost-metric-test")
	if err != nil {
		t.Fatal(err)
	}
	labels := Labels{TenantID: "tenant", AppID: "app", Channel: "feishu", Operation: "model", Status: "missing"}
	telemetry.Request(context.Background(), labels, time.Millisecond, 3, 7)
	telemetry.ModelUsageMissing(context.Background(), labels)
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	foundCost, foundMissing := false, false
	for _, scope := range collected.ScopeMetrics {
		for _, item := range scope.Metrics {
			switch item.Name {
			case "agent.cost.micros":
				value, ok := item.Data.(metricdata.Sum[int64])
				foundCost = ok && len(value.DataPoints) == 1 && value.DataPoints[0].Value == 7
			case "agent.model.usage_missing":
				value, ok := item.Data.(metricdata.Sum[int64])
				foundMissing = ok && len(value.DataPoints) == 1 && value.DataPoints[0].Value == 1
			}
		}
	}
	if !foundCost || !foundMissing {
		t.Fatalf("cost=%v usage_missing=%v", foundCost, foundMissing)
	}
}

func TestSpanAttributesHashCallerControlledIdentifiers(t *testing.T) {
	canary := "secret-or-message-canary"
	attributes := (SpanFields{TenantID: "tenant", AppID: "app", Channel: "http", RequestID: canary, TraceID: canary}).attributes()
	for _, item := range attributes {
		if strings.Contains(item.Value.Emit(), canary) {
			t.Fatalf("span attribute %q leaked caller-controlled value", item.Key)
		}
		switch string(item.Key) {
		case "request.id", "correlation.trace_id", "message", "content", "secret":
			t.Fatalf("unsafe span attribute key %q", item.Key)
		}
	}
}
func TestTraceContextRoundTrip(t *testing.T) {
	old := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(old)
	telemetry, err := New("test")
	if err != nil {
		t.Fatal(err)
	}
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled}))
	carrier := telemetry.Inject(ctx)
	restored := telemetry.Extract(context.Background(), carrier)
	if got := trace.SpanContextFromContext(restored).TraceID(); got != traceID {
		t.Fatalf("trace ID=%s", got)
	}
}

func TestTraceContextRejectsBaggageAndUnknownFields(t *testing.T) {
	canary := "secret-canary-must-not-propagate"
	safe := safeTraceCarrier(map[string]string{
		"TraceParent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01",
		"tracestate":  "vendor=value",
		"baggage":     "authorization=" + canary,
		"message":     canary,
	})
	if len(safe) != 1 || safe["traceparent"] == "" {
		t.Fatalf("safe trace carrier=%v", safe)
	}
	for key, value := range safe {
		if strings.Contains(key, "baggage") || strings.Contains(value, canary) {
			t.Fatalf("unsafe trace field survived: %s", key)
		}
	}
}
