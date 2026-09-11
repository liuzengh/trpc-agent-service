package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func collectMetrics(t *testing.T, reader *metric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var resourceMetrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resourceMetrics); err != nil {
		t.Fatal(err)
	}
	return resourceMetrics
}

func intMetricValue(t *testing.T, resourceMetrics metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, metricData := range scopeMetrics.Metrics {
			if metricData.Name != name {
				continue
			}
			switch data := metricData.Data.(type) {
			case metricdata.Sum[int64]:
				if len(data.DataPoints) == 0 {
					return 0
				}
				return data.DataPoints[0].Value
			case metricdata.Gauge[int64]:
				if len(data.DataPoints) == 0 {
					return 0
				}
				return data.DataPoints[0].Value
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

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

func TestOTelObserverTracksActiveExecutions(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	observer, err := NewOTelObserver(sdktrace.NewTracerProvider().Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatal(err)
	}
	_, finish := observer.StartExecution(context.Background(), ExecutionAttributes{
		TenantID: "tenant-a", AppCode: "support", Channel: "web", ConfigVersion: 1,
	})
	if got := intMetricValue(t, collectMetrics(t, reader), "agent.execution.active"); got != 1 {
		t.Fatalf("active executions = %d, want 1", got)
	}
	finish(nil)
	if got := intMetricValue(t, collectMetrics(t, reader), "agent.execution.active"); got != 0 {
		t.Fatalf("active executions after finish = %d, want 0", got)
	}
}

func TestOTelObserverExportsDatabaseAndRedisPoolSaturation(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	observer, err := NewOTelObserver(sdktrace.NewTracerProvider().Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.ObserveDatabasePool(func() DatabasePoolStats {
		return DatabasePoolStats{
			MaxOpenConnections: 100, OpenConnections: 37, InUse: 31, Idle: 6,
			WaitCount: 42, WaitDuration: 1250 * time.Millisecond,
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := observer.ObserveRedisPool(func() RedisPoolStats {
		return RedisPoolStats{Hits: 90, Misses: 10, Timeouts: 3, WaitCount: 11, WaitDuration: 750 * time.Millisecond, TotalConnections: 20, IdleConnections: 7, StaleConnections: 2}
	}); err != nil {
		t.Fatal(err)
	}
	if err := observer.ObserveWorkerCapacity(func() int64 { return 8 }); err != nil {
		t.Fatal(err)
	}
	metrics := collectMetrics(t, reader)
	for name, want := range map[string]int64{
		"database.pool.connections.max":    100,
		"database.pool.connections.open":   37,
		"database.pool.connections.in_use": 31,
		"database.pool.connections.idle":   6,
		"database.pool.waits":              42,
		"database.pool.wait_duration.ms":   1250,
		"redis.pool.hits":                  90,
		"redis.pool.misses":                10,
		"redis.pool.timeouts":              3,
		"redis.pool.waits":                 11,
		"redis.pool.wait_duration.ms":      750,
		"redis.pool.connections.total":     20,
		"redis.pool.connections.idle":      7,
		"redis.pool.connections.stale":     2,
		"worker.execution.slots":           8,
	} {
		if got := intMetricValue(t, metrics, name); got != want {
			t.Fatalf("%s = %d, want %d", name, got, want)
		}
	}
}

func TestOTelObserverRecordsKafkaCapacity(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	observer, err := NewOTelObserver(sdktrace.NewTracerProvider().Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatal(err)
	}
	observer.RecordKafkaCapacity(context.Background(), KafkaCapacityAttributes{
		Topic: "agent.inbound.v1", ConsumerGroup: "trpc-agent-worker", Lag: 37, Partitions: 80,
	})
	metrics := collectMetrics(t, reader)
	if got := intMetricValue(t, metrics, "messaging.kafka.consumer.lag"); got != 37 {
		t.Fatalf("Kafka lag = %d, want 37", got)
	}
	if got := intMetricValue(t, metrics, "messaging.kafka.topic.partitions"); got != 80 {
		t.Fatalf("Kafka partitions = %d, want 80", got)
	}
	observer.RecordKafkaCapacityFailure(context.Background(), "agent.inbound.v1", "trpc-agent-worker")
	metrics = collectMetrics(t, reader)
	if got := intMetricValue(t, metrics, "messaging.kafka.capacity.sample.failures"); got != 1 {
		t.Fatalf("Kafka capacity sample failures = %d, want 1", got)
	}
}

func TestOTelObserverRecordsOutboxBacklog(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	observer, err := NewOTelObserver(sdktrace.NewTracerProvider().Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatal(err)
	}
	observer.RecordOutboxBacklog(context.Background(), OutboxBacklogAttributes{
		TenantID: "tenant-a", Channel: "telegram", Pending: 12, OldestAge: 45 * time.Second,
	})
	metrics := collectMetrics(t, reader)
	if got := intMetricValue(t, metrics, "outbox.pending.events"); got != 12 {
		t.Fatalf("outbox pending = %d, want 12", got)
	}
	if got := intMetricValue(t, metrics, "outbox.oldest.age.seconds"); got != 45 {
		t.Fatalf("outbox oldest age = %d, want 45", got)
	}
}

func TestOTelObserverRecordsGovernanceToolAndDeadLetterMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	observer, err := NewOTelObserver(sdktrace.NewTracerProvider().Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	observer.RecordDeadLetter(ctx, DeadLetterAttributes{TenantID: "tenant-a", ErrorClass: "permanent"})
	observer.RecordToolExecution(ctx, ToolExecutionAttributes{TenantID: "tenant-a", ToolName: "request_refund", Outcome: "failed", Latency: 125 * time.Millisecond})
	observer.RecordGovernanceRejection(ctx, GovernanceRejectionAttributes{TenantID: "tenant-a", AppCode: "support", Reason: "token_budget"})

	metrics := collectMetrics(t, reader)
	for name, want := range map[string]int64{
		"messaging.dlq.messages":  1,
		"tool.execution.total":    1,
		"tool.execution.failures": 1,
		"governance.rejections":   1,
	} {
		if got := intMetricValue(t, metrics, name); got != want {
			t.Fatalf("%s = %d, want %d", name, got, want)
		}
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

func TestOTelObserverTracksActiveSSERequestsSeparately(t *testing.T) {
	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	observer, err := NewOTelObserver(sdktrace.NewTracerProvider().Tracer("platform-tests"), meterProvider.Meter("platform-tests"))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	handler := observer.WrapHTTP(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		once.Do(func() { close(started) })
		<-release
	}))
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api/v1/chat/stream?tenant=support", nil))
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}
	metrics := collectMetrics(t, reader)
	found := false
	for _, scopeMetrics := range metrics.ScopeMetrics {
		for _, metricData := range scopeMetrics.Metrics {
			if metricData.Name != "http.server.active_requests" {
				continue
			}
			for _, point := range metricData.Data.(metricdata.Sum[int64]).DataPoints {
				attrs := point.Attributes.ToSlice()
				for _, attr := range attrs {
					if string(attr.Key) == "http.route" && attr.Value.AsString() == "/api/v1/chat/stream" && point.Value == 1 {
						found = true
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("active SSE request metric not found")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not finish")
	}
}
