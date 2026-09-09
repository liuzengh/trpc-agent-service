package telemetry

import (
	"context"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type recordingMetricEventSink struct {
	mu     sync.Mutex
	events []control.MetricEvent
}

func (s *recordingMetricEventSink) Record(_ context.Context, event control.MetricEvent) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
}

func TestBusinessMetricsExposeRequiredFamilies(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics, err := NewBusinessMetrics(provider.Meter("phase7-test"))
	if err != nil {
		t.Fatal(err)
	}
	defer metrics.Close()
	defer provider.Shutdown(context.Background())

	ctx := context.Background()
	metrics.Request(ctx, "tenant-a", "telegram", "succeeded")
	metrics.Duration(ctx, 0.1, "request")
	metrics.ModelDuration.Record(ctx, 0.2)
	metrics.Tokens(ctx, 3, "total")
	metrics.Tool(ctx, "allowed")
	metrics.OutboundMessage(ctx, "telegram", "succeeded")
	metrics.StorageError(ctx, "postgres", "sql_unavailable")
	metrics.PersistenceEvent(ctx, "postgres", "succeeded")
	metrics.AssignmentEvent(ctx, "admitted", "")
	metrics.HeartbeatEvent(ctx, "task", "succeeded")
	metrics.DegradedEvent(ctx, "false", "")
	metrics.TelemetryDrop(ctx, "audit_sink", "queue_full")
	SetInflight(2)

	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"trpc_agent_requests_total": false, "trpc_agent_request_duration_seconds": false,
		"trpc_agent_model_call_duration_seconds": false, "trpc_agent_model_tokens_total": false,
		"trpc_agent_tool_calls_total": false, "trpc_agent_im_outbound_total": false,
		"trpc_agent_storage_errors_total": false, "trpc_agent_persistence_total": false,
		"trpc_agent_assignments_total": false, "trpc_agent_heartbeats_total": false,
		"trpc_agent_degraded_total": false, "trpc_agent_telemetry_dropped_total": false,
		"trpc_agent_inflight_tasks": false,
	}
	for _, scope := range data.ScopeMetrics {
		for _, current := range scope.Metrics {
			if _, ok := want[current.Name]; ok {
				want[current.Name] = true
			}
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("metric %q was not collected", name)
		}
	}
}

func TestRecordRequestEmitsBoundedDiscreteEvent(t *testing.T) {
	sink := &recordingMetricEventSink{}
	restore := SetMetricEventSink(sink)
	defer restore()

	RecordRequest(context.Background(), "tenant-a", "telegram", "succeeded", 0.25)
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.events) != 1 {
		t.Fatalf("events = %d, want 1", len(sink.events))
	}
	event := sink.events[0]
	if err := event.Validate(); err != nil {
		t.Fatalf("event is invalid: %v", err)
	}
	if event.TenantID != "tenant-a" || event.Name != "request" || event.Value != 0.25 || event.Count != 1 {
		t.Fatalf("event = %#v", event)
	}
	if len(event.Labels) != 3 || event.Labels["channel"] != "telegram" || event.Labels["status"] != "succeeded" || event.Labels["operation"] != "request" {
		t.Fatalf("labels = %#v", event.Labels)
	}
}
