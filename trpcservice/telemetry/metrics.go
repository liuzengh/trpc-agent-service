package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var activeMetrics atomic.Pointer[BusinessMetrics]

type MetricEventSink interface {
	Record(context.Context, control.MetricEvent)
}

var (
	eventSinkMu sync.RWMutex
	eventSink   MetricEventSink
	eventSeq    atomic.Uint64
)

// SetMetricEventSink installs the process-wide discrete metric event sink and
// returns a restore function for orderly shutdown and tests.
func SetMetricEventSink(sink MetricEventSink) func() {
	eventSinkMu.Lock()
	previous := eventSink
	eventSink = sink
	eventSinkMu.Unlock()
	return func() {
		eventSinkMu.Lock()
		if eventSink == sink {
			eventSink = previous
		}
		eventSinkMu.Unlock()
	}
}

func emitMetricEvent(ctx context.Context, event control.MetricEvent) {
	eventSinkMu.RLock()
	sink := eventSink
	eventSinkMu.RUnlock()
	if sink == nil {
		return
	}
	event.ID = metricEventID()
	event.OccurredAt = time.Now().UTC()
	sink.Record(ctx, event)
}

func metricEventID() string {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err == nil {
		return "metric-" + hex.EncodeToString(raw)
	}
	return fmt.Sprintf("metric-%d-%d", time.Now().UnixNano(), eventSeq.Add(1))
}

// BusinessMetrics keeps metric names and dimensions bounded. IDs are never
// accepted as labels; callers should use tenant/channel/backend dimensions.
type BusinessMetrics struct {
	Requests        metric.Int64Counter
	RequestDuration metric.Float64Histogram
	ModelDuration   metric.Float64Histogram
	ModelTokens     metric.Int64Counter
	ToolCalls       metric.Int64Counter
	Outbound        metric.Int64Counter
	StorageErrors   metric.Int64Counter
	Persistence     metric.Int64Counter
	Assignments     metric.Int64Counter
	Heartbeats      metric.Int64Counter
	Degraded        metric.Int64Counter
	TelemetryDrops  metric.Int64Counter
	Inflight        metric.Int64ObservableGauge
	registration    metric.Registration
}

var inflightTasks atomic.Int64

func NewBusinessMetrics(m metric.Meter) (*BusinessMetrics, error) {
	requests, err := m.Int64Counter("trpc_agent_requests_total")
	if err != nil {
		return nil, err
	}
	duration, err := m.Float64Histogram("trpc_agent_request_duration_seconds", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	modelDuration, err := m.Float64Histogram("trpc_agent_model_call_duration_seconds", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	tokens, err := m.Int64Counter("trpc_agent_model_tokens_total")
	if err != nil {
		return nil, err
	}
	tools, err := m.Int64Counter("trpc_agent_tool_calls_total")
	if err != nil {
		return nil, err
	}
	outbound, err := m.Int64Counter("trpc_agent_im_outbound_total")
	if err != nil {
		return nil, err
	}
	storageErrors, err := m.Int64Counter("trpc_agent_storage_errors_total")
	if err != nil {
		return nil, err
	}
	persistence, err := m.Int64Counter("trpc_agent_persistence_total")
	if err != nil {
		return nil, err
	}
	assignments, err := m.Int64Counter("trpc_agent_assignments_total")
	if err != nil {
		return nil, err
	}
	heartbeats, err := m.Int64Counter("trpc_agent_heartbeats_total")
	if err != nil {
		return nil, err
	}
	degraded, err := m.Int64Counter("trpc_agent_degraded_total")
	if err != nil {
		return nil, err
	}
	drops, err := m.Int64Counter("trpc_agent_telemetry_dropped_total")
	if err != nil {
		return nil, err
	}
	inflight, err := m.Int64ObservableGauge("trpc_agent_inflight_tasks")
	if err != nil {
		return nil, err
	}
	registration, err := m.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(inflight, inflightTasks.Load())
		return nil
	}, inflight)
	if err != nil {
		return nil, err
	}
	return &BusinessMetrics{
		Requests: requests, RequestDuration: duration, ModelDuration: modelDuration,
		ModelTokens: tokens, ToolCalls: tools, Outbound: outbound, StorageErrors: storageErrors,
		Persistence: persistence, Assignments: assignments, Heartbeats: heartbeats,
		Degraded: degraded, TelemetryDrops: drops, Inflight: inflight, registration: registration,
	}, nil
}

func (m *BusinessMetrics) Close() {
	if m != nil && m.registration != nil {
		_ = m.registration.Unregister()
	}
}

func (m *BusinessMetrics) Request(ctx context.Context, tenant, channel, status string) {
	if m == nil {
		return
	}
	m.Requests.Add(ctx, 1, metric.WithAttributes(attribute.String("tenant_id", tenant), attribute.String("channel", channel), attribute.String("status", status)))
}
func (m *BusinessMetrics) Duration(ctx context.Context, seconds float64, operation string) {
	if m == nil {
		return
	}
	m.RequestDuration.Record(ctx, seconds, metric.WithAttributes(attribute.String("operation", operation)))
}
func (m *BusinessMetrics) Tokens(ctx context.Context, count int64, tokenType string) {
	if m == nil || count < 0 {
		return
	}
	m.ModelTokens.Add(ctx, count, metric.WithAttributes(attribute.String("token_type", tokenType)))
}
func (m *BusinessMetrics) Tool(ctx context.Context, status string) {
	if m == nil {
		return
	}
	m.ToolCalls.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
}
func (m *BusinessMetrics) OutboundMessage(ctx context.Context, channel, status string) {
	if m == nil {
		return
	}
	m.Outbound.Add(ctx, 1, metric.WithAttributes(attribute.String("channel", channel), attribute.String("status", status)))
}
func (m *BusinessMetrics) StorageError(ctx context.Context, backend, errorType string) {
	if m == nil {
		return
	}
	m.StorageErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("backend_kind", backend), attribute.String("error_type", errorType)))
}
func (m *BusinessMetrics) PersistenceEvent(ctx context.Context, backend, status string) {
	if m == nil {
		return
	}
	m.Persistence.Add(ctx, 1, metric.WithAttributes(attribute.String("backend_kind", backend), attribute.String("status", status)))
}
func (m *BusinessMetrics) AssignmentEvent(ctx context.Context, status, reason string) {
	if m == nil {
		return
	}
	m.Assignments.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status), attribute.String("reason", reason)))
}
func (m *BusinessMetrics) HeartbeatEvent(ctx context.Context, operation, status string) {
	if m == nil {
		return
	}
	m.Heartbeats.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", operation), attribute.String("status", status)))
}
func (m *BusinessMetrics) DegradedEvent(ctx context.Context, status, reason string) {
	if m == nil {
		return
	}
	m.Degraded.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status), attribute.String("reason", reason)))
}
func (m *BusinessMetrics) TelemetryDrop(ctx context.Context, operation, reason string) {
	if m == nil {
		return
	}
	m.TelemetryDrops.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", operation), attribute.String("reason", reason)))
}

func RecordRequest(ctx context.Context, tenant, channel, status string, seconds float64) {
	if m := activeMetrics.Load(); m != nil {
		m.Request(ctx, tenant, channel, status)
		m.Duration(ctx, seconds, "request")
	}
	if tenant == "" {
		tenant = "unknown"
	}
	emitMetricEvent(ctx, control.MetricEvent{
		TenantID: tenant, Name: "request", Value: seconds, Count: 1,
		Labels: map[string]string{"channel": channel, "status": status, "operation": "request"},
	})
}
func RecordOutbound(ctx context.Context, channel, status string) {
	if m := activeMetrics.Load(); m != nil {
		m.OutboundMessage(ctx, channel, status)
	}
}
func RecordModel(ctx context.Context, seconds float64) {
	if m := activeMetrics.Load(); m != nil {
		m.ModelDuration.Record(ctx, seconds)
	}
}
func RecordTokens(ctx context.Context, prompt, completion, total int) {
	if m := activeMetrics.Load(); m != nil {
		m.Tokens(ctx, int64(prompt), "prompt")
		m.Tokens(ctx, int64(completion), "completion")
		m.Tokens(ctx, int64(total), "total")
	}
}
func RecordTool(ctx context.Context, status string) {
	if m := activeMetrics.Load(); m != nil {
		m.Tool(ctx, status)
	}
}
func RecordStorageError(ctx context.Context, backend, errorType string) {
	if m := activeMetrics.Load(); m != nil {
		m.StorageError(ctx, backend, errorType)
	}
}
func RecordPersistence(ctx context.Context, backend, status string) {
	if m := activeMetrics.Load(); m != nil {
		m.PersistenceEvent(ctx, backend, status)
	}
}
func RecordAssignment(ctx context.Context, status, reason string) {
	if m := activeMetrics.Load(); m != nil {
		m.AssignmentEvent(ctx, status, reason)
	}
}
func RecordHeartbeat(ctx context.Context, operation, status string) {
	if m := activeMetrics.Load(); m != nil {
		m.HeartbeatEvent(ctx, operation, status)
	}
}
func RecordDegraded(ctx context.Context, status, reason string) {
	if m := activeMetrics.Load(); m != nil {
		m.DegradedEvent(ctx, status, reason)
	}
}
func RecordTelemetryDrop(ctx context.Context, operation, reason string) {
	if m := activeMetrics.Load(); m != nil {
		m.TelemetryDrop(ctx, operation, reason)
	}
}
func SetInflight(value int) {
	if value < 0 {
		value = 0
	}
	inflightTasks.Store(int64(value))
}
