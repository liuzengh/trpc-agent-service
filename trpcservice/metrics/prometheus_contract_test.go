package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestPrometheusCapacityMetricNamesMatchOperationsContract(t *testing.T) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(registry), otelprometheus.WithoutScopeInfo())
	if err != nil {
		t.Fatal(err)
	}
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	t.Cleanup(func() { _ = meterProvider.Shutdown(context.Background()) })
	observer, err := NewOTelObserver(sdktrace.NewTracerProvider().Tracer("capacity-contract"), meterProvider.Meter("capacity-contract"))
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.ObserveDatabasePool(func() DatabasePoolStats {
		return DatabasePoolStats{MaxOpenConnections: 100, InUse: 70, WaitCount: 2, WaitDuration: time.Second}
	}); err != nil {
		t.Fatal(err)
	}
	if err := observer.ObserveRedisPool(func() RedisPoolStats {
		return RedisPoolStats{Timeouts: 1, WaitCount: 3, WaitDuration: 2 * time.Second}
	}); err != nil {
		t.Fatal(err)
	}
	if err := observer.ObserveWorkerCapacity(func() int64 { return 4 }); err != nil {
		t.Fatal(err)
	}
	ctx, finishExecution := observer.StartExecution(context.Background(), ExecutionAttributes{
		TenantID: "support", AppCode: "assistant", Channel: "web", ConfigVersion: 1,
	})
	defer finishExecution(nil)
	_, finishInbound := observer.StartInbound(ctx, InboundAttributes{TenantID: "support", AppCode: "assistant", Channel: "web"})
	finishInbound(errors.New("publish failed"))
	observer.RecordKafkaCapacity(ctx, KafkaCapacityAttributes{Topic: "agent.inbound.v1", ConsumerGroup: "workers", Lag: 7, Partitions: 8})
	observer.RecordKafkaCapacityFailure(ctx, "agent.inbound.v1", "workers")
	observer.RecordOutboxBacklog(ctx, OutboxBacklogAttributes{TenantID: "support", Channel: "web", Pending: 2, OldestAge: 90 * time.Second})
	observer.RecordModelUsage(ctx, ModelUsageAttributes{
		TenantID: "support", AppCode: "assistant", ProviderID: "mock", ModelName: "mock-model",
		PromptTokens: 100, CachedPromptTokens: 50, CompletionTokens: 20, TotalTokens: 120,
	})
	observer.RecordDeadLetter(ctx, DeadLetterAttributes{TenantID: "support", ErrorClass: "permanent"})
	observer.RecordToolExecution(ctx, ToolExecutionAttributes{TenantID: "support", ToolName: "query_order", Outcome: "failed", Latency: 25 * time.Millisecond})
	observer.RecordGovernanceRejection(ctx, GovernanceRejectionAttributes{TenantID: "support", AppCode: "assistant", Reason: "token_budget"})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	for _, name := range []string{
		"agent_execution_active",
		"worker_execution_slots",
		"messaging_kafka_consumer_lag",
		"messaging_kafka_topic_partitions",
		"messaging_kafka_capacity_sample_failures_total",
		"database_pool_connections_in_use",
		"database_pool_connections_max",
		"database_pool_waits_total",
		"redis_pool_timeouts_total",
		"outbox_pending_events",
		"outbox_oldest_age_seconds",
		"channel_inbound_failures_total",
		"model_tokens_total",
		"messaging_dlq_messages_total",
		"tool_execution_total",
		"tool_execution_failures_total",
		"tool_execution_duration_ms",
		"governance_rejections_total",
	} {
		if _, ok := names[name]; !ok {
			t.Fatalf("Prometheus metric %q missing; exported=%v", name, metricNames(names))
		}
	}
}

func metricNames(names map[string]struct{}) []string {
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	return result
}
