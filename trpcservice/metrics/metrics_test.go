package metrics_test

import (
	"context"
	"testing"
	"time"

	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPricingCatalogKeepsUnknownCostUnknown(t *testing.T) {
	catalog, err := platformmetrics.ParsePricingJSON(`{"openai/gpt-4.1":{"input_per_million":2,"output_per_million":8}}`)
	if err != nil {
		t.Fatalf("parse pricing: %v", err)
	}
	recorder, err := platformmetrics.New(noop.NewMeterProvider(), catalog)
	if err != nil {
		t.Fatalf("new metrics recorder: %v", err)
	}
	if cost := recorder.EstimateCost("openai", "unknown", 10, 20); cost != nil {
		t.Fatalf("unknown model cost = %v, want unknown", *cost)
	}
	cost := recorder.EstimateCost("openai", "gpt-4.1", 1_000_000, 500_000)
	if cost == nil || *cost != 6 {
		t.Fatalf("priced model cost = %v, want 6", cost)
	}
}

func TestPricingCatalogRejectsNegativePrices(t *testing.T) {
	if _, err := platformmetrics.NewPricingCatalog(map[string]platformmetrics.ModelPrice{
		"openai/gpt-4.1": {InputPerMillion: -1},
	}); err == nil {
		t.Fatal("negative model price was accepted")
	}
}

func TestOperationsSnapshotExportsRequiredGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	recorder, err := platformmetrics.New(provider, platformmetrics.PricingCatalog{})
	if err != nil {
		t.Fatalf("new metrics recorder: %v", err)
	}
	recorder.SetOperationsSnapshot(platformmetrics.OperationsSnapshot{
		ActiveExecutions:  3,
		WorkerCount:       2,
		WorkerCapacity:    8,
		QueueBacklog:      5,
		RetryBacklog:      1,
		ReplyBacklog:      2,
		PendingApprovals:  4,
		ActiveMigrations:  1,
		StuckMigrations:   0,
		MigrationProgress: 0.75,
		AuditBacklog:      0,
	})
	recorder.SetBackendReadiness("postgres", true)
	recorder.SetBackendReadiness("redis", false)

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	intValues := map[string]int64{}
	floatValues := map[string]float64{}
	backendValues := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, value := range scope.Metrics {
			switch points := value.Data.(type) {
			case metricdata.Gauge[int64]:
				if value.Name == "trpc_agent_service.backend.ready" {
					for _, point := range points.DataPoints {
						provider, ok := point.Attributes.Value("provider")
						if ok {
							backendValues[provider.AsString()] = point.Value
						}
					}
				} else if len(points.DataPoints) > 0 {
					intValues[value.Name] = points.DataPoints[0].Value
				}
			case metricdata.Gauge[float64]:
				if len(points.DataPoints) > 0 {
					floatValues[value.Name] = points.DataPoints[0].Value
				}
			}
		}
	}
	for name, want := range map[string]int64{
		"trpc_agent_service.execution.active": 3,
		"trpc_agent_service.worker.capacity":  8,
		"trpc_agent_service.queue.backlog":    5,
		"trpc_agent_service.approval.pending": 4,
	} {
		if got := intValues[name]; got != want {
			t.Fatalf("metric %s = %d, want %d", name, got, want)
		}
	}
	if got := floatValues["trpc_agent_service.worker.utilization"]; got != 0.375 {
		t.Fatalf("worker utilization = %v, want 0.375", got)
	}
	if got := floatValues["trpc_agent_service.migration.progress"]; got != 0.75 {
		t.Fatalf("migration progress = %v, want 0.75", got)
	}
	if backendValues["postgres"] != 1 || backendValues["redis"] != 0 {
		t.Fatalf("backend readiness = %#v, want postgres=1 redis=0", backendValues)
	}
}

func TestExecutionMetricsKeepPinnedConfigVersion(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	recorder, err := platformmetrics.New(provider, platformmetrics.PricingCatalog{})
	if err != nil {
		t.Fatalf("new metrics recorder: %v", err)
	}
	recorder.RecordExecution(context.Background(), platformmetrics.Labels{
		TenantID:      "tenant-a",
		AppID:         "support",
		ConfigVersion: "v2",
	}, 1500*time.Millisecond, "timeout")
	recorder.RecordGovernanceRejected(context.Background(), platformmetrics.Labels{
		TenantID:      "tenant-a",
		AppID:         "support",
		ConfigVersion: "v2",
	}, "budget_rejected")

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	foundCount := false
	foundError := false
	foundLatency := false
	foundBudgetRejection := false
	for _, scope := range data.ScopeMetrics {
		for _, value := range scope.Metrics {
			switch points := value.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range points.DataPoints {
					version, ok := point.Attributes.Value("config_version")
					if !ok || version.AsString() != "v2" {
						continue
					}
					switch value.Name {
					case "trpc_agent_service.execution.count":
						foundCount = point.Value == 1
					case "trpc_agent_service.execution.error.count":
						foundError = point.Value == 1
					case "trpc_agent_service.governance.rejected":
						errorType, ok := point.Attributes.Value("error_type")
						foundBudgetRejection = ok && errorType.AsString() == "budget_rejected" && point.Value == 1
					}
				}
			case metricdata.Histogram[float64]:
				if value.Name != "trpc_agent_service.execution.latency" {
					continue
				}
				for _, point := range points.DataPoints {
					version, ok := point.Attributes.Value("config_version")
					if ok && version.AsString() == "v2" && point.Count == 1 && point.Sum == 1.5 {
						foundLatency = true
					}
				}
			}
		}
	}
	if !foundCount || !foundError || !foundLatency || !foundBudgetRejection {
		t.Fatalf("candidate metrics missing pinned version: count=%t error=%t latency=%t budget=%t", foundCount, foundError, foundLatency, foundBudgetRejection)
	}
}
