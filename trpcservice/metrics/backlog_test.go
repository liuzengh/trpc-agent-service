package metrics

import (
	"context"
	"errors"
	"testing"

	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestBacklogSnapshotDoesNotReportHealthyZerosOnFailure(t *testing.T) {
	reader := sdk.NewManualReader()
	provider := sdk.NewMeterProvider(sdk.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	fail := false
	unregister, err := registerBacklog(provider.Meter("test"), func(context.Context) ([]BacklogSample, error) {
		if fail {
			return nil, errors.New("database unavailable")
		}
		return []BacklogSample{{TenantID: "tenant", Stage: "outbound", State: "pending", Items: 3, OldestAgeSeconds: 42}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unregister()
	for _, bad := range []bool{false, true} {
		fail = bad
		var result metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &result); err != nil {
			t.Fatal(err)
		}
		up, items := int64(-1), int64(-1)
		for _, scope := range result.ScopeMetrics {
			for _, m := range scope.Metrics {
				if g, ok := m.Data.(metricdata.Gauge[int64]); ok {
					for _, p := range g.DataPoints {
						switch m.Name {
						case "agent.backlog.snapshot_up":
							up = p.Value
						case "agent.backlog.items":
							items = p.Value
						}
					}
				}
			}
		}
		if (!bad && (up != 1 || items != 3)) || (bad && (up != 0 || items != -1)) {
			t.Fatalf("bad=%t up=%d items=%d", bad, up, items)
		}
	}
}
