package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type failingMetricSession struct{ session.Service }

func (s failingMetricSession) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	return nil, errors.New("secret-backend-error")
}

func TestSessionBackendLatencyAndFailureMetrics(t *testing.T) {
	reader := sdk.NewManualReader()
	provider := sdk.NewMeterProvider(sdk.WithReader(reader))
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer func() { _ = provider.Shutdown(context.Background()); otel.SetMeterProvider(old) }()
	base := inmemory.NewSessionService()
	svc := observeSession(base, "inmemory")
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(svc)
	key := session.Key{AppName: "t/tenant/a/app", UserID: "secret-user", SessionID: "secret-session"}
	if _, err := observeSession(failingMetricSession{base}, "inmemory").GetSession(context.Background(), key); err == nil {
		t.Fatal("expected missing-session error")
	}
	if _, err := svc.CreateSession(context.Background(), key, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetSession(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	var result metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &result); err != nil {
		t.Fatal(err)
	}
	var count uint64
	failed := false
	for _, scope := range result.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "agent.storage.call.duration" {
				continue
			}
			hist := m.Data.(metricdata.Histogram[float64])
			for _, point := range hist.DataPoints {
				count += point.Count
				for _, kv := range point.Attributes.ToSlice() {
					if strings.Contains(kv.Value.AsString(), "secret-") {
						t.Fatal("private identity in metric")
					}
					if string(kv.Key) == "operation.result" && kv.Value.AsString() == "error" {
						failed = true
					}
				}
			}
		}
	}
	if count != 3 || !failed {
		t.Fatalf("backend metrics incomplete: count=%d failed=%t", count, failed)
	}
}
