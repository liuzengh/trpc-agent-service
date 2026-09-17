package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

func TestBrokerRegistryExportsStableAggregateAndShardMetrics(t *testing.T) {
	registry := &metrics.BrokerRegistry{}
	registry.ObserveBrokerBacklog(broker.Backlog{ObservedAt: time.Now().UTC(), Shards: []broker.ShardBacklog{
		{Shard: 1, Pending: 2, Undelivered: 3, OldestAge: 5 * time.Second}, {Shard: 0, Undelivered: 7, OldestAge: time.Second}}})
	recorder := httptest.NewRecorder()
	registry.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, expected := range []string{`trpc_broker_backlog{shard="0",state="undelivered"} 7`, `trpc_broker_delivery_lag_seconds{shard="1"} 5`, "trpc_broker_backlog_total 12", "trpc_broker_delivery_lag_max_seconds 5"} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %q in:\n%s", expected, body)
		}
	}
	for _, forbidden := range []string{"tenant_id", "session_id", "request_id", "user_id"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("forbidden label %q", forbidden)
		}
	}
}

func TestBrokerRegistryOmitsAutoscalingGaugesWhenSnapshotIsStale(t *testing.T) {
	registry := &metrics.BrokerRegistry{SnapshotTTL: time.Second}
	registry.ObserveBrokerBacklog(broker.Backlog{ObservedAt: time.Now().Add(-2 * time.Second), Shards: []broker.ShardBacklog{{Shard: 0, Undelivered: 1}}})
	recorder := httptest.NewRecorder()
	registry.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, "trpc_broker_autoscaling_snapshot_ready 0") || strings.Contains(body, "trpc_broker_backlog_total") {
		t.Fatalf("body=%s", body)
	}
}

func TestBrokerRegistryUnknownSnapshotIsNotZeroBacklog(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&metrics.BrokerRegistry{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, "trpc_broker_backlog_snapshot_ready 0") || strings.Contains(body, "trpc_broker_backlog_total") {
		t.Fatalf("body=%s", body)
	}
}
