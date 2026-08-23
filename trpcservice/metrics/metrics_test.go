package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegistryExposesBoundedMetrics(t *testing.T) {
	gatherer := prometheus.NewRegistry()
	registry := New(gatherer)
	registry.Inbound.WithLabelValues("tenant", "feishu").Inc()
	registry.AgentRuns.WithLabelValues("tenant", "success").Inc()
	registry.AgentLatency.WithLabelValues("tenant").Observe(0.1)
	registry.Reply.WithLabelValues("tenant", "feishu", "success").Inc()
	registry.ChannelReady.WithLabelValues("tenant", "feishu", "binding").Set(1)
	registry.LeaseContention.WithLabelValues("session").Inc()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 6 {
		t.Fatalf("metric families=%d", len(families))
	}
	if New(nil) == nil {
		t.Fatal("nil registerer should still return metrics")
	}
}
