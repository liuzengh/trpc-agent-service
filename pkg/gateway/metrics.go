package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
)

var webhookOutcomes = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "agent_gateway_webhooks_total",
	Help: "Webhook outcomes after tenant resolution.",
}, []string{"tenant_id", "channel", "result"})

func recordWebhook(tenantID, channel, result string) {
	webhookOutcomes.WithLabelValues(telemetry.MetricTenantLabel(tenantID), channel, result).Inc()
}
