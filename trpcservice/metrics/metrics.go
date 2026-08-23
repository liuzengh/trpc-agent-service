// Package metrics exposes bounded-cardinality Prometheus metrics.
package metrics

import "github.com/prometheus/client_golang/prometheus"

type Registry struct {
	Inbound         *prometheus.CounterVec
	Duplicate       *prometheus.CounterVec
	AgentRuns       *prometheus.CounterVec
	AgentLatency    *prometheus.HistogramVec
	Reply           *prometheus.CounterVec
	ChannelReady    *prometheus.GaugeVec
	LeaseContention *prometheus.CounterVec
}

func New(registerer prometheus.Registerer) *Registry {
	r := &Registry{
		Inbound: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "trpc_agent_service", Name: "inbound_messages_total",
			Help: "Normalized IM messages received.",
		}, []string{"tenant", "channel"}),
		Duplicate: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "trpc_agent_service", Name: "duplicate_messages_total",
			Help: "Inbound messages rejected by the idempotency key.",
		}, []string{"tenant", "channel"}),
		AgentRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "trpc_agent_service", Name: "agent_runs_total",
			Help: "Agent run outcomes.",
		}, []string{"tenant", "result"}),
		AgentLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "trpc_agent_service", Name: "agent_run_duration_seconds",
			Help: "End-to-end Agent execution latency.", Buckets: prometheus.DefBuckets,
		}, []string{"tenant"}),
		Reply: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "trpc_agent_service", Name: "reply_deliveries_total",
			Help: "IM reply delivery outcomes.",
		}, []string{"tenant", "channel", "result"}),
		ChannelReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "trpc_agent_service", Name: "channel_ready",
			Help: "Whether a channel binding currently has a ready long connection.",
		}, []string{"tenant", "channel", "binding"}),
		LeaseContention: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "trpc_agent_service", Name: "lease_contention_total",
			Help: "Failed attempts to acquire a unique channel or session lease.",
		}, []string{"scope"}),
	}
	if registerer != nil {
		registerer.MustRegister(r.Inbound, r.Duplicate, r.AgentRuns, r.AgentLatency, r.Reply, r.ChannelReady, r.LeaseContention)
	}
	return r
}
