// Package metrics exposes tenant-aware Prometheus metrics.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Recorder is the cross-component metrics contract.
type Recorder interface {
	ObserveRequest(tenant, channel, decision, errorType string, latency time.Duration)
	ObserveModel(tenant, model string, latency time.Duration, tokens int)
	ObserveTool(tenant, tool string, latency time.Duration, failed bool)
	ObserveDelivery(tenant, channel string, failed bool)
	ObserveSessionBackend(backend string, latency time.Duration)
}

// Metrics owns a private registry so tests and multiple service instances do
// not collide through Prometheus globals.
type Metrics struct {
	registry *prometheus.Registry

	requests       *prometheus.CounterVec
	requestLatency *prometheus.HistogramVec
	modelCalls     *prometheus.CounterVec
	modelLatency   *prometheus.HistogramVec
	toolLatency    *prometheus.HistogramVec
	deliveries     *prometheus.CounterVec
	errors         *prometheus.CounterVec
	tokens         *prometheus.CounterVec
	sessionLatency *prometheus.HistogramVec
}

// New constructs and registers all platform metrics.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "agent_requests_total",
			Help: "Inbound and execution decisions by tenant and channel.",
		}, []string{"tenant", "channel", "decision"}),
		requestLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "agent_request_latency_seconds",
			Help:    "Gateway and execution latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"tenant", "channel", "decision"}),
		modelCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "model_calls_total",
			Help: "Model calls by tenant and model.",
		}, []string{"tenant", "model"}),
		modelLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "model_latency_seconds",
			Help: "Model call latency.",
		}, []string{"tenant", "model"}),
		toolLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "tool_latency_seconds",
			Help: "Tool call latency.",
		}, []string{"tenant", "tool", "status"}),
		deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "im_delivery_total",
			Help: "IM delivery outcomes.",
		}, []string{"tenant", "channel", "status"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "agent_errors_total",
			Help: "Platform errors by tenant and type.",
		}, []string{"tenant", "error_type"}),
		tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "token_usage_total",
			Help: "Model token consumption.",
		}, []string{"tenant", "model"}),
		sessionLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "session_backend_latency_seconds",
			Help: "Session backend operation latency.",
		}, []string{"backend"}),
	}
	m.registry.MustRegister(
		m.requests, m.requestLatency, m.modelCalls, m.modelLatency,
		m.toolLatency, m.deliveries, m.errors, m.tokens, m.sessionLatency,
	)
	return m
}

// Handler returns a Prometheus exposition handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ObserveRequest implements Recorder.
func (m *Metrics) ObserveRequest(
	tenant, channel, decision, errorType string,
	latency time.Duration,
) {
	m.requests.WithLabelValues(tenant, channel, decision).Inc()
	m.requestLatency.WithLabelValues(tenant, channel, decision).Observe(latency.Seconds())
	if errorType != "" {
		m.errors.WithLabelValues(tenant, errorType).Inc()
	}
}

// ObserveModel implements Recorder.
func (m *Metrics) ObserveModel(tenant, model string, latency time.Duration, tokens int) {
	m.modelCalls.WithLabelValues(tenant, model).Inc()
	m.modelLatency.WithLabelValues(tenant, model).Observe(latency.Seconds())
	if tokens > 0 {
		m.tokens.WithLabelValues(tenant, model).Add(float64(tokens))
	}
}

// ObserveTool implements Recorder.
func (m *Metrics) ObserveTool(tenant, tool string, latency time.Duration, failed bool) {
	status := "success"
	if failed {
		status = "failure"
	}
	m.toolLatency.WithLabelValues(tenant, tool, status).Observe(latency.Seconds())
}

// ObserveDelivery implements Recorder.
func (m *Metrics) ObserveDelivery(tenant, channel string, failed bool) {
	status := "success"
	if failed {
		status = "failure"
	}
	m.deliveries.WithLabelValues(tenant, channel, status).Inc()
}

// ObserveSessionBackend implements Recorder.
func (m *Metrics) ObserveSessionBackend(backend string, latency time.Duration) {
	m.sessionLatency.WithLabelValues(backend).Observe(latency.Seconds())
}
