// Package metrics owns observability for the platform: tracing init, the
// Prometheus meter, and the instruments shared by the gateway, worker and
// sender.
//
// Tracing is delegated to the framework's telemetry package: atrace.Start
// installs a global OTLP tracer provider and activates the framework's own
// spans (Runner / Tool / Model / Session), so platform spans and framework
// spans land in the same trace. The endpoint is configured through the
// standard OTEL_EXPORTER_OTLP_ENDPOINT env var.
//
// Platform-side spans cover the parts the framework cannot see: the IM
// callback, the Stream enqueue/consume boundary, and outbound delivery.
// The trace crosses the async Stream boundary via a W3C traceparent field
// carried in the message payload.
//
// Metrics export via an in-process Prometheus exporter; the /metrics handler
// is served on the internal metrics listener every role starts
// (TRPC_METRICS_ADDR), never on the gateway's public callback mux — the
// series carry per-tenant traffic volumes and token spend.
package metrics

import (
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Platform instruments. Labels reuse the log field names (channel, ...);
// tenant_id is stamped by the Gateway after tenant routing (empty when
// routing is disabled).
//
// Instruments are created at package load through the global meter, which
// defers to the provider installed by InitMetrics — before that (e.g. in
// tests) they record to a no-op provider and never panic.
var (
	// InboundTotal counts messages accepted by the gateway.
	InboundTotal otelmetric.Int64Counter
	// DedupDroppedTotal counts messages dropped as duplicates.
	DedupDroppedTotal otelmetric.Int64Counter
	// ProcessDuration is the worker-side processing time per message.
	ProcessDuration otelmetric.Float64Histogram
	// ProcessErrorTotal counts failed message processings.
	ProcessErrorTotal otelmetric.Int64Counter
	// OutboundTotal counts IM send attempts by result (ok / error / skipped_duplicate).
	OutboundTotal otelmetric.Int64Counter
	// TokensTotal counts LLM token usage by kind (prompt / completion).
	TokensTotal otelmetric.Int64Counter
	// EndToEndDuration is the full message latency: IM callback arrival to the
	// reply landing on the IM (15s P95 budget).
	EndToEndDuration otelmetric.Float64Histogram
	// GatewayRejectedTotal counts callbacks rejected before enqueue, by reason
	// (rate_limited / backpressure): the noisy-neighbor and overload defenses
	// firing.
	GatewayRejectedTotal otelmetric.Int64Counter
	// SendRateLimitedTotal counts outbound messages re-queued because the
	// per-{channel, tenant} send bucket was exhausted within the wait window.
	SendRateLimitedTotal otelmetric.Int64Counter
	// AuditDroppedTotal counts audit events lost (queue overload, rows the
	// schema rejected): a non-zero rate means the compliance trail has holes.
	AuditDroppedTotal otelmetric.Int64Counter
	// LLMCallDuration is one model call's latency by result (ok / error),
	// measured between the framework's before/after model callbacks.
	LLMCallDuration otelmetric.Float64Histogram
	// ToolCallDuration is one tool execution's latency by result (ok / error),
	// measured between the framework's before/after tool callbacks.
	ToolCallDuration otelmetric.Float64Histogram
	// SessionStoreDuration is the session backend's latency per operation by
	// result (ok / error).
	SessionStoreDuration otelmetric.Float64Histogram
	// CostUSDTotal counts per-tenant LLM spend in USD by model (the priced
	// counterpart of TokensTotal).
	CostUSDTotal otelmetric.Float64Counter
)

func init() {
	meter := otel.Meter("trpc-agent-service")
	var err error
	if InboundTotal, err = meter.Int64Counter("im_inbound_total"); err != nil {
		panic(err)
	}
	if DedupDroppedTotal, err = meter.Int64Counter("im_dedup_dropped_total"); err != nil {
		panic(err)
	}
	if ProcessDuration, err = meter.Float64Histogram("worker_process_duration",
		otelmetric.WithUnit("ms")); err != nil {
		panic(err)
	}
	if ProcessErrorTotal, err = meter.Int64Counter("worker_process_error_total"); err != nil {
		panic(err)
	}
	if OutboundTotal, err = meter.Int64Counter("im_outbound_total"); err != nil {
		panic(err)
	}
	if TokensTotal, err = meter.Int64Counter("llm_tokens_total"); err != nil {
		panic(err)
	}
	if EndToEndDuration, err = meter.Float64Histogram("im_end_to_end_duration",
		otelmetric.WithUnit("ms")); err != nil {
		panic(err)
	}
	if GatewayRejectedTotal, err = meter.Int64Counter("gateway_rejected_total"); err != nil {
		panic(err)
	}
	if SendRateLimitedTotal, err = meter.Int64Counter("send_rate_limited_total"); err != nil {
		panic(err)
	}
	if AuditDroppedTotal, err = meter.Int64Counter("audit_dropped_total"); err != nil {
		panic(err)
	}
	if LLMCallDuration, err = meter.Float64Histogram("llm_call_duration",
		otelmetric.WithUnit("ms")); err != nil {
		panic(err)
	}
	if ToolCallDuration, err = meter.Float64Histogram("tool_call_duration",
		otelmetric.WithUnit("ms")); err != nil {
		panic(err)
	}
	if SessionStoreDuration, err = meter.Float64Histogram("session_store_duration",
		otelmetric.WithUnit("ms")); err != nil {
		panic(err)
	}
	if CostUSDTotal, err = meter.Float64Counter("llm_cost_usd_total"); err != nil {
		panic(err)
	}
}

// InitMetrics installs a Prometheus-backed meter provider and returns the
// /metrics HTTP handler.
func InitMetrics() (http.Handler, error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter)))
	return promhttp.Handler(), nil
}
