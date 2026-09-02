// Package metrics exposes tenant-aware OpenTelemetry metrics for the platform.
// It reuses the framework's meter provider (trpc-agent-go telemetry/metric),
// so the framework's own InvokeAgent/ExecuteTool/token metrics and the
// platform metrics below share one OTLP pipeline.
package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	fmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
)

const meterName = "trpc-agent-service"

var (
	meter            otelmetric.Meter
	inboundMessages  otelmetric.Int64Counter
	outboundMessages otelmetric.Int64Counter
	agentRunDuration otelmetric.Float64Histogram
	agentRunErrors   otelmetric.Int64Counter
	toolCallDuration otelmetric.Float64Histogram
	tokenUsage       otelmetric.Int64Counter
	imDelivery       otelmetric.Int64Counter
	tenantCost       otelmetric.Float64Counter
	sessionLatency   otelmetric.Float64Histogram
)

var (
	attrTenant  = attribute.Key("tenant_id")
	attrAgent   = attribute.Key("agent_id")
	attrChannel = attribute.Key("channel")
	attrOK      = attribute.Key("ok")
)

// init binds the instruments to the framework's default (noop) provider so
// recording is always safe, even before main wires a real OTLP provider.
// Init() re-binds them to the configured provider when telemetry is enabled.
func init() { Init() }

// Init registers the platform's metrics on the framework's meter provider.
// Safe to call repeatedly; with no provider wired it binds to a noop provider,
// so recording never panics.
func Init() {
	meter = fmetric.GetMeterProvider().Meter(meterName)

	inboundMessages, _ = meter.Int64Counter(
		"platform.inbound_messages",
		otelmetric.WithDescription("Number of inbound messages"),
	)
	outboundMessages, _ = meter.Int64Counter(
		"platform.outbound_messages",
		otelmetric.WithDescription("Number of outbound replies"),
	)
	agentRunDuration, _ = meter.Float64Histogram(
		"platform.agent_run_duration",
		otelmetric.WithDescription("Agent run latency"),
		otelmetric.WithUnit("s"),
	)
	agentRunErrors, _ = meter.Int64Counter(
		"platform.agent_run_errors",
		otelmetric.WithDescription("Number of failed agent runs"),
	)
	toolCallDuration, _ = meter.Float64Histogram(
		"platform.tool_call_duration",
		otelmetric.WithDescription("Tool call latency"),
		otelmetric.WithUnit("s"),
	)
	tokenUsage, _ = meter.Int64Counter(
		"platform.token_usage",
		otelmetric.WithDescription("Total tokens consumed"),
		otelmetric.WithUnit("{token}"),
	)
	imDelivery, _ = meter.Int64Counter(
		"platform.im_delivery_total",
		otelmetric.WithDescription("IM delivery attempts, partitioned by ok"),
	)
	tenantCost, _ = meter.Float64Counter(
		"platform.tenant_cost",
		otelmetric.WithDescription("Per-tenant cost"),
	)
	sessionLatency, _ = meter.Float64Histogram(
		"platform.session_latency",
		otelmetric.WithDescription("Session backend latency"),
		otelmetric.WithUnit("s"),
	)
}

// InboundMessage counts one inbound message for a tenant/channel.
func InboundMessage(ctx context.Context, tenantID, channel string) {
	inboundMessages.Add(ctx, 1, otelmetric.WithAttributes(attrTenant.String(tenantID), attrChannel.String(channel)))
}

// OutboundMessage counts one outbound reply.
func OutboundMessage(ctx context.Context, tenantID, channel string) {
	outboundMessages.Add(ctx, 1, otelmetric.WithAttributes(attrTenant.String(tenantID), attrChannel.String(channel)))
}

// AgentRun records the latency of one agent run.
func AgentRun(ctx context.Context, tenantID, agentID string, dur time.Duration) {
	agentRunDuration.Record(ctx, dur.Seconds(), otelmetric.WithAttributes(attrTenant.String(tenantID), attrAgent.String(agentID)))
}

// AgentError counts one failed agent run.
func AgentError(ctx context.Context, tenantID, agentID string) {
	agentRunErrors.Add(ctx, 1, otelmetric.WithAttributes(attrTenant.String(tenantID), attrAgent.String(agentID)))
}

// ToolCallDuration records the latency of tool invocation during a run.
func ToolCallDuration(ctx context.Context, tenantID, agentID string, dur time.Duration) {
	toolCallDuration.Record(ctx, dur.Seconds(), otelmetric.WithAttributes(attrTenant.String(tenantID), attrAgent.String(agentID)))
}

// TokenUsage accumulates consumed tokens for a tenant.
func TokenUsage(ctx context.Context, tenantID string, tokens int64) {
	tokenUsage.Add(ctx, tokens, otelmetric.WithAttributes(attrTenant.String(tenantID)))
}

// IMDelivery records one IM delivery attempt, partitioned by ok.
func IMDelivery(ctx context.Context, channel string, ok bool) {
	imDelivery.Add(ctx, 1, otelmetric.WithAttributes(attrChannel.String(channel), attrOK.Bool(ok)))
}

// TenantCost accumulates per-tenant cost.
func TenantCost(ctx context.Context, tenantID string, cost float64) {
	tenantCost.Add(ctx, cost, otelmetric.WithAttributes(attrTenant.String(tenantID)))
}

// SessionLatency records session-backend access latency.
func SessionLatency(ctx context.Context, tenantID string, dur time.Duration) {
	sessionLatency.Record(ctx, dur.Seconds(), otelmetric.WithAttributes(attrTenant.String(tenantID)))
}
