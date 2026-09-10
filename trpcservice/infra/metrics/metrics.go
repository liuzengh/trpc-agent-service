// Package metrics exposes tenant-aware OpenTelemetry metrics for the platform.
// It reuses the framework's meter provider (trpc-agent-go telemetry/metric),
// so the framework's own InvokeAgent/ExecuteTool/token metrics and the
// platform metrics below share one OTLP pipeline.
package metrics

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	fmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
)

const meterName = "trpc-agent-service"

// instruments is an immutable snapshot of the platform's OTel instruments.
// Init builds it and swaps it into place atomically, so a late Init (e.g. the
// real OTLP provider wired in main) and concurrent recording never race.
type instruments struct {
	inboundMessages   otelmetric.Int64Counter
	outboundMessages  otelmetric.Int64Counter
	agentRunDuration  otelmetric.Float64Histogram
	agentRunErrors    otelmetric.Int64Counter
	modelCallDuration otelmetric.Float64Histogram
	toolCallDuration  otelmetric.Float64Histogram
	tokenUsage        otelmetric.Int64Counter
	imDelivery        otelmetric.Int64Counter
	tenantCost        otelmetric.Float64Counter
	sessionLatency    otelmetric.Float64Histogram
	deadLetters       otelmetric.Int64Counter
}

// current is the active instrument set. It starts bound to the noop provider
// (via init -> Init) so recording is always safe before a real provider is
// wired.
var current atomic.Pointer[instruments]

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

// Init registers the platform's metrics on the framework's meter provider and
// swaps them into place. Safe to call repeatedly; with no provider wired it
// binds to a noop provider, so recording never panics.
func Init() {
	meter := fmetric.GetMeterProvider().Meter(meterName)

	ins := &instruments{}

	ins.inboundMessages, _ = meter.Int64Counter(
		"platform.inbound_messages",
		otelmetric.WithDescription("Number of inbound messages"),
	)
	ins.outboundMessages, _ = meter.Int64Counter(
		"platform.outbound_messages",
		otelmetric.WithDescription("Number of outbound replies"),
	)
	ins.agentRunDuration, _ = meter.Float64Histogram(
		"platform.agent_run_duration",
		otelmetric.WithDescription("Agent run latency"),
		otelmetric.WithUnit("s"),
	)
	ins.agentRunErrors, _ = meter.Int64Counter(
		"platform.agent_run_errors",
		otelmetric.WithDescription("Number of failed agent runs"),
	)
	ins.modelCallDuration, _ = meter.Float64Histogram(
		"platform.model_call_duration",
		otelmetric.WithDescription("Model call latency, summed per turn"),
		otelmetric.WithUnit("s"),
	)
	ins.toolCallDuration, _ = meter.Float64Histogram(
		"platform.tool_call_duration",
		otelmetric.WithDescription("Tool call latency"),
		otelmetric.WithUnit("s"),
	)
	ins.tokenUsage, _ = meter.Int64Counter(
		"platform.token_usage",
		otelmetric.WithDescription("Total tokens consumed"),
		otelmetric.WithUnit("{token}"),
	)
	ins.imDelivery, _ = meter.Int64Counter(
		"platform.im_delivery_total",
		otelmetric.WithDescription("IM delivery attempts, partitioned by ok"),
	)
	ins.tenantCost, _ = meter.Float64Counter(
		"platform.tenant_cost",
		otelmetric.WithDescription("Per-tenant cost"),
	)
	ins.sessionLatency, _ = meter.Float64Histogram(
		"platform.session_latency",
		otelmetric.WithDescription("Session backend latency"),
		otelmetric.WithUnit("s"),
	)
	ins.deadLetters, _ = meter.Int64Counter(
		"platform.dlq_total",
		otelmetric.WithDescription("Messages moved to the dead-letter queue"),
	)

	current.Store(ins)
}

// InboundMessage counts one inbound message for a tenant/channel.
func InboundMessage(ctx context.Context, tenantID, channel string) {
	current.Load().inboundMessages.Add(ctx, 1, otelmetric.WithAttributes(attrTenant.String(tenantID), attrChannel.String(channel)))
}

// OutboundMessage counts one outbound reply.
func OutboundMessage(ctx context.Context, tenantID, channel string) {
	current.Load().outboundMessages.Add(ctx, 1, otelmetric.WithAttributes(attrTenant.String(tenantID), attrChannel.String(channel)))
}

// AgentRun records the latency of one agent run.
func AgentRun(ctx context.Context, tenantID, agentID string, dur time.Duration) {
	current.Load().agentRunDuration.Record(ctx, dur.Seconds(), otelmetric.WithAttributes(attrTenant.String(tenantID), attrAgent.String(agentID)))
}

// AgentError counts one failed agent run.
func AgentError(ctx context.Context, tenantID, agentID string) {
	current.Load().agentRunErrors.Add(ctx, 1, otelmetric.WithAttributes(attrTenant.String(tenantID), attrAgent.String(agentID)))
}

// ModelCallDuration records the summed model-call latency of one agent turn.
func ModelCallDuration(ctx context.Context, tenantID, agentID string, dur time.Duration) {
	current.Load().modelCallDuration.Record(ctx, dur.Seconds(), otelmetric.WithAttributes(attrTenant.String(tenantID), attrAgent.String(agentID)))
}

// ToolCallDuration records the latency of tool invocation during a run.
func ToolCallDuration(ctx context.Context, tenantID, agentID string, dur time.Duration) {
	current.Load().toolCallDuration.Record(ctx, dur.Seconds(), otelmetric.WithAttributes(attrTenant.String(tenantID), attrAgent.String(agentID)))
}

// TokenUsage accumulates consumed tokens for a tenant.
func TokenUsage(ctx context.Context, tenantID string, tokens int64) {
	current.Load().tokenUsage.Add(ctx, tokens, otelmetric.WithAttributes(attrTenant.String(tenantID)))
}

// IMDelivery records one IM delivery attempt, partitioned by ok.
func IMDelivery(ctx context.Context, channel string, ok bool) {
	current.Load().imDelivery.Add(ctx, 1, otelmetric.WithAttributes(attrChannel.String(channel), attrOK.Bool(ok)))
}

// TenantCost accumulates per-tenant cost.
func TenantCost(ctx context.Context, tenantID string, cost float64) {
	current.Load().tenantCost.Add(ctx, cost, otelmetric.WithAttributes(attrTenant.String(tenantID)))
}

// SessionLatency records session-backend access latency.
func SessionLatency(ctx context.Context, tenantID string, dur time.Duration) {
	current.Load().sessionLatency.Record(ctx, dur.Seconds(), otelmetric.WithAttributes(attrTenant.String(tenantID)))
}

// DeadLetter counts one message moved to the dead-letter queue.
func DeadLetter(ctx context.Context, tenantID, channel string) {
	current.Load().deadLetters.Add(ctx, 1, otelmetric.WithAttributes(attrTenant.String(tenantID), attrChannel.String(channel)))
}

// TraceContextFromID returns parent carrying the given trace id as a remote
// span context, so spans started on it join the trace stamped by the IM
// gateway. An empty or malformed id returns parent unchanged.
func TraceContextFromID(parent context.Context, traceID string) context.Context {
	if traceID == "" {
		return parent
	}
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return parent
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(parent, sc)
}
