// Package metrics exposes tenant-aware OpenTelemetry metrics.
package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Recorder struct {
	inbound          metric.Int64Counter
	idempotentHit    metric.Int64Counter
	runs             metric.Int64Counter
	runLatency       metric.Float64Histogram
	deliveries       metric.Int64Counter
	deliveryLatency  metric.Float64Histogram
	promptTokens     metric.Int64Counter
	completionTokens metric.Int64Counter
	cost             metric.Float64Counter
	channelPolls     metric.Int64Counter
	channelLag       metric.Float64Histogram
}

func New() (*Recorder, error) {
	meter := otel.Meter("trpc-agent-service/platform")
	inbound, err := meter.Int64Counter("agent.inbound.messages")
	if err != nil {
		return nil, fmt.Errorf("create inbound counter: %w", err)
	}
	idempotentHit, err := meter.Int64Counter("agent.idempotency.replays")
	if err != nil {
		return nil, fmt.Errorf("create idempotency counter: %w", err)
	}
	runs, err := meter.Int64Counter("agent.runs")
	if err != nil {
		return nil, fmt.Errorf("create run counter: %w", err)
	}
	runLatency, err := meter.Float64Histogram("agent.run.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create run latency histogram: %w", err)
	}
	deliveries, err := meter.Int64Counter("agent.reply.deliveries")
	if err != nil {
		return nil, fmt.Errorf("create delivery counter: %w", err)
	}
	deliveryLatency, err := meter.Float64Histogram("agent.reply.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create delivery latency histogram: %w", err)
	}
	promptTokens, err := meter.Int64Counter("agent.model.prompt_tokens", metric.WithUnit("{token}"))
	if err != nil {
		return nil, fmt.Errorf("create prompt token counter: %w", err)
	}
	completionTokens, err := meter.Int64Counter("agent.model.completion_tokens", metric.WithUnit("{token}"))
	if err != nil {
		return nil, fmt.Errorf("create completion token counter: %w", err)
	}
	cost, err := meter.Float64Counter("agent.model.cost", metric.WithUnit("USD"))
	if err != nil {
		return nil, fmt.Errorf("create model cost counter: %w", err)
	}
	channelPolls, err := meter.Int64Counter("agent.channel.polls")
	if err != nil {
		return nil, err
	}
	channelLag, err := meter.Float64Histogram("agent.channel.checkpoint_lag", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return &Recorder{
		inbound: inbound, idempotentHit: idempotentHit,
		runs: runs, runLatency: runLatency,
		deliveries: deliveries, deliveryLatency: deliveryLatency,
		promptTokens: promptTokens, completionTokens: completionTokens, cost: cost,
		channelPolls: channelPolls, channelLag: channelLag,
	}, nil
}

func (r *Recorder) RecordChannelPoll(ctx context.Context, tenantID, status string, lag time.Duration) {
	if r == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("tenant.id", tenantID), attribute.String("channel.type", "wecom_mcp"), attribute.String("poll.status", status))
	r.channelPolls.Add(ctx, 1, attrs)
	if status == "ok" {
		r.channelLag.Record(ctx, lag.Seconds(), attrs)
	}
}

func (r *Recorder) RecordUsage(
	ctx context.Context,
	tenantID string,
	promptTokens int,
	completionTokens int,
	cost float64,
) {
	if r == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("tenant.id", tenantID))
	r.promptTokens.Add(ctx, int64(promptTokens), attrs)
	r.completionTokens.Add(ctx, int64(completionTokens), attrs)
	r.cost.Add(ctx, cost, attrs)
}

func (r *Recorder) RecordInbound(ctx context.Context, tenantID string, channel string, duplicate bool) {
	if r == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("tenant.id", tenantID),
		attribute.String("channel.type", channel),
	)
	r.inbound.Add(ctx, 1, attrs)
	if duplicate {
		r.idempotentHit.Add(ctx, 1, attrs)
	}
}

func (r *Recorder) RecordRun(
	ctx context.Context,
	tenantID string,
	status string,
	duration time.Duration,
) {
	if r == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("tenant.id", tenantID),
		attribute.String("run.status", status),
	)
	r.runs.Add(ctx, 1, attrs)
	r.runLatency.Record(ctx, duration.Seconds(), attrs)
}

func (r *Recorder) RecordDelivery(
	ctx context.Context,
	tenantID string,
	channel string,
	status string,
	duration time.Duration,
) {
	if r == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("tenant.id", tenantID),
		attribute.String("channel.type", channel),
		attribute.String("delivery.status", status),
	)
	r.deliveries.Add(ctx, 1, attrs)
	r.deliveryLatency.Record(ctx, duration.Seconds(), attrs)
}
