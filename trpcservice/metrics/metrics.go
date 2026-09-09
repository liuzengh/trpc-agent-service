// Package metrics exposes tenant-aware OpenTelemetry metrics and owns the
// process-wide telemetry setup (proposal doc 3.5): traces carry the
// end-to-end spans from IM callback to reply, metrics carry the
// tenant-labelled counters and histograms.
package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/metric"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// ServiceName labels every span, metric data point, and resource of this
// process. It is also the instrumentation scope name handed to otel.Tracer:
// the tRPC-Agent-Go framework is itself uninstrumented, so the full link
// (proposal doc 3.5) is drawn by this platform alone.
const ServiceName = "trpc-agent-service"

// Setup installs the global tracer and meter providers described by cfg and
// returns the Recorder plus a shutdown func that flushes both pipelines.
// Exporters set to off leave the global noop providers in place, so the rest
// of the platform can instrument unconditionally at zero cost.
func Setup(cfg config.TelemetryConfig) (*Recorder, func(context.Context) error, error) {
	res := resource.NewSchemaless(
		attribute.String("service.name", ServiceName),
	)

	var (
		tp       *tracesdk.TracerProvider
		mp       *metricsdk.MeterProvider
		shutdown []func(context.Context) error
	)

	switch cfg.Traces.Exporter {
	case config.ExporterOff, "":
	case config.ExporterStdout:
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, nil, fmt.Errorf("stdout trace exporter: %w", err)
		}
		tp = tracesdk.NewTracerProvider(tracesdk.WithBatcher(exp), tracesdk.WithResource(res))
	case config.ExporterOTLP:
		exp, err := otlptracehttp.New(context.Background(), otlptracehttp.WithEndpointURL(cfg.Traces.Endpoint))
		if err != nil {
			return nil, nil, fmt.Errorf("otlp trace exporter: %w", err)
		}
		tp = tracesdk.NewTracerProvider(tracesdk.WithBatcher(exp), tracesdk.WithResource(res))
	default:
		return nil, nil, fmt.Errorf("unknown trace exporter %q", cfg.Traces.Exporter)
	}
	if tp != nil {
		otel.SetTracerProvider(tp)
		shutdown = append(shutdown, tp.Shutdown)
	}

	interval := cfg.Metrics.Interval
	if interval <= 0 {
		interval = config.DefaultMetricInterval
	}
	switch cfg.Metrics.Exporter {
	case config.ExporterOff, "":
	case config.ExporterStdout:
		exp, err := stdoutmetric.New()
		if err != nil {
			return nil, nil, fmt.Errorf("stdout metric exporter: %w", err)
		}
		mp = metricsdk.NewMeterProvider(
			metricsdk.WithReader(metricsdk.NewPeriodicReader(exp, metricsdk.WithInterval(interval))),
			metricsdk.WithResource(res),
		)
	case config.ExporterOTLP:
		exp, err := otlpmetrichttp.New(context.Background(), otlpmetrichttp.WithEndpointURL(cfg.Metrics.Endpoint))
		if err != nil {
			return nil, nil, fmt.Errorf("otlp metric exporter: %w", err)
		}
		mp = metricsdk.NewMeterProvider(
			metricsdk.WithReader(metricsdk.NewPeriodicReader(exp, metricsdk.WithInterval(interval))),
			metricsdk.WithResource(res),
		)
	default:
		return nil, nil, fmt.Errorf("unknown metric exporter %q", cfg.Metrics.Exporter)
	}
	if mp != nil {
		otel.SetMeterProvider(mp)
		shutdown = append(shutdown, mp.Shutdown)
	}

	rec, err := NewRecorder(otel.Meter(ServiceName))
	if err != nil {
		return nil, nil, err
	}
	return rec, func(ctx context.Context) error {
		var first error
		for _, fn := range shutdown {
			if err := fn(ctx); err != nil && first == nil {
				first = err
			}
		}
		return first
	}, nil
}

// Recorder wraps the platform's metric instruments. A nil *Recorder is a
// valid no-op so governance wiring never needs nil checks.
type Recorder struct {
	messages   metric.Int64Counter
	blocks     metric.Int64Counter
	latencyMS  metric.Float64Histogram
	tokens     metric.Int64Counter
	sendErrors metric.Int64Counter
}

// NewRecorder builds the instruments on meter. Names follow the
// trpcservice.* namespace; every instrument carries tenant_id.
func NewRecorder(meter metric.Meter) (*Recorder, error) {
	r := &Recorder{}
	var err error
	if r.messages, err = meter.Int64Counter("trpcservice.messages",
		metric.WithDescription("Inbound messages by tenant, channel, and result")); err != nil {
		return nil, err
	}
	if r.blocks, err = meter.Int64Counter("trpcservice.guardrail.blocks",
		metric.WithDescription("Guardrail rejections by tenant, stage, and rule")); err != nil {
		return nil, err
	}
	if r.latencyMS, err = meter.Float64Histogram("trpcservice.model.latency_ms",
		metric.WithDescription("Model call latency in milliseconds by tenant")); err != nil {
		return nil, err
	}
	if r.tokens, err = meter.Int64Counter("trpcservice.model.tokens",
		metric.WithDescription("Model token usage by tenant and direction")); err != nil {
		return nil, err
	}
	if r.sendErrors, err = meter.Int64Counter("trpcservice.im.send_errors",
		metric.WithDescription("Failed IM deliveries by tenant and channel")); err != nil {
		return nil, err
	}
	return r, nil
}

// Message counts one inbound message and its outcome.
func (r *Recorder) Message(tenant, channel, result string) {
	if r == nil {
		return
	}
	r.messages.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.String("channel", channel),
		attribute.String("result", result),
	))
}

// GuardrailBlock counts one policy rejection.
func (r *Recorder) GuardrailBlock(tenant, stage, rule string) {
	if r == nil {
		return
	}
	r.blocks.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.String("stage", stage),
		attribute.String("rule", rule),
	))
}

// ModelLatency observes one model call duration.
func (r *Recorder) ModelLatency(tenant string, d time.Duration) {
	if r == nil {
		return
	}
	r.latencyMS.Record(context.Background(), float64(d.Milliseconds()),
		metric.WithAttributes(attribute.String("tenant", tenant)))
}

// Tokens adds model token usage; kind is "prompt" or "completion".
func (r *Recorder) Tokens(tenant, kind string, n int) {
	if r == nil || n <= 0 {
		return
	}
	r.tokens.Add(context.Background(), int64(n), metric.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.String("type", kind),
	))
}

// SendError counts one failed IM delivery.
func (r *Recorder) SendError(tenant, channel string) {
	if r == nil {
		return
	}
	r.sendErrors.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("tenant", tenant),
		attribute.String("channel", channel),
	))
}
