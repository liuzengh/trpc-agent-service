// Package telemetry owns the platform's bounded, fail-open OTel integration.
package telemetry

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otlpmetrichttp "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otlptracehttp "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	frameworkmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
	frameworktrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

type Config struct {
	Enabled             bool
	Endpoint            string
	ServiceName         string
	ServiceVersion      string
	ServiceInstanceID   string
	SampleRatio         float64
	SpanQueueSize       int
	SpanBatchSize       int
	SpanBatchTimeout    time.Duration
	ExportTimeout       time.Duration
	MetricInterval      time.Duration
	MetricExportTimeout time.Duration
}

type Provider struct {
	TracerProvider     *sdktrace.TracerProvider
	MeterProvider      *sdkmetric.MeterProvider
	Tracer             trace.Tracer
	Meter              metric.Meter
	Metrics            *BusinessMetrics
	closeOnce          sync.Once
	closeErr           error
	oldTP              trace.TracerProvider
	oldMP              metric.MeterProvider
	oldProp            propagation.TextMapPropagator
	oldFrameworkTP     trace.TracerProvider
	oldFrameworkTracer trace.Tracer
	oldFrameworkMP     metric.MeterProvider
	oldMetrics         *BusinessMetrics
}

func New(ctx context.Context, cfg Config) (*Provider, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p := &Provider{oldTP: otel.GetTracerProvider(), oldMP: otel.GetMeterProvider(), oldProp: otel.GetTextMapPropagator(), oldFrameworkTP: frameworktrace.TracerProvider, oldFrameworkTracer: frameworktrace.Tracer, oldFrameworkMP: frameworkmetric.GetMeterProvider(), oldMetrics: activeMetrics.Load()}
	if !cfg.Enabled || strings.TrimSpace(cfg.Endpoint) == "" {
		p.Tracer = otel.Tracer("trpc-agent-service")
		p.Meter = otel.Meter("trpc-agent-service")
		p.Metrics, _ = NewBusinessMetrics(p.Meter)
		activeMetrics.Store(p.Metrics)
		return p, nil
	}
	endpoint, err := url.Parse(strings.TrimRight(cfg.Endpoint, "/"))
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, errors.New("telemetry endpoint must be an absolute HTTP(S) URL")
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "trpc-agent-service"
	}
	if cfg.ServiceVersion == "" {
		cfg.ServiceVersion = "phase7"
	}
	if cfg.ServiceInstanceID == "" {
		cfg.ServiceInstanceID, _ = os.Hostname()
	}
	if cfg.ServiceInstanceID == "" {
		cfg.ServiceInstanceID = "unknown"
	}
	if cfg.SampleRatio < 0 || cfg.SampleRatio > 1 {
		return nil, errors.New("telemetry sample ratio must be between 0 and 1")
	}
	if cfg.SpanQueueSize <= 0 {
		cfg.SpanQueueSize = 2048
	}
	if cfg.SpanBatchSize <= 0 {
		cfg.SpanBatchSize = 512
	}
	if cfg.SpanBatchTimeout <= 0 {
		cfg.SpanBatchTimeout = time.Second
	}
	if cfg.ExportTimeout <= 0 {
		cfg.ExportTimeout = 2 * time.Second
	}
	if cfg.MetricInterval <= 0 {
		cfg.MetricInterval = 5 * time.Second
	}
	if cfg.MetricExportTimeout <= 0 {
		cfg.MetricExportTimeout = 2 * time.Second
	}
	res, _ := resource.New(ctx, resource.WithAttributes(
		attribute.String("service.name", cfg.ServiceName),
		attribute.String("service.version", cfg.ServiceVersion),
		attribute.String("service.instance.id", cfg.ServiceInstanceID),
	))
	traceOpts := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(endpoint.String() + "/v1/traces")}
	if endpoint.Scheme == "http" {
		traceOpts = append(traceOpts, otlptracehttp.WithInsecure())
	}
	traceExporter, err := otlptracehttp.New(ctx, traceOpts...)
	if err != nil {
		return nil, err
	}
	var sampler sdktrace.Sampler = sdktrace.NeverSample()
	if cfg.SampleRatio == 1 {
		sampler = sdktrace.AlwaysSample()
	} else if cfg.SampleRatio > 0 {
		sampler = sdktrace.TraceIDRatioBased(cfg.SampleRatio)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithResource(res), sdktrace.WithBatcher(traceExporter, sdktrace.WithMaxQueueSize(cfg.SpanQueueSize), sdktrace.WithMaxExportBatchSize(cfg.SpanBatchSize), sdktrace.WithBatchTimeout(cfg.SpanBatchTimeout), sdktrace.WithExportTimeout(cfg.ExportTimeout)))
	metricOpts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpointURL(endpoint.String() + "/v1/metrics")}
	if endpoint.Scheme == "http" {
		metricOpts = append(metricOpts, otlpmetrichttp.WithInsecure())
	}
	metricExporter, err := otlpmetrichttp.New(ctx, metricOpts...)
	if err != nil {
		_ = tp.Shutdown(ctx)
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(cfg.MetricInterval), sdkmetric.WithTimeout(cfg.MetricExportTimeout))),
		sdkmetric.WithView(boundedMetricView),
	)
	p.TracerProvider, p.MeterProvider, p.Tracer, p.Meter = tp, mp, tp.Tracer(cfg.ServiceName), mp.Meter(cfg.ServiceName)
	p.Metrics, _ = NewBusinessMetrics(p.Meter)
	activeMetrics.Store(p.Metrics)
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	frameworktrace.TracerProvider = tp
	frameworktrace.Tracer = tp.Tracer(cfg.ServiceName)
	if err := frameworkmetric.InitMeterProvider(mp); err != nil {
		_ = mp.Shutdown(ctx)
		_ = tp.Shutdown(ctx)
		return nil, err
	}
	return p, nil
}

func boundedMetricView(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
	// Framework metrics currently contain per-user/session dimensions and
	// duplicate instrument names with incompatible descriptions. Platform
	// aggregate metrics cover the required bounded business dimensions.
	if strings.HasPrefix(instrument.Scope.Name, "trpc_agent_go.internal.") {
		return sdkmetric.Stream{Aggregation: sdkmetric.AggregationDrop{}}, true
	}
	return sdkmetric.Stream{}, false
}

func (p *Provider) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return p.Tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}
func (p *Provider) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		if p.Metrics != nil {
			p.Metrics.Close()
		}
		if p.MeterProvider != nil {
			_ = p.MeterProvider.Shutdown(ctx)
		}
		if p.TracerProvider != nil {
			p.closeErr = p.TracerProvider.Shutdown(ctx)
		}
		if p.oldTP != nil {
			otel.SetTracerProvider(p.oldTP)
		}
		if p.oldMP != nil {
			otel.SetMeterProvider(p.oldMP)
		}
		if p.oldProp != nil {
			otel.SetTextMapPropagator(p.oldProp)
		}
		frameworktrace.TracerProvider = p.oldFrameworkTP
		frameworktrace.Tracer = p.oldFrameworkTracer
		if p.oldFrameworkMP != nil {
			_ = frameworkmetric.InitMeterProvider(p.oldFrameworkMP)
		}
		activeMetrics.Store(p.oldMetrics)
	})
	return p.closeErr
}
