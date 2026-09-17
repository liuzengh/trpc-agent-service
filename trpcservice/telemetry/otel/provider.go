package otel

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	gootel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"

	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	servicetelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	agentmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
)

const instrumentationName = "github.com/liuzengh/trpc-agent-service"

type Config struct {
	Endpoint, ServiceName, ServiceVersion, Role string
	AllowInsecure                               bool
	BatchTimeout, ExportTimeout, MetricInterval time.Duration
	MaxQueueSize, MaxExportBatchSize            int
	Logger                                      *servicelog.Logger
}

type Provider struct {
	tracer oteltrace.Tracer
	meter  otelmetric.Meter
	logger *servicelog.Logger

	traces  *sdktrace.TracerProvider
	metrics *metric.MeterProvider
	once    sync.Once
	err     error
}

func New(ctx context.Context, config Config) (*Provider, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	base := strings.TrimRight(config.Endpoint, "/")
	traceExporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(base+"/v1/traces"))
	if err != nil {
		return nil, fmt.Errorf("initialize OTLP trace exporter: %w", err)
	}
	metricExporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(base+"/v1/metrics"))
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, fmt.Errorf("initialize OTLP metric exporter: %w", err)
	}
	provider, err := newProvider(ctx, config, traceExporter, metricExporter)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		_ = metricExporter.Shutdown(ctx)
		return nil, err
	}
	if err := installFrameworkTelemetry(provider); err != nil {
		_ = provider.Shutdown(ctx)
		return nil, err
	}
	return provider, nil
}

// installFrameworkTelemetry makes the service exporter the single OTel
// pipeline for service-owned boundaries and trpc-agent-go's framework metrics.
// The SDK keeps its metric instruments behind an explicit initialization hook.
// Its cached tracing API is deliberately not rebound here: v1.11.2 defaults to
// capturing prompt, response and tool payload attributes, but exposes no
// public policy builder that can safely suppress them while reusing this
// provider. Service-owned model and tool spans remain the safe trace surface
// until that upstream capability is available.
func installFrameworkTelemetry(provider *Provider) error {
	if provider == nil || provider.traces == nil || provider.metrics == nil {
		return errors.New("invalid telemetry provider")
	}
	if err := agentmetric.InitMeterProvider(provider.metrics); err != nil {
		return fmt.Errorf("initialize trpc-agent-go metrics: %w", err)
	}
	gootel.SetTracerProvider(provider.traces)
	gootel.SetMeterProvider(provider.metrics)
	gootel.SetTextMapPropagator(propagation.TraceContext{})
	return nil
}

func newProvider(ctx context.Context, config Config, traceExporter sdktrace.SpanExporter, metricExporter metric.Exporter) (*Provider, error) {
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(config.ServiceName), semconv.ServiceVersion(config.ServiceVersion),
		attribute.String("service.role", config.Role)))
	if err != nil {
		return nil, fmt.Errorf("initialize telemetry resource: %w", err)
	}
	traces := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(traceExporter,
		sdktrace.WithMaxQueueSize(config.MaxQueueSize), sdktrace.WithMaxExportBatchSize(config.MaxExportBatchSize),
		sdktrace.WithBatchTimeout(config.BatchTimeout), sdktrace.WithExportTimeout(config.ExportTimeout)))
	reader := metric.NewPeriodicReader(metricExporter, metric.WithInterval(config.MetricInterval), metric.WithTimeout(config.ExportTimeout))
	metrics := metric.NewMeterProvider(metric.WithResource(res), metric.WithReader(reader))
	return &Provider{tracer: traces.Tracer(instrumentationName), meter: metrics.Meter(instrumentationName),
		logger: config.Logger, traces: traces, metrics: metrics}, nil
}

func validateConfig(config Config) error {
	parsed, err := url.Parse(config.Endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "https" && parsed.Scheme != "http") || (parsed.Scheme == "http" && !config.AllowInsecure) {
		return errors.New("invalid OTLP endpoint")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return errors.New("OTLP endpoint must not contain a path")
	}
	if strings.TrimSpace(config.ServiceName) == "" || strings.TrimSpace(config.ServiceVersion) == "" || strings.TrimSpace(config.Role) == "" ||
		config.BatchTimeout <= 0 || config.ExportTimeout <= 0 || config.MetricInterval <= 0 || config.MaxQueueSize < 1 ||
		config.MaxExportBatchSize < 1 || config.MaxExportBatchSize > config.MaxQueueSize {
		return errors.New("invalid telemetry configuration")
	}
	return nil
}

func (p *Provider) StartSpan(ctx context.Context, operation servicetelemetry.Operation, attributes ...servicetelemetry.Attribute) (context.Context, servicetelemetry.Span) {
	if p == nil || p.tracer == nil || !servicetelemetry.ValidOperation(operation) {
		return servicetelemetry.Noop().StartSpan(ctx, operation, attributes...)
	}
	ctx, span := p.tracer.Start(ctx, string(operation), oteltrace.WithAttributes(convertAttributes(attributes)...))
	return ctx, wrappedSpan{span: span}
}

func (p *Provider) Counter(descriptor servicetelemetry.MetricDescriptor) servicetelemetry.Counter {
	if p == nil || p.meter == nil || !servicetelemetry.ValidMetricDescriptor(descriptor) {
		return servicetelemetry.Noop().Counter(descriptor)
	}
	instrument, err := p.meter.Int64Counter(string(descriptor))
	return wrappedCounter{instrument: instrument, err: err}
}

func (p *Provider) Histogram(descriptor servicetelemetry.MetricDescriptor) servicetelemetry.Histogram {
	if p == nil || p.meter == nil || !servicetelemetry.ValidMetricDescriptor(descriptor) {
		return servicetelemetry.Noop().Histogram(descriptor)
	}
	instrument, err := p.meter.Float64Histogram(string(descriptor), otelmetric.WithUnit("s"))
	return wrappedHistogram{instrument: instrument, err: err}
}

func (p *Provider) Logger(component servicetelemetry.Component) servicetelemetry.Logger {
	return wrappedLogger{logger: p.logger, component: component}
}

func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		p.err = errors.Join(p.metrics.Shutdown(ctx), p.traces.Shutdown(ctx))
	})
	return p.err
}

type wrappedSpan struct{ span oteltrace.Span }

func (s wrappedSpan) End(err error) {
	if err != nil {
		s.span.SetStatus(codes.Error, "")
		s.span.SetAttributes(attribute.String("outcome", "error"))
	} else {
		s.span.SetAttributes(attribute.String("outcome", "success"))
	}
	s.span.End()
}

type wrappedCounter struct {
	instrument otelmetric.Int64Counter
	err        error
}

func (c wrappedCounter) Add(ctx context.Context, value int64, attributes ...servicetelemetry.Attribute) {
	if c.err == nil && c.instrument != nil {
		c.instrument.Add(ctx, value, otelmetric.WithAttributes(convertAttributes(attributes)...))
	}
}

type wrappedHistogram struct {
	instrument otelmetric.Float64Histogram
	err        error
}

func (h wrappedHistogram) Record(ctx context.Context, value float64, attributes ...servicetelemetry.Attribute) {
	if h.err == nil && h.instrument != nil {
		h.instrument.Record(ctx, value, otelmetric.WithAttributes(convertAttributes(attributes)...))
	}
}

type wrappedLogger struct {
	logger    *servicelog.Logger
	component servicetelemetry.Component
}

func (l wrappedLogger) Info(ctx context.Context, event servicetelemetry.LogEvent, attributes ...servicetelemetry.Attribute) {
	l.write(ctx, false, event, attributes)
}
func (l wrappedLogger) Error(ctx context.Context, event servicetelemetry.LogEvent, attributes ...servicetelemetry.Attribute) {
	l.write(ctx, true, event, attributes)
}
func (l wrappedLogger) write(ctx context.Context, isError bool, event servicetelemetry.LogEvent, attributes []servicetelemetry.Attribute) {
	if l.logger == nil {
		return
	}
	values := make([]servicelog.Attribute, 0, len(attributes)+2)
	values = append(values, servicelog.String("component", string(l.component)), servicelog.String("event", string(event)))
	for _, item := range attributes {
		if item.Valid() {
			values = append(values, servicelog.String(item.Key(), item.Value()))
		}
	}
	if isError {
		l.logger.Error(ctx, string(event), values...)
		return
	}
	l.logger.Info(ctx, string(event), values...)
}

func convertAttributes(values []servicetelemetry.Attribute) []attribute.KeyValue {
	result := make([]attribute.KeyValue, 0, len(values))
	for _, item := range values {
		if item.Valid() {
			result = append(result, attribute.String(item.Key(), item.Value()))
		}
	}
	return result
}

var _ servicetelemetry.Provider = (*Provider)(nil)
