// Package telemetry initializes the standard OpenTelemetry providers and
// carries W3C context across the durable execution boundary.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"

	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	frameworktrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

const (
	defaultServiceName = "trpc-agent-service"
	protocolGRPC       = "grpc"
	protocolHTTP       = "http"
)

// Config controls optional OTLP export. Empty endpoints intentionally create
// SDK providers without exporters, so the service remains fully functional.
type Config struct {
	ServiceName    string
	ServiceVersion string
	Protocol       string
	TraceEndpoint  string
	MetricEndpoint string
}

// Runtime owns the providers installed for this process.
type Runtime struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
}

// Start installs OTel providers and the W3C Trace Context propagator.
func Start(ctx context.Context, cfg Config) (*Runtime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = defaultServiceName
	}
	if cfg.ServiceVersion == "" {
		cfg.ServiceVersion = "dev"
	}
	if cfg.Protocol == "" {
		cfg.Protocol = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	if cfg.Protocol == "" {
		cfg.Protocol = protocolGRPC
	}
	if cfg.Protocol != protocolGRPC && cfg.Protocol != protocolHTTP {
		return nil, fmt.Errorf("unsupported OTLP protocol %q", cfg.Protocol)
	}
	if cfg.TraceEndpoint == "" {
		cfg.TraceEndpoint = firstNonEmpty(
			os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
			os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		)
	}
	if cfg.MetricEndpoint == "" {
		cfg.MetricEndpoint = firstNonEmpty(
			os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
			os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		)
	}
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create telemetry resource: %w", err)
	}
	tp, err := newTracerProvider(ctx, res, cfg.Protocol, cfg.TraceEndpoint)
	if err != nil {
		return nil, err
	}
	mp, err := newMeterProvider(ctx, res, cfg.Protocol, cfg.MetricEndpoint)
	if err != nil {
		_ = tp.Shutdown(ctx)
		return nil, err
	}
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	// tRPC-Agent-Go already instruments model/session/runner internals. Point
	// its public tracer at the same provider and drop payload attributes before
	// they can be marshalled by framework instrumentation.
	frameworktrace.TracerProvider = tp
	frameworktrace.Tracer = tp.Tracer(cfg.ServiceName + "/framework")
	installFrameworkPayloadPolicy()
	return &Runtime{TracerProvider: tp, MeterProvider: mp}, nil
}

// NewNoop creates a provider runtime with no exporters.
func NewNoop(ctx context.Context, serviceName string) *Runtime {
	if ctx == nil {
		ctx = context.Background()
	}
	if serviceName == "" {
		serviceName = defaultServiceName
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", serviceName),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	frameworktrace.TracerProvider = tp
	frameworktrace.Tracer = tp.Tracer(serviceName + "/framework")
	installFrameworkPayloadPolicy()
	return &Runtime{TracerProvider: tp, MeterProvider: mp}
}

// Close flushes and stops both providers.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var result error
	if r.TracerProvider != nil {
		result = errors.Join(result, r.TracerProvider.Shutdown(ctx))
	}
	if r.MeterProvider != nil {
		result = errors.Join(result, r.MeterProvider.Shutdown(ctx))
	}
	return result
}

func newTracerProvider(ctx context.Context, res *resource.Resource, protocol, endpoint string) (*sdktrace.TracerProvider, error) {
	if strings.TrimSpace(endpoint) == "" {
		return sdktrace.NewTracerProvider(sdktrace.WithResource(res)), nil
	}
	var exporter sdktrace.SpanExporter
	var err error
	switch protocol {
	case protocolHTTP:
		options, optionErr := httpTraceOptions(endpoint)
		if optionErr != nil {
			return nil, optionErr
		}
		exporter, err = otlptracehttp.New(ctx, options...)
	default:
		options, optionErr := grpcTraceOptions(endpoint)
		if optionErr != nil {
			return nil, optionErr
		}
		exporter, err = otlptracegrpc.New(ctx, options...)
	}
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
	), nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource, protocol, endpoint string) (*sdkmetric.MeterProvider, error) {
	if strings.TrimSpace(endpoint) == "" {
		return sdkmetric.NewMeterProvider(sdkmetric.WithResource(res)), nil
	}
	var reader sdkmetric.Reader
	var err error
	switch protocol {
	case protocolHTTP:
		options, optionErr := httpMetricOptions(endpoint)
		if optionErr != nil {
			return nil, optionErr
		}
		var exporter *otlpmetrichttp.Exporter
		exporter, err = otlpmetrichttp.New(ctx, options...)
		if err == nil {
			reader = sdkmetric.NewPeriodicReader(exporter)
		}
	default:
		options, optionErr := grpcMetricOptions(endpoint)
		if optionErr != nil {
			return nil, optionErr
		}
		var exporter *otlpmetricgrpc.Exporter
		exporter, err = otlpmetricgrpc.New(ctx, options...)
		if err == nil {
			reader = sdkmetric.NewPeriodicReader(exporter)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("create OTLP metric exporter: %w", err)
	}
	return sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader)), nil
}

func grpcTraceOptions(endpoint string) ([]otlptracegrpc.Option, error) {
	host, secure, err := endpointHost(endpoint)
	if err != nil {
		return nil, err
	}
	options := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(host)}
	if !secure {
		options = append(options, otlptracegrpc.WithInsecure())
	}
	return options, nil
}

func grpcMetricOptions(endpoint string) ([]otlpmetricgrpc.Option, error) {
	host, secure, err := endpointHost(endpoint)
	if err != nil {
		return nil, err
	}
	options := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(host)}
	if !secure {
		options = append(options, otlpmetricgrpc.WithInsecure())
	}
	return options, nil
}

func httpTraceOptions(endpoint string) ([]otlptracehttp.Option, error) {
	host, secure, path, err := endpointParts(endpoint)
	if err != nil {
		return nil, err
	}
	if path == "/" {
		path = "/v1/traces"
	}
	options := []otlptracehttp.Option{otlptracehttp.WithEndpoint(host), otlptracehttp.WithURLPath(path)}
	if !secure {
		options = append(options, otlptracehttp.WithInsecure())
	}
	return options, nil
}

func httpMetricOptions(endpoint string) ([]otlpmetrichttp.Option, error) {
	host, secure, path, err := endpointParts(endpoint)
	if err != nil {
		return nil, err
	}
	if path == "/" {
		path = "/v1/metrics"
	}
	options := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(host), otlpmetrichttp.WithURLPath(path)}
	if !secure {
		options = append(options, otlpmetrichttp.WithInsecure())
	}
	return options, nil
}

func endpointHost(endpoint string) (string, bool, error) {
	host, secure, _, err := endpointParts(endpoint)
	return host, secure, err
}

func endpointParts(endpoint string) (string, bool, string, error) {
	value := strings.TrimSpace(endpoint)
	if value == "" {
		return "", false, "", errors.New("OTLP endpoint is required")
	}
	if !strings.Contains(value, "://") {
		value = "http://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return "", false, "", fmt.Errorf("invalid OTLP endpoint %q", endpoint)
	}
	path := parsed.Path
	if path == "" {
		path = "/"
	}
	return parsed.Host, parsed.Scheme == "https", path, nil
}

// StartSpan is the only service-level span helper; callers provide only safe
// boundary attributes.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	return otel.Tracer(defaultServiceName).Start(ctx, name, trace.WithAttributes(attrs...))
}

// Inject serializes W3C trace context for durable transport.
func Inject(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	result := make(map[string]string, len(carrier))
	for key, value := range carrier {
		result[key] = value
	}
	return result
}

// TraceID returns the current standard OTel trace ID, or empty if no valid
// span context is present.
func TraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return ""
	}
	return spanContext.TraceID().String()
}

// TraceParent returns the propagated W3C traceparent value.
func TraceParent(ctx context.Context) string {
	return Inject(ctx)["traceparent"]
}

// TraceState returns the propagated W3C tracestate value.
func TraceState(ctx context.Context) string {
	return Inject(ctx)["tracestate"]
}

// Extract restores W3C trace context from durable transport.
func Extract(ctx context.Context, carrier map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

// ExtractHTTP restores W3C trace context from HTTP headers.
func ExtractHTTP(ctx context.Context, headers map[string]string) context.Context {
	return Extract(ctx, headers)
}

// MarkError records only a classified and redacted error on a span.
func MarkError(span trace.Span, errorType string, err error) {
	if span == nil || err == nil {
		return
	}
	span.SetStatus(codes.Error, errorType)
	span.SetAttributes(
		attribute.String("error.type", errorType),
		attribute.String("error.message", platformlog.SafeError(err)),
	)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func installFrameworkPayloadPolicy() {
	policy := frameworktrace.SpanAttributePolicy{}
	operations := []frameworktrace.SpanOperation{
		frameworktrace.OperationChat,
		frameworktrace.OperationInvokeAgent,
		frameworktrace.OperationWorkflow,
		frameworktrace.OperationExecuteTool,
	}
	keys := []frameworktrace.AttributeKey{
		frameworktrace.AttrLLMRequest,
		frameworktrace.AttrLLMResponse,
		frameworktrace.AttrInputMessages,
		frameworktrace.AttrInputMessagesOTel,
		frameworktrace.AttrOutputMessages,
		frameworktrace.AttrOutputMessagesOTel,
	}
	for _, operation := range operations {
		for _, key := range keys {
			frameworktrace.WithAttributeRule(operation, key, frameworktrace.Drop())(&policy)
		}
	}
	frameworktrace.SetSpanAttributePolicy(policy)
}
