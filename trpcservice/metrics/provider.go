package metrics

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const traceSampleRatioEnv = "TRPC_AGENT_TRACE_SAMPLE_RATIO"

// Providers owns the process-global OTLP providers installed for production.
// An empty OTEL_EXPORTER_OTLP_ENDPOINT intentionally selects the SDK no-op
// providers for offline tests and local commands.
type Providers struct {
	traces  *sdktrace.TracerProvider
	metrics *sdkmetric.MeterProvider
	once    sync.Once
	err     error
}

// ConfigureOTLP installs trace and metric providers that export over OTLP/gRPC.
// Endpoint/TLS/header settings use the standard OpenTelemetry environment
// variables, so credentials never enter application configuration or logs.
func ConfigureOTLP(ctx context.Context, serviceName, nodeID string) (*Providers, error) {
	if ctx == nil || strings.TrimSpace(serviceName) == "" || strings.TrimSpace(nodeID) == "" {
		return nil, errors.New("metrics: context, service name, and node ID are required")
	}
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) == "" {
		return &Providers{}, nil
	}
	ratio, err := traceSampleRatio()
	if err != nil {
		return nil, err
	}
	res, err := serviceResource(serviceName, nodeID)
	if err != nil {
		return nil, errors.New("metrics: create telemetry resource failed")
	}
	traceExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, errors.New("metrics: create OTLP trace exporter failed")
	}
	metricExporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, errors.New("metrics: create OTLP metric exporter failed")
	}
	traces := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithBatcher(traceExporter),
	)
	reader := sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(10*time.Second), sdkmetric.WithTimeout(5*time.Second))
	metrics := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
	otel.SetTracerProvider(traces)
	otel.SetMeterProvider(metrics)
	otel.SetTextMapPropagator(defaultPropagator())
	return &Providers{traces: traces, metrics: metrics}, nil
}

func serviceResource(serviceName, nodeID string) (*resource.Resource, error) {
	return resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(serviceName),
		semconv.ServiceInstanceID(nodeID),
	))
}

func traceSampleRatio() (float64, error) {
	value := strings.TrimSpace(os.Getenv(traceSampleRatioEnv))
	if value == "" {
		return 0.1, nil
	}
	ratio, err := strconv.ParseFloat(value, 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return 0, errors.New("metrics: trace sample ratio must be between 0 and 1")
	}
	return ratio, nil
}

func defaultPropagator() propagation.TextMapPropagator {
	return propagation.TraceContext{}
}

// Shutdown flushes metrics and traces before their owning process exits.
func (providers *Providers) Shutdown(ctx context.Context) error {
	if providers == nil {
		return nil
	}
	providers.once.Do(func() {
		var metricErr, traceErr error
		if providers.metrics != nil {
			metricErr = providers.metrics.Shutdown(ctx)
		}
		if providers.traces != nil {
			traceErr = providers.traces.Shutdown(ctx)
		}
		providers.err = errors.Join(metricErr, traceErr)
	})
	return providers.err
}
