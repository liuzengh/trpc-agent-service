package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	agentlangfuse "trpc.group/trpc-go/trpc-agent-go/telemetry/langfuse"
	agentmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

type telemetryRuntime struct {
	Observer          *metrics.OTelObserver
	PrometheusHandler http.Handler
	traceProvider     *sdktrace.TracerProvider
	meterProvider     *sdkmetric.MeterProvider
}

func (t *telemetryRuntime) Close() {
	if t == nil {
		return
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if t.meterProvider != nil {
		_ = t.meterProvider.Shutdown(shutdownContext)
	}
	if t.traceProvider != nil {
		_ = t.traceProvider.Shutdown(shutdownContext)
	}
}

// composeTelemetry installs one shared OTel trace/meter graph for platform and
// framework telemetry. Optional exporters attach to those providers instead of
// duplicating LLM instrumentation in the service layer.
func composeTelemetry(ctx context.Context, getenv environment) (*telemetryRuntime, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	traceOptions := make([]sdktrace.TracerProviderOption, 0, 1)
	meterOptions := make([]sdkmetric.Option, 0, 2)
	if endpoint := strings.TrimSpace(getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); endpoint != "" {
		host, insecure, err := otlpEndpoint(endpoint)
		if err != nil {
			return nil, err
		}
		otlpTraceOptions := []otlptracehttp.Option{otlptracehttp.WithEndpoint(host)}
		otlpMetricOptions := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(host)}
		if insecure {
			otlpTraceOptions = append(otlpTraceOptions, otlptracehttp.WithInsecure())
			otlpMetricOptions = append(otlpMetricOptions, otlpmetrichttp.WithInsecure())
		}
		traceExporter, err := otlptracehttp.New(ctx, otlpTraceOptions...)
		if err != nil {
			return nil, fmt.Errorf("construct OTLP trace exporter: %w", err)
		}
		metricExporter, err := otlpmetrichttp.New(ctx, otlpMetricOptions...)
		if err != nil {
			_ = traceExporter.Shutdown(ctx)
			return nil, fmt.Errorf("construct OTLP metric exporter: %w", err)
		}
		traceOptions = append(traceOptions, sdktrace.WithBatcher(traceExporter))
		meterOptions = append(meterOptions, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)))
	}

	var prometheusHandler http.Handler
	if envBool(getenv, "PROMETHEUS_ENABLED") {
		registry := prometheus.NewRegistry()
		exporter, err := otelprometheus.New(
			otelprometheus.WithRegisterer(registry),
			otelprometheus.WithoutScopeInfo(),
		)
		if err != nil {
			return nil, fmt.Errorf("construct Prometheus exporter: %w", err)
		}
		meterOptions = append(meterOptions, sdkmetric.WithReader(exporter))
		prometheusHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	}

	traceProvider := sdktrace.NewTracerProvider(traceOptions...)
	meterProvider := sdkmetric.NewMeterProvider(meterOptions...)
	otel.SetTracerProvider(traceProvider)
	otel.SetMeterProvider(meterProvider)
	agenttrace.TracerProvider = traceProvider
	agenttrace.Tracer = traceProvider.Tracer("trpc-agent-go")
	if err := agentmetric.InitMeterProvider(meterProvider); err != nil {
		_ = traceProvider.Shutdown(ctx)
		_ = meterProvider.Shutdown(ctx)
		return nil, fmt.Errorf("initialize framework GenAI metrics: %w", err)
	}
	if err := attachLangfuse(ctx, getenv); err != nil {
		_ = traceProvider.Shutdown(ctx)
		_ = meterProvider.Shutdown(ctx)
		return nil, err
	}
	observer, err := metrics.NewOTelObserver(traceProvider.Tracer("trpc-agent-service"), meterProvider.Meter("trpc-agent-service"))
	if err != nil {
		_ = traceProvider.Shutdown(ctx)
		_ = meterProvider.Shutdown(ctx)
		return nil, err
	}
	return &telemetryRuntime{
		Observer:          observer,
		PrometheusHandler: prometheusHandler,
		traceProvider:     traceProvider,
		meterProvider:     meterProvider,
	}, nil
}

func attachLangfuse(ctx context.Context, getenv environment) error {
	secretKey := strings.TrimSpace(getenv("LANGFUSE_SECRET_KEY"))
	publicKey := strings.TrimSpace(getenv("LANGFUSE_PUBLIC_KEY"))
	host := strings.TrimSpace(getenv("LANGFUSE_HOST"))
	if secretKey == "" && publicKey == "" && host == "" {
		return nil
	}
	if secretKey == "" || publicKey == "" || host == "" {
		return fmt.Errorf("LANGFUSE_SECRET_KEY, LANGFUSE_PUBLIC_KEY, and LANGFUSE_HOST must be configured together")
	}
	options := []agentlangfuse.Option{
		agentlangfuse.WithSecretKey(secretKey),
		agentlangfuse.WithPublicKey(publicKey),
		agentlangfuse.WithHost(host),
	}
	if envBool(getenv, "LANGFUSE_INSECURE") {
		options = append(options, agentlangfuse.WithInsecure())
	}
	if raw := strings.TrimSpace(getenv("LANGFUSE_OBSERVATION_LEAF_VALUE_MAX_BYTES")); raw != "" {
		maxBytes, err := strconv.Atoi(raw)
		if err != nil {
			return fmt.Errorf("LANGFUSE_OBSERVATION_LEAF_VALUE_MAX_BYTES must be an integer")
		}
		options = append(options, agentlangfuse.WithObservationLeafValueMaxBytes(maxBytes))
	}
	// The framework registers its Langfuse span processor on the shared SDK
	// provider. Provider shutdown below therefore flushes both OTLP and Langfuse.
	if _, err := agentlangfuse.Start(ctx, options...); err != nil {
		return fmt.Errorf("initialize Langfuse telemetry: %w", err)
	}
	return nil
}

func mountPrometheus(handler, metricsHandler http.Handler) http.Handler {
	if metricsHandler == nil {
		return handler
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler)
	mux.Handle("/", handler)
	return mux
}

func otlpEndpoint(raw string) (host string, insecure bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, fmt.Errorf("OTLP endpoint is required")
	}
	if !strings.Contains(raw, "://") {
		return raw, true, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", false, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must be host:port or an http(s) URL")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", false, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must not include a path, query, or fragment")
	}
	return parsed.Host, parsed.Scheme == "http", nil
}
