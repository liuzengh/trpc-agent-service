package telemetry

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Runtime owns the process telemetry providers. A nil *Runtime and a Runtime
// composed with ModeNone are both safe: spans and metrics go to no-op
// providers and nothing is exported.
type Runtime struct {
	config            Config
	tracerProvider    *sdktrace.TracerProvider
	meterProvider     *sdkmetric.MeterProvider
	tracer            trace.Tracer
	meter             Metrics
	logger            *Logger
	configInstruments configInstruments
	shutdownOnce      sync.Once
	shutdownErr       error
}

// Compose builds the runtime. ModeNone never builds exporters or starts
// background goroutines. Partial failures clean up everything they created.
func Compose(ctx context.Context, config Config, logger *Logger) (*Runtime, error) {
	config, err := config.WithDefaults()
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = NewNopLogger()
	}
	runtime := &Runtime{config: config, logger: logger, tracer: noop.NewTracerProvider().Tracer(""), meter: newNoopMetrics()}
	if config.Mode == ModeNone {
		return runtime, nil
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(defaultServiceName),
		attribute.String("deployment.environment", config.Environment),
		semconv.ServiceVersion(config.ServiceVersion),
	))
	if err != nil {
		return nil, err
	}
	traceOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(config.Endpoint)}
	if config.InsecureLocalOK {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
	}
	traceExporter, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, err
	}
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(config.SampleRatio))
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter,
			sdktrace.WithMaxExportBatchSize(config.BatchSize),
			sdktrace.WithMaxQueueSize(config.QueueSize),
			sdktrace.WithBatchTimeout(config.ExportTimeout)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	metricOpts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(config.Endpoint)}
	if config.InsecureLocalOK {
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		_ = tracerProvider.Shutdown(context.Background())
		return nil, err
	}
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(config.ExportTimeout))),
		sdkmetric.WithResource(res),
	)
	// Replace the process-global propagator and tracer provider so server
	// spans created through the otel API carry the composed providers. The
	// default prior to composition is a no-op, so this is safe and idempotent
	// for tests that compose per test.
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	runtime.tracerProvider = tracerProvider
	runtime.meterProvider = meterProvider
	runtime.tracer = tracerProvider.Tracer(defaultServiceName)
	runtime.meter = newOtelMetrics(meterProvider)
	runtime.configInstruments = newConfigInstruments(meterProvider)
	return runtime, nil
}

// Config returns the effective configuration.
func (r *Runtime) Config() Config {
	if r == nil {
		return Config{Mode: ModeNone}
	}
	return r.config
}

// Tracer returns the runtime tracer. Nil-safe.
func (r *Runtime) Tracer() trace.Tracer {
	if r == nil {
		return noop.NewTracerProvider().Tracer("")
	}
	return r.tracer
}

// Metrics returns the low-cardinality instrument registry. Nil-safe.
func (r *Runtime) Metrics() Metrics {
	if r == nil {
		return newNoopMetrics()
	}
	return r.meter
}

// Logger returns the structured logger. Nil-safe.
func (r *Runtime) Logger() *Logger {
	if r == nil {
		return NewNopLogger()
	}
	return r.logger
}

// ForceFlush bounds the export pipeline flush. Best-effort: failures are
// logged with category-only detail and never propagate.
func (r *Runtime) ForceFlush(ctx context.Context) {
	if r == nil || r.tracerProvider == nil {
		return
	}
	flushCtx, cancel := context.WithTimeout(ctx, r.config.FlushTimeout)
	defer cancel()
	if err := r.tracerProvider.ForceFlush(flushCtx); err != nil {
		r.logger.EventRateLimited(context.Background(), 8, "telemetry_flush_failed", "telemetry", "flush", "failed", 30*time.Second,
			"error_category", SafeError(err))
	}
}

// ForceFlushAndWait flushes and reports a bounded error result.
func (r *Runtime) ForceFlushAndWait(ctx context.Context) error {
	if r == nil || r.tracerProvider == nil {
		return nil
	}
	flushCtx, cancel := context.WithTimeout(ctx, r.config.FlushTimeout)
	defer cancel()
	return r.tracerProvider.ForceFlush(flushCtx)
}

// Shutdown is bounded and idempotent. It flushes and shuts down the meter and
// trace providers in order; repeated calls return the first result.
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.shutdownOnce.Do(func() {
		shutdownCtx, cancel := context.WithTimeout(ctx, r.config.FlushTimeout)
		defer cancel()
		if r.meterProvider != nil {
			if err := r.meterProvider.Shutdown(shutdownCtx); err != nil && r.shutdownErr == nil {
				r.shutdownErr = err
			}
		}
		if r.tracerProvider != nil {
			if err := r.tracerProvider.Shutdown(shutdownCtx); err != nil && r.shutdownErr == nil {
				r.shutdownErr = err
			}
		}
		if r.shutdownErr != nil {
			r.logger.EventRateLimited(context.Background(), 8, "telemetry_shutdown_failed", "telemetry", "shutdown", "failed", 30*time.Second,
				"error_category", SafeError(r.shutdownErr))
		}
	})
	return r.shutdownErr
}

// NewRuntimeForTest wraps externally managed providers (in-memory exporters
// in tests) into a Runtime without ownership transfer: Shutdown on the test
// runtime is a no-op for these providers.
func NewRuntimeForTest(tracerProvider *sdktrace.TracerProvider, meterProvider *sdkmetric.MeterProvider) *Runtime {
	runtime := &Runtime{config: Config{Mode: ModeOTLP, FlushTimeout: 10 * time.Second}, tracer: tracerProvider.Tracer(defaultServiceName)}
	runtime.meter = newOtelMetrics(meterProvider)
	runtime.configInstruments = newConfigInstruments(meterProvider)
	runtime.tracerProvider = tracerProvider
	runtime.meterProvider = meterProvider
	return runtime
}

// Deadline helper for bounded internal waits.
func bounded(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}
