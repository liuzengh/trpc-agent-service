// Package telemetry exports optional OpenTelemetry traces and metrics for the
// durable channel pipeline.
//
// It is off unless an operator turns it on, and what it records is an explicit
// whitelist rather than automatic instrumentation. The upstream Runner's own
// tracing captures model input, tool arguments and error text, so this package
// builds its own providers, installs none of them globally, and offers no way
// to attach a message, an external account or an error to a record.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
)

const (
	// EnabledEnvVar turns the export on. It matches exactly, like every other
	// switch this process reads: an operator who wrote TRUE gets a refusal
	// rather than a process that quietly recorded nothing.
	EnabledEnvVar = "TRPC_SERVICE_TELEMETRY_ENABLED"

	// EndpointEnvVar is the OTLP/HTTP collector to export to. Optional.
	EndpointEnvVar = "TRPC_SERVICE_OTLP_ENDPOINT"

	// DefaultEndpoint is a collector on the same machine, on the standard
	// OTLP/HTTP port.
	DefaultEndpoint = "http://127.0.0.1:4318"
)

var (
	// ErrConfig is the sentinel behind every refusal in this package.
	ErrConfig = errors.New("telemetry: invalid configuration")

	// ErrExportFailed replaces whatever an exporter returns. An OTLP error
	// quotes the endpoint and the collector's answer, and the SDK hands export
	// errors to the process error handler, which logs them — so what may be
	// logged is decided here rather than by the collector.
	ErrExportFailed = errors.New("telemetry: an export attempt failed")

	// ErrShutdownIncomplete reports that the bounded flush did not finish. It
	// carries no detail for the same reason.
	ErrShutdownIncomplete = errors.New("telemetry: shutdown did not complete")
)

// scopeName is the instrumentation scope every span and instrument is created
// under.
const scopeName = "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"

// serviceName identifies this process in the exported resource.
const serviceName = "trpc-agent-service"

// shutdownTimeout bounds the flush at exit. A collector that stopped answering
// must not hold the process open.
const shutdownTimeout = 5 * time.Second

// exportTimeout bounds one export, retries included.
//
// It is separate from the bound above, and it is the one that matters for a
// collector that accepts a connection and never answers: the batch processor
// drains on a background context of its own, so a shutdown that stopped waiting
// would otherwise leave the export retrying behind it. Kept under the shutdown
// bound, so the flush ends by dropping the record rather than by giving up on
// an attempt still in flight.
const exportTimeout = 3 * time.Second

// retryInterval is the wait between export attempts inside that bound.
const retryInterval = 500 * time.Millisecond

// Config is the whole telemetry configuration of one process.
type Config struct {
	Enabled bool
	// Endpoint is the checked origin of the collector, never the raw value the
	// operator wrote; see Load.
	Endpoint string
}

// Load reads that configuration. Disabled is the default and reads nothing
// further.
func Load(getenv func(string) string) (Config, error) {
	enabled, err := parseEnabled(getenv(EnabledEnvVar))
	if err != nil || !enabled {
		return Config{}, err
	}
	endpoint := getenv(EndpointEnvVar)
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	checked, err := checkEndpoint(endpoint)
	if err != nil {
		return Config{}, err
	}
	return Config{Enabled: true, Endpoint: checked}, nil
}

// parseEnabled matches exactly; see EnabledEnvVar.
func parseEnabled(value string) (bool, error) {
	switch value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf(
			"%w: %s must be \"true\" or \"false\" (leave it unset for \"false\")",
			ErrConfig, EnabledEnvVar)
	}
}

// checkEndpoint accepts an origin and returns it rebuilt from its parts.
//
// Only a scheme and a host survive. A URL with user information, a query or a
// fragment is refused rather than trimmed, because those are the places a
// credential is written; and the refusal names the variable and the shape it
// wants, never the value it was given, so a misconfigured token cannot reach a
// log through a startup error.
func checkEndpoint(value string) (string, error) {
	refuse := fmt.Errorf(
		"%w: %s must be an origin such as %q — an http or https scheme and a host, "+
			"with no user information, path, query or fragment",
		ErrConfig, EndpointEnvVar, DefaultEndpoint)
	parsed, err := url.Parse(value)
	if err != nil {
		return "", refuse
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", refuse
	}
	if parsed.Host == "" || parsed.Opaque != "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return "", refuse
	}
	return (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host}).String(), nil
}

// Describe reports what was configured, for the startup log. The endpoint it
// renders is the checked one, which by construction carries no credential.
func (c Config) Describe() string {
	if !c.Enabled {
		return "disabled"
	}
	return fmt.Sprintf("enabled, exporting OTLP/HTTP to %s", c.Endpoint)
}

// Telemetry owns one process's providers and the instruments recorded on them.
//
// A nil *Telemetry is the disabled process, and every method here accepts one:
// a caller that did not turn telemetry on has one code path, not two.
type Telemetry struct {
	tracer   trace.Tracer
	stages   metric.Int64Counter
	duration metric.Float64Histogram
	// shutdown is set only by Open, which owns the providers it built. New is
	// handed providers that stay the caller's to close.
	shutdown func(context.Context) error
}

// Open builds the providers cfg describes. A disabled configuration returns a
// nil Telemetry and no error.
//
// ctx bounds construction only. Nothing here dials the collector — the first
// connection is made by the background queue when it exports — so a collector
// that is down delays no startup and blocks no message.
func Open(ctx context.Context, cfg Config) (*Telemetry, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	endpoint, err := checkEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	useSafeErrorHandler()
	spans, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithTimeout(exportTimeout),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         true,
			InitialInterval: retryInterval,
			MaxInterval:     retryInterval,
			MaxElapsedTime:  exportTimeout,
		}))
	if err != nil {
		return nil, fmt.Errorf("%w: the trace exporter could not be built", ErrConfig)
	}
	measurements, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(endpoint),
		otlpmetrichttp.WithTimeout(exportTimeout),
		otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
			Enabled:         true,
			InitialInterval: retryInterval,
			MaxInterval:     retryInterval,
			MaxElapsedTime:  exportTimeout,
		}))
	if err != nil {
		_ = spans.Shutdown(ctx)
		return nil, fmt.Errorf("%w: the metric exporter could not be built", ErrConfig)
	}
	attributes := resource.NewSchemaless(
		attribute.String("service.name", serviceName),
		attribute.String("service.version", trpcservice.Version),
	)
	// A batch processor and a periodic reader, which are both bounded queues on
	// their own goroutines. That is the whole reason a collector outage cannot
	// reach the pipeline: nothing on the message path waits for an export, and
	// a queue that fills drops records instead of blocking the caller that
	// produced them.
	tracers := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(safeSpanExporter{spans}),
		sdktrace.WithResource(attributes),
	)
	meters := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(safeMetricExporter{measurements})),
		sdkmetric.WithResource(attributes),
	)
	// Deliberately no otel.SetTracerProvider or otel.SetMeterProvider: these
	// providers are reachable only through the recorders below, so turning
	// telemetry on cannot start any other component's instrumentation.
	telemetry, err := New(tracers, meters)
	if err != nil {
		_ = tracers.Shutdown(ctx)
		_ = meters.Shutdown(ctx)
		return nil, err
	}
	telemetry.shutdown = func(ctx context.Context) error {
		return errors.Join(tracers.Shutdown(ctx), meters.Shutdown(ctx))
	}
	return telemetry, nil
}

// New builds a Telemetry over providers the caller owns and goes on owning:
// Shutdown then flushes nothing, because these providers are not this package's
// to close. Open is the ordinary constructor.
func New(tracers trace.TracerProvider, meters metric.MeterProvider) (*Telemetry, error) {
	if tracers == nil || meters == nil {
		return nil, fmt.Errorf("%w: telemetry needs a tracer and a meter provider", ErrConfig)
	}
	meter := meters.Meter(scopeName)
	stages, err := meter.Int64Counter(stageCountMetric,
		metric.WithDescription("Channel pipeline stages recorded, by outcome."),
		metric.WithUnit("{stage}"))
	if err != nil {
		return nil, fmt.Errorf("%w: the stage counter could not be built", ErrConfig)
	}
	duration, err := meter.Float64Histogram(stageDurationMetric,
		metric.WithDescription("How long one channel pipeline stage took."),
		metric.WithUnit("ms"))
	if err != nil {
		return nil, fmt.Errorf("%w: the stage histogram could not be built", ErrConfig)
	}
	return &Telemetry{
		tracer:   tracers.Tracer(scopeName),
		stages:   stages,
		duration: duration,
	}, nil
}

// Shutdown flushes what is queued and releases the exporters it built. It is
// bounded, and it reports a fixed error: a flush failure names the collector
// and quotes its answer, and this one reaches the process exit status.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil || t.shutdown == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()
	if t.shutdown(ctx) != nil {
		return ErrShutdownIncomplete
	}
	return nil
}

// useSafeErrorHandler replaces the process error handler with one that says
// only that an export failed.
//
// The wrappers below cover what an exporter *returns*, and a partial success is
// not returned: the OTLP client hands the collector's own message to
// otel.Handle and then reports success. This handler is the only place this
// version of the SDK offers to stop that text — the HTTP exporters take no
// client of one's own — and it keeps exactly what the default handler already
// logged, which is that an export did not get through.
//
// It is global, and it is the one global this package writes. It is written
// only when an operator turned telemetry on, so a process with telemetry off is
// untouched, and it is written on every Open rather than once, so that it is
// not left to the order in which somebody else set theirs.
func useSafeErrorHandler() {
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
		log.Print(ErrExportFailed)
	}))
}

// safeSpanExporter replaces every error the wrapped exporter returns with a
// fixed one; see ErrExportFailed.
type safeSpanExporter struct{ sdktrace.SpanExporter }

func (e safeSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if e.SpanExporter.ExportSpans(ctx, spans) != nil {
		return ErrExportFailed
	}
	return nil
}

func (e safeSpanExporter) Shutdown(ctx context.Context) error {
	if e.SpanExporter.Shutdown(ctx) != nil {
		return ErrExportFailed
	}
	return nil
}

// safeMetricExporter is safeSpanExporter for measurements. Temporality and
// Aggregation are the wrapped exporter's own.
type safeMetricExporter struct{ sdkmetric.Exporter }

func (e safeMetricExporter) Export(ctx context.Context, data *metricdata.ResourceMetrics) error {
	if e.Exporter.Export(ctx, data) != nil {
		return ErrExportFailed
	}
	return nil
}

func (e safeMetricExporter) ForceFlush(ctx context.Context) error {
	if e.Exporter.ForceFlush(ctx) != nil {
		return ErrExportFailed
	}
	return nil
}

func (e safeMetricExporter) Shutdown(ctx context.Context) error {
	if e.Exporter.Shutdown(ctx) != nil {
		return ErrExportFailed
	}
	return nil
}
