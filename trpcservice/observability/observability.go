// Package observability defines the provider-neutral runtime telemetry contract.
// Concrete SDKs are intentionally hidden behind these small interfaces.
package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Attribute is a sanitized key-value annotation attached to telemetry.
type Attribute struct{ Key, Value string }

// Span records a single operation and its outcome.
type Span interface {
	End()
	SetAttributes(...Attribute)
	SetStatus(Status, string)
	RecordError(error)
}

// Tracer starts spans for named operations.
type Tracer interface {
	Start(context.Context, string, ...Attribute) (context.Context, Span)
}

// Counter records an integer measurement.
type Counter interface {
	Add(context.Context, int64, ...Attribute)
}

// Histogram records a floating-point measurement.
type Histogram interface {
	Record(context.Context, float64, ...Attribute)
}

// UpDownCounter records a signed integer measurement that may increase or decrease.
type UpDownCounter interface {
	Add(context.Context, int64, ...Attribute)
}

// Meter creates instruments for named measurements.
type Meter interface {
	Counter(string) Counter
	Histogram(string) Histogram
	UpDownCounter(string) UpDownCounter
}

// Logger emits structured, redacted diagnostic records.
type Logger interface {
	Log(context.Context, Level, string, ...Attribute)
}

// Provider supplies tracing, metrics, logging, and shutdown lifecycle hooks.
type Provider interface {
	Tracer(string) Tracer
	Meter(string) Meter
	Logger() Logger
	Shutdown(context.Context) error
}

// Status is the outcome code recorded on a span.
type Status string

const (
	// StatusUnset leaves the span outcome unspecified.
	StatusUnset Status = "unset"
	// StatusOK marks a successful operation.
	StatusOK Status = "ok"
	// StatusError marks a failed operation.
	StatusError Status = "error"
)

// Level controls structured log severity.
type Level int

const (
	// LevelDebug is verbose diagnostic output.
	LevelDebug Level = iota
	// LevelInfo is normal operational output.
	LevelInfo
	// LevelWarn is a recoverable operational condition.
	LevelWarn
	// LevelError is an operation failure.
	LevelError
)

const (
	// OperationHTTPRequest identifies HTTP request work.
	OperationHTTPRequest = "http.request"
	// OperationGatewayDispatch identifies gateway dispatch work.
	OperationGatewayDispatch = "gateway.dispatch"
	// OperationRunnerExecution identifies model runner work.
	OperationRunnerExecution = "runner.execution"
	// OperationModelCall identifies model provider work.
	OperationModelCall = "model.call"
	// OperationToolCall identifies tool provider work.
	OperationToolCall = "tool.call"
	// OperationStorageOperation identifies persistence work.
	OperationStorageOperation = "storage.operation"
	// OperationChannelReceive identifies inbound channel work.
	OperationChannelReceive = "channel.receive"
	// OperationChannelSend identifies outbound channel work.
	OperationChannelSend = "channel.send"
)

// Config supplies provider names and optional telemetry implementations.
type Config struct {
	ServiceName    string
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Logger         *slog.Logger
	Shutdown       func(context.Context) error
}

// OTLPConfig contains only exporter configuration; no secret is retained in the provider.
type OTLPConfig struct {
	ServiceName string
	Endpoint    string
	Headers     map[string]string
	Insecure    bool
}

// NewOTLPProvider creates a standard OTLP/HTTP provider. An empty endpoint deliberately
// selects the no-op provider so local development never requires an exporter.
func NewOTLPProvider(ctx context.Context, config OTLPConfig) (Provider, error) {
	if config.Endpoint == "" {
		return NewNoopProvider(), nil
	}
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	if err := validateOTLPEndpoint(config.Endpoint); err != nil {
		return NewNoopProvider(), err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	options := otlpTraceEndpointOptions(config.Endpoint)
	if config.Insecure {
		options = append(options, otlptracehttp.WithInsecure())
	}
	if len(config.Headers) > 0 {
		// Exporter credentials configure transport authentication; they are never
		// copied into spans/logs. Redacting them here would make authenticated
		// OTLP export impossible.
		headers := make(map[string]string, len(config.Headers))
		for key, value := range config.Headers {
			headers[key] = value
		}
		options = append(options, otlptracehttp.WithHeaders(headers))
	}
	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return NewNoopProvider(), err
	}
	if config.ServiceName == "" {
		config.ServiceName = "trpc-agent-service"
	}
	resourceValue, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(config.ServiceName)))
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return NewNoopProvider(), err
	}
	metricOptions := otlpMetricEndpointOptions(config.Endpoint)
	if config.Insecure {
		metricOptions = append(metricOptions, otlpmetrichttp.WithInsecure())
	}
	if len(config.Headers) > 0 {
		headers := make(map[string]string, len(config.Headers))
		for key, value := range config.Headers {
			headers[key] = value
		}
		metricOptions = append(metricOptions, otlpmetrichttp.WithHeaders(headers))
	}
	metricExporter, err := otlpmetrichttp.New(ctx, metricOptions...)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return NewNoopProvider(), err
	}
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(resourceValue))
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(resourceValue),
	)
	shutdown := func(shutdownCtx context.Context) error {
		return errors.Join(tracerProvider.Shutdown(shutdownCtx), meterProvider.Shutdown(shutdownCtx))
	}
	return NewProvider(Config{ServiceName: config.ServiceName, TracerProvider: tracerProvider, MeterProvider: meterProvider, Shutdown: shutdown}), nil
}

func otlpTraceEndpointOptions(endpoint string) []otlptracehttp.Option {
	if strings.Contains(endpoint, "://") {
		return []otlptracehttp.Option{otlptracehttp.WithEndpointURL(otlpSignalEndpointURL(endpoint, "/v1/traces"))}
	}
	return []otlptracehttp.Option{otlptracehttp.WithEndpoint(endpoint)}
}

func otlpMetricEndpointOptions(endpoint string) []otlpmetrichttp.Option {
	if strings.Contains(endpoint, "://") {
		return []otlpmetrichttp.Option{otlpmetrichttp.WithEndpointURL(otlpSignalEndpointURL(endpoint, "/v1/metrics"))}
	}
	return []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(endpoint)}
}

func otlpSignalEndpointURL(endpoint, suffix string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	path := strings.TrimRight(u.Path, "/")
	if !strings.HasSuffix(path, suffix) {
		path += suffix
	}
	u.Path = path
	return u.String()
}

func validateOTLPEndpoint(endpoint string) error {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || strings.ContainsAny(endpoint, "\r\n\t ") {
		return errors.New("OTLP endpoint must not contain whitespace")
	}
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errors.New("OTLP endpoint URL is invalid")
		}
		return nil
	}
	if strings.ContainsAny(endpoint, "/?#") {
		return errors.New("OTLP endpoint host must not contain a path")
	}
	return nil
}

type provider struct {
	tracer   Tracer
	meter    Meter
	logger   Logger
	shutdown func(context.Context) error
	once     sync.Once
	closeErr error
}

// NewProvider creates a provider using configured or process-default SDKs.
func NewProvider(config Config) Provider {
	if config.ServiceName == "" {
		config.ServiceName = "trpc-agent-service"
	}
	tp := config.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := config.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	p := &provider{tracer: otelTracer{tracer: tp.Tracer(config.ServiceName)}, meter: otelMeter{meter: mp.Meter(config.ServiceName)}, logger: slogLogger{logger: logger}}
	if config.Shutdown != nil {
		p.shutdown = config.Shutdown
	} else {
		p.shutdown = func(context.Context) error { return nil }
	}
	return p
}

// NewNoopProvider creates a provider that discards telemetry.
func NewNoopProvider() Provider {
	return NewProvider(Config{TracerProvider: tracenoop.NewTracerProvider(), MeterProvider: metricnoop.NewMeterProvider(), Logger: slog.New(slog.NewTextHandler(discardWriter{}, nil))})
}

func (p *provider) Tracer(string) Tracer { return p.tracer }
func (p *provider) Meter(string) Meter   { return p.meter }
func (p *provider) Logger() Logger       { return p.logger }
func (p *provider) Shutdown(ctx context.Context) error {
	p.once.Do(func() { p.closeErr = p.shutdown(ctx) })
	return p.closeErr
}

type discardWriter struct{}

func (discardWriter) Write(b []byte) (int, error) { return len(b), nil }

type otelTracer struct{ tracer trace.Tracer }

func (t otelTracer) Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span) {
	attrs = sanitizeAttributes(attrs)
	values := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		values = append(values, attribute.String(a.Key, a.Value))
	}
	next, span := t.tracer.Start(ctx, name, trace.WithAttributes(values...))
	return next, otelSpan{span: span}
}

type otelSpan struct{ span trace.Span }

func (s otelSpan) End() { s.span.End() }
func (s otelSpan) SetAttributes(attrs ...Attribute) {
	attrs = sanitizeAttributes(attrs)
	values := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		values = append(values, attribute.String(a.Key, a.Value))
	}
	s.span.SetAttributes(values...)
}
func (s otelSpan) SetStatus(status Status, description string) {
	code := codes.Unset
	if status == StatusOK {
		code = codes.Ok
	}
	if status == StatusError {
		code = codes.Error
	}
	s.span.SetStatus(code, RedactString(description))
}
func (s otelSpan) RecordError(err error) {
	if err != nil {
		s.span.RecordError(errors.New(ErrorClass(err)))
	}
}

type otelMeter struct{ meter metric.Meter }

func (m otelMeter) Counter(name string) Counter {
	c, _ := m.meter.Int64Counter(name)
	return otelCounter{c}
}
func (m otelMeter) Histogram(name string) Histogram {
	h, _ := m.meter.Float64Histogram(name)
	return otelHistogram{h}
}
func (m otelMeter) UpDownCounter(name string) UpDownCounter {
	c, _ := m.meter.Int64UpDownCounter(name)
	return otelUpDownCounter{c}
}
func kv(attrs []Attribute) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, attribute.String(a.Key, a.Value))
	}
	return out
}

type otelCounter struct{ c metric.Int64Counter }

func (c otelCounter) Add(ctx context.Context, v int64, attrs ...Attribute) {
	c.c.Add(ctx, v, metric.WithAttributes(kv(attrs)...))
}

type otelHistogram struct{ h metric.Float64Histogram }

func (h otelHistogram) Record(ctx context.Context, v float64, attrs ...Attribute) {
	h.h.Record(ctx, v, metric.WithAttributes(kv(attrs)...))
}

type otelUpDownCounter struct{ c metric.Int64UpDownCounter }

func (c otelUpDownCounter) Add(ctx context.Context, v int64, attrs ...Attribute) {
	c.c.Add(ctx, v, metric.WithAttributes(kv(attrs)...))
}

type slogLogger struct{ logger *slog.Logger }

func (l slogLogger) Log(ctx context.Context, level Level, msg string, attrs ...Attribute) {
	attrs = sanitizeAttributes(attrs)
	args := make([]any, 0, len(attrs)*2)
	for _, a := range attrs {
		args = append(args, a.Key, RedactString(a.Value))
	}
	slogLevel := slog.LevelInfo
	switch level {
	case LevelDebug:
		slogLevel = slog.LevelDebug
	case LevelWarn:
		slogLevel = slog.LevelWarn
	case LevelError:
		slogLevel = slog.LevelError
	}
	l.logger.Log(ctx, slogLevel, RedactString(msg), args...)
}

var sensitivePattern = regexp.MustCompile(`(?i)(bearer\s+|api[_-]?key\s*[=:]\s*|token\s*[=:]\s*|authorization\s*[=:]\s*|secret(?:[_-]?ref)?\s*[=:]\s*|password\s*[=:]\s*)[^\s,;]+`)
var dsnPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)([^/@\s]+):([^/@\s]+)@`)
var bearerPattern = regexp.MustCompile(`(?i)Bearer\s+[^\s,;]+`)
var attributeHighCardinalityPattern = regexp.MustCompile(`(?i)(session|user|message|request|trace|[0-9a-f]{16,}|https?://)`)
var allowedAttributeKeys = map[string]struct{}{
	"component": {}, "operation": {}, "status": {}, "error_class": {},
	"tenant_hash": {}, "app_hash": {}, "model_family": {}, "provider": {}, "channel": {}, "currency": {},
}
var allowedAttributeValues = map[string]map[string]struct{}{
	"component":    {"http": {}, "gateway": {}, "runner": {}, "model": {}, "tool": {}, "storage": {}, "channel": {}},
	"status":       {"started": {}, "active": {}, "complete": {}, "ok": {}, "error": {}, "success": {}, "failure": {}, "canceled": {}, "timeout": {}, "retry": {}, "dead_letter": {}},
	"error_class":  {"": {}, "error": {}, "canceled": {}, "timeout": {}, "invalid": {}, "unauthenticated": {}, "not_ready": {}, "rate_limited": {}, "duplicate": {}, "unavailable": {}, "storage": {}, "model": {}, "tool": {}},
	"provider":     {"openai": {}, "postgres": {}, "inmemory": {}, "other": {}},
	"channel":      {"api": {}, "telegram": {}, "wecom": {}, "wecom_aibot": {}, "outbox": {}, "other": {}},
	"model_family": {"gpt": {}, "claude": {}, "gemini": {}, "other": {}},
}
var allowedOperations = map[string]struct{}{
	OperationHTTPRequest: {}, OperationGatewayDispatch: {}, OperationRunnerExecution: {},
	OperationModelCall: {}, OperationToolCall: {}, OperationStorageOperation: {},
	OperationChannelReceive: {}, OperationChannelSend: {},
}

func sanitizeAttributes(attrs []Attribute) []Attribute {
	out := make([]Attribute, 0, len(attrs))
	for _, attr := range attrs {
		if _, ok := allowedAttributeKeys[attr.Key]; !ok {
			continue
		}
		if attr.Key == "operation" {
			if _, ok := allowedOperations[attr.Value]; !ok {
				continue
			}
		} else if values, ok := allowedAttributeValues[attr.Key]; ok {
			if _, allowed := values[attr.Value]; !allowed {
				continue
			}
		} else if attr.Key == "tenant_hash" || attr.Key == "app_hash" {
			if len(attr.Value) != 16 || strings.Trim(attr.Value, "0123456789abcdef") != "" {
				continue
			}
		} else if attr.Key == "currency" {
			if len(attr.Value) != 3 || attr.Value != strings.ToLower(attr.Value) || attr.Value == "xxx" || strings.Trim(attr.Value, "abcdefghijklmnopqrstuvwxyz") != "" {
				continue
			}
		} else if len(attr.Value) > 64 || strings.ContainsAny(attr.Value, "\r\n") || attributeHighCardinalityPattern.MatchString(attr.Value) {
			continue
		}
		out = append(out, Attribute{Key: attr.Key, Value: RedactString(attr.Value)})
	}
	return out
}

// RedactString removes credentials and connection secrets from text.
func RedactString(value string) string {
	value = bearerPattern.ReplaceAllString(value, "Bearer <redacted>")
	value = dsnPattern.ReplaceAllString(value, `${1}<redacted>@`)
	value = sensitivePattern.ReplaceAllString(value, `${1}<redacted>`)
	return value
}

// RedactFields copies fields while masking sensitive keys and values.
func RedactFields(fields map[string]string) map[string]string {
	out := make(map[string]string, len(fields))
	for key, value := range fields {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "password") || (strings.Contains(lower, "api") && strings.Contains(lower, "key")) || lower == "authorization" || strings.Contains(lower, "message") || strings.Contains(lower, "raw") {
			out[key] = "<redacted>"
		} else {
			out[key] = RedactString(value)
		}
	}
	return out
}

type contextKey string

const (
	requestIDKey contextKey = "trpcservice.observability.request_id"
	traceIDKey   contextKey = "trpcservice.observability.trace_id"
)

// WithCorrelation adds request and trace identifiers to ctx.
func WithCorrelation(ctx context.Context, requestID, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	return context.WithValue(ctx, traceIDKey, traceID)
}

// RequestID returns the request identifier stored in ctx.
func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

// TraceID returns the trace identifier stored in ctx.
func TraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(traceIDKey).(string)
	return value
}

// TraceParentFromContext returns a valid W3C traceparent carrier value, or an
// empty string when ctx has no valid remote/local span context.
func TraceParentFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	value := carrier.Get("traceparent")
	if !validTraceParent(value) {
		return ""
	}
	return value
}

// ContextWithTraceParent restores a valid W3C traceparent without retaining
// baggage. Invalid values leave ctx unchanged.
func ContextWithTraceParent(ctx context.Context, value string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if !validTraceParent(value) {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": value})
}

// NormalizeTraceParent returns a canonical valid W3C traceparent carrier, or
// an empty string when value is missing or malformed.
func NormalizeTraceParent(value string) string {
	return TraceParentFromContext(ContextWithTraceParent(context.Background(), value))
}

// ContextWithoutTraceParent preserves cancellation and values while removing
// any ambient span, so a subsequent operation starts a new root trace.
func ContextWithoutTraceParent(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}

func validTraceParent(value string) bool {
	if value == "" {
		return false
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": value})
	return trace.SpanContextFromContext(ctx).IsValid()
}

// ErrorClass maps an error to a stable telemetry class.
func ErrorClass(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "error"
}

// DurationMilliseconds returns elapsed time since start in milliseconds.
func DurationMilliseconds(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}

// StartOperation is the common hook used by Model, Tool, Storage and Channel
// adapters. The returned finish function records a stable status/error class
// and always ends the span; it never exposes provider error text.
func StartOperation(ctx context.Context, provider Provider, operation, component string) (context.Context, Span, func(error)) {
	if ctx == nil {
		ctx = context.Background()
	}
	if provider == nil {
		provider = NewNoopProvider()
	}
	started, span := provider.Tracer("trpcservice.operations").Start(ctx, operation, Attribute{Key: "component", Value: component}, Attribute{Key: "operation", Value: operation})
	var once sync.Once
	finish := func(err error) {
		once.Do(func() {
			if err != nil {
				class := ErrorClass(err)
				span.SetAttributes(Attribute{Key: "error_class", Value: class})
				span.SetStatus(StatusError, class)
				span.RecordError(err)
			} else {
				span.SetStatus(StatusOK, "")
			}
			span.End()
		})
	}
	return started, span, finish
}
