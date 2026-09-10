// Package metrics exposes tenant-aware OpenTelemetry metrics.
package metrics

import (
	"context"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type ExecutionAttributes struct {
	TenantID, AppCode, Channel string
	ProviderRequestID          string
	ConfigVersion              uint64
}

type StoreAttributes struct {
	TenantID, Backend, Operation string
}

type SenderAttributes struct {
	TenantID, Channel, BindingID string
}

// ModelUsageAttributes contains provider-reported totals from one Runtime call.
type ModelUsageAttributes struct {
	TenantID, AppCode, ProviderID, ModelName                        string
	PromptTokens, CachedPromptTokens, CompletionTokens, TotalTokens int
	CostMicros                                                      int64
}

// ModelUsageObserver is optional so deterministic test observers need not
// implement model accounting.
type ModelUsageObserver interface {
	RecordModelUsage(context.Context, ModelUsageAttributes)
}
type Observer interface {
	StartExecution(context.Context, ExecutionAttributes) (context.Context, func(error))
}

type StoreObserver interface {
	StartStore(context.Context, StoreAttributes) (context.Context, func(error))
}

type SenderObserver interface {
	StartSender(context.Context, SenderAttributes) (context.Context, func(error))
}

type HTTPObserver interface {
	WrapHTTP(http.Handler) http.Handler
}

type OTelObserver struct {
	tracer trace.Tracer

	total, failures metric.Int64Counter
	duration        metric.Float64Histogram

	httpTotal, httpFailures metric.Int64Counter
	httpDuration            metric.Float64Histogram

	storeTotal, storeFailures metric.Int64Counter
	storeDuration             metric.Float64Histogram

	senderTotal, senderFailures                                                                          metric.Int64Counter
	senderDuration                                                                                       metric.Float64Histogram
	modelPromptTokens, modelCachedPromptTokens, modelCompletionTokens, modelTotalTokens, modelCostMicros metric.Int64Counter
}

func NewOTelObserver(tracer trace.Tracer, meter metric.Meter) (*OTelObserver, error) {
	total, err := meter.Int64Counter("agent.execution.total")
	if err != nil {
		return nil, err
	}
	failures, err := meter.Int64Counter("agent.execution.failures")
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("agent.execution.duration.ms")
	if err != nil {
		return nil, err
	}
	httpTotal, err := meter.Int64Counter("http.server.requests")
	if err != nil {
		return nil, err
	}
	httpFailures, err := meter.Int64Counter("http.server.failures")
	if err != nil {
		return nil, err
	}
	httpDuration, err := meter.Float64Histogram("http.server.duration.ms")
	if err != nil {
		return nil, err
	}
	storeTotal, err := meter.Int64Counter("platform.store.operations")
	if err != nil {
		return nil, err
	}
	storeFailures, err := meter.Int64Counter("platform.store.failures")
	if err != nil {
		return nil, err
	}
	storeDuration, err := meter.Float64Histogram("platform.store.duration.ms")
	if err != nil {
		return nil, err
	}
	senderTotal, err := meter.Int64Counter("channel.delivery.attempts")
	if err != nil {
		return nil, err
	}
	senderFailures, err := meter.Int64Counter("channel.delivery.failures")
	if err != nil {
		return nil, err
	}
	senderDuration, err := meter.Float64Histogram("channel.delivery.duration.ms")
	if err != nil {
		return nil, err
	}
	modelPromptTokens, err := meter.Int64Counter("model.tokens.prompt")
	if err != nil {
		return nil, err
	}
	modelCachedPromptTokens, err := meter.Int64Counter("model.tokens.prompt.cached")
	if err != nil {
		return nil, err
	}
	modelCompletionTokens, err := meter.Int64Counter("model.tokens.completion")
	if err != nil {
		return nil, err
	}
	modelTotalTokens, err := meter.Int64Counter("model.tokens.total")
	if err != nil {
		return nil, err
	}
	modelCostMicros, err := meter.Int64Counter("model.cost.microunits")
	if err != nil {
		return nil, err
	}
	return &OTelObserver{
		tracer: tracer, total: total, failures: failures, duration: duration,
		httpTotal: httpTotal, httpFailures: httpFailures, httpDuration: httpDuration,
		storeTotal: storeTotal, storeFailures: storeFailures, storeDuration: storeDuration,
		senderTotal: senderTotal, senderFailures: senderFailures, senderDuration: senderDuration,
		modelPromptTokens: modelPromptTokens, modelCachedPromptTokens: modelCachedPromptTokens,
		modelCompletionTokens: modelCompletionTokens, modelTotalTokens: modelTotalTokens, modelCostMicros: modelCostMicros,
	}, nil
}

// RecordModelUsage records real provider-reported token usage. No prompt,
// completion, user or request identifiers are exported as metric labels.
func (o *OTelObserver) RecordModelUsage(ctx context.Context, usage ModelUsageAttributes) {
	attributes := []attribute.KeyValue{
		attribute.String("tenant.id", usage.TenantID),
		attribute.String("app.code", usage.AppCode),
		attribute.String("model.provider_id", usage.ProviderID),
		attribute.String("model.name", usage.ModelName),
	}
	o.modelPromptTokens.Add(ctx, int64(usage.PromptTokens), metric.WithAttributes(attributes...))
	o.modelCachedPromptTokens.Add(ctx, int64(usage.CachedPromptTokens), metric.WithAttributes(attributes...))
	o.modelCompletionTokens.Add(ctx, int64(usage.CompletionTokens), metric.WithAttributes(attributes...))
	o.modelTotalTokens.Add(ctx, int64(usage.TotalTokens), metric.WithAttributes(attributes...))
	o.modelCostMicros.Add(ctx, usage.CostMicros, metric.WithAttributes(attributes...))
}

func (o *OTelObserver) StartExecution(ctx context.Context, execution ExecutionAttributes) (context.Context, func(error)) {
	metricAttributes := []attribute.KeyValue{
		attribute.String("tenant.id", execution.TenantID),
		attribute.String("app.code", execution.AppCode),
		attribute.String("channel", execution.Channel),
		attribute.Int64("config.version", int64(execution.ConfigVersion)),
	}
	spanAttributes := append([]attribute.KeyValue(nil), metricAttributes...)
	if requestID := strings.TrimSpace(execution.ProviderRequestID); requestID != "" {
		spanAttributes = append(spanAttributes, attribute.String("external.request.id", requestID))
	}
	ctx, span := o.tracer.Start(ctx, "agent.runtime.handle", trace.WithAttributes(spanAttributes...))
	started := time.Now()
	o.total.Add(ctx, 1, metric.WithAttributes(metricAttributes...))
	return ctx, func(err error) {
		o.duration.Record(ctx, float64(time.Since(started).Microseconds())/1000, metric.WithAttributes(metricAttributes...))
		if err != nil {
			o.failures.Add(ctx, 1, metric.WithAttributes(metricAttributes...))
			span.RecordError(err)
			span.SetStatus(codes.Error, "execution failed")
		}
		span.End()
	}
}

// StartStore starts a child operation without storing query text, identifiers,
// message content, credentials, or payloads in telemetry.
func (o *OTelObserver) StartStore(ctx context.Context, operation StoreAttributes) (context.Context, func(error)) {
	attributes := []attribute.KeyValue{
		attribute.String("tenant.id", operation.TenantID),
		attribute.String("db.system", operation.Backend),
		attribute.String("db.operation.name", operation.Operation),
	}
	return o.startOperation(ctx, "platform.store.operation", attributes, o.storeTotal, o.storeFailures, o.storeDuration, "store operation failed")
}

// StartSender records the external side effect at the durable Outbox boundary.
// Recipient, message body, provider receipt, and idempotency key are excluded.
func (o *OTelObserver) StartSender(ctx context.Context, delivery SenderAttributes) (context.Context, func(error)) {
	attributes := []attribute.KeyValue{
		attribute.String("tenant.id", delivery.TenantID),
		attribute.String("channel", delivery.Channel),
		attribute.String("channel.binding_id", delivery.BindingID),
	}
	return o.startOperation(ctx, "channel.sender.send", attributes, o.senderTotal, o.senderFailures, o.senderDuration, "channel delivery failed")
}

func (o *OTelObserver) startOperation(ctx context.Context, name string, attributes []attribute.KeyValue, total, failures metric.Int64Counter, duration metric.Float64Histogram, failureMessage string) (context.Context, func(error)) {
	ctx, span := o.tracer.Start(ctx, name, trace.WithAttributes(attributes...))
	started := time.Now()
	total.Add(ctx, 1, metric.WithAttributes(attributes...))
	return ctx, func(err error) {
		duration.Record(ctx, float64(time.Since(started).Microseconds())/1000, metric.WithAttributes(attributes...))
		if err != nil {
			failures.Add(ctx, 1, metric.WithAttributes(attributes...))
			span.RecordError(err)
			span.SetStatus(codes.Error, failureMessage)
		}
		span.End()
	}
}

// WrapHTTP extracts W3C context and emits low-cardinality route telemetry. It
// intentionally never exports paths containing identifiers, request headers,
// query strings, bodies, cookies, or user identity.
func (o *OTelObserver) WrapHTTP(next http.Handler) http.Handler {
	if next == nil {
		return nil
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(request.Context(), propagation.HeaderCarrier(request.Header))
		baseAttributes := []attribute.KeyValue{
			attribute.String("http.request.method", request.Method),
			attribute.String("http.route", normalizeHTTPRoute(request.URL.Path)),
		}
		spanAttributes := append([]attribute.KeyValue(nil), baseAttributes...)
		if requestID := externalRequestID(request.Header); requestID != "" {
			spanAttributes = append(spanAttributes, attribute.String("external.request.id", requestID))
		}
		ctx, span := o.tracer.Start(ctx, "http.server.request", trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(spanAttributes...))
		started := time.Now()
		recorder := &responseRecorder{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(recorder, request.WithContext(ctx))
		attributes := append(baseAttributes, attribute.Int("http.response.status_code", recorder.status))
		o.httpTotal.Add(ctx, 1, metric.WithAttributes(attributes...))
		o.httpDuration.Record(ctx, float64(time.Since(started).Microseconds())/1000, metric.WithAttributes(attributes...))
		span.SetAttributes(attribute.Int("http.response.status_code", recorder.status))
		if recorder.status >= http.StatusInternalServerError {
			o.httpFailures.Add(ctx, 1, metric.WithAttributes(attributes...))
			span.SetStatus(codes.Error, "HTTP server error")
		}
		span.End()
	})
}

func externalRequestID(header http.Header) string {
	for _, name := range []string{"X-Request-ID", "X-WeCom-Trace", "X-Tt-Logid"} {
		if value := strings.TrimSpace(header.Get(name)); value != "" && len(value) <= 256 && strings.IndexFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) < 0 {
			return value
		}
	}
	return ""
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (w *responseRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseRecorder) Write(body []byte) (int, error) {
	return w.ResponseWriter.Write(body)
}

func (w *responseRecorder) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func normalizeHTTPRoute(path string) string {
	switch {
	case path == "/healthz", path == "/readyz":
		return path
	case strings.HasPrefix(path, "/api/v1/auth/"):
		return "/api/v1/auth/{operation}"
	case strings.HasPrefix(path, "/api/v1/"):
		return "/api/v1/{resource}"
	case strings.HasPrefix(path, "/console/"):
		return "/console/{asset}"
	default:
		return "other"
	}
}

var _ Observer = (*OTelObserver)(nil)
var _ StoreObserver = (*OTelObserver)(nil)
var _ SenderObserver = (*OTelObserver)(nil)
var _ HTTPObserver = (*OTelObserver)(nil)
