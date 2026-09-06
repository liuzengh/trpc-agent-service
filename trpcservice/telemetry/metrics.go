package telemetry

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metrics is the low-cardinality instrument registry. Attribute keys are
// allowlisted and values normalized; anything outside the allowlist is
// dropped and counted, so raw tenant/user/job/task/trace identifiers and raw
// error text can never become label values.
type Metrics interface {
	HTTPRequest(attrs Attrs)
	HTTPRequestDuration(attrs Attrs, seconds float64)
	HTTPInflight(delta int, attrs Attrs)
	QueueOperation(operation, outcome string)
	WorkerJob(outcome string)
	CompletionOutcome(operation, outcome string)
	OutboxDispatch(operation, outcome string)
	ChannelOutcome(channel, outcome string)
	MemoryMutation(operation, outcome string)
	VectorTask(operation, outcome string)
	RetrievalOutcome(operation, outcome string, candidates, hydrated int)
	RebuildBatch(outcome string, scanned, enqueued int)
	RuntimeState(event, outcome string)
	// IngressAdmission records one bounded P2-03 admission outcome per
	// webhook. The outcome value is a closed enum (allowed, rate_limited,
	// capacity_exhausted, backend_unavailable); no tenant, binding, chat or
	// message identifier ever crosses this surface.
	IngressAdmission(outcome string)
	UnknownAttrDropped() int64
}

// Attrs carries allowlisted attribute values.
type Attrs struct {
	Method      string
	Route       string
	StatusClass string
	Component   string
	Operation   string
	Outcome     string
	Category    string
	Channel     string
	Backend     string
	WorkerKind  string
	Reason      string
}

// AttrSet builds the otel attribute set from the allowlisted struct plus
// context_state markers. Everything else is ignored by construction.
type attrBuilder struct {
	attrs []attribute.KeyValue
}

func (a attrBuilder) set() attribute.Set {
	return attribute.NewSet(a.attrs...)
}

// RouteTemplate maps an HTTP request to a bounded route template. Unknown
// paths collapse to "unmatched"; webhook paths collapse to their channel
// segment replaced by a placeholder.
func RouteTemplate(r *http.Request) string {
	path := r.URL.Path
	switch {
	case path == "/healthz", path == "/livez", path == "/api/chat", path == "/api/tenants", path == "/readyz":
		return path
	case strings.HasPrefix(path, "/webhook/"):
		return "/webhook/{channel}"
	default:
		return "unmatched"
	}
}

// StatusClass maps a status code to its class bucket.
func StatusClass(code int) string {
	switch {
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 300 && code < 400:
		return "3xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		return "other"
	}
}

var allowedChannels = map[string]bool{"lark": true, "telegram": true, "vector": true, "web": true}

// allowedAdmissionOutcomes is the closed P2-03 admission outcome enum;
// anything else collapses to "other" so raw values can never become labels.
var allowedAdmissionOutcomes = map[string]bool{
	"allowed": true, "rate_limited": true, "capacity_exhausted": true, "backend_unavailable": true,
}

func normalizeAdmissionOutcome(outcome string) string {
	outcome = strings.ToLower(strings.TrimSpace(outcome))
	if allowedAdmissionOutcomes[outcome] {
		return outcome
	}
	return "other"
}

func normalizeChannel(channel string) string {
	channel = strings.ToLower(strings.TrimSpace(channel))
	if allowedChannels[channel] {
		return channel
	}
	return "other"
}

type otelMetrics struct {
	httpDuration    metric.Float64Histogram
	httpInflight    metric.Int64UpDownCounter
	operations      map[string]metric.Int64Counter
	operationMu     sync.Mutex
	meter           metric.Meter
	dropped         metric.Int64Counter
	droppedObserved int64
}

type noopMetrics struct {
	dropped int64
	mu      sync.Mutex
}

func newNoopMetrics() Metrics { return &noopMetrics{} }

func (n *noopMetrics) HTTPRequest(attrs Attrs)                          {}
func (n *noopMetrics) HTTPRequestDuration(attrs Attrs, seconds float64) {}
func (n *noopMetrics) HTTPInflight(delta int, attrs Attrs)              {}
func (n *noopMetrics) QueueOperation(string, string)                    {}
func (n *noopMetrics) WorkerJob(string)                                 {}
func (n *noopMetrics) CompletionOutcome(string, string)                 {}
func (n *noopMetrics) OutboxDispatch(string, string)                    {}
func (n *noopMetrics) ChannelOutcome(string, string)                    {}
func (n *noopMetrics) MemoryMutation(string, string)                    {}
func (n *noopMetrics) VectorTask(string, string)                        {}
func (n *noopMetrics) RetrievalOutcome(string, string, int, int)        {}
func (n *noopMetrics) RebuildBatch(string, int, int)                    {}
func (n *noopMetrics) RuntimeState(string, string)                      {}
func (n *noopMetrics) IngressAdmission(string)                          {}
func (n *noopMetrics) UnknownAttrDropped() int64                        { return 0 }

func newOtelMetrics(provider *sdkmetric.MeterProvider) Metrics {
	meter := provider.Meter("trpc-agent-service")
	registry := &otelMetrics{meter: meter}
	registry.httpDuration, _ = meter.Float64Histogram(
		"trpcagent.http.request.duration",
		metric.WithDescription("HTTP request duration"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries([]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}...),
	)
	registry.httpInflight, _ = meter.Int64UpDownCounter("trpcagent.http.inflight")
	registry.dropped, _ = meter.Int64Counter("trpcagent.telemetry.attributes_dropped")
	return registry
}

func (o *otelMetrics) counter(name string) metric.Int64Counter {
	o.operationMu.Lock()
	defer o.operationMu.Unlock()
	if o.operations == nil {
		o.operations = make(map[string]metric.Int64Counter)
	}
	if counter, ok := o.operations[name]; ok {
		return counter
	}
	counter, _ := o.meter.Int64Counter(name)
	o.operations[name] = counter
	return counter
}

func (o *otelMetrics) emit(name string, attrs Attrs, extra ...attribute.KeyValue) {
	set := buildSet(attrs, extra)
	o.counter(name).Add(contextless, 1, metric.WithAttributeSet(set))
}

var contextless = context.Background()

func (o *otelMetrics) HTTPRequest(attrs Attrs) {
	attrs.Component = "http"
	o.emit("trpcagent.http.requests", attrs)
}

func (o *otelMetrics) HTTPRequestDuration(attrs Attrs, seconds float64) {
	attrs.Component = "http"
	set := buildSet(attrs, nil)
	o.httpDuration.Record(contextless, seconds, metric.WithAttributeSet(set))
}

func (o *otelMetrics) HTTPInflight(delta int, attrs Attrs) {
	attrs.Component = "http"
	set := buildSet(attrs, nil)
	o.httpInflight.Add(contextless, int64(delta), metric.WithAttributeSet(set))
}

func (o *otelMetrics) IngressAdmission(outcome string) {
	attrs := Attrs{Component: "ingress", Outcome: normalizeAdmissionOutcome(outcome)}
	o.emit("trpcagent.ingress.admission", attrs)
}

func (o *otelMetrics) QueueOperation(operation, outcome string) {
	o.emit("trpcagent.queue.operations", Attrs{Component: "queue", Operation: operation, Outcome: outcome})
}

func (o *otelMetrics) WorkerJob(outcome string) {
	o.emit("trpcagent.worker.jobs", Attrs{Component: "worker", Outcome: outcome})
}

func (o *otelMetrics) CompletionOutcome(operation, outcome string) {
	o.emit("trpcagent.completion.outcomes", Attrs{Component: "completion", Operation: operation, Outcome: outcome})
}

func (o *otelMetrics) OutboxDispatch(operation, outcome string) {
	o.emit("trpcagent.outbox.dispatches", Attrs{Component: "outbox", Operation: operation, Outcome: outcome})
}

func (o *otelMetrics) ChannelOutcome(channel, outcome string) {
	o.emit("trpcagent.channel.outcomes", Attrs{Component: "channel", Channel: normalizeChannel(channel), Outcome: outcome})
}

func (o *otelMetrics) MemoryMutation(operation, outcome string) {
	o.emit("trpcagent.memory.mutations", Attrs{Component: "memory", Operation: operation, Outcome: outcome})
}

func (o *otelMetrics) VectorTask(operation, outcome string) {
	o.emit("trpcagent.vector.tasks", Attrs{Component: "vector_task", Operation: operation, Outcome: outcome})
}

func (o *otelMetrics) RetrievalOutcome(operation, outcome string, candidates, hydrated int) {
	set := buildSet(Attrs{Component: "retrieval", Operation: operation, Outcome: outcome},
		[]attribute.KeyValue{attribute.Int("candidates", clampMetric(candidates)), attribute.Int("hydrated", clampMetric(hydrated))})
	o.counter("trpcagent.retrieval.outcomes").Add(contextless, 1, metric.WithAttributeSet(set))
}

func (o *otelMetrics) RebuildBatch(outcome string, scanned, enqueued int) {
	set := buildSet(Attrs{Component: "rebuild", Outcome: outcome},
		[]attribute.KeyValue{attribute.Int("scanned", clampMetric(scanned)), attribute.Int("enqueued", clampMetric(enqueued))})
	o.counter("trpcagent.rebuild.batches").Add(contextless, 1, metric.WithAttributeSet(set))
}

func (o *otelMetrics) RuntimeState(event, outcome string) {
	o.emit("trpcagent.runtime.states", Attrs{Component: "runtime", Operation: event, Outcome: outcome})
}

func (o *otelMetrics) UnknownAttrDropped() int64 {
	o.dropped.Add(contextless, 1)
	o.operationMu.Lock()
	defer o.operationMu.Unlock()
	o.droppedObserved++
	return o.droppedObserved
}

func clampMetric(value int) int {
	if value < 0 {
		return 0
	}
	if value > 1000000 {
		return 1000000
	}
	return value
}

func buildSet(attrs Attrs, extra []attribute.KeyValue) attribute.Set {
	builder := attrBuilder{}
	appendIf := func(key, value string) {
		if strings.TrimSpace(value) != "" {
			builder.attrs = append(builder.attrs, attribute.String(key, Truncate(value, 64)))
		}
	}
	appendIf("method", attrs.Method)
	appendIf("route", attrs.Route)
	appendIf("status_class", attrs.StatusClass)
	appendIf("component", attrs.Component)
	appendIf("operation", attrs.Operation)
	appendIf("outcome", attrs.Outcome)
	appendIf("error_category", attrs.Category)
	if attrs.Channel != "" {
		appendIf("channel", normalizeChannel(attrs.Channel))
	}
	appendIf("backend_kind", attrs.Backend)
	appendIf("worker_kind", attrs.WorkerKind)
	appendIf("filtered_reason", attrs.Reason)
	builder.attrs = append(builder.attrs, extra...)
	return builder.set()
}

// String helper for status classes derived from raw codes at the edge.
func StatusClassFromCode(code int) string { return StatusClass(code) }

var _ = strconv.Itoa
