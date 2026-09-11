// Package telemetry owns Worker process observation, not execution facts. Its
// bounded logger and background OTLP reader never decide whether a Run commits.
package telemetry

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
)

// Export is optional. An absent endpoint makes no outbound telemetry request;
// JSON phase logs remain available without deploying a separate metrics stack.
type Export struct {
	Endpoint          string
	Interval, Timeout time.Duration
}

func (e Export) Validate() error {
	if e.Endpoint == "" {
		if e.Interval != 0 || e.Timeout != 0 {
			return errors.New("telemetry endpoint required for export timing")
		}
		return nil
	}
	u, err := url.Parse(e.Endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(e.Endpoint, "#") || u.RawPath != "" || u.Fragment != "" || u.Path != "/v1/metrics" || (u.Scheme != "https" && u.Scheme != "http") || e.Interval < time.Millisecond || e.Timeout < time.Millisecond {
		return errors.New("invalid explicit OTLP metrics export")
	}
	if u.Scheme == "http" {
		ip := net.ParseIP(u.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return errors.New("remote OTLP metrics endpoint requires HTTPS")
		}
	}
	return nil
}

type logRecord struct {
	sdkLevel        string
	traceID, spanID string
	operation       application.Observation
	storage         application.StorageObservation
	isStorage       bool
}

type Recorder struct {
	provider      *sdkmetric.MeterProvider
	operations    metric.Int64Counter
	durations     metric.Float64Histogram
	tokens        metric.Int64Counter
	backlog       metric.Int64Gauge
	age           metric.Float64Gauge
	dropped       metric.Int64Counter
	sampleSuccess metric.Int64Gauge
	queue         chan logRecord
	stop, done    chan struct{}
	closed        atomic.Bool
	shutdown      sync.Once
	logger        *slog.Logger
}

func New(ctx context.Context, worker string, out io.Writer, export Export, resources ...*resource.Resource) (*Recorder, error) {
	if len(resources) > 1 {
		return nil, errors.New("one process telemetry resource required")
	}
	if out == nil || worker == "" {
		return nil, errors.New("telemetry requires a worker identity and log writer")
	}
	if err := export.Validate(); err != nil {
		return nil, err
	}
	var readers []sdkmetric.Reader
	if export.Endpoint != "" {
		exporter, err := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(export.Endpoint), otlpmetrichttp.WithTimeout(export.Timeout), otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}), otlpmetrichttp.WithHeaders(map[string]string{}), otlpmetrichttp.WithTLSClientConfig(&tls.Config{MinVersion: tls.VersionTLS13}))
		if err != nil {
			return nil, errors.New("initialize Worker OTLP metrics exporter")
		}
		readers = append(readers, sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(export.Interval), sdkmetric.WithTimeout(export.Timeout)))
	}
	var res *resource.Resource
	if len(resources) == 1 {
		res = resources[0]
	}
	return newRecorderWithResource(worker, out, res, readers...)
}
func newRecorder(worker string, out io.Writer, readers ...sdkmetric.Reader) (*Recorder, error) {
	return newRecorderWithResource(worker, out, nil, readers...)
}
func newRecorderWithResource(worker string, out io.Writer, res *resource.Resource, readers ...sdkmetric.Reader) (*Recorder, error) {
	if res == nil {
		res = resource.NewSchemaless(attribute.String("service.name", "agent-worker"), attribute.String("service.version", "worker-v1"), attribute.String("service.instance.id", worker), attribute.String("service.namespace", "agent-platform"), attribute.String("deployment.environment.name", "unspecified"))
	}
	options := []sdkmetric.Option{sdkmetric.WithResource(res)}
	for _, reader := range readers {
		options = append(options, sdkmetric.WithReader(reader))
	}
	p := sdkmetric.NewMeterProvider(options...)
	r := &Recorder{provider: p, queue: make(chan logRecord, 256), stop: make(chan struct{}), done: make(chan struct{}), logger: slog.New(slog.NewJSONHandler(out, nil)).With("service", "agent-worker", "worker_id", boundedID(worker))}
	meter := p.Meter("agent-worker/execution-v1")
	var err error
	r.operations, err = meter.Int64Counter("worker.operation.count")
	if err != nil {
		return nil, err
	}
	r.durations, err = meter.Float64Histogram("worker.operation.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	r.tokens, err = meter.Int64Counter("worker.provider.tokens", metric.WithUnit("{token}"))
	if err != nil {
		return nil, err
	}
	r.backlog, err = meter.Int64Gauge("worker.backlog", metric.WithUnit("{item}"))
	if err != nil {
		return nil, err
	}
	r.age, err = meter.Float64Gauge("worker.oldest.age", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	r.dropped, err = meter.Int64Counter("worker.observation.dropped")
	if err != nil {
		return nil, err
	}
	r.sampleSuccess, err = meter.Int64Gauge("worker.storage.sample.success")
	if err != nil {
		return nil, err
	}
	go r.writeLogs()
	return r, nil
}
func boundedID(v string) string {
	if len(v) > 256 {
		return "invalid_identity"
	}
	return v
}
func operation(v string) string {
	switch v {
	case "memory_apply", "memory_finalize", "terminalize", "intake", "advance", "manifest", "claim", "prepare", "credential_resolve", "session_open", "session_load", "execute", "usage", "session_stage", "complete", "renew", "fence", "manifest_apply", "wire_reject", "reply_publish", "drain", "storage_sample", "startup":
		return v
	default:
		return "other"
	}
}
func outcome(v string) string {
	switch v {
	case "memory_apply_failed", "memory_finalize_failed", "ok", "fenced", "conflict", "capacity", "manifest_wait", "manifest_contract_wait", "session_wait", "invalid", "session_preparation", "session_invalid", "credential_denied", "cancelled", "deadline", "dependency", "failed":
		return v
	default:
		return "other"
	}
}
func stage(v string) string {
	switch v {
	case "", "parse_config", "connect", "target_query", "namespace_query", "table_query", "candidate_acl":
		return v
	default:
		return "other"
	}
}
func (r *Recorder) Observe(ctx context.Context, o application.Observation) {
	if r == nil || r.closed.Load() {
		return
	}
	o.Operation, o.Result, o.Stage = operation(o.Operation), outcome(o.Result), stage(o.Stage)
	labels := metric.WithAttributes(attribute.String("operation", o.Operation), attribute.String("result", o.Result))
	r.operations.Add(ctx, 1, labels)
	if o.Duration > 0 {
		r.durations.Record(ctx, o.Duration.Seconds(), labels)
	}
	if o.Operation == "usage" && o.Result == "ok" {
		for _, v := range []struct {
			kind string
			n    int64
		}{{"input", o.InputTokens}, {"output", o.OutputTokens}, {"total", o.TotalTokens}} {
			if v.n >= 0 {
				r.tokens.Add(ctx, v.n, metric.WithAttributes(attribute.String("kind", v.kind)))
			}
		}
	}
	// Routine polling/lease checks still have metrics without flooding logs.
	if o.Operation == "advance" || o.Result == "manifest_wait" || o.Result == "manifest_contract_wait" || o.Result == "session_wait" || ((o.Operation == "fence" || o.Operation == "renew" || o.Operation == "manifest") && o.Result == "ok") {
		return
	}
	o.TenantID, o.RunID, o.AttemptID = boundedID(o.TenantID), boundedID(o.RunID), boundedID(o.AttemptID)
	select {
	case r.queue <- operationRecord(ctx, o):
	default:
		r.dropped.Add(ctx, 1)
	}
}
func (r *Recorder) writeLogs() {
	defer close(r.done)
	write := func(record logRecord) {
		if record.sdkLevel != "" {
			level := slog.LevelInfo
			switch record.sdkLevel {
			case "warn":
				level = slog.LevelWarn
			case "error", "fatal":
				level = slog.LevelError
			}
			r.logger.Log(context.Background(), level, "worker.sdk", "event", "sdk_log", "sdk_level", record.sdkLevel)
			return
		}

		if record.isStorage {
			s := record.storage
			r.logger.Info("worker.backlog", "queued", s.Queued, "running", s.Running, "retry_wait", s.RetryWait, "reply_pending", s.ReplyPending, "active_local", s.ActiveLocal, "oldest_run_seconds", s.OldestRunSeconds, "oldest_reply_seconds", s.OldestReplySeconds)
			return
		}
		o := record.operation
		logger := r.logger
		if record.traceID != "" {
			logger = logger.With("trace_id", record.traceID, "span_id", record.spanID)
		}
		logger.Info("worker.operation", "operation", o.Operation, "result", o.Result, "stage", o.Stage, "tenant_id", o.TenantID, "run_id", o.RunID, "attempt_id", o.AttemptID, "duration_seconds", o.Duration.Seconds(), "input_tokens", o.InputTokens, "output_tokens", o.OutputTokens, "total_tokens", o.TotalTokens)
	}
	for {
		select {
		case o := <-r.queue:
			write(o)
		case <-r.stop:
			for {
				select {
				case o := <-r.queue:
					write(o)
				default:
					return
				}
			}
		}
	}
}

// Sample records database-wide gauges: use max, not sum, across replicas sharing
// the Execution database. The caller omits stale values if sampling fails.
func (r *Recorder) Sample(ctx context.Context, s application.StorageObservation) {
	if r == nil || r.closed.Load() {
		return
	}
	for _, v := range []struct {
		kind string
		n    int64
	}{{"queued", s.Queued}, {"running", s.Running}, {"retry_wait", s.RetryWait}, {"reply_pending", s.ReplyPending}, {"active_local", s.ActiveLocal}} {
		r.backlog.Record(ctx, v.n, metric.WithAttributes(attribute.String("kind", v.kind)))
	}
	r.age.Record(ctx, s.OldestRunSeconds, metric.WithAttributes(attribute.String("kind", "unsettled_run")))
	r.age.Record(ctx, s.OldestReplySeconds, metric.WithAttributes(attribute.String("kind", "unpublished_reply")))
	select {
	case r.queue <- logRecord{storage: s, isStorage: true}:
	default:
		r.dropped.Add(ctx, 1)
	}
}
func (r *Recorder) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.shutdown.Do(func() { r.closed.Store(true); close(r.stop) })
	err := r.provider.Shutdown(ctx)
	select {
	case <-r.done:
		return err
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}

func (r *Recorder) SampleStatus(ctx context.Context, ok bool) {
	if r == nil || r.closed.Load() {
		return
	}
	var value int64
	if ok {
		value = 1
	}
	r.sampleSuccess.Record(ctx, value)
	if !ok {
		r.Observe(ctx, application.Observation{Operation: "storage_sample", Result: "dependency"})
	}
}

// Capture before asynchronous logging; the writer goroutine has no run context.
func operationRecord(ctx context.Context, o application.Observation) logRecord {
	record := logRecord{operation: o}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		record.traceID = sc.TraceID().String()
		record.spanID = sc.SpanID().String()
	}
	return record
}

// SDKLog deliberately accepts no free text. SDK's default context helpers do
// not preserve their ctx, so these process-level records do not invent a Run ID.
func (r *Recorder) SDKLog(ctx context.Context, level string) {
	if r == nil || r.closed.Load() {
		return
	}
	switch level {
	case "info", "warn", "error", "fatal":
	default:
		return
	}
	select {
	case r.queue <- logRecord{sdkLevel: level}:
	default:
		r.dropped.Add(ctx, 1)
	}
}
