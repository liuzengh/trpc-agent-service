// Package metrics exposes tenant-aware OpenTelemetry metrics.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
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

type InboundAttributes struct {
	TenantID, AppCode, Channel string
}

type KafkaCapacityAttributes struct {
	Topic, ConsumerGroup string
	Lag                  int64
	Partitions           int
}

type OutboxBacklogAttributes struct {
	TenantID, Channel string
	Pending           int64
	OldestAge         time.Duration
}

// ModelUsageAttributes contains provider-reported totals from one Runtime call.
type ModelUsageAttributes struct {
	TenantID, AppCode, ProviderID, ModelName                        string
	PromptTokens, CachedPromptTokens, CompletionTokens, TotalTokens int
	CostMicros                                                      int64
}

type DeadLetterAttributes struct {
	TenantID, ErrorClass string
}

type ToolExecutionAttributes struct {
	TenantID, ToolName, Outcome string
	Latency                     time.Duration
}

type GovernanceRejectionAttributes struct {
	TenantID, AppCode, Reason string
}

// DatabasePoolStats is the capacity-relevant subset of database/sql.DBStats.
// Keeping this package on a small value type makes the observer reusable by
// alternate SQL pool implementations without importing their drivers.
type DatabasePoolStats struct {
	MaxOpenConnections int
	OpenConnections    int
	InUse              int
	Idle               int
	WaitCount          int64
	WaitDuration       time.Duration
}

// RedisPoolStats is the capacity-relevant subset of go-redis PoolStats.
type RedisPoolStats struct {
	Hits, Misses, Timeouts, WaitCount                   uint32
	WaitDuration                                        time.Duration
	TotalConnections, IdleConnections, StaleConnections uint32
}

// ModelUsageObserver is optional so deterministic test observers need not
// implement model accounting.
type ModelUsageObserver interface {
	RecordModelUsage(context.Context, ModelUsageAttributes)
}
type DeadLetterObserver interface {
	RecordDeadLetter(context.Context, DeadLetterAttributes)
}
type ToolExecutionObserver interface {
	RecordToolExecution(context.Context, ToolExecutionAttributes)
}
type GovernanceObserver interface {
	RecordGovernanceRejection(context.Context, GovernanceRejectionAttributes)
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

type InboundObserver interface {
	StartInbound(context.Context, InboundAttributes) (context.Context, func(error))
}

type HTTPObserver interface {
	WrapHTTP(http.Handler) http.Handler
}

type OTelObserver struct {
	tracer trace.Tracer
	meter  metric.Meter

	total, failures metric.Int64Counter
	active          metric.Int64UpDownCounter
	duration        metric.Float64Histogram

	httpTotal, httpFailures metric.Int64Counter
	httpActive              metric.Int64UpDownCounter
	httpDuration            metric.Float64Histogram

	storeTotal, storeFailures metric.Int64Counter
	storeDuration             metric.Float64Histogram

	backendRequests, backendFailures, backendCircuitOpens, backendProbeFailures metric.Int64Counter
	backendDuration, backendProbeDuration                                       metric.Float64Histogram
	backendCircuitState                                                         metric.Int64Gauge

	senderTotal, senderFailures                                                                          metric.Int64Counter
	senderDuration                                                                                       metric.Float64Histogram
	inboundTotal, inboundFailures                                                                        metric.Int64Counter
	inboundDuration                                                                                      metric.Float64Histogram
	kafkaLag, kafkaPartitions                                                                            metric.Int64Gauge
	kafkaCapacityFailures                                                                                metric.Int64Counter
	outboxPending, outboxOldestAge                                                                       metric.Int64Gauge
	modelPromptTokens, modelCachedPromptTokens, modelCompletionTokens, modelTotalTokens, modelCostMicros metric.Int64Counter
	dlqTotal                                                                                             metric.Int64Counter
	toolTotal, toolFailures                                                                              metric.Int64Counter
	toolDuration                                                                                         metric.Float64Histogram
	governanceRejections                                                                                 metric.Int64Counter
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
	active, err := meter.Int64UpDownCounter("agent.execution.active")
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
	httpActive, err := meter.Int64UpDownCounter("http.server.active_requests")
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
	backendRequests, err := meter.Int64Counter("backend.requests")
	if err != nil {
		return nil, err
	}
	backendFailures, err := meter.Int64Counter("backend.failures")
	if err != nil {
		return nil, err
	}
	backendDuration, err := meter.Float64Histogram("backend.request.duration.ms")
	if err != nil {
		return nil, err
	}
	backendCircuitState, err := meter.Int64Gauge("backend.circuit.state")
	if err != nil {
		return nil, err
	}
	backendCircuitOpens, err := meter.Int64Counter("backend.circuit.opens")
	if err != nil {
		return nil, err
	}
	backendProbeFailures, err := meter.Int64Counter("backend.probe.failures")
	if err != nil {
		return nil, err
	}
	backendProbeDuration, err := meter.Float64Histogram("backend.probe.duration.ms")
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
	inboundTotal, err := meter.Int64Counter("channel.inbound.messages")
	if err != nil {
		return nil, err
	}
	inboundFailures, err := meter.Int64Counter("channel.inbound.failures")
	if err != nil {
		return nil, err
	}
	inboundDuration, err := meter.Float64Histogram("channel.inbound.publish.duration.ms")
	if err != nil {
		return nil, err
	}
	kafkaLag, err := meter.Int64Gauge("messaging.kafka.consumer.lag")
	if err != nil {
		return nil, err
	}
	kafkaPartitions, err := meter.Int64Gauge("messaging.kafka.topic.partitions")
	if err != nil {
		return nil, err
	}
	kafkaCapacityFailures, err := meter.Int64Counter("messaging.kafka.capacity.sample.failures")
	if err != nil {
		return nil, err
	}
	outboxPending, err := meter.Int64Gauge("outbox.pending.events")
	if err != nil {
		return nil, err
	}
	outboxOldestAge, err := meter.Int64Gauge("outbox.oldest.age.seconds")
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
	dlqTotal, err := meter.Int64Counter("messaging.dlq.messages")
	if err != nil {
		return nil, err
	}
	toolTotal, err := meter.Int64Counter("tool.execution.total")
	if err != nil {
		return nil, err
	}
	toolFailures, err := meter.Int64Counter("tool.execution.failures")
	if err != nil {
		return nil, err
	}
	toolDuration, err := meter.Float64Histogram("tool.execution.duration.ms")
	if err != nil {
		return nil, err
	}
	governanceRejections, err := meter.Int64Counter("governance.rejections")
	if err != nil {
		return nil, err
	}
	return &OTelObserver{
		tracer: tracer, meter: meter, total: total, failures: failures, active: active, duration: duration,
		httpTotal: httpTotal, httpFailures: httpFailures, httpActive: httpActive, httpDuration: httpDuration,
		storeTotal: storeTotal, storeFailures: storeFailures, storeDuration: storeDuration,
		backendRequests: backendRequests, backendFailures: backendFailures, backendDuration: backendDuration,
		backendCircuitState: backendCircuitState, backendCircuitOpens: backendCircuitOpens,
		backendProbeFailures: backendProbeFailures, backendProbeDuration: backendProbeDuration,
		senderTotal: senderTotal, senderFailures: senderFailures, senderDuration: senderDuration,
		inboundTotal: inboundTotal, inboundFailures: inboundFailures, inboundDuration: inboundDuration,
		kafkaLag: kafkaLag, kafkaPartitions: kafkaPartitions, kafkaCapacityFailures: kafkaCapacityFailures,
		outboxPending: outboxPending, outboxOldestAge: outboxOldestAge,
		modelPromptTokens: modelPromptTokens, modelCachedPromptTokens: modelCachedPromptTokens,
		modelCompletionTokens: modelCompletionTokens, modelTotalTokens: modelTotalTokens, modelCostMicros: modelCostMicros,
		dlqTotal:  dlqTotal,
		toolTotal: toolTotal, toolFailures: toolFailures, toolDuration: toolDuration,
		governanceRejections: governanceRejections,
	}, nil
}

func (o *OTelObserver) ObserveDatabasePool(source func() DatabasePoolStats) error {
	if o == nil || o.meter == nil || source == nil {
		return fmt.Errorf("database pool metric source is required")
	}
	maxOpen, err := o.meter.Int64ObservableGauge("database.pool.connections.max")
	if err != nil {
		return err
	}
	open, err := o.meter.Int64ObservableGauge("database.pool.connections.open")
	if err != nil {
		return err
	}
	inUse, err := o.meter.Int64ObservableGauge("database.pool.connections.in_use")
	if err != nil {
		return err
	}
	idle, err := o.meter.Int64ObservableGauge("database.pool.connections.idle")
	if err != nil {
		return err
	}
	waits, err := o.meter.Int64ObservableCounter("database.pool.waits")
	if err != nil {
		return err
	}
	waitDuration, err := o.meter.Int64ObservableCounter("database.pool.wait_duration.ms")
	if err != nil {
		return err
	}
	_, err = o.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		stats := source()
		observer.ObserveInt64(maxOpen, int64(stats.MaxOpenConnections))
		observer.ObserveInt64(open, int64(stats.OpenConnections))
		observer.ObserveInt64(inUse, int64(stats.InUse))
		observer.ObserveInt64(idle, int64(stats.Idle))
		observer.ObserveInt64(waits, stats.WaitCount)
		observer.ObserveInt64(waitDuration, stats.WaitDuration.Milliseconds())
		return nil
	}, maxOpen, open, inUse, idle, waits, waitDuration)
	return err
}

func (o *OTelObserver) ObserveRedisPool(source func() RedisPoolStats) error {
	if o == nil || o.meter == nil || source == nil {
		return fmt.Errorf("Redis pool metric source is required")
	}
	hits, err := o.meter.Int64ObservableCounter("redis.pool.hits")
	if err != nil {
		return err
	}
	misses, err := o.meter.Int64ObservableCounter("redis.pool.misses")
	if err != nil {
		return err
	}
	timeouts, err := o.meter.Int64ObservableCounter("redis.pool.timeouts")
	if err != nil {
		return err
	}
	waits, err := o.meter.Int64ObservableCounter("redis.pool.waits")
	if err != nil {
		return err
	}
	waitDuration, err := o.meter.Int64ObservableCounter("redis.pool.wait_duration.ms")
	if err != nil {
		return err
	}
	total, err := o.meter.Int64ObservableGauge("redis.pool.connections.total")
	if err != nil {
		return err
	}
	idle, err := o.meter.Int64ObservableGauge("redis.pool.connections.idle")
	if err != nil {
		return err
	}
	stale, err := o.meter.Int64ObservableCounter("redis.pool.connections.stale")
	if err != nil {
		return err
	}
	_, err = o.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		stats := source()
		observer.ObserveInt64(hits, int64(stats.Hits))
		observer.ObserveInt64(misses, int64(stats.Misses))
		observer.ObserveInt64(timeouts, int64(stats.Timeouts))
		observer.ObserveInt64(waits, int64(stats.WaitCount))
		observer.ObserveInt64(waitDuration, stats.WaitDuration.Milliseconds())
		observer.ObserveInt64(total, int64(stats.TotalConnections))
		observer.ObserveInt64(idle, int64(stats.IdleConnections))
		observer.ObserveInt64(stale, int64(stats.StaleConnections))
		return nil
	}, hits, misses, timeouts, waits, waitDuration, total, idle, stale)
	return err
}

func (o *OTelObserver) ObserveWorkerCapacity(source func() int64) error {
	if o == nil || o.meter == nil || source == nil {
		return fmt.Errorf("worker capacity metric source is required")
	}
	slots, err := o.meter.Int64ObservableGauge("worker.execution.slots")
	if err != nil {
		return err
	}
	_, err = o.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(slots, source())
		return nil
	}, slots)
	return err
}

// RecordModelUsage records platform accounting dimensions that framework GenAI
// telemetry does not own: tenant/application/provider attribution and cost.
// Token counters intentionally share those accounting labels; they are not a
// replacement for the framework's standard gen_ai.client.* telemetry. No
// prompt, completion, user or request identifiers are exported as labels.
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
	// Prometheus exports an UpDownCounter as a Gauge. The Prometheus client
	// rejects exemplars on Gauges, so active-capacity gauges must not inherit a
	// sampled SpanContext even though the execution counter/histogram should.
	gaugeCtx := context.Background()
	o.active.Add(gaugeCtx, 1, metric.WithAttributes(metricAttributes...))
	return ctx, func(err error) {
		o.active.Add(gaugeCtx, -1, metric.WithAttributes(metricAttributes...))
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

func (o *OTelObserver) StartInbound(ctx context.Context, inbound InboundAttributes) (context.Context, func(error)) {
	attributes := []attribute.KeyValue{
		attribute.String("tenant.id", inbound.TenantID),
		attribute.String("app.code", inbound.AppCode),
		attribute.String("channel", inbound.Channel),
	}
	return o.startOperation(ctx, "channel.inbound.publish", attributes, o.inboundTotal, o.inboundFailures, o.inboundDuration, "channel inbound publish failed")
}

func (o *OTelObserver) RecordKafkaCapacity(ctx context.Context, capacity KafkaCapacityAttributes) {
	if o == nil || capacity.Lag < 0 || capacity.Partitions < 0 {
		return
	}
	attributes := []attribute.KeyValue{
		attribute.String("messaging.destination.name", capacity.Topic),
		attribute.String("messaging.kafka.consumer.group", capacity.ConsumerGroup),
	}
	_ = ctx // Capacity gauges intentionally avoid trace exemplars; see StartExecution.
	gaugeCtx := context.Background()
	o.kafkaLag.Record(gaugeCtx, capacity.Lag, metric.WithAttributes(attributes...))
	o.kafkaPartitions.Record(gaugeCtx, int64(capacity.Partitions), metric.WithAttributes(attributes...))
}

func (o *OTelObserver) RecordKafkaCapacityFailure(ctx context.Context, topic, consumerGroup string) {
	if o == nil {
		return
	}
	o.kafkaCapacityFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("messaging.destination.name", strings.TrimSpace(topic)),
		attribute.String("messaging.kafka.consumer.group", strings.TrimSpace(consumerGroup)),
	))
}

func (o *OTelObserver) RecordOutboxBacklog(ctx context.Context, backlog OutboxBacklogAttributes) {
	if o == nil || backlog.Pending < 0 || backlog.OldestAge < 0 {
		return
	}
	attributes := []attribute.KeyValue{
		attribute.String("tenant.id", backlog.TenantID),
		attribute.String("channel", backlog.Channel),
	}
	_ = ctx // Capacity gauges intentionally avoid trace exemplars; see StartExecution.
	gaugeCtx := context.Background()
	o.outboxPending.Record(gaugeCtx, backlog.Pending, metric.WithAttributes(attributes...))
	o.outboxOldestAge.Record(gaugeCtx, int64(backlog.OldestAge/time.Second), metric.WithAttributes(attributes...))
}

func (o *OTelObserver) RecordDeadLetter(ctx context.Context, deadLetter DeadLetterAttributes) {
	if o == nil {
		return
	}
	o.dlqTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant.id", deadLetter.TenantID),
		attribute.String("error.class", deadLetter.ErrorClass),
	))
}

func (o *OTelObserver) RecordToolExecution(ctx context.Context, execution ToolExecutionAttributes) {
	if o == nil || execution.Latency < 0 {
		return
	}
	attributes := []attribute.KeyValue{
		attribute.String("tenant.id", execution.TenantID),
		attribute.String("tool.name", execution.ToolName),
		attribute.String("tool.outcome", execution.Outcome),
	}
	o.toolTotal.Add(ctx, 1, metric.WithAttributes(attributes...))
	o.toolDuration.Record(ctx, float64(execution.Latency.Microseconds())/1000, metric.WithAttributes(attributes...))
	if execution.Outcome == "failed" {
		o.toolFailures.Add(ctx, 1, metric.WithAttributes(attributes...))
	}
}

func (o *OTelObserver) RecordGovernanceRejection(ctx context.Context, rejection GovernanceRejectionAttributes) {
	if o == nil {
		return
	}
	o.governanceRejections.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant.id", rejection.TenantID),
		attribute.String("app.code", rejection.AppCode),
		attribute.String("governance.reason", rejection.Reason),
	))
}

func (o *OTelObserver) RecordBackendOperation(ctx context.Context, observation backendhealth.OperationObservation) {
	if o == nil {
		return
	}
	attributes := backendHealthAttributes(observation.Key)
	attributes = append(attributes, attribute.String("backend.operation", observation.Operation))
	if observation.FastFailed {
		attributes = append(attributes, attribute.Bool("backend.fast_fail", true))
	}
	o.backendRequests.Add(ctx, 1, metric.WithAttributes(attributes...))
	o.backendDuration.Record(ctx, float64(observation.Duration.Microseconds())/1000, metric.WithAttributes(attributes...))
	if observation.Err != nil {
		o.backendFailures.Add(ctx, 1, metric.WithAttributes(attributes...))
	}
}

func (o *OTelObserver) RecordBackendTransition(ctx context.Context, transition backendhealth.TransitionObservation) {
	if o == nil || transition.From == transition.To {
		return
	}
	gaugeCtx := context.Background()
	base := backendHealthAttributes(transition.Key)
	if transition.From != "" {
		from := append(append([]attribute.KeyValue(nil), base...), attribute.String("backend.circuit.state", string(transition.From)))
		o.backendCircuitState.Record(gaugeCtx, 0, metric.WithAttributes(from...))
	}
	to := append(append([]attribute.KeyValue(nil), base...), attribute.String("backend.circuit.state", string(transition.To)))
	o.backendCircuitState.Record(gaugeCtx, 1, metric.WithAttributes(to...))
	if transition.To == backendhealth.StateOpen {
		o.backendCircuitOpens.Add(ctx, 1, metric.WithAttributes(base...))
	}
}

func (o *OTelObserver) RecordBackendProbe(ctx context.Context, observation backendhealth.ProbeObservation) {
	if o == nil {
		return
	}
	attributes := backendHealthAttributes(observation.Key)
	o.backendProbeDuration.Record(ctx, float64(observation.Duration.Microseconds())/1000, metric.WithAttributes(attributes...))
	if observation.Err != nil {
		o.backendProbeFailures.Add(ctx, 1, metric.WithAttributes(attributes...))
	}
}

func backendHealthAttributes(key backendhealth.Key) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("backend.profile.id", key.ProfileID),
		attribute.String("backend.domain", key.Domain),
		attribute.String("db.system", key.Driver),
	}
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
		gaugeCtx := context.Background()
		o.httpActive.Add(gaugeCtx, 1, metric.WithAttributes(baseAttributes...))
		defer o.httpActive.Add(gaugeCtx, -1, metric.WithAttributes(baseAttributes...))
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
	case path == "/api/v1/chat/stream":
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
var _ InboundObserver = (*OTelObserver)(nil)
var _ HTTPObserver = (*OTelObserver)(nil)
var _ DeadLetterObserver = (*OTelObserver)(nil)
var _ ToolExecutionObserver = (*OTelObserver)(nil)
var _ GovernanceObserver = (*OTelObserver)(nil)
var _ backendhealth.Observer = (*OTelObserver)(nil)
