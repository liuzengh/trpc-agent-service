package telemetry

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/propagation"
)

type Operation string

const (
	OperationGatewaySubmit     Operation = "gateway.submit"
	OperationRelayDispatch     Operation = "relay.dispatch"
	OperationRelayReply        Operation = "relay.reply"
	OperationRelayWakeup       Operation = "relay.wakeup"
	OperationRelayControl      Operation = "relay.control"
	OperationAuditExport       Operation = "audit.export"
	OperationWorkerExecute     Operation = "worker.execute"
	OperationChannelDeliver    Operation = "channel.deliver"
	OperationChannelIngress    Operation = "channel.ingress"
	OperationChannelPreprocess Operation = "channel.preprocess"
	OperationModelGenerate     Operation = "model.generate"
	OperationToolExecute       Operation = "tool.execute"
	OperationSessionOpen       Operation = "session.open"
	OperationSessionCommit     Operation = "session.commit"
	OperationSessionTerminal   Operation = "session.get_terminal"
	OperationSessionReadFence  Operation = "session.read_fence"
	OperationSessionLoad       Operation = "session.load"
	OperationMemoryRead        Operation = "memory.read"
	OperationMemorySearch      Operation = "memory.search"
	OperationMemoryAdd         Operation = "memory.add"
	OperationMemoryUpdate      Operation = "memory.update"
	OperationMemoryDelete      Operation = "memory.delete"
	OperationMemoryClear       Operation = "memory.clear"
	OperationMemoryIngest      Operation = "memory.ingest"
)

type MetricDescriptor string

const (
	MetricAuditExportTotal       MetricDescriptor = "trpc_audit_export_total"
	MetricAuditExportDuration    MetricDescriptor = "trpc_audit_export_duration_seconds"
	MetricAuditOutboxLag         MetricDescriptor = "trpc_audit_outbox_lag_seconds"
	MetricAuditOutboxBacklog     MetricDescriptor = "trpc_audit_outbox_active_backlog"
	MetricAuditAutoscalingLag    MetricDescriptor = "trpc_audit_outbox_lag_max_seconds"
	MetricBrokerBacklog          MetricDescriptor = "trpc_broker_backlog_total"
	MetricBrokerDeliveryLag      MetricDescriptor = "trpc_broker_delivery_lag_max_seconds"
	MetricOperationTotal         MetricDescriptor = "trpc_operation_total"
	MetricOperationDuration      MetricDescriptor = "trpc_operation_duration_seconds"
	MetricSessionBackendDuration MetricDescriptor = "trpc_session_backend_duration_seconds"
	MetricGovernanceBudgetDenied MetricDescriptor = "trpc_governance_budget_denied_total"
	MetricGovernanceUsageMissing MetricDescriptor = "trpc_governance_usage_missing_total"
)

type Component string

const (
	ComponentGateway         Component = "gateway"
	ComponentBusinessRelay   Component = "business-relay"
	ComponentAuditRelay      Component = "audit-relay"
	ComponentWorker          Component = "worker"
	ComponentChannelDelivery Component = "channel-delivery"
	ComponentChannelIngress  Component = "channel-ingress"
	ComponentPreprocess      Component = "preprocess"
)

type attributeKey string

const (
	attributeComponent   attributeKey = "component"
	attributeOutcome     attributeKey = "outcome"
	attributeDestination attributeKey = "destination"
	attributeOperation   attributeKey = "operation"
)

type Attribute struct {
	key   attributeKey
	value string
}

func ComponentAttribute(value Component) Attribute {
	return Attribute{key: attributeComponent, value: string(value)}
}

type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeError   Outcome = "error"
)

func OutcomeAttribute(value Outcome) Attribute {
	return Attribute{key: attributeOutcome, value: string(value)}
}

type Destination string

const DestinationPostgreSQL Destination = "postgres"

func DestinationAttribute(value Destination) Attribute {
	return Attribute{key: attributeDestination, value: string(value)}
}

func OperationAttribute(value Operation) Attribute {
	return Attribute{key: attributeOperation, value: string(value)}
}

func (a Attribute) Key() string   { return string(a.key) }
func (a Attribute) Value() string { return a.value }

func (a Attribute) Valid() bool {
	switch a.key {
	case attributeComponent:
		return ValidComponent(Component(a.value))
	case attributeOutcome:
		return a.value == string(OutcomeSuccess) || a.value == string(OutcomeError)
	case attributeDestination:
		return a.value == string(DestinationPostgreSQL)
	case attributeOperation:
		return ValidOperation(Operation(a.value))
	default:
		return false
	}
}

type LogEvent string

const (
	LogAuditRelayDegraded LogEvent = "audit.relay.degraded"
	LogAuditBacklogAlert  LogEvent = "audit.backlog.alert"
)

type Span interface{ End(error) }

type Counter interface {
	Add(context.Context, int64, ...Attribute)
}

type Histogram interface {
	Record(context.Context, float64, ...Attribute)
}

type Logger interface {
	Info(context.Context, LogEvent, ...Attribute)
	Error(context.Context, LogEvent, ...Attribute)
}

type Provider interface {
	StartSpan(context.Context, Operation, ...Attribute) (context.Context, Span)
	Counter(MetricDescriptor) Counter
	Histogram(MetricDescriptor) Histogram
	Logger(Component) Logger
	Shutdown(context.Context) error
}

func OrNoop(provider Provider) Provider {
	if provider != nil {
		return provider
	}
	return Noop()
}

// StartOperation restores the persisted W3C parent, starts one controlled
// operation, and returns an idempotent completion function. It records only
// fixed descriptors and whitelisted attributes.
func StartOperation(ctx context.Context, provider Provider, traceParent string, operation Operation, attributes ...Attribute) (context.Context, func(error)) {
	if ctx == nil {
		ctx = context.Background()
	}
	if traceParent != "" {
		ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceParent})
	}
	provider = OrNoop(provider)
	attributes = append(append([]Attribute(nil), attributes...), OperationAttribute(operation))
	ctx, span := provider.StartSpan(ctx, operation, attributes...)
	started := time.Now()
	var once sync.Once
	return ctx, func(err error) {
		once.Do(func() {
			outcome := OutcomeSuccess
			if err != nil {
				outcome = OutcomeError
			}
			measured := append(append([]Attribute(nil), attributes...), OutcomeAttribute(outcome))
			provider.Counter(MetricOperationTotal).Add(ctx, 1, measured...)
			provider.Histogram(MetricOperationDuration).Record(ctx, time.Since(started).Seconds(), measured...)
			span.End(err)
		})
	}
}

func EffectiveTraceParent(ctx context.Context, fallback string) string {
	if ctx == nil {
		return fallback
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	if value := carrier.Get("traceparent"); value != "" {
		return value
	}
	return fallback
}

func ValidOperation(value Operation) bool {
	switch value {
	case OperationGatewaySubmit, OperationRelayDispatch, OperationRelayReply, OperationRelayWakeup,
		OperationRelayControl, OperationAuditExport, OperationWorkerExecute, OperationChannelDeliver,
		OperationChannelIngress, OperationChannelPreprocess, OperationModelGenerate, OperationToolExecute,
		OperationSessionOpen, OperationSessionCommit, OperationSessionTerminal, OperationSessionReadFence, OperationSessionLoad,
		OperationMemoryRead, OperationMemorySearch, OperationMemoryAdd, OperationMemoryUpdate, OperationMemoryDelete,
		OperationMemoryClear, OperationMemoryIngest:
		return true
	default:
		return false
	}
}

func ValidComponent(value Component) bool {
	switch value {
	case ComponentGateway, ComponentBusinessRelay, ComponentAuditRelay, ComponentWorker, ComponentChannelDelivery,
		ComponentChannelIngress, ComponentPreprocess:
		return true
	default:
		return false
	}
}

func ValidMetricDescriptor(value MetricDescriptor) bool {
	switch value {
	case MetricAuditExportTotal, MetricAuditExportDuration, MetricAuditOutboxLag, MetricOperationTotal, MetricOperationDuration, MetricSessionBackendDuration,
		MetricGovernanceBudgetDenied, MetricGovernanceUsageMissing:
		return true
	default:
		return false
	}
}

var noopProvider Provider = noop{}

func Noop() Provider { return noopProvider }

// Enabled reports whether the provider will emit signals. Roles use this to
// avoid wrapping framework calls or allocating response pumps when telemetry
// is intentionally disabled.
func Enabled(provider Provider) bool {
	if provider == nil {
		return false
	}
	_, isNoop := provider.(noop)
	return !isNoop
}

type noop struct{}
type noopSpan struct{}
type noopCounter struct{}
type noopHistogram struct{}
type noopLogger struct{}

func (noop) StartSpan(ctx context.Context, _ Operation, _ ...Attribute) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (noop) Counter(MetricDescriptor) Counter                       { return noopCounter{} }
func (noop) Histogram(MetricDescriptor) Histogram                   { return noopHistogram{} }
func (noop) Logger(Component) Logger                                { return noopLogger{} }
func (noop) Shutdown(context.Context) error                         { return nil }
func (noopSpan) End(error)                                          {}
func (noopCounter) Add(context.Context, int64, ...Attribute)        {}
func (noopHistogram) Record(context.Context, float64, ...Attribute) {}
func (noopLogger) Info(context.Context, LogEvent, ...Attribute)     {}
func (noopLogger) Error(context.Context, LogEvent, ...Attribute)    {}
