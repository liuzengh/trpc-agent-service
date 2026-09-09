package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// configInstruments owns the P1-08 configuration-publication instruments. It
// is additive to the P1-07 allowlist: one low-cardinality counter over the
// fixed operation/outcome vocabularies, and one span helper. Nothing here
// changes delivery semantics, readiness behavior or exporter ownership.
type configInstruments struct {
	operations metric.Int64Counter
}

var (
	configOperationAllowlist = map[string]struct{}{
		"create": {}, "validate": {}, "publish": {}, "rollout": {}, "rollback": {},
		"read": {}, "assign": {},
	}
	configOutcomeAllowlist = map[string]struct{}{
		"committed": {}, "rejected": {}, "stale_version": {}, "conflict": {},
		"failed": {}, "unavailable": {}, "hit": {}, "unmanaged": {}, "not_readable": {}, "skipped": {},
	}
)

func newConfigInstruments(provider *sdkmetric.MeterProvider) configInstruments {
	if provider == nil {
		return configInstruments{}
	}
	meter := provider.Meter("trpcagent.configpub")
	counter, err := meter.Int64Counter("trpcagent.config.operation",
		metric.WithDescription("configuration publication operations by operation and outcome"))
	if err != nil {
		return configInstruments{}
	}
	return configInstruments{operations: counter}
}

// ConfigOperation counts one configuration publication operation with
// allowlisted, low-cardinality attributes only.
func (r *Runtime) ConfigOperation(operation, outcome string) {
	if r == nil || r.configInstruments.operations == nil {
		return
	}
	if _, ok := configOperationAllowlist[operation]; !ok {
		operation = "unknown"
		r.meter.UnknownAttrDropped()
	}
	if _, ok := configOutcomeAllowlist[outcome]; !ok {
		outcome = "unknown"
		r.meter.UnknownAttrDropped()
	}
	r.configInstruments.operations.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("operation", operation),
			attribute.String("outcome", outcome),
		))
}

// StartConfigSpan starts a bounded configuration-publication span. The span
// ends when the returned function is called with a bounded outcome string.
func (r *Runtime) StartConfigSpan(ctx context.Context, operation string) (context.Context, func(outcome string)) {
	if r == nil {
		return ctx, func(string) {}
	}
	spanCtx, span := r.Tracer().Start(ctx, "config "+operation)
	return spanCtx, func(outcome string) {
		if outcome != "" && outcome != "committed" && outcome != "hit" {
			span.RecordError(errorsNew("outcome:" + outcome))
		}
		span.End()
	}
}
