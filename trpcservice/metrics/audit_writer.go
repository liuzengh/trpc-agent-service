package metrics

import (
	"context"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
)

// AuditWriter decorates an audit writer with best-effort usage telemetry. The
// underlying append remains the source of truth: counters are updated only
// after a new event was durably accepted, never for duplicates or failures.
type AuditWriter struct {
	delegate audit.Writer
	catalog  Catalog
}

// WrapAuditWriter adds usage/cost telemetry to delegate. A nil writer or
// provider is returned unchanged, and an already wrapped writer is not nested.
func WrapAuditWriter(delegate audit.Writer, provider observability.Provider) audit.Writer {
	if delegate == nil || provider == nil {
		return delegate
	}
	if _, ok := delegate.(*AuditWriter); ok {
		return delegate
	}
	return &AuditWriter{delegate: delegate, catalog: New(provider)}
}

// Append preserves the audit writer's result and records only a newly accepted
// event's cost/token delta. Telemetry errors are deliberately ignored so they
// cannot turn a successful mandatory audit append into a business failure.
func (writer *AuditWriter) Append(ctx context.Context, event audit.Event) (audit.AppendResult, error) {
	if writer == nil || writer.delegate == nil {
		return audit.AppendResult{}, audit.ErrInvalid
	}
	result, err := writer.delegate.Append(ctx, event)
	if err != nil || result.Duplicate {
		return result, err
	}
	accepted := result.Event
	// Writers normally return the accepted immutable event. Keep the input as a
	// compatibility fallback for small adapters that only return a digest.
	if accepted.EventID == "" {
		accepted = event
	}
	usage := accepted.Cost
	if usage == nil {
		return result, err
	}
	modelUsage := audit.UsageTotal{Channel: accepted.Channel, Provider: usage.Provider, Model: usage.Model, Currency: usage.Currency}
	if usage.InputTokens != nil {
		modelUsage.InputTokens = *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		modelUsage.OutputTokens = *usage.OutputTokens
	}
	if usage.ModelCostMinor != nil {
		modelUsage.ModelCostMinor = *usage.ModelCostMinor
	}
	if usage.ToolCostMinor != nil {
		modelUsage.ToolCostMinor = *usage.ToolCostMinor
	}
	if modelUsage.InputTokens > 0 || modelUsage.OutputTokens > 0 || modelUsage.ModelCostMinor > 0 {
		modelOnly := modelUsage
		modelOnly.ToolCostMinor = 0
		_ = writer.catalog.Usage(ctx, modelOnly, map[string]string{"component": "model", "model_family": usage.Model})
	}
	if modelUsage.ToolCostMinor > 0 {
		_ = writer.catalog.Usage(ctx, audit.UsageTotal{Channel: accepted.Channel, Provider: usage.Provider, Model: usage.Model, Currency: usage.Currency, ToolCostMinor: modelUsage.ToolCostMinor}, map[string]string{"component": "tool", "model_family": usage.Model})
	}
	return result, err
}
