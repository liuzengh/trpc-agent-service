package metrics

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Operation measures backend/model calls, not whole Agent turns. Callers must
// pass bounded configuration metadata only, never users, sessions or payloads.
type Operation struct {
	calls   metric.Int64Counter
	latency metric.Float64Histogram
}

func NewOperation(kind string) *Operation {
	if kind != "model" && kind != "storage" && kind != "audit" {
		return nil
	}
	meter := otel.Meter("trpc-agent-service/operations")
	calls, err := meter.Int64Counter("agent." + kind + ".calls")
	if err != nil {
		return nil
	}
	latency, err := meter.Float64Histogram("agent."+kind+".call.duration", metric.WithUnit("s"))
	if err != nil {
		return nil
	}
	return &Operation{calls, latency}
}
func (o *Operation) Finish(ctx context.Context, started time.Time, tenantID, appID, operation, backend string, err error) {
	if o == nil {
		return
	}
	result := "ok"
	if err != nil {
		result = "error"
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			result = "canceled"
		}
	}
	attrs := metric.WithAttributes(attribute.String("tenant.id", tenantID), attribute.String("agent.app.id", appID), attribute.String("operation.name", operation), attribute.String("backend.type", backend), attribute.String("operation.result", result))
	o.calls.Add(ctx, 1, attrs)
	o.latency.Record(ctx, time.Since(started).Seconds(), attrs)
}
