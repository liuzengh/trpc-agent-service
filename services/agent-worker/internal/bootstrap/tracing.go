package bootstrap

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// This lifecycle wrapper leaves Processor outcomes and scheduling untouched.
// M2 will restore a durable parent before invoking it; without that parent the
// current attempt is a local trace, not an end-to-end IM trace.
type tracedProcessor struct {
	delegate processor
	tracer   trace.Tracer
}

func (p tracedProcessor) Advance(ctx context.Context, r domain.Run) (err error) {
	ctx, span := p.tracer.Start(ctx, "worker.run.attempt", trace.WithAttributes(
		attribute.String("app.run.id", r.Request.RunID), attribute.String("app.session.id", r.SessionID)))
	defer func() {
		result := application.ObservationResult(err)
		span.SetAttributes(attribute.String("app.outcome", result))
		if err != nil {
			span.SetStatus(codes.Error, "")
			span.SetAttributes(attribute.String("error.type", result))
		}
		span.End()
	}()
	return p.delegate.Advance(ctx, r)
}
