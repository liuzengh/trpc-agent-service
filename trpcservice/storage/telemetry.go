package storage

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func startStorageSpan(
	ctx context.Context,
	operation string,
	appName string,
) (context.Context, trace.Span) {
	ctx, span := otel.Tracer("trpc-agent-service/storage").Start(ctx, "storage."+operation)
	span.SetAttributes(attribute.String("agent.app.scope", appName))
	if tenantID, appID, err := runtimecontext.ParseStorageScope(appName); err == nil {
		span.SetAttributes(
			attribute.String("tenant.id", tenantID),
			attribute.String("agent.app.id", appID),
		)
	}
	return ctx, span
}
