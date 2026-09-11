package telemetrytrace

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"go.opentelemetry.io/otel/trace"
)

// Resume restores the durable causal parent while retaining today's lifecycle.
// A legacy/malformed carrier starts a diagnostic root, not an unrelated polling
// trace. An ambient span is retained as a link, never as the message's parent.
func Resume(t trace.Tracer, ctx context.Context, carrier tracecontext.Carrier, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ambient := trace.SpanContextFromContext(ctx)
	ctx = carrier.Restore(trace.ContextWithSpanContext(ctx, trace.SpanContext{}))
	parent := trace.SpanContextFromContext(ctx)
	if ambient.IsValid() && !ambient.Equal(parent) {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: ambient}))
	}
	return Start(t, ctx, name, opts...)
}
