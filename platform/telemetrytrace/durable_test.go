package telemetrytrace

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"testing"
)

func TestDurableResumePreservesLifecycleNotAmbientParent(t *testing.T) {
	for _, valid := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "durable"}[valid], func(t *testing.T) {
			ex := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
			defer tp.Shutdown(context.Background())
			tr := tp.Tracer("test")
			ctx, ambient := tr.Start(context.Background(), "ambient")
			defer ambient.End()
			ctx, cancel := context.WithCancel(ctx)
			carrier := tracecontext.Carrier{}
			if valid {
				carrier.Traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
			}
			resumed, span := Resume(tr, ctx, carrier, "gateway.reply.deliver")
			cancel()
			if resumed.Err() != context.Canceled {
				t.Fatal("lost lifecycle")
			}
			span.End()
			s := ex.GetSpans()[0]
			if len(s.Links) != 1 || s.Links[0].SpanContext.SpanID() != ambient.SpanContext().SpanID() {
				t.Fatal("lost ambient link")
			}
			if valid {
				if s.Parent.SpanID() != trace.SpanContextFromContext(carrier.Restore(context.Background())).SpanID() {
					t.Fatal("lost durable parent")
				}
			} else if s.Parent.IsValid() || s.SpanContext.TraceID() == ambient.SpanContext().TraceID() {
				t.Fatal("legacy row inherited polling trace")
			}
		})
	}
}
