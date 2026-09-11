package trpcagent

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestOverlayTraceAggregatesPartialAppends(t *testing.T) {
	ex := tracetest.NewInMemoryExporter()
	p := sdktrace.NewTracerProvider(sdktrace.WithSyncer(ex))
	defer p.Shutdown(context.Background())
	s, err := newOverlay("tenant", "session", nil, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	s.tracer = p.Tracer("worker")
	ctx, parent := s.tracer.Start(context.Background(), "worker.runner.run")
	defer parent.End()
	sess, err := s.GetSession(ctx, s.key)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err = s.AppendEvent(ctx, sess, &event.Event{Response: &model.Response{IsPartial: true}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.AppendEvent(ctx, sess, &event.Event{Response: &model.Response{Done: true}}); err != nil {
		t.Fatal(err)
	}
	count, bytes := s.stats()
	if count != 101 || bytes <= 0 {
		t.Fatal("aggregate missing", count, bytes)
	}
	spans := ex.GetSpans()
	gets, appends := 0, 0
	for _, span := range spans {
		if span.Name == "session.overlay.get" {
			gets++
		}
		if span.Name == "session.overlay.append" {
			appends++
		}
		if span.Parent.SpanID() != parent.SpanContext().SpanID() {
			t.Fatal("overlay parent lost")
		}
	}
	if gets != 1 || appends != 1 {
		t.Fatal("per-token spans or missing actual append", gets, appends)
	}
	t.Log("OVERLAY_TRACE=PASS partial_calls=100 full_append_spans=1 aggregate=101")
}
