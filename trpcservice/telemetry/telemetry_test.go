package telemetry_test

import (
	"context"
	"sync"
	"testing"

	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestW3CTraceContextSurvivesDurableBoundary(t *testing.T) {
	runtime := platformtelemetry.NewNoop(context.Background(), "test-service")
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })

	parentCtx, parentSpan := platformtelemetry.StartSpan(context.Background(), "parent")
	defer parentSpan.End()
	traceID := platformtelemetry.TraceID(parentCtx)
	if traceID == "" {
		t.Fatal("parent span did not create a trace id")
	}
	carrier := platformtelemetry.Inject(parentCtx)
	if carrier["traceparent"] == "" {
		t.Fatal("traceparent was not injected")
	}

	workerCtx := platformtelemetry.Extract(context.Background(), carrier)
	workerCtx, workerSpan := platformtelemetry.StartSpan(workerCtx, "worker")
	defer workerSpan.End()
	if got := platformtelemetry.TraceID(workerCtx); got != traceID {
		t.Fatalf("worker trace id = %q, want %q", got, traceID)
	}
	if platformtelemetry.TraceParent(workerCtx) == "" {
		t.Fatal("worker traceparent was not injectable")
	}
}

func TestDurableBoundaryProducesParentChildSpans(t *testing.T) {
	exporter := &spanRecorder{}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
	)
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})

	parentCtx, parentSpan := platformtelemetry.StartSpan(context.Background(), "gateway")
	carrier := platformtelemetry.Inject(parentCtx)
	workerCtx := platformtelemetry.Extract(context.Background(), carrier)
	_, workerSpan := platformtelemetry.StartSpan(workerCtx, "worker")
	workerSpan.End()
	parentSpan.End()

	spans := exporter.spansSnapshot()
	if len(spans) != 2 {
		t.Fatalf("exported spans = %d, want 2", len(spans))
	}
	var parent, child sdktrace.ReadOnlySpan
	for _, span := range spans {
		switch span.Name() {
		case "gateway":
			parent = span
		case "worker":
			child = span
		}
	}
	if parent == nil || child == nil {
		t.Fatalf("exported span names = %q, %q", parentName(parent), parentName(child))
	}
	if parent.SpanContext().TraceID() != child.SpanContext().TraceID() {
		t.Fatalf("parent trace id = %s, child trace id = %s", parent.SpanContext().TraceID(), child.SpanContext().TraceID())
	}
	if child.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Fatalf("child parent span id = %s, want %s", child.Parent().SpanID(), parent.SpanContext().SpanID())
	}
	if child.SpanContext().SpanID() == parent.SpanContext().SpanID() {
		t.Fatal("parent and child reused the same span id")
	}
}

func parentName(span sdktrace.ReadOnlySpan) string {
	if span == nil {
		return "<nil>"
	}
	return span.Name()
}

type spanRecorder struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (r *spanRecorder) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, spans...)
	return nil
}

func (*spanRecorder) Shutdown(context.Context) error { return nil }

func (r *spanRecorder) spansSnapshot() []sdktrace.ReadOnlySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sdktrace.ReadOnlySpan(nil), r.spans...)
}

func TestStartWithoutOTLPEndpointSucceeds(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	runtime, err := platformtelemetry.Start(context.Background(), platformtelemetry.Config{
		ServiceName: "test-service",
		Protocol:    "grpc",
	})
	if err != nil {
		t.Fatalf("start telemetry without endpoint: %v", err)
	}
	if runtime == nil || runtime.TracerProvider == nil || runtime.MeterProvider == nil {
		t.Fatal("telemetry did not return SDK providers")
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("close telemetry: %v", err)
	}
}

func TestStartRejectsUnknownProtocol(t *testing.T) {
	if _, err := platformtelemetry.Start(context.Background(), platformtelemetry.Config{Protocol: "udp"}); err == nil {
		t.Fatal("unknown telemetry protocol was accepted")
	}
}
