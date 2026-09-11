package approval_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformtrace "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

type observedExporter struct {
	sdktrace.SpanExporter
	observer *tracetest.InMemoryExporter
}

func (e observedExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := e.observer.ExportSpans(ctx, spans); err != nil {
		return err
	}
	return e.SpanExporter.ExportSpans(ctx, spans)
}

func flowTracing(t *testing.T) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	previous, previousPropagation := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	previousFramework, previousTracer := agenttrace.TracerProvider, agenttrace.Tracer
	observer := tracetest.NewInMemoryExporter()
	var exporter sdktrace.SpanExporter = observer
	// Explicit opt-in: send the same sanitized spans to a local OTLP collector.
	if endpoint := os.Getenv("TEST_TRACE_OTLP_ENDPOINT"); endpoint != "" {
		remote, err := otlptracegrpc.New(context.Background(), otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
		if err != nil {
			t.Fatal(err)
		}
		exporter = observedExporter{remote, observer}
	}
	provider, err := platformtrace.NewTracerProvider(exporter, config.TelemetryConfig{
		ServiceName: "trpc-agent-telegram-trace-preflight", SampleRatio: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	platformtrace.BindFrameworkTracing(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(previousPropagation)
		agenttrace.TracerProvider, agenttrace.Tracer = previousFramework, previousTracer
		agenttrace.SetSpanAttributePolicy(agenttrace.SpanAttributePolicy{})
	})
	return provider, observer
}

func verifyFlowTrace(t *testing.T, spans tracetest.SpanStubs, initialTrace, decisionTrace, command string) {
	t.Helper()
	if initialTrace == "" || decisionTrace == "" || initialTrace == decisionTrace {
		t.Fatal("callbacks must create independent traces")
	}
	required := []string{"POST /callbacks/telegram/{callback_key}", "channel.callback", "gateway.accept", "queue.publish",
		"worker.agent.run", "invoke_agent ", "chat ", "execute_tool ", "storage.session.get", "storage.session.event.append", "reply.send"}
	for _, prefix := range required {
		found := false
		for _, span := range spans {
			if span.SpanContext.TraceID().String() == initialTrace && strings.HasPrefix(span.Name, prefix) {
				found = true
			}
		}
		if !found {
			t.Errorf("initial trace missing span %q", prefix)
		}
	}
	linked, decisionReplied, decisionModel := false, false, false
	for _, span := range spans {
		if span.SpanContext.TraceID().String() != decisionTrace {
			continue
		}
		if span.Name == "approval.decide" {
			for _, link := range span.Links {
				if link.SpanContext.TraceID().String() == initialTrace {
					linked = true
				}
			}
		}
		if span.Name == "reply.send" {
			decisionReplied = true
		}
		if strings.HasPrefix(span.Name, "chat ") {
			decisionModel = true
		}
	}
	if !linked || !decisionReplied {
		t.Fatalf("decision trace linked=%t replied=%t", linked, decisionReplied)
	}
	if decisionModel != (command == "批准") {
		t.Fatalf("decision model execution=%t for %s", decisionModel, command)
	}
	if command == "批准" {
		for _, name := range []string{"storage.memory.add", "storage.memory.read", "execute_tool dangerous_demo"} {
			found := false
			for _, span := range spans {
				if span.SpanContext.TraceID().String() == decisionTrace && span.Name == name {
					found = true
				}
			}
			if !found {
				t.Errorf("approved trace missing %q", name)
			}
		}
	}
	raw, err := json.Marshal(spans)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"fake-secret", "fake-token", "模型伪造", "memory-secret-canary", "argument-secret-canary"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("trace contains sensitive canary %q", secret)
		}
	}
	t.Logf("trace verified: command=%s initial=%s decision=%s spans=%d", command, initialTrace, decisionTrace, len(spans))
}
