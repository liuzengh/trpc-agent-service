package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

func TestMetadataExporterDropsPayloadsAndErrors(t *testing.T) {
	const secret = "trace-secret-canary-never-export"
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(metadataExporter{exporter}),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "privacy-test"), attribute.String("secret", secret))))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("test").Start(context.Background(), "execute_tool demo")
	span.SetAttributes(attribute.String("gen_ai.tool.name", "demo"), attribute.String("tenant.id", "tenant-a"),
		attribute.String("trpc.go.agent.llm_request", secret), attribute.String("gen_ai.system_instructions", secret),
		attribute.String("gen_ai.tool.call.arguments", secret), attribute.String("gen_ai.tool.call.result", secret),
		attribute.String("error.message", secret), attribute.String("authorization", secret))
	span.SetStatus(codes.Error, secret)
	span.RecordError(errors.New(secret))
	span.AddEvent(secret, trace.WithAttributes(attribute.String("body", secret)))
	span.AddLink(trace.Link{SpanContext: span.SpanContext(), Attributes: []attribute.KeyValue{attribute.String("secret", secret)}})
	span.End()
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans=%d", len(spans))
	}
	raw, err := json.Marshal(spans)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatal("sensitive content escaped to exporter")
	}
	if len(spans[0].Attributes) != 2 || len(spans[0].Links) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatal("metadata/status/link identity was lost")
	}
}

func TestFrameworkAndHTTPUseOneProviderWithoutCallbackKey(t *testing.T) {
	previous := otel.GetTracerProvider()
	previousFrameworkProvider, previousFrameworkTracer := agenttrace.TracerProvider, agenttrace.Tracer
	exporter := tracetest.NewInMemoryExporter()
	provider, err := NewTracerProvider(exporter, config.TelemetryConfig{ServiceName: "bridge-test", SampleRatio: 1})
	if err != nil {
		t.Fatal(err)
	}
	otel.SetTracerProvider(provider)
	BindFrameworkTracing(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
		agenttrace.TracerProvider, agenttrace.Tracer = previousFrameworkProvider, previousFrameworkTracer
		agenttrace.SetSpanAttributePolicy(agenttrace.SpanAttributePolicy{})
	})
	handler := HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, span := agenttrace.Tracer.Start(r.Context(), "chat test-model")
		span.End()
		w.WriteHeader(http.StatusAccepted)
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/callbacks/telegram/callback-secret-canary", nil))
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 || recorder.Header().Get("X-Trace-ID") == "" {
		t.Fatalf("spans=%d", len(spans))
	}
	var root, child tracetest.SpanStub
	for _, span := range spans {
		if strings.Contains(span.Name, "callback-secret-canary") {
			t.Fatal("callback key leaked in span name")
		}
		if span.Name == "chat test-model" {
			child = span
		} else {
			root = span
		}
	}
	if child.Parent.SpanID() != root.SpanContext.SpanID() || child.SpanContext.TraceID() != root.SpanContext.TraceID() {
		t.Fatal("framework trace is disconnected from HTTP")
	}
}
