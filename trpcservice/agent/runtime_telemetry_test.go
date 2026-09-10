package agent

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
	tracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRuntimeEmitsRedactedExecutionTrace(t *testing.T) {
	t.Parallel()
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	defer func() { _ = tracerProvider.Shutdown(context.Background()) }()
	observer, err := metrics.NewOTelObserver(tracerProvider.Tracer("test"), metric.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatalf("NewOTelObserver() error = %v", err)
	}
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	runtime, err := NewRuntime(repository, assembly.NewFactory(testutil.NewFakeModel("runtime reply")), storage.NewMemoryIdempotencyStore(), storage.NewMemoryStateStore(), time.Minute, time.Hour, WithObserver(observer))
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	if _, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{MessageID: "message-1", Channel: channels.Telegram, ConversationID: "chat-1", SenderID: "user-1", Text: "secret user text"}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "agent.runtime.handle" {
		t.Fatalf("ended spans = %d, want one runtime span", len(spans))
	}
	if value, ok := findAttribute(spans[0].Attributes(), "tenant.id"); !ok || value.AsString() != "tenant-a" {
		t.Fatalf("tenant.id = %v, want tenant-a", value)
	}
	for _, item := range spans[0].Attributes() {
		if item.Value.Type().String() == "STRING" && item.Value.AsString() == "secret user text" {
			t.Fatal("telemetry must not include raw user text")
		}
	}
}

func findAttribute(attributes []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, item := range attributes {
		if string(item.Key) == key {
			return item.Value, true
		}
	}
	return attribute.Value{}, false
}
