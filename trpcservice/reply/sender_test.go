package reply

import (
	"context"
	"testing"
	"time"

	agentruntime "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

func TestSenderDeliversCompletedRun(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	provider := tracesdk.NewTracerProvider(tracesdk.WithSampler(tracesdk.AlwaysSample()))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	journal := gateway.NewMemoryJournal()
	queue := workqueue.NewMemoryQueue(4)
	runtime := agentruntime.NewDemoRuntime()
	adapter := channels.NewTestAdapter()
	registry, err := channels.NewRegistry(adapter)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	t.Cleanup(func() {
		_ = runtime.Close()
		_ = queue.Close()
		_ = journal.Close()
		_ = repository.Close()
	})
	resolver, _ := routing.NewControlPlaneResolver(repository)
	intake, _ := gateway.NewIntake(resolver, journal)
	rootCtx, rootSpan := otel.Tracer("test").Start(context.Background(), "callback")
	rootTraceID := rootSpan.SpanContext().TraceID().String()
	accepted, err := intake.Accept(rootCtx, gateway.IntakeRequest{
		BindingKey:        "tutorial-http",
		ExternalMessageID: "reply-message",
		UserID:            "alice",
		SessionID:         "reply-session",
		ChatType:          "direct",
		Text:              "hello",
	})
	rootSpan.End()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	relay, _ := gateway.NewOutboxRelay(journal, queue, gateway.RelayOptions{
		WorkerID: "relay", BatchSize: 10, ClaimLease: time.Second,
		PollInterval: time.Second, RetryDelay: time.Millisecond,
	})
	_, _ = relay.RelayOnce(context.Background())
	agentWorker, _ := worker.New(queue, journal, runtime, worker.Options{
		WorkerID: "worker", MaxAttempts: 3, RetryDelay: time.Millisecond,
	})
	if _, err := agentWorker.ProcessOne(context.Background()); err != nil {
		t.Fatalf("process Agent task: %v", err)
	}
	auditWriter := audit.NewMemoryWriter()
	sender, err := New(journal, repository, registry, Options{
		WorkerID: "sender", BatchSize: 10, ClaimLease: time.Second,
		PollInterval: time.Second, RetryDelay: time.Millisecond, MaxAttempts: 3,
		Audit: auditWriter,
	})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}
	sent, err := sender.ProcessOnce(context.Background())
	if err != nil || sent != 1 || len(adapter.Deliveries()) != 1 {
		t.Fatalf("sent=%d deliveries=%+v err=%v", sent, adapter.Deliveries(), err)
	}
	delivery := adapter.Deliveries()[0]
	if delivery.Message.RequestID != accepted.RequestID || delivery.Message.Text == "" {
		t.Fatalf("delivery = %+v", delivery)
	}
	if status, ok := journal.OutboundStatus(delivery.Message.OutboundID); !ok || status != "sent" {
		t.Fatalf("outbound status=%q ok=%t", status, ok)
	}
	events := auditWriter.Events()
	if len(events) != 1 || events[0].Decision != "reply_sent" ||
		events[0].RequestID != accepted.RequestID || events[0].TraceID != rootTraceID {
		t.Fatalf("audit events=%+v", events)
	}
}

func TestSplitTextUsesRunes(t *testing.T) {
	parts := splitText("你好世界", 3)
	if len(parts) != 2 || parts[0] != "你好世" || parts[1] != "界" {
		t.Fatalf("parts = %#v", parts)
	}
}
