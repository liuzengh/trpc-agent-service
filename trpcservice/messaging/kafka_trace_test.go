package messaging

import (
	"context"
	"testing"

	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestWorkerPropagatesEnvelopeTraceParentToProcessor(t *testing.T) {
	consumer := &fakeConsumer{delivery: Delivery{Envelope: Envelope{Version: CurrentEnvelopeVersion, EventID: "event-1", TenantID: "tenant-a", SessionKey: "tenant-a/support/telegram/chat-1", Type: "agent.reply.completed", Payload: []byte(`{}`), TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}}}
	worker, err := NewWorker(consumer, ProcessorFunc(func(ctx context.Context, _ Envelope) error {
		if !oteltrace.SpanContextFromContext(ctx).IsValid() {
			t.Fatal("processor context must carry the envelope trace parent")
		}
		return nil
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
}
