package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestOutboxDispatcherPublishesSessionPartitionedEnvelopeBeforeMarkingDelivered(t *testing.T) {
	t.Parallel()

	store := storage.NewMemoryStateStore()
	_, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID:      "tenant-a",
		AppCode:       "support",
		SessionKey:    "tenant-a/support/telegram/chat-1",
		MessageID:     "update-42",
		Channel:       "telegram",
		BindingID:     "telegram-bot",
		TraceID:       "trace-42",
		Action:        "agent.reply",
		Result:        "success",
		OutboxType:    "agent.reply.completed",
		OutboxPayload: []byte(`{"message_id":"update-42"}`),
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}

	producer := &recordingProducer{}
	dispatcher, err := NewOutboxDispatcher(store, producer)
	if err != nil {
		t.Fatalf("NewOutboxDispatcher() error = %v", err)
	}
	dispatched, err := dispatcher.DispatchTenant(context.Background(), "tenant-a", 10)
	if err != nil {
		t.Fatalf("DispatchTenant() error = %v", err)
	}
	if dispatched != 1 {
		t.Fatalf("dispatched events = %d, want 1", dispatched)
	}
	if len(producer.envelopes) != 1 {
		t.Fatalf("published envelopes = %d, want 1", len(producer.envelopes))
	}
	if got, want := producer.envelopes[0].SessionKey, "tenant-a/support/telegram/chat-1"; got != want {
		t.Fatalf("session key = %q, want %q", got, want)
	}
	if got, want := string(producer.envelopes[0].PartitionKey()), "tenant-a/support/telegram/chat-1"; got != want {
		t.Fatalf("partition key = %q, want %q", got, want)
	}
	pending, err := store.ListPendingOutbox(context.Background(), "tenant-a", 10)
	if err != nil {
		t.Fatalf("ListPendingOutbox() error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending outbox events = %d, want 0 after publish", len(pending))
	}
}

func TestWorkerAcknowledgesPermanentFailureOnlyAfterDLQPublish(t *testing.T) {
	t.Parallel()

	consumer := &fakeConsumer{delivery: Delivery{Envelope: Envelope{
		Version:    CurrentEnvelopeVersion,
		EventID:    "event-1",
		TenantID:   "tenant-a",
		SessionKey: "tenant-a/support/telegram/chat-1",
		Type:       "agent.reply.completed",
		Payload:    []byte(`{}`),
		Attempt:    3,
	}}}
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error {
		return Permanent(errors.New("invalid message"))
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}

	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if consumer.dlqCalls != 1 {
		t.Fatalf("DLQ calls = %d, want 1", consumer.dlqCalls)
	}
	if consumer.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1 after successful DLQ", consumer.commitCalls)
	}
}

type recordingProducer struct {
	envelopes []Envelope
}

func (p *recordingProducer) Publish(_ context.Context, envelope Envelope) error {
	p.envelopes = append(p.envelopes, envelope)
	return nil
}

type fakeConsumer struct {
	delivery    Delivery
	commitCalls int
	dlqCalls    int
}

func (c *fakeConsumer) Receive(context.Context) (Delivery, error) {
	return c.delivery, nil
}

func (c *fakeConsumer) Commit(context.Context, Delivery) error {
	c.commitCalls++
	return nil
}

func (c *fakeConsumer) PublishDLQ(context.Context, DeadLetter) error {
	c.dlqCalls++
	return nil
}
