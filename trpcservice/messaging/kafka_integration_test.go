package messaging

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFranzWorkerConsumesAndCommitsKafkaEnvelope(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not set")
	}
	topic := "trpc-agent-integration"
	group := fmt.Sprintf("trpc-agent-integration-%d", time.Now().UnixNano())

	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("new consumer client error = %v", err)
	}
	t.Cleanup(consumerClient.Close)
	consumer, err := NewFranzConsumer(consumerClient, topic+".dlq")
	if err != nil {
		t.Fatalf("NewFranzConsumer() error = %v", err)
	}

	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("new producer client error = %v", err)
	}
	t.Cleanup(producerClient.Close)
	producer, err := NewFranzProducer(producerClient, topic)
	if err != nil {
		t.Fatalf("NewFranzProducer() error = %v", err)
	}
	envelope := Envelope{
		Version:    CurrentEnvelopeVersion,
		EventID:    "event-1",
		TenantID:   "tenant-a",
		SessionKey: "tenant-a/support/telegram/chat-1",
		Type:       "agent.reply.completed",
		Payload:    []byte(`{}`),
	}
	if err := producer.Publish(context.Background(), envelope); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	processed := 0
	worker, err := NewWorker(consumer, ProcessorFunc(func(_ context.Context, received Envelope) error {
		if received.EventID != envelope.EventID {
			t.Fatalf("received event ID = %q, want %q", received.EventID, envelope.EventID)
		}
		processed++
		return nil
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	processContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := worker.RunOnce(processContext); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed messages = %d, want 1", processed)
	}
}
