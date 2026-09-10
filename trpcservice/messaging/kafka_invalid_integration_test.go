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

func TestFranzWorkerDeadLettersMalformedJSONThenConsumesNextRecord(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS is not set")
	}
	topic := fmt.Sprintf("trpc-agent-invalid-%d", time.Now().UnixNano())
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumerGroup(topic+"-group"),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("new Kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("new producer: %v", err)
	}
	t.Cleanup(producer.Close)
	key := []byte("tenant-a/support/web/chat-1")
	if err := producer.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Key: key, Value: []byte("{not-json")}).FirstErr(); err != nil {
		t.Fatalf("produce malformed record: %v", err)
	}
	valid := Envelope{Version: CurrentEnvelopeVersion, EventID: "after-invalid", TenantID: "tenant-a", SessionKey: string(key), Type: "inbound.message", Payload: []byte("{}")}
	encodedProducer, err := NewFranzProducer(producer, topic)
	if err != nil {
		t.Fatalf("NewFranzProducer(): %v", err)
	}
	if err := encodedProducer.Publish(context.Background(), valid); err != nil {
		t.Fatalf("produce valid record: %v", err)
	}
	consumer, err := NewFranzConsumer(client, topic+".dlq")
	if err != nil {
		t.Fatalf("NewFranzConsumer(): %v", err)
	}
	processed := 0
	worker, err := NewWorker(consumer, ProcessorFunc(func(_ context.Context, envelope Envelope) error {
		if envelope.EventID != valid.EventID {
			t.Fatalf("processed event = %q, want %q", envelope.EventID, valid.EventID)
		}
		processed++
		return nil
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("malformed RunOnce(): %v", err)
	}
	if err := worker.RunOnce(ctx); err != nil {
		t.Fatalf("following RunOnce(): %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed records = %d, want 1", processed)
	}
}
