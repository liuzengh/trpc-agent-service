package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FranzProducer is the franz-go implementation of Producer. Topic selection is
// fixed at construction so callers cannot publish to arbitrary tenant topics.
type FranzProducer struct {
	client *kgo.Client
	topic  string
}

// NewFranzProducer constructs a Kafka producer around a configured franz-go
// client. The composition root owns broker, TLS, SASL, retry, and close policy.
func NewFranzProducer(client *kgo.Client, topic string) (*FranzProducer, error) {
	if client == nil {
		return nil, fmt.Errorf("franz Kafka client is required")
	}
	if topic == "" {
		return nil, fmt.Errorf("Kafka topic is required")
	}
	return &FranzProducer{client: client, topic: topic}, nil
}

// Publish encodes the versioned envelope and uses the stable session key as the
// Kafka record key, preserving per-session partition order.
func (p *FranzProducer) Publish(ctx context.Context, envelope Envelope) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	value, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal Kafka envelope: %w", err)
	}
	results := p.client.ProduceSync(ctx, &kgo.Record{
		Topic:   p.topic,
		Key:     envelope.PartitionKey(),
		Value:   value,
		Headers: traceHeaders(envelope.TraceParent),
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka envelope: %w", err)
	}
	return nil
}

// FranzConsumer adapts an ordered franz-go consumer group to the Worker
// Consumer contract. It disables no ordering semantics itself: records are
// returned and committed in Kafka's partition order by a single Worker loop.
type FranzConsumer struct {
	client   *kgo.Client
	dlqTopic string

	mu      sync.Mutex
	pending []*kgo.Record
}

// NewFranzConsumer constructs the worker-side Kafka adapter.
func NewFranzConsumer(client *kgo.Client, dlqTopic string) (*FranzConsumer, error) {
	if client == nil {
		return nil, fmt.Errorf("franz Kafka client is required")
	}
	if dlqTopic == "" {
		return nil, fmt.Errorf("Kafka DLQ topic is required")
	}
	return &FranzConsumer{client: client, dlqTopic: dlqTopic}, nil
}

// Receive polls Kafka only when the local ordered batch buffer is empty.
func (c *FranzConsumer) Receive(ctx context.Context) (Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.pending) == 0 {
		fetches := c.client.PollFetches(ctx)
		if errors := fetches.Errors(); len(errors) > 0 {
			return Delivery{}, fmt.Errorf("poll Kafka: %w", errors[0].Err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			c.pending = append(c.pending, record)
		})
	}
	if len(c.pending) == 0 {
		return Delivery{}, fmt.Errorf("poll Kafka returned no records")
	}
	record := c.pending[0]
	c.pending = c.pending[1:]
	rawPayload := append([]byte(nil), record.Value...)
	var envelope Envelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		return Delivery{RawPayload: rawPayload, DecodeError: fmt.Errorf("%w: decode Kafka JSON: %v", ErrInvalidEnvelope, err), Opaque: record}, nil
	}
	if envelope.TraceParent == "" {
		envelope.TraceParent = traceParentFromHeaders(record.Headers)
	}
	return Delivery{Envelope: envelope, RawPayload: rawPayload, Opaque: record}, nil
}

// Commit commits exactly the Kafka record supplied by Receive.
func (c *FranzConsumer) Commit(ctx context.Context, delivery Delivery) error {
	record, ok := delivery.Opaque.(*kgo.Record)
	if !ok || record == nil {
		return fmt.Errorf("Kafka delivery does not contain a record")
	}
	if err := c.client.CommitRecords(ctx, record); err != nil {
		return fmt.Errorf("commit Kafka record: %w", err)
	}
	return nil
}

// PublishDLQ stores the original versioned envelope and classified failure in
// the dedicated DLQ topic before the source offset may be committed.
func (c *FranzConsumer) PublishDLQ(ctx context.Context, deadLetter DeadLetter) error {
	value, err := json.Marshal(deadLetter)
	if err != nil {
		return fmt.Errorf("marshal Kafka dead letter: %w", err)
	}
	results := c.client.ProduceSync(ctx, &kgo.Record{
		Topic:   c.dlqTopic,
		Key:     deadLetter.Envelope.PartitionKey(),
		Value:   value,
		Headers: traceHeaders(deadLetter.Envelope.TraceParent),
	})
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka dead letter: %w", err)
	}
	return nil
}

func traceHeaders(traceParent string) []kgo.RecordHeader {
	if strings.TrimSpace(traceParent) == "" {
		return nil
	}
	return []kgo.RecordHeader{{Key: "traceparent", Value: []byte(traceParent)}}
}

func traceParentFromHeaders(headers []kgo.RecordHeader) string {
	for _, header := range headers {
		if strings.EqualFold(header.Key, "traceparent") {
			return strings.TrimSpace(string(header.Value))
		}
	}
	return ""
}
