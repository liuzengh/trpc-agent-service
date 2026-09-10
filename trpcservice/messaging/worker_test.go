package messaging

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// scriptedConsumer is a deterministic Consumer that replays a prepared queue and
// records every commit and DLQ handoff.
type scriptedConsumer struct {
	deliveries []Delivery
	commits    int
	dlqs       []DeadLetter
}

func (c *scriptedConsumer) Receive(ctx context.Context) (Delivery, error) {
	if err := ctx.Err(); err != nil {
		return Delivery{}, err
	}
	if len(c.deliveries) == 0 {
		return Delivery{}, io.EOF
	}
	next := c.deliveries[0]
	c.deliveries = c.deliveries[1:]
	return next, nil
}

func (c *scriptedConsumer) Commit(_ context.Context, _ Delivery) error {
	c.commits++
	return nil
}

func (c *scriptedConsumer) PublishDLQ(_ context.Context, deadLetter DeadLetter) error {
	c.dlqs = append(c.dlqs, deadLetter)
	return nil
}

func validEnvelope(attempt int) Envelope {
	return Envelope{
		Version:    CurrentEnvelopeVersion,
		EventID:    "event-1",
		TenantID:   "tenant-a",
		SessionKey: "tenant-a/support/telegram/chat-1",
		Type:       "inbound",
		Payload:    []byte(`{"binding_id":"telegram-bot-a"}`),
		Attempt:    attempt,
	}
}

func TestWorkerCommitsOnlyAfterSuccessfulProcessing(t *testing.T) {
	t.Parallel()

	consumer := &scriptedConsumer{deliveries: []Delivery{{Envelope: validEnvelope(0)}}}
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error { return nil }), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if consumer.commits != 1 {
		t.Fatalf("commits = %d, want 1", consumer.commits)
	}
	if len(consumer.dlqs) != 0 {
		t.Fatalf("DLQ handoffs = %d, want 0", len(consumer.dlqs))
	}
}

func TestWorkerRetryableErrorLeavesMessageUncommitted(t *testing.T) {
	t.Parallel()

	consumer := &scriptedConsumer{deliveries: []Delivery{{Envelope: validEnvelope(0)}}}
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error {
		return Retryable(errors.New("transient"))
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	err = worker.RunOnce(context.Background())
	if !errors.Is(err, ErrRetryScheduled) {
		t.Fatalf("RunOnce() error = %v, want ErrRetryScheduled", err)
	}
	if consumer.commits != 0 {
		t.Fatalf("commits = %d, want 0 on retryable failure", consumer.commits)
	}
	if len(consumer.dlqs) != 0 {
		t.Fatalf("DLQ handoffs = %d, want 0 on retryable failure", len(consumer.dlqs))
	}
}

func TestWorkerPermanentErrorGoesToDLQAndCommits(t *testing.T) {
	t.Parallel()

	envelope := validEnvelope(0)
	envelope.TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	consumer := &scriptedConsumer{deliveries: []Delivery{{Envelope: envelope}}}
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error {
		return Permanent(errors.New("poison message"))
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v, want nil after DLQ handoff", err)
	}
	if consumer.commits != 1 {
		t.Fatalf("commits = %d, want 1 after DLQ", consumer.commits)
	}
	if len(consumer.dlqs) != 1 || consumer.dlqs[0].ErrorClass != "permanent" {
		t.Fatalf("DLQ = %+v, want one permanent dead letter", consumer.dlqs)
	}
	// The dead letter must preserve the original trace context so a later
	// replay can resume the same distributed trace.
	if got, want := consumer.dlqs[0].Envelope.TraceParent, envelope.TraceParent; got != want {
		t.Fatalf("DLQ traceparent = %q, want %q", got, want)
	}
}

func TestWorkerExhaustedRetriesGoToDLQ(t *testing.T) {
	t.Parallel()

	// maxAttempts=3 means attempts 0..2 may retry; attempt=2 exhausts them.
	consumer := &scriptedConsumer{deliveries: []Delivery{{Envelope: validEnvelope(2)}}}
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error {
		return Retryable(errors.New("still down"))
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v, want nil after exhausted DLQ", err)
	}
	if len(consumer.dlqs) != 1 || consumer.dlqs[0].ErrorClass != "retry_exhausted" {
		t.Fatalf("DLQ = %+v, want one retry_exhausted dead letter", consumer.dlqs)
	}
	if consumer.commits != 1 {
		t.Fatalf("commits = %d, want 1 after exhausted DLQ", consumer.commits)
	}
}

func TestWorkerInvalidEnvelopeGoesToDLQWithoutProcessing(t *testing.T) {
	t.Parallel()

	invalid := validEnvelope(0)
	invalid.SessionKey = "other-tenant/session"
	processed := false
	consumer := &scriptedConsumer{deliveries: []Delivery{{Envelope: invalid}}}
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error {
		processed = true
		return nil
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v, want nil after invalid-envelope DLQ", err)
	}
	if processed {
		t.Fatal("processor ran for invalid envelope, want skipped")
	}
	if len(consumer.dlqs) != 1 || consumer.dlqs[0].ErrorClass != "invalid_envelope" {
		t.Fatalf("DLQ = %+v, want one invalid_envelope dead letter", consumer.dlqs)
	}
}

func TestWorkerMalformedKafkaJSONGoesToDLQBeforeLaterRecords(t *testing.T) {
	t.Parallel()

	malformed := []byte(`{not-json`)
	consumer := &scriptedConsumer{deliveries: []Delivery{
		{RawPayload: malformed, DecodeError: ErrInvalidEnvelope},
		{Envelope: validEnvelope(0)},
	}}
	processed := 0
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error {
		processed++
		return nil
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("malformed RunOnce() error = %v", err)
	}
	if processed != 0 || consumer.commits != 1 {
		t.Fatalf("after malformed record: processed=%d commits=%d, want 0/1", processed, consumer.commits)
	}
	if len(consumer.dlqs) != 1 || string(consumer.dlqs[0].RawPayload) != string(malformed) || consumer.dlqs[0].ErrorClass != "invalid_json" {
		t.Fatalf("DLQ = %+v, want original malformed payload", consumer.dlqs)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("later valid RunOnce() error = %v", err)
	}
	if processed != 1 || consumer.commits != 2 {
		t.Fatalf("after valid record: processed=%d commits=%d, want 1/2", processed, consumer.commits)
	}
}

// TestWorkerRetriesTheSameDeliveryBeforeAdvancing verifies that a retryable
// failure cannot let a later message overtake an earlier message from the same
// consumer. The retry attempt is observable by the processor.
func TestWorkerRetriesTheSameDeliveryBeforeAdvancing(t *testing.T) {
	t.Parallel()

	first := validEnvelope(0)
	first.EventID = "event-first"
	second := validEnvelope(0)
	second.EventID = "event-second"
	consumer := &scriptedConsumer{deliveries: []Delivery{{Envelope: first}, {Envelope: second}}}
	seen := make([]Envelope, 0, 2)
	worker, err := NewWorker(consumer, ProcessorFunc(func(_ context.Context, envelope Envelope) error {
		seen = append(seen, envelope)
		if len(seen) == 1 {
			return Retryable(errors.New("transient"))
		}
		return nil
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if err := worker.RunOnce(context.Background()); !errors.Is(err, ErrRetryScheduled) {
		t.Fatalf("first RunOnce() error = %v, want ErrRetryScheduled", err)
	}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("second RunOnce() error = %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("processed deliveries = %d, want 2", len(seen))
	}
	if seen[1].EventID != first.EventID {
		t.Fatalf("second delivery = %q, want retry of %q", seen[1].EventID, first.EventID)
	}
	if seen[1].Attempt != 1 {
		t.Fatalf("retry attempt = %d, want 1", seen[1].Attempt)
	}
	if consumer.commits != 1 {
		t.Fatalf("commits = %d, want only retried first delivery committed", consumer.commits)
	}
}

func TestWorkerDrainLeavesNewDeliveryUncommitted(t *testing.T) {
	t.Parallel()

	consumer := &scriptedConsumer{deliveries: []Delivery{{Envelope: validEnvelope(0)}}}
	processed := 0
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error {
		processed++
		return nil
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	worker.BeginDrain()
	if err := worker.RunOnce(context.Background()); !errors.Is(err, ErrWorkerDraining) {
		t.Fatalf("RunOnce() error = %v, want ErrWorkerDraining", err)
	}
	if processed != 0 {
		t.Fatalf("processed = %d, want 0 after drain begins", processed)
	}
	if consumer.commits != 0 || len(consumer.dlqs) != 0 {
		t.Fatalf("commits=%d dlq=%d, want delivery left untouched for Kafka rebalance", consumer.commits, len(consumer.dlqs))
	}
	if worker.ActiveDeliveries() != 0 {
		t.Fatalf("active deliveries = %d, want 0", worker.ActiveDeliveries())
	}
}

func TestWorkerDrainWaitsForActiveDeliveryThroughCommit(t *testing.T) {
	t.Parallel()

	consumer := &scriptedConsumer{deliveries: []Delivery{{Envelope: validEnvelope(0)}}}
	started := make(chan struct{})
	release := make(chan struct{})
	worker, err := NewWorker(consumer, ProcessorFunc(func(context.Context, Envelope) error {
		close(started)
		<-release
		return nil
	}), 3)
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- worker.RunOnce(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start delivery")
	}
	worker.BeginDrain()
	if got := worker.ActiveDeliveries(); got != 1 {
		t.Fatalf("active deliveries = %d, want 1 while current delivery is running", got)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := worker.WaitDrain(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitDrain() error = %v, want deadline while delivery is active", err)
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if consumer.commits != 1 {
		t.Fatalf("commits = %d, want active delivery committed before drain completes", consumer.commits)
	}
	waitCtx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := worker.WaitDrain(waitCtx); err != nil {
		t.Fatalf("WaitDrain() error = %v", err)
	}
}
