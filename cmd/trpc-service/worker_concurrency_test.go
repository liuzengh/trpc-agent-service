package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
)

type blockingWorkerConsumer struct {
	eventID   string
	delivered atomic.Bool
}

func (c *blockingWorkerConsumer) Receive(ctx context.Context) (messaging.Delivery, error) {
	if c.delivered.Swap(true) {
		<-ctx.Done()
		return messaging.Delivery{}, ctx.Err()
	}
	select {
	case <-ctx.Done():
		return messaging.Delivery{}, ctx.Err()
	default:
	}
	return messaging.Delivery{Envelope: messaging.Envelope{
		Version:    messaging.CurrentEnvelopeVersion,
		EventID:    c.eventID,
		TenantID:   "tenant-a",
		SessionKey: "tenant-a/support/session/" + c.eventID,
		Type:       "inbound.message",
		Payload:    []byte(`{"message_id":"` + c.eventID + `"}`),
	}}, nil
}

func (*blockingWorkerConsumer) Commit(context.Context, messaging.Delivery) error       { return nil }
func (*blockingWorkerConsumer) PublishDLQ(context.Context, messaging.DeadLetter) error { return nil }

func TestStartKafkaWorkersRunsIndependentConsumersConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	processor := messaging.ProcessorFunc(func(_ context.Context, envelope messaging.Envelope) error {
		started <- envelope.EventID
		<-release
		return nil
	})

	workers := make([]*messaging.Worker, 0, 2)
	for _, eventID := range []string{"message-a", "message-b"} {
		worker, err := messaging.NewWorker(&blockingWorkerConsumer{eventID: eventID}, processor, 3)
		if err != nil {
			t.Fatal(err)
		}
		workers = append(workers, worker)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errors, done := startKafkaWorkers(ctx, workers)

	seen := map[string]bool{}
	deadline := time.After(time.Second)
	for len(seen) < 2 {
		select {
		case eventID := <-started:
			seen[eventID] = true
		case err := <-errors:
			t.Fatalf("worker stopped before both slots ran: %v", err)
		case <-deadline:
			t.Fatalf("workers did not run concurrently; started=%v", seen)
		}
	}
	close(release)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker set did not stop after cancellation")
	}
}

func TestWorkerSetLifecycleAggregatesAllSlots(t *testing.T) {
	processorStarted := make(chan struct{}, 2)
	release := make(chan struct{})
	workers := make([]*messaging.Worker, 0, 2)
	for _, eventID := range []string{"message-a", "message-b"} {
		worker, err := messaging.NewWorker(&blockingWorkerConsumer{eventID: eventID}, messaging.ProcessorFunc(func(context.Context, messaging.Envelope) error {
			processorStarted <- struct{}{}
			<-release
			return nil
		}), 3)
		if err != nil {
			t.Fatal(err)
		}
		workers = append(workers, worker)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errors, done := startKafkaWorkers(ctx, workers)
	for range 2 {
		select {
		case <-processorStarted:
		case err := <-errors:
			t.Fatalf("worker failed: %v", err)
		case <-time.After(time.Second):
			t.Fatal("worker did not start")
		}
	}
	if got := activeWorkerDeliveries(workers); got != 2 {
		t.Fatalf("activeWorkerDeliveries() = %d, want 2", got)
	}
	beginWorkerDrain(workers)
	waitDone := make(chan error, 1)
	go func() { waitDone <- waitWorkerDrain(context.Background(), workers) }()
	select {
	case err := <-waitDone:
		t.Fatalf("waitWorkerDrain returned before active work finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker drain did not finish")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker set did not stop")
	}
}

func TestWorkerConcurrencyEnvironmentIsBounded(t *testing.T) {
	tests := []struct {
		value   string
		want    int
		wantErr bool
	}{
		{"", defaultWorkerConcurrency, false},
		{"8", 8, false},
		{"0", 0, true},
		{"65", 0, true},
		{"invalid", 0, true},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			got, err := workerConcurrency(func(name string) string {
				if name == "WORKER_CONCURRENCY" {
					return test.value
				}
				return ""
			})
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("workerConcurrency(%q) = %d, %v", test.value, got, err)
			}
		})
	}
}

var _ messaging.Consumer = (*blockingWorkerConsumer)(nil)
