package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
)

func TestOutboxRelayPublishesAcceptedTask(t *testing.T) {
	journal := NewMemoryJournal()
	queue := workqueue.NewMemoryQueue(4)
	t.Cleanup(func() {
		_ = queue.Close()
		_ = journal.Close()
	})
	if _, err := journal.Accept(
		context.Background(),
		testInboundRequest(t, "relay-message", "hello"),
	); err != nil {
		t.Fatalf("accept: %v", err)
	}
	relay, err := NewOutboxRelay(journal, queue, RelayOptions{
		WorkerID:     "relay-1",
		BatchSize:    10,
		ClaimLease:   time.Second,
		PollInterval: time.Second,
		RetryDelay:   time.Second,
	})
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	published, err := relay.RelayOnce(context.Background())
	if err != nil || published != 1 {
		t.Fatalf("published=%d err=%v", published, err)
	}
	delivery, err := queue.Receive(context.Background())
	if err != nil || delivery.Task().MessageID != "relay-message" {
		t.Fatalf("delivery=%v err=%v", delivery, err)
	}
	if err := delivery.Ack(context.Background()); err != nil {
		t.Fatalf("ack: %v", err)
	}
	again, err := relay.RelayOnce(context.Background())
	if err != nil || again != 0 {
		t.Fatalf("second relay=%d err=%v", again, err)
	}
}
