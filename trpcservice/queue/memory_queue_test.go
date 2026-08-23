package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
)

func TestMemoryQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	q := NewMemoryQueue(0)
	task := store.DispatchTask{ID: "task"}
	if err := q.Publish(ctx, task); err != nil {
		t.Fatal(err)
	}
	delivery, err := q.Receive(ctx, "worker", 0)
	if err != nil || delivery.Task.ID != "task" || delivery.ID == "" {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if err := q.Retry(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	retried, err := q.Receive(ctx, "worker", 0)
	if err != nil || retried.Task.Attempts != 1 {
		t.Fatalf("retried=%+v err=%v", retried, err)
	}
	if err := q.Dead(ctx, retried, errors.New("failed")); err != nil {
		t.Fatal(err)
	}
	if dead := <-q.dead; dead.Task.ID != "task" {
		t.Fatalf("dead=%+v", dead)
	}
	if err := q.Ack(ctx, retried); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := q.Receive(ctx, "worker", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("closed receive error=%v", err)
	}
}
