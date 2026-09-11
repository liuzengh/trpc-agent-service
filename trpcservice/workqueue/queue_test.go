package workqueue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestMemoryQueuePublishAckAndRetry(t *testing.T) {
	queue := NewMemoryQueue(2)
	t.Cleanup(func() { _ = queue.Close() })
	task := AgentTask{RequestID: "request-1"}
	if err := queue.Publish(context.Background(), task); err != nil {
		t.Fatalf("publish: %v", err)
	}
	delivery, err := queue.Receive(context.Background())
	if err != nil || delivery.Task().RequestID != task.RequestID {
		t.Fatalf("delivery=%v err=%v", delivery, err)
	}
	if err := delivery.Retry(context.Background()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	retried, err := queue.Receive(context.Background())
	if err != nil || retried.Task().Attempt != 1 {
		t.Fatalf("retried task=%+v err=%v", retried.Task(), err)
	}
	if err := retried.Ack(context.Background()); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestRedisQueuePublishReceiveAndAck(t *testing.T) {
	server := miniredis.RunT(t)
	queue, err := NewRedisQueue(context.Background(), RedisOptions{
		URL:          "redis://" + server.Addr() + "/0",
		KeyPrefix:    "queue-test",
		Stream:       "tasks",
		Group:        "workers",
		Consumer:     "worker-a",
		BlockTimeout: 20 * time.Millisecond,
		ClaimMinIdle: 20 * time.Millisecond,
		MaxLen:       100,
	})
	if err != nil {
		t.Fatalf("new Redis queue: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	if err := queue.Publish(context.Background(), AgentTask{RequestID: "request-1"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	delivery, err := queue.Receive(context.Background())
	if err != nil || delivery.Task().RequestID != "request-1" {
		t.Fatalf("delivery=%v err=%v", delivery, err)
	}
	if err := delivery.Ack(context.Background()); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestRedisQueueReclaimsPendingDelivery(t *testing.T) {
	server := miniredis.RunT(t)
	first := newRedisTestQueue(t, server, "worker-a")
	second := newRedisTestQueue(t, server, "worker-b")
	if err := first.Publish(context.Background(), AgentTask{RequestID: "recover"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := first.Receive(context.Background()); err != nil {
		t.Fatalf("first receive: %v", err)
	}
	// A live consumer now renews its delivery. Stop its transport to model
	// process loss before expecting another consumer to reclaim the task.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	reclaimed, err := second.Receive(context.Background())
	if err != nil || reclaimed.Task().RequestID != "recover" {
		t.Fatalf("reclaimed=%v err=%v", reclaimed, err)
	}
	if err := reclaimed.Ack(context.Background()); err != nil {
		t.Fatalf("ack reclaimed: %v", err)
	}
}

func newRedisTestQueue(
	t *testing.T,
	server *miniredis.Miniredis,
	consumer string,
) *RedisQueue {
	t.Helper()
	queue, err := NewRedisQueue(context.Background(), RedisOptions{
		URL:          "redis://" + server.Addr() + "/0",
		KeyPrefix:    "reclaim-test",
		Stream:       "tasks",
		Group:        "workers",
		Consumer:     consumer,
		BlockTimeout: 20 * time.Millisecond,
		ClaimMinIdle: 20 * time.Millisecond,
		MaxLen:       100,
	})
	if err != nil {
		t.Fatalf("new Redis queue: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	return queue
}
