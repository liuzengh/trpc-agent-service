package queue

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
	"github.com/google/uuid"
)

func TestRedisQueueAndLeaseIntegration(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	name := "trpc:test:" + uuid.NewString()
	redisQueue, err := NewRedisQueue(ctx, addr, name, "workers")
	if err != nil {
		t.Fatal(err)
	}
	defer redisQueue.Close()
	task := store.DispatchTask{ID: uuid.NewString()}
	if err := redisQueue.Publish(ctx, task); err != nil {
		t.Fatal(err)
	}
	delivery, err := redisQueue.Receive(ctx, "worker-1", time.Second)
	if err != nil || delivery.Task.ID != task.ID {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if err := redisQueue.Ack(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	locker := redisQueue.Locker()
	first, err := locker.Acquire(ctx, name, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := locker.Acquire(ctx, name, time.Minute); !errors.Is(err, ErrLeaseBusy) {
		t.Fatalf("second lease err=%v", err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := locker.Acquire(ctx, name, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release(ctx)
	if second.Fence() <= first.Fence() {
		t.Fatal("fencing token did not advance")
	}
}

func TestRedisQueueClaimsAbandonedPendingDelivery(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	name := "trpc:test:claim:" + uuid.NewString()
	redisQueue, err := NewRedisQueue(ctx, addr, name, "workers", WithClaimMinIdle(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer redisQueue.Close()
	task := store.DispatchTask{ID: uuid.NewString()}
	if err := redisQueue.Publish(ctx, task); err != nil {
		t.Fatal(err)
	}
	first, err := redisQueue.Receive(ctx, "worker-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	second, err := redisQueue.Receive(ctx, "worker-2", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.Task.ID != task.ID {
		t.Fatalf("claimed=%+v first=%+v", second, first)
	}
	if err := redisQueue.Ack(ctx, second); err != nil {
		t.Fatal(err)
	}
}
