package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	redisclient "github.com/redis/go-redis/v9"
)

func TestTenantControlQueueReclaimsCrashThenAcknowledges(t *testing.T) {
	client, cleanup := redisContractClient(t)
	t.Cleanup(cleanup)
	publisher, err := NewPublisher(client, Config{Environment: "tenant-control-reclaim"})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewTenantControlQueue(client, publisher, TenantControlQueueConfig{Group: "controllers", ReadBlock: time.Millisecond, ReclaimIdle: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	event := relay.TenantControlEvent{TenantID: "tenant", Kind: "tenant-config", AggregateID: "tenant", IdempotencyKey: "tenant:2", PayloadRef: "tenant://tenant/2", Version: 2}
	if err := publisher.PublishTenantControl(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("controller crashed")
	if err := queue.ConsumeTenantControl(context.Background(), relay.TenantControlConsumerOptions{ConsumerID: "controller-a"}, func(context.Context, relay.TenantControlDelivery) error {
		return crash
	}); !errors.Is(err, crash) {
		t.Fatalf("consume err=%v", err)
	}
	if pending, err := client.XPending(context.Background(), publisher.TenantControlStream(), "controllers").Result(); err != nil || pending.Count != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	var reclaimed []relay.TenantControlDelivery
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reclaimed, err = queue.ReclaimTenantControls(context.Background(), relay.TenantControlConsumerOptions{ConsumerID: "controller-b"})
		if err != nil {
			t.Fatal(err)
		}
		if len(reclaimed) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(reclaimed) != 1 || reclaimed[0].Event != event {
		t.Fatalf("reclaimed=%#v", reclaimed)
	}
	if err := queue.AckTenantControl(context.Background(), reclaimed[0]); err != nil {
		t.Fatal(err)
	}
	if pending, err := client.XPending(context.Background(), publisher.TenantControlStream(), "controllers").Result(); err != nil || pending.Count != 0 {
		t.Fatalf("pending after ack=%#v err=%v", pending, err)
	}
}

func TestExecutionControlQueueReclaimsCrashThenAcknowledges(t *testing.T) {
	client, cleanup := redisContractClient(t)
	t.Cleanup(cleanup)
	publisher, err := NewPublisher(client, Config{Environment: "execution-control-reclaim"})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewExecutionControlQueue(client, publisher, ExecutionControlQueueConfig{Group: "workers", ReadBlock: time.Millisecond, ReclaimIdle: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	event := relay.ExecutionControlEvent{TenantID: "tenant", Kind: "execution-control", AggregateID: "request", IdempotencyKey: "cancel:1", PayloadRef: "cancel://tenant/request/1", Version: 1}
	if err := publisher.PublishExecutionControl(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("worker crashed")
	if err := queue.ConsumeExecutionControl(context.Background(), relay.ExecutionControlConsumerOptions{ConsumerID: "worker-a"}, func(context.Context, relay.ExecutionControlDelivery) error {
		return crash
	}); !errors.Is(err, crash) {
		t.Fatalf("consume err=%v", err)
	}
	if pending, err := client.XPending(context.Background(), publisher.ExecutionControlStream(), "workers").Result(); err != nil || pending.Count != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	var reclaimed []relay.ExecutionControlDelivery
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reclaimed, err = queue.ReclaimExecutionControls(context.Background(), relay.ExecutionControlConsumerOptions{ConsumerID: "worker-b"})
		if err != nil {
			t.Fatal(err)
		}
		if len(reclaimed) == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(reclaimed) != 1 || reclaimed[0].Event != event {
		t.Fatalf("reclaimed=%#v", reclaimed)
	}
	if err := queue.AckExecutionControl(context.Background(), reclaimed[0]); err != nil {
		t.Fatal(err)
	}
	if pending, err := client.XPending(context.Background(), publisher.ExecutionControlStream(), "workers").Result(); err != nil || pending.Count != 0 {
		t.Fatalf("pending after ack=%#v err=%v", pending, err)
	}
}

func TestTenantControlQueueDeadLettersMalformedEvent(t *testing.T) {
	client, cleanup := redisContractClient(t)
	t.Cleanup(cleanup)
	publisher, err := NewPublisher(client, Config{Environment: "tenant-control-poison"})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewTenantControlQueue(client, publisher, TenantControlQueueConfig{Group: "controllers", ReadBlock: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.XAdd(context.Background(), &redisclient.XAddArgs{Stream: publisher.TenantControlStream(), Values: map[string]any{
		"event": "{invalid", "tenant_id": "tenant", "kind": "tenant-config", "version": "1", "idempotency_key": "bad",
	}}).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- queue.ConsumeTenantControl(ctx, relay.TenantControlConsumerOptions{ConsumerID: "controller"}, func(context.Context, relay.TenantControlDelivery) error {
			t.Error("poison tenant control reached handler")
			return nil
		})
	}()
	waitForRedisStreamLength(t, client, publisher.TenantControlDeadLetterStream(), 1)
	cancel()
	waitForConsumerExit(t, done)
}

func TestExecutionControlQueueDeadLettersMalformedEvent(t *testing.T) {
	client, cleanup := redisContractClient(t)
	t.Cleanup(cleanup)
	publisher, err := NewPublisher(client, Config{Environment: "execution-control-poison"})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewExecutionControlQueue(client, publisher, ExecutionControlQueueConfig{Group: "workers", ReadBlock: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.XAdd(context.Background(), &redisclient.XAddArgs{Stream: publisher.ExecutionControlStream(), Values: map[string]any{
		"event": "{invalid", "tenant_id": "tenant", "kind": "execution-control", "version": "1", "idempotency_key": "bad",
	}}).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- queue.ConsumeExecutionControl(ctx, relay.ExecutionControlConsumerOptions{ConsumerID: "worker"}, func(context.Context, relay.ExecutionControlDelivery) error {
			t.Error("poison execution control reached handler")
			return nil
		})
	}()
	waitForRedisStreamLength(t, client, publisher.ExecutionControlDeadLetterStream(), 1)
	cancel()
	waitForConsumerExit(t, done)
}

func waitForRedisStreamLength(t *testing.T, client *redisclient.Client, stream string, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if length, err := client.XLen(context.Background(), stream).Result(); err == nil && length == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	length, err := client.XLen(context.Background(), stream).Result()
	t.Fatalf("stream=%s length=%d want=%d err=%v", stream, length, want, err)
}

func waitForConsumerExit(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop")
	}
}
