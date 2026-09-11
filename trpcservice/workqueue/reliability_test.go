package workqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func reliabilityQueue(t *testing.T, server *miniredis.Miniredis, capacity int64, idle time.Duration) *RedisQueue {
	t.Helper()
	q, err := NewRedisQueue(context.Background(), RedisOptions{URL: "redis://" + server.Addr() + "/0", KeyPrefix: "safe", Stream: "tasks", Group: "workers", Consumer: "configured-same-prefix", BlockTimeout: time.Millisecond, ClaimMinIdle: idle, MaxLen: capacity})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func TestQueueKeepsPendingAndUnreadAtCapacity(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	q := reliabilityQueue(t, server, 2, time.Second)
	if err := q.Publish(ctx, AgentTask{RequestID: "first"}); err != nil {
		t.Fatal(err)
	}
	d, err := q.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(ctx, AgentTask{RequestID: "second"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := q.Publish(ctx, AgentTask{RequestID: "third"}); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("expected backpressure: %v", err)
		}
	}
	if n := q.client.XLen(ctx, q.stream).Val(); n != 2 {
		t.Fatalf("pending/unread records trimmed: %d", n)
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(ctx, AgentTask{RequestID: "third"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"second", "third"} {
		next, err := q.Receive(ctx)
		if err != nil || next.Task().RequestID != want {
			t.Fatalf("lost task %s: %v", want, err)
		}
		if err := next.Ack(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLiveDeliveryRenewsAndOldConsumerCannotAckReclaimedTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := miniredis.RunT(t)
	first := reliabilityQueue(t, server, 10, 150*time.Millisecond)
	second := reliabilityQueue(t, server, 10, 150*time.Millisecond)
	if first.consumer == second.consumer {
		t.Fatal("configuration reused transport ownership identity")
	}
	if err := first.Publish(ctx, AgentTask{RequestID: "long"}); err != nil {
		t.Fatal(err)
	}
	d, err := first.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	lease := d.(LeasedDelivery)
	time.Sleep(400 * time.Millisecond)
	if _, err := second.Receive(ctx); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("live task reclaimed: %v", err)
	}
	if lease.Context().Err() != nil {
		t.Fatal("healthy delivery lost its lease")
	}
	lease.Close() // abandoned handler, not an ACK
	time.Sleep(180 * time.Millisecond)
	next, err := second.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(ctx); !errors.Is(err, ErrDeliveryOwnership) {
		t.Fatalf("stale consumer ACK: %v", err)
	}
	if n := first.client.XPending(ctx, first.stream, first.group).Val().Count; n != 1 {
		t.Fatal("stale ACK removed new owner's task")
	}
	if err := next.Retry(ctx); err != nil {
		t.Fatal(err)
	}
	retried, err := first.Receive(ctx)
	if err != nil || retried.Task().Attempt != 1 {
		t.Fatalf("atomic retry missing: %v", err)
	}
	if err := retried.Ack(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOwnershipLossCancelsDeliveryContext(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	first := reliabilityQueue(t, server, 10, 100*time.Millisecond)
	second := reliabilityQueue(t, server, 10, 100*time.Millisecond)
	if err := first.Publish(ctx, AgentTask{RequestID: "stolen"}); err != nil {
		t.Fatal(err)
	}
	d, err := first.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw := d.(*redisDelivery)
	if err := second.client.XClaim(ctx, &redis.XClaimArgs{Stream: first.stream, Group: first.group, Consumer: second.consumer, MinIdle: 0, Messages: []string{raw.messageID}}).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-raw.Context().Done():
		if !errors.Is(context.Cause(raw.Context()), ErrDeliveryOwnership) {
			t.Fatal("wrong cancellation cause")
		}
	case <-time.After(time.Second):
		t.Fatal("ownership loss did not cancel runtime context")
	}
	if err := raw.Retry(ctx); !errors.Is(err, ErrDeliveryOwnership) {
		t.Fatalf("stale retry published duplicate: %v", err)
	}
}

func TestQueueRefusesToTrimAnotherConsumerGroup(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	q := reliabilityQueue(t, server, 1, time.Second)
	if err := q.Publish(ctx, AgentTask{RequestID: "keep"}); err != nil {
		t.Fatal(err)
	}
	if err := q.client.XGroupCreate(ctx, q.stream, "external-group", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(ctx, AgentTask{RequestID: "new"}); err == nil {
		t.Fatal("shared group must fail closed")
	}
	if q.client.XLen(ctx, q.stream).Val() != 1 {
		t.Fatal("conflicting group lost history")
	}
}
