//go:build integration

package redis_test

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
)

var redisTestURL = flag.String("redis-test-url", os.Getenv("TRPC_AGENT_SERVICE_REDIS_TEST_URL"), "Redis integration URL")

func TestStreamPublishesAndAcknowledgesDelivery(t *testing.T) {
	if *redisTestURL == "" {
		t.Skip("TRPC_AGENT_SERVICE_REDIS_TEST_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := platformredis.NewClient(ctx, *redisTestURL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	stream, err := platformredis.NewStream(client, "trpc-agent-service:test:"+uuid.NewString(), "workers", time.Millisecond)
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("init stream: %v", err)
	}
	dispatch := queue.Dispatch{
		OutboxID:    1,
		TenantID:    "tenant-a",
		AppID:       "support",
		RequestID:   "request-1",
		TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		TraceState:  "vendor=value",
	}
	if err := stream.Publish(ctx, dispatch); err != nil {
		t.Fatalf("publish: %v", err)
	}
	delivery, err := stream.Receive(ctx, "worker-1", time.Second)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if delivery.Dispatch != dispatch {
		t.Fatalf("dispatch = %#v, want %#v", delivery.Dispatch, dispatch)
	}
	if err := stream.Ack(ctx, delivery); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestStreamReclaimsAbandonedPendingDelivery(t *testing.T) {
	if *redisTestURL == "" {
		t.Skip("TRPC_AGENT_SERVICE_REDIS_TEST_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := platformredis.NewClient(ctx, *redisTestURL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	stream, err := platformredis.NewStream(client, "trpc-agent-service:reclaim-test:"+uuid.NewString(), "workers", 5*time.Millisecond)
	if err != nil {
		t.Fatalf("new stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("init stream: %v", err)
	}
	dispatch := queue.Dispatch{OutboxID: 1, TenantID: "tenant-a", AppID: "support", RequestID: "request-reclaim"}
	if err := stream.Publish(ctx, dispatch); err != nil {
		t.Fatalf("publish: %v", err)
	}
	first, err := stream.Receive(ctx, "worker-a", time.Second)
	if err != nil {
		t.Fatalf("first receive: %v", err)
	}
	// Simulate worker-a crashing before ACK. No local cleanup is performed.
	time.Sleep(20 * time.Millisecond)
	recovered, err := stream.Receive(ctx, "worker-b", time.Second)
	if err != nil {
		t.Fatalf("reclaim pending delivery: %v", err)
	}
	if recovered.ID != first.ID || recovered.Dispatch != dispatch {
		t.Fatalf("recovered delivery = %#v, want id=%q dispatch=%#v", recovered, first.ID, dispatch)
	}
	if err := stream.Ack(ctx, recovered); err != nil {
		t.Fatalf("ack recovered delivery: %v", err)
	}
}

func TestSessionLeaseSerializesOnePartition(t *testing.T) {
	if *redisTestURL == "" {
		t.Skip("TRPC_AGENT_SERVICE_REDIS_TEST_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := platformredis.NewClient(ctx, *redisTestURL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	first, err := platformredis.NewSessionLocker(client, time.Second)
	if err != nil {
		t.Fatalf("new first locker: %v", err)
	}
	second, err := platformredis.NewSessionLocker(client, time.Second)
	if err != nil {
		t.Fatalf("new second locker: %v", err)
	}
	partition := "tenant-a:app:support:session:" + uuid.NewString()
	lock, err := first.Lock(ctx, partition)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	blockedCtx, cancelBlocked := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancelBlocked()
	if _, err := second.Lock(blockedCtx, partition); err == nil {
		t.Fatal("second locker acquired a held partition")
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("release first lease: %v", err)
	}
	secondLock, err := second.Lock(ctx, partition)
	if err != nil {
		t.Fatalf("acquire released lease: %v", err)
	}
	if err := secondLock.Release(); err != nil {
		t.Fatalf("release second lease: %v", err)
	}
}
