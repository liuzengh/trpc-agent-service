package cluster_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/cluster"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

func TestRedisStreamDistributesWorkAcrossNodes(t *testing.T) {
	redisURL := os.Getenv("TRPC_AGENT_REDIS_TEST_URL")
	if redisURL == "" {
		t.Skip("set TRPC_AGENT_REDIS_TEST_URL to run Redis integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	shared, err := backend.OpenRedis(ctx, redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()
	prefix := fmt.Sprintf("pr11-test-%d", time.Now().UnixNano())
	var mu sync.Mutex
	executions := make(map[string]int)
	nodes := make(map[string]int)
	handler := func(node string) cluster.RunHandler {
		return func(_ context.Context, request gateway.RunRequest) error {
			mu.Lock()
			executions[request.InboxID]++
			nodes[node]++
			mu.Unlock()
			return nil
		}
	}
	first, err := cluster.NewWorkQueue(ctx, shared, handler("node-a"), cluster.WorkQueueConfig{NodeID: "node-a", Prefix: prefix, Concurrency: 2, ReadBlock: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	second, err := cluster.NewWorkQueue(ctx, shared, handler("node-b"), cluster.WorkQueueConfig{NodeID: "node-b", Prefix: prefix, Concurrency: 2, ReadBlock: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	defer second.Close(context.Background())
	for index := 0; index < 50; index++ {
		id := fmt.Sprintf("request-%d", index)
		if err := first.Submit(gateway.RunRequest{InboxID: id, ClaimToken: "claim-" + id}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(executions)
		mu.Unlock()
		if count == 50 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(executions) != 50 {
		t.Fatalf("executions=%d nodes=%v", len(executions), nodes)
	}
	for requestID, count := range executions {
		if count != 1 {
			t.Fatalf("request %q executions=%d", requestID, count)
		}
	}
	if len(nodes) != 2 {
		t.Fatalf("work was not shared by both nodes: %v", nodes)
	}
}

func TestRedisProducerOnlyGatewayQueuesUntilWorkerStarts(t *testing.T) {
	redisURL := os.Getenv("TRPC_AGENT_REDIS_TEST_URL")
	if redisURL == "" {
		t.Skip("set TRPC_AGENT_REDIS_TEST_URL to run Redis integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	shared, err := backend.OpenRedis(ctx, redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()
	prefix := fmt.Sprintf("pr19-role-test-%d", time.Now().UnixNano())
	producer, err := cluster.NewWorkSubmitter(ctx, shared, prefix)
	if err != nil {
		t.Fatal(err)
	}
	request := gateway.RunRequest{InboxID: "queued-before-worker", ClaimToken: "claim"}
	if err := producer.Submit(request); err != nil {
		t.Fatal(err)
	}
	executed := make(chan gateway.RunRequest, 1)
	// No consumer exists yet: the Gateway producer must not execute the request.
	select {
	case got := <-executed:
		t.Fatalf("request executed without a Worker: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
	consumer, err := cluster.NewWorkQueue(ctx, shared, func(_ context.Context, got gateway.RunRequest) error {
		executed <- got
		return nil
	}, cluster.WorkQueueConfig{NodeID: "worker-a", Prefix: prefix, Concurrency: 1, ReadBlock: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close(context.Background())
	select {
	case got := <-executed:
		if got.InboxID != request.InboxID || got.ClaimToken != request.ClaimToken {
			t.Fatalf("request=%+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued request was not recovered by the Worker")
	}
}
