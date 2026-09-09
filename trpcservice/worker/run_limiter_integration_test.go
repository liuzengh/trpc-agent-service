package worker_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestRedisRunLimiterCoordinatesNodesAndExpiresPermits(t *testing.T) {
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
	prefix := fmt.Sprintf("pr18-test-%d", time.Now().UnixNano())
	first := &worker.RedisRunLimiter{Redis: shared, Prefix: prefix}
	second := &worker.RedisRunLimiter{Redis: shared, Prefix: prefix}

	one, acquired, err := first.TryAcquire(ctx, "tenant", "one", "claim-one", 2, time.Second)
	if err != nil || !acquired {
		t.Fatalf("one acquired=%v err=%v", acquired, err)
	}
	two, acquired, err := second.TryAcquire(ctx, "tenant", "two", "claim-two", 2, time.Second)
	if err != nil || !acquired {
		t.Fatalf("two acquired=%v err=%v", acquired, err)
	}
	if _, acquired, err := second.TryAcquire(ctx, "tenant", "three", "claim-three", 2, time.Second); err != nil || acquired {
		t.Fatalf("over quota acquired=%v err=%v", acquired, err)
	}
	// Shrinking the published quota does not interrupt existing runs, but no new
	// run enters until the active count falls below the new limit.
	if _, acquired, err := first.TryAcquire(ctx, "tenant", "three", "claim-three", 1, time.Second); err != nil || acquired {
		t.Fatalf("shrunken quota acquired=%v err=%v", acquired, err)
	}
	first.Release(one)
	second.Release(two)
	three, acquired, err := second.TryAcquire(ctx, "tenant", "three", "claim-three", 1, 60*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("after drain acquired=%v err=%v", acquired, err)
	}
	// Simulate a crashed Worker: no release occurs, and TTL makes capacity
	// available on another node.
	time.Sleep(100 * time.Millisecond)
	four, acquired, err := first.TryAcquire(ctx, "tenant", "four", "claim-four", 1, time.Second)
	if err != nil || !acquired {
		t.Fatalf("expired permit not recovered acquired=%v err=%v", acquired, err)
	}
	second.Release(three) // stale cleanup cannot remove the new member.
	if err := first.Renew(ctx, four, time.Second); err != nil {
		t.Fatalf("stale cleanup removed replacement: %v", err)
	}
	first.Release(four)
}
