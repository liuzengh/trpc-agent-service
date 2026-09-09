package messaging

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

func TestRedis7ReliableMessagingSmoke(t *testing.T) {
	rawURL := os.Getenv("PHASE3_REDIS_SMOKE_URL")
	if rawURL == "" {
		t.Skip("PHASE3_REDIS_SMOKE_URL is not set")
	}
	redisURL, err := config.NormalizeRedisURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.MessagingConfig{
		RedisURL: redisURL, KeyPrefix: fmt.Sprintf("phase3-smoke:%d", time.Now().UnixNano()),
		LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Minute, ReplyWaitTimeout: time.Second,
	}
	store, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupSmokeKeys(t, store)
		_ = store.Close()
	})
	ctx := context.Background()
	if err := store.Ready(ctx); err != nil {
		t.Fatal(err)
	}

	task := testTask("smoke-complete", "smoke-message-complete")
	if _, created, err := store.Submit(ctx, task); err != nil || !created {
		t.Fatalf("Submit() = (created=%v, err=%v)", created, err)
	}
	delivery, err := store.ReadTask(ctx, "smoke-worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(ctx, delivery, "smoke-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Heartbeat(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, lease, message.OutboundMessage{
		Channel: task.Channel, BindingID: task.ChannelBindingID, RequestID: task.RequestID,
		TraceID: task.TraceID, SessionID: task.SessionID, Text: "redis-7-ok",
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx, task.InboxID())
	if err != nil || snapshot.Result == nil || snapshot.Result.Reply.Text != "redis-7-ok" {
		t.Fatalf("terminal snapshot = (%#v, %v)", snapshot, err)
	}
	reply, err := store.ReadReply(ctx, "smoke-gateway", time.Second)
	if err != nil || reply.Result.TaskID != task.TaskID {
		t.Fatalf("ReadReply() = (%#v, %v)", reply, err)
	}
	if err := store.AckReply(ctx, reply.StreamID); err != nil {
		t.Fatal(err)
	}

	staleTask := testTask("smoke-stale", "smoke-message-stale")
	if _, _, err := store.Submit(ctx, staleTask); err != nil {
		t.Fatal(err)
	}
	staleDelivery, err := store.ReadTask(ctx, "smoke-worker-lost", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(ctx, staleDelivery, "smoke-worker-lost"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(cfg.LeaseDuration + 100*time.Millisecond)
	claimed, err := store.ClaimStale(ctx, "smoke-worker-recovery", 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimStale() = (%#v, %v)", claimed, err)
	}
	if err := store.Recover(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.ReadTask(ctx, "smoke-worker-recovery", time.Second)
	if err != nil || recovered.Task.Attempt != 2 {
		t.Fatalf("recovered delivery = (%#v, %v)", recovered, err)
	}

	pauseCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := store.client.Do(ctx, "CLIENT", "PAUSE", 300, "ALL").Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.Ready(pauseCtx); err == nil {
		t.Fatal("Ready() unexpectedly succeeded while Redis was paused")
	}
	time.Sleep(350 * time.Millisecond)
	if err := store.Ready(ctx); err != nil {
		t.Fatalf("Ready() did not recover: %v", err)
	}
}

func cleanupSmokeKeys(t *testing.T, store *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var cursor uint64
	for {
		keys, next, err := store.client.Scan(ctx, cursor, store.basePrefix+":*", 100).Result()
		if err != nil {
			t.Logf("scan smoke keys: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := store.client.Del(ctx, keys...).Err(); err != nil && err != redis.Nil {
				t.Logf("delete smoke keys: %v", err)
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}
