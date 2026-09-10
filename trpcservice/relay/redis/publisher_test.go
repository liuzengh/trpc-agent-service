package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	redisclient "github.com/redis/go-redis/v9"
)

func TestPublisherRedis7Contract(t *testing.T) {
	client, cleanup := redisContractClient(t)
	t.Cleanup(cleanup)
	publisher, err := NewPublisher(client, Config{Environment: "relay-contract"})
	if err != nil {
		t.Fatal(err)
	}
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	if err := publisher.PublishReply(context.Background(), destination, channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", ConfigVersion: 1, DeliveryKey: "r1_reply", ContentRef: "result://request", Target: channel.DeliveryTarget{Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message"}, Final: true}); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishWakeup(context.Background(), relay.WakeupEvent{TenantID: "tenant", AggregateID: "request", IdempotencyKey: "wakeup:request", PayloadRef: "execution://tenant/request"}); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishTenantControl(context.Background(), relay.TenantControlEvent{TenantID: "tenant", Kind: "tenant-control", AggregateID: "tenant", IdempotencyKey: "tenant:2", PayloadRef: "tenant://tenant/2", Version: 2}); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishExecutionControl(context.Background(), relay.ExecutionControlEvent{TenantID: "tenant", Kind: "execution-control", AggregateID: "request", IdempotencyKey: "cancel:1", PayloadRef: "cancel-intent://tenant/request/1", Version: 1}); err != nil {
		t.Fatal(err)
	}
	queue, err := NewExecutionControlQueue(client, publisher, ExecutionControlQueueConfig{Group: "runtime-node", ReadBlock: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	delivered := make(chan struct{})
	consumeDone := make(chan error, 1)
	go func() {
		consumeDone <- queue.ConsumeExecutionControl(consumeCtx, relay.ExecutionControlConsumerOptions{ConsumerID: "worker-1"}, func(_ context.Context, delivery relay.ExecutionControlDelivery) error {
			if delivery.Event.AggregateID != "request" {
				t.Errorf("delivery=%#v", delivery)
			}
			close(delivered)
			return nil
		})
	}()
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("execution-control event was not delivered")
	}
	cancelConsume()
	select {
	case <-consumeDone:
	case <-time.After(time.Second):
		t.Fatal("execution-control consumer did not stop")
	}
	for _, stream := range []string{publisher.ReplyStream(destination), publisher.WakeupStream(), publisher.TenantControlStream(), publisher.ExecutionControlStream()} {
		if length, err := client.XLen(context.Background(), stream).Result(); err != nil || length < 1 {
			t.Fatalf("stream=%s length=%d err=%v", stream, length, err)
		}
		t.Cleanup(func() { _ = client.Del(context.Background(), stream).Err() })
	}
}

func TestWakeupQueueDeadLettersPoisonWithoutStopping(t *testing.T) {
	client, cleanup := redisContractClient(t)
	t.Cleanup(cleanup)
	publisher, err := NewPublisher(client, Config{Environment: fmt.Sprintf("wakeup_poison_%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewWakeupQueue(client, publisher, WakeupQueueConfig{Group: "workers", ReadBlock: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.XAdd(context.Background(), &redisclient.XAddArgs{Stream: publisher.WakeupStream(), Values: map[string]any{
		"schema_version": "1", "event": "{broken", "tenant_id": "tenant", "idempotency_key": "bad",
	}}).Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- queue.Consume(ctx, relay.WakeupConsumerOptions{ConsumerID: "worker"}, func(context.Context, relay.WakeupDelivery) error {
			t.Error("poison event reached handler")
			return nil
		})
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if length, _ := client.XLen(context.Background(), publisher.WakeupDeadLetterStream()).Result(); length == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if length, err := client.XLen(context.Background(), publisher.WakeupDeadLetterStream()).Result(); err != nil || length != 1 {
		t.Fatalf("dead-letter length=%d err=%v", length, err)
	}
	pending, err := client.XPending(context.Background(), publisher.WakeupStream(), "workers").Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wakeup consumer did not stop")
	}
	t.Cleanup(func() {
		_ = client.Del(context.Background(), publisher.WakeupStream(), publisher.WakeupDeadLetterStream()).Err()
	})
}

func TestReplyQueueReclaimsAdapterCrashWithoutEarlyACK(t *testing.T) {
	client, cleanup := redisContractClient(t)
	t.Cleanup(cleanup)
	publisher, err := NewPublisher(client, Config{Environment: fmt.Sprintf("reply_reclaim_%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", ConfigVersion: 1, DeliveryKey: "r1_reply", ContentRef: "result://request", Target: channel.DeliveryTarget{Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message"}, Final: true}
	if err := publisher.PublishReply(context.Background(), destination, event); err != nil {
		t.Fatal(err)
	}
	queue, err := NewReplyQueue(client, publisher, ReplyQueueConfig{Group: "adapter-owners", ReadBlock: 10 * time.Millisecond, ReclaimIdle: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	crash := errors.New("adapter crashed")
	err = queue.ConsumeReplies(context.Background(), destination, channel.ReplyConsumerOptions{ConsumerID: "owner-1"}, func(context.Context, channel.ReplyDelivery) error {
		return crash
	})
	if !errors.Is(err, crash) {
		t.Fatalf("consume err=%v", err)
	}
	pending, err := client.XPending(context.Background(), publisher.ReplyStream(destination), "adapter-owners").Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	var reclaimed []channel.ReplyDelivery
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reclaimed, err = queue.ReclaimReplies(context.Background(), destination, channel.ReplyConsumerOptions{ConsumerID: "owner-2"})
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
	if err := queue.AckReply(context.Background(), destination, reclaimed[0]); err != nil {
		t.Fatal(err)
	}
	pending, err = client.XPending(context.Background(), publisher.ReplyStream(destination), "adapter-owners").Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending after ACK=%#v err=%v", pending, err)
	}
	t.Cleanup(func() {
		_ = client.Del(context.Background(), publisher.ReplyStream(destination), publisher.ReplyStream(destination)+":dead-letter").Err()
	})
}

func TestReplyQueueDeadLettersCrossBindingEntry(t *testing.T) {
	client, cleanup := redisContractClient(t)
	t.Cleanup(cleanup)
	publisher, err := NewPublisher(client, Config{Environment: fmt.Sprintf("reply_poison_%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account"}
	stream := publisher.ReplyStream(destination)
	if err := client.XAdd(context.Background(), &redisclient.XAddArgs{Stream: stream, Values: map[string]any{
		"schema_version": "1", "event": `{"schema_version":1,"tenant_id":"other"}`, "tenant_id": "other", "binding_id": "binding", "delivery_key": "reply",
	}}).Err(); err != nil {
		t.Fatal(err)
	}
	queue, err := NewReplyQueue(client, publisher, ReplyQueueConfig{Group: "adapter-owners", ReadBlock: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- queue.ConsumeReplies(ctx, destination, channel.ReplyConsumerOptions{ConsumerID: "owner"}, func(context.Context, channel.ReplyDelivery) error {
			t.Error("poison reply reached Adapter")
			return nil
		})
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if length, _ := client.XLen(context.Background(), stream+":dead-letter").Result(); length == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if length, err := client.XLen(context.Background(), stream+":dead-letter").Result(); err != nil || length != 1 {
		t.Fatalf("dead-letter length=%d err=%v", length, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reply consumer did not stop")
	}
	t.Cleanup(func() { _ = client.Del(context.Background(), stream, stream+":dead-letter").Err() })
}

// redisContractClient keeps the Redis 7 path available through
// TRPC_REDIS_TEST_ADDR while making stream and reclaim contracts part of the
// ordinary unit/coverage suite. miniredis implements the Redis Stream and
// consumer-group operations used here; production Redis compatibility remains
// verified by the Compose admission job.
func redisContractClient(t *testing.T) (*redisclient.Client, func()) {
	t.Helper()
	if address := os.Getenv("TRPC_REDIS_TEST_ADDR"); address != "" {
		client := redisclient.NewClient(&redisclient.Options{Addr: address})
		return client, func() { _ = client.Close() }
	}
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	return client, func() {
		_ = client.Close()
		server.Close()
	}
}
