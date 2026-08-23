package platform

import (
	"context"
	"testing"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/agent"
	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/config"
	"github.com/DocJlm/trpc-agent-service/trpcservice/queue"
	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

func TestOfflineMessagePipeline(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: "127.0.0.1:0",
		Database: config.Database{QueueName: "test", ConsumerGroup: "test"},
		Tenants: []tenant.Tenant{{
			ID: "tenant", Name: "Tenant", Enabled: true,
			Agent:   tenant.AgentProfile{ID: "assistant", Version: "1"},
			Backend: tenant.BackendProfile{Session: "memory"},
		}},
	}
	repository := store.NewMemoryRepository()
	memoryQueue := queue.NewMemoryQueue(16)
	ctx, cancel := context.WithCancel(context.Background())
	app, err := New(ctx, cfg, Options{
		Repository: repository, Queue: memoryQueue, Locker: queue.NewMemoryLocker(), Engine: agent.EchoEngine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	done := make(chan error, 2)
	go func() { done <- app.dispatchRelay(ctx) }()
	go func() { done <- app.worker(ctx) }()

	message := channels.InboundEnvelope{
		TenantID: "tenant", BindingID: "binding", Channel: "feishu",
		ExternalMessageID: "message-1", ExternalUserID: "user", ExternalConversationID: "chat",
		ConversationType: channels.ConversationP2P, Content: "hello", ReceivedAt: time.Now(),
		TraceID: "trace-1", ReplyToken: "message-1",
	}
	if err := app.acceptInbound(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := app.acceptInbound(ctx, message); err != nil {
		t.Fatalf("duplicate input should be a no-op: %v", err)
	}
	replyID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(message.IdempotencyKey()+"|reply")).String()
	deadline := time.Now().Add(3 * time.Second)
	for {
		exists, existsErr := repository.ReplyExists(ctx, replyID)
		if existsErr != nil {
			t.Fatal(existsErr)
		}
		if exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reply was not committed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats, err := repository.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.InboundTotal != 1 || stats.AuditTotal != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	cancel()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("pipeline shutdown: %v", err)
		}
	}
}
