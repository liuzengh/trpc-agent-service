package messaging

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

func TestRedisIMProgressHubPublishesCurrentFullText(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hub, err := NewRedisIMProgressHub(client)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events, closeSubscription, err := hub.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSubscription()
	want := IMProgressEvent{
		TenantID: "support", AppCode: "assistant", ConfigVersion: 3,
		Channel: channels.Telegram, BindingID: "bot-a", ConversationID: "chat-1", ConversationScope: channels.ConversationDirect,
		ProgressMessageID: "91", RequestID: "update-1", Content: "完整的当前回答",
	}
	if err := hub.PublishProgress(ctx, want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-events:
		if got.Content != want.Content || got.BindingID != want.BindingID || got.ProgressMessageID != want.ProgressMessageID {
			t.Fatalf("progress event = %#v, want %#v", got, want)
		}
	case <-ctx.Done():
		t.Fatal("progress event was not delivered")
	}
}

func TestIMProgressEventValidationMatrix(t *testing.T) {
	t.Parallel()
	valid := IMProgressEvent{
		TenantID: "support", AppCode: "assistant", ConfigVersion: 1,
		Channel: channels.Telegram, BindingID: "support-main", ConversationID: "customer-chat",
		ConversationScope: channels.ConversationDirect, ProgressMessageID: "progress-1",
		RequestID: "request-1", Content: "正在查询订单",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid progress event error = %v", err)
	}
	tests := []struct {
		name string
		edit func(*IMProgressEvent)
	}{
		{"tenant", func(e *IMProgressEvent) { e.TenantID = "" }},
		{"app", func(e *IMProgressEvent) { e.AppCode = "" }},
		{"version", func(e *IMProgressEvent) { e.ConfigVersion = 0 }},
		{"unsupported channel", func(e *IMProgressEvent) { e.Channel = channels.Channel("unknown") }},
		{"web channel", func(e *IMProgressEvent) { e.Channel = channels.Web }},
		{"binding", func(e *IMProgressEvent) { e.BindingID = "" }},
		{"conversation", func(e *IMProgressEvent) { e.ConversationID = "" }},
		{"progress message", func(e *IMProgressEvent) { e.ProgressMessageID = "" }},
		{"request", func(e *IMProgressEvent) { e.RequestID = "" }},
		{"content", func(e *IMProgressEvent) { e.Content = " " }},
		{"scope", func(e *IMProgressEvent) { e.ConversationScope = channels.ConversationScope("other") }},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			event := valid
			tt.edit(&event)
			if err := event.Validate(); err == nil {
				t.Fatal("invalid progress event error = nil")
			}
		})
	}
}

func TestRedisIMProgressHubValidatesClientAndPublishFailure(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisIMProgressHub(nil); err == nil {
		t.Fatal("nil Redis client error = nil")
	}
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hub, err := NewRedisIMProgressHub(client)
	if err != nil {
		t.Fatal(err)
	}
	invalid := IMProgressEvent{}
	if err := hub.PublishProgress(context.Background(), invalid); err == nil {
		t.Fatal("invalid event publish error = nil")
	}
	server.Close()
	valid := IMProgressEvent{
		TenantID: "support", AppCode: "assistant", ConfigVersion: 1,
		Channel: channels.Telegram, BindingID: "support-main", ConversationID: "customer-chat",
		ConversationScope: channels.ConversationDirect, ProgressMessageID: "progress-1",
		RequestID: "request-1", Content: "正在查询订单",
	}
	if err := hub.PublishProgress(context.Background(), valid); err == nil || !strings.Contains(err.Error(), "publish IM progress") {
		t.Fatalf("PublishProgress(redis unavailable) error = %v", err)
	}
}

func TestRedisIMProgressHubSubscribeRejectsUnavailableRedis(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hub, err := NewRedisIMProgressHub(client)
	if err != nil {
		t.Fatal(err)
	}
	server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := hub.Subscribe(ctx); err == nil || !strings.Contains(err.Error(), "subscribe IM progress") {
		t.Fatalf("Subscribe(unavailable Redis) error = %v", err)
	}
}
