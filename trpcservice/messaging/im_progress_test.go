package messaging

import (
	"context"
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
		TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 3,
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
