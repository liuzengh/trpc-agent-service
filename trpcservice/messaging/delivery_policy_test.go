package messaging

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

func TestDeliveryPolicySplitsTelegramWithoutLosingUnicode(t *testing.T) {
	server := miniredis.RunT(t)
	policy, err := NewRedisChannelDeliveryPolicy(redis.NewClient(&redis.Options{Addr: server.Addr()}))
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Repeat("你", 8100)
	parts, err := policy.Segments(channels.Telegram, text)
	if err != nil || len(parts) != 3 {
		t.Fatalf("Segments() parts=%d error=%v", len(parts), err)
	}
	if strings.Join(parts, "") != text {
		t.Fatal("segmentation changed message content")
	}
}

func TestDeliveryPolicyUsesSharedRedisPacingKey(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	policy, _ := NewRedisChannelDeliveryPolicy(client)
	target := channels.ReplyTarget{Channel: channels.Telegram, BindingID: "bot", ConversationID: "chat"}
	if err := policy.Wait(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if len(server.Keys()) != 1 {
		t.Fatalf("pacing keys = %v", server.Keys())
	}
}
