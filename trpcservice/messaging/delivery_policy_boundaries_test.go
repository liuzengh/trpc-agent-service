package messaging

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestDeliveryPolicySplitsUTF8ByteLimitedChannelsWithoutLosingContent(t *testing.T) {
	t.Parallel()
	policy := &RedisChannelDeliveryPolicy{}
	text := strings.Repeat("` }\n", 5000)
	for _, channel := range []channels.Channel{channels.WeCom, channels.Feishu} {
		channel := channel
		t.Run(string(channel), func(t *testing.T) {
			t.Parallel()
			parts, err := policy.Segments(channel, text)
			if err != nil {
				t.Fatalf("Segments() error = %v", err)
			}
			if len(parts) < 2 {
				t.Fatalf("Segments() produced %d part, want multiple parts", len(parts))
			}
			for index, part := range parts {
				if len(part) > 19_000 {
					t.Fatalf("part %d has %d UTF-8 bytes, want at most 19000", index, len(part))
				}
			}
			if strings.Join(parts, "") != text {
				t.Fatal("segmentation changed UTF-8 message content")
			}
		})
	}
}

func TestDeliveryPolicyValidatesConfigurationAndMessages(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisChannelDeliveryPolicy(nil); err == nil {
		t.Fatal("NewRedisChannelDeliveryPolicy(nil) succeeded")
	}
	policy := &RedisChannelDeliveryPolicy{}
	if _, err := policy.Segments(channels.Web, "  "); err == nil {
		t.Fatal("Segments() accepted blank text")
	}
	if _, err := policy.Segments(channels.Channel("unknown"), "reply"); err == nil {
		t.Fatal("Segments() accepted an unknown channel")
	}
	parts, err := policy.Segments(channels.Web, "reply")
	if err != nil || len(parts) != 1 || parts[0] != "reply" {
		t.Fatalf("Web Segments() = %#v, %v", parts, err)
	}
}

func TestDeliveryPolicyWaitHandlesUnpacedAndMissingRedis(t *testing.T) {
	t.Parallel()
	var policy *RedisChannelDeliveryPolicy
	if err := policy.Wait(context.Background(), channels.ReplyTarget{Channel: channels.Web}); err != nil {
		t.Fatalf("unpaced Web Wait() error = %v", err)
	}
	if err := policy.Wait(context.Background(), channels.ReplyTarget{Channel: channels.Telegram}); err == nil {
		t.Fatal("paced Telegram Wait() succeeded without Redis")
	}
}
