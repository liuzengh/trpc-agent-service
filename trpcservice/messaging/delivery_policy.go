package messaging

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

// ChannelDeliveryPolicy is the single platform policy for reply segmentation
// and distributed pacing. Provider senders stay focused on protocol I/O.
type ChannelDeliveryPolicy interface {
	Segments(channels.Channel, string) ([]string, error)
	Wait(context.Context, channels.ReplyTarget) error
}

type RedisChannelDeliveryPolicy struct {
	client redis.UniversalClient
}

func NewRedisChannelDeliveryPolicy(client redis.UniversalClient) (*RedisChannelDeliveryPolicy, error) {
	if client == nil {
		return nil, fmt.Errorf("channel delivery Redis client is required")
	}
	return &RedisChannelDeliveryPolicy{client: client}, nil
}

func (p *RedisChannelDeliveryPolicy) Segments(channel channels.Channel, text string) ([]string, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("channel reply text is required")
	}
	switch channel {
	case channels.Web:
		return []string{text}, nil
	case channels.Telegram:
		// Telegram sendMessage accepts at most 4096 characters. Keep a small
		// margin so future formatting/entity handling cannot cross the boundary.
		return splitByRunes(text, 4000), nil
	case channels.WeCom:
		// WeCom Smart Bot markdown is capped at 20480 UTF-8 bytes.
		return splitByUTF8Bytes(text, 19_000), nil
	case channels.Feishu:
		// Keep external IM replies bounded even when the provider allows a larger
		// body; this avoids giant cards/messages and gives retries small units.
		return splitByUTF8Bytes(text, 19_000), nil
	default:
		return nil, fmt.Errorf("unsupported delivery channel %q", channel)
	}
}

var channelPaceScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local next_at = tonumber(redis.call('GET', KEYS[1]))
if next_at and next_at > now then
  return next_at - now
end
redis.call('SET', KEYS[1], now + interval, 'PX', interval * 3)
return 0
`)

func (p *RedisChannelDeliveryPolicy) Wait(ctx context.Context, target channels.ReplyTarget) error {
	interval := channelPace(target.Channel)
	if interval <= 0 {
		return nil
	}
	if p == nil || p.client == nil {
		return fmt.Errorf("channel delivery policy is not configured")
	}
	digest := sha256.Sum256([]byte(string(target.Channel) + "\x00" + target.BindingID + "\x00" + target.ConversationID))
	key := fmt.Sprintf("trpc-agent:channel-pace:%x", digest[:16])
	for {
		waitMillis, err := channelPaceScript.Run(ctx, p.client, []string{key}, time.Now().UnixMilli(), interval.Milliseconds()).Int64()
		if err != nil {
			return fmt.Errorf("pace channel delivery: %w", err)
		}
		if waitMillis <= 0 {
			return nil
		}
		timer := time.NewTimer(time.Duration(waitMillis) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func channelPace(channel channels.Channel) time.Duration {
	switch channel {
	case channels.Telegram:
		return time.Second
	case channels.WeCom, channels.Feishu:
		return 200 * time.Millisecond
	default:
		return 0
	}
}

func splitByRunes(text string, limit int) []string {
	if utf8.RuneCountInString(text) <= limit {
		return []string{text}
	}
	runes := []rune(text)
	parts := make([]string, 0, (len(runes)+limit-1)/limit)
	for len(runes) > 0 {
		end := min(limit, len(runes))
		end = preferredRuneBreak(runes, end)
		parts = append(parts, string(runes[:end]))
		runes = runes[end:]
	}
	return parts
}

func splitByUTF8Bytes(text string, limit int) []string {
	if len(text) <= limit {
		return []string{text}
	}
	parts := make([]string, 0, len(text)/limit+1)
	for len(text) > 0 {
		if len(text) <= limit {
			parts = append(parts, text)
			break
		}
		end := limit
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		if cut := preferredByteBreak(text[:end]); cut > 0 {
			end = cut
		}
		parts = append(parts, text[:end])
		text = text[end:]
	}
	return parts
}

func preferredRuneBreak(runes []rune, end int) int {
	if end == len(runes) {
		return end
	}
	minimum := end * 3 / 4
	for index := end - 1; index >= minimum; index-- {
		if runes[index] == '\n' || runes[index] == ' ' {
			return index + 1
		}
	}
	return end
}

func preferredByteBreak(value string) int {
	minimum := len(value) * 3 / 4
	if index := strings.LastIndex(value[minimum:], "\n"); index >= 0 {
		return minimum + index + 1
	}
	if index := strings.LastIndex(value[minimum:], " "); index >= 0 {
		return minimum + index + 1
	}
	return len(value)
}

var _ ChannelDeliveryPolicy = (*RedisChannelDeliveryPolicy)(nil)
