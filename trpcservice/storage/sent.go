package storage

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// SentMarker implements outbound idempotency (key
// sent:{channel}:{binding}:{msg_id}, TTL dedupTTLFor(channel)): the sender
// checks before
// calling the IM API and marks after a successful send, so a redelivery after
// "sent but not acked" cannot push the same reply to the user twice. The
// binding dimension keeps two tenants' replies apart — msg_id uniqueness is
// guaranteed by the IM per corp/app only.
type SentMarker struct {
	rdb *redis.Client
}

// NewSentMarker creates a SentMarker on an established Redis client.
func NewSentMarker(rdb *redis.Client) *SentMarker {
	return &SentMarker{rdb: rdb}
}

func sentKey(channel, binding, msgID string) string {
	return fmt.Sprintf("sent:%s:%s:%s", channel, binding, msgID)
}

// IsSent reports whether the reply to this message was already delivered.
func (m *SentMarker) IsSent(ctx context.Context, channel, binding, msgID string) (bool, error) {
	n, err := m.rdb.Exists(ctx, sentKey(channel, binding, msgID)).Result()
	if err != nil {
		return false, fmt.Errorf("check sent %s:%s:%s: %w", channel, binding, msgID, err)
	}
	return n > 0, nil
}

// MarkSent records the reply as delivered; imMsgID is the message ID returned
// by the IM platform (empty for channels without one).
func (m *SentMarker) MarkSent(ctx context.Context, channel, binding, msgID, imMsgID string) error {
	if err := m.rdb.Set(ctx, sentKey(channel, binding, msgID), imMsgID, dedupTTLFor(channel)).Err(); err != nil {
		return fmt.Errorf("mark sent %s:%s:%s: %w", channel, binding, msgID, err)
	}
	return nil
}
