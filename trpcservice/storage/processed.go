package storage

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ProcessedMarker is the execution-layer idempotency: the
// worker marks a message done:{channel}:{binding}:{msg_id} (dedupTTLFor TTL)
// after its
// reply is enqueued. A Stream redelivery (reaper takeover after a crash, or a
// delayed Ack) finds the marker and skips reprocessing — without it a
// redelivered message would run the LLM again, journal duplicate events, and
// double the token spend. The (session_id, event_seq) unique constraint
// remains the last-resort backstop below this. The binding dimension keeps
// two tenants' callbacks apart: msg_id uniqueness is guaranteed by the IM per
// corp/app only.
//
// Note the boundary: a crash DURING processing (before the mark) still
// re-runs — that window is inherent to at-least-once delivery; the outbound
// sent: key stops the duplicate reply from reaching the user.
type ProcessedMarker struct {
	rdb *redis.Client
}

// NewProcessedMarker creates the marker on an established Redis client.
func NewProcessedMarker(rdb *redis.Client) *ProcessedMarker {
	return &ProcessedMarker{rdb: rdb}
}

// IsDone reports whether the message was already fully processed.
func (m *ProcessedMarker) IsDone(ctx context.Context, channel, binding, msgID string) (bool, error) {
	n, err := m.rdb.Exists(ctx, doneKey(channel, binding, msgID)).Result()
	if err != nil {
		return false, fmt.Errorf("done check: %w", err)
	}
	return n > 0, nil
}

// MarkDone records the message as fully processed (per-channel TTL matching
// the inbound dedup window).
func (m *ProcessedMarker) MarkDone(ctx context.Context, channel, binding, msgID string) error {
	key := doneKey(channel, binding, msgID)
	if err := m.rdb.Set(ctx, key, "1", dedupTTLFor(channel)).Err(); err != nil {
		return fmt.Errorf("done mark: %w", err)
	}
	return nil
}

func doneKey(channel, binding, msgID string) string {
	return fmt.Sprintf("done:%s:%s:%s", channel, binding, msgID)
}
