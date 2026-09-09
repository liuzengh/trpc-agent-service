package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// The three Stream queues.
const (
	// StreamInbound is the Gateway→Worker inbound queue (consumer group workers).
	StreamInbound = "stream:inbound"
	// StreamOutbound is the Worker→Channel Adapter outbound queue (consumer group senders).
	StreamOutbound = "stream:outbound"
	// StreamDeadletter receives messages that exhausted redelivery attempts.
	StreamDeadletter = "stream:deadletter"

	// StreamMaxLen caps the queue length (XADD MAXLEN ~) so a backlog cannot
	// exhaust Redis memory; crossing the threshold should trigger an alert.
	StreamMaxLen = 100000

	// BackpressureThreshold is the queue length at which the gateway stops
	// accepting new messages: the IM is told to retry later, which is the
	// boundary of the no-loss guarantee.
	BackpressureThreshold = StreamMaxLen * 80 / 100

	// payloadField is the field name of the payload inside a Stream entry.
	payloadField = "payload"
)

// Message is a message consumed from a Stream.
type Message struct {
	ID      string // Stream entry ID, used for Ack
	Payload []byte // payload (JSON by convention; encoding is the caller's job)
}

// Stream wraps Redis Stream send/receive: capped enqueue, consumer-group
// blocking reads, and Ack after processing.
type Stream struct {
	rdb *redis.Client
}

// NewStream creates a Stream transceiver on an established Redis client.
func NewStream(rdb *redis.Client) *Stream {
	return &Stream{rdb: rdb}
}

// Add enqueues one message and returns its entry ID.
func (s *Stream) Add(ctx context.Context, stream string, payload []byte) (string, error) {
	id, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		MaxLen: StreamMaxLen,
		Approx: true, // approximate trimming by node avoids per-entry exact trimming cost
		Values: map[string]any{payloadField: payload},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd %s: %w", stream, err)
	}
	return id, nil
}

// Len returns the stream length (XLEN), for the gateway's backpressure check.
func (s *Stream) Len(ctx context.Context, stream string) (int64, error) {
	n, err := s.rdb.XLen(ctx, stream).Result()
	if err != nil {
		return 0, fmt.Errorf("xlen %s: %w", stream, err)
	}
	return n, nil
}

// Pending reports a consumer group's backlog: how many messages sit pending
// and how long the oldest has been waiting. A missing group reports zeros.
func (s *Stream) Pending(ctx context.Context, stream, group string) (count int64, oldestIdle time.Duration, err error) {
	summary, err := s.rdb.XPending(ctx, stream, group).Result()
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("xpending %s %s: %w", stream, group, err)
	}
	if summary.Count == 0 {
		return 0, 0, nil
	}
	// IDs are time-ordered, so the first entry of the extended form is the
	// oldest pending message.
	ext, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: 1,
	}).Result()
	if err != nil {
		return summary.Count, 0, fmt.Errorf("xpending ext %s %s: %w", stream, group, err)
	}
	if len(ext) > 0 {
		oldestIdle = ext[0].Idle
	}
	return summary.Count, oldestIdle, nil
}

// EnsureGroup creates the consumer group if missing; an existing group is
// skipped idempotently. MkStream also creates the stream, so a group may
// exist before the first message.
func (s *Stream) EnsureGroup(ctx context.Context, stream, group string) error {
	err := s.rdb.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !errors.Is(err, redis.Nil) && !isBusyGroup(err) {
		return fmt.Errorf("xgroup create %s %s: %w", stream, group, err)
	}
	return nil
}

// Read blocks for messages as a consumer-group member. A block timeout with
// no new messages returns (nil, nil) — normal idling, not an error.
func (s *Stream) Read(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]Message, error) {
	res, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // idle: block timed out with no new messages
	}
	if err != nil {
		return nil, fmt.Errorf("xreadgroup %s %s: %w", stream, group, err)
	}

	var msgs []Message
	for _, sm := range res {
		for _, xm := range sm.Messages {
			payload, _ := xm.Values[payloadField].(string)
			msgs = append(msgs, Message{ID: xm.ID, Payload: []byte(payload)})
		}
	}
	return msgs, nil
}

// Ack confirms processed messages; un-acked ones stay pending, available for
// XCLAIM takeover and redelivery after a crash.
func (s *Stream) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if err := s.rdb.XAck(ctx, stream, group, ids...).Err(); err != nil {
		return fmt.Errorf("xack %s %s: %w", stream, group, err)
	}
	return nil
}

// AutoClaim transfers ownership of pending messages idle for longer than
// minIdle to consumer, and returns them for reprocessing. This is how a
// surviving node takes over the messages of a crashed consumer.
func (s *Stream) AutoClaim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]Message, error) {
	var msgs []Message
	start := "0"
	for {
		xms, next, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: consumer,
			MinIdle:  minIdle,
			Start:    start,
			Count:    count,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return msgs, fmt.Errorf("xautoclaim %s %s: %w", stream, group, err)
		}
		for _, xm := range xms {
			payload, _ := xm.Values[payloadField].(string)
			msgs = append(msgs, Message{ID: xm.ID, Payload: []byte(payload)})
		}
		// Redis reports the end of the sweep as "0-0"; "0" is accepted too so the
		// loop stops on the sentinel rather than paying one extra round trip.
		if next == "0" || next == "0-0" || len(xms) == 0 {
			return msgs, nil
		}
		start = next
	}
}

// attemptsKey namespaces the redelivery counter by consumer group. One stream
// can be consumed by several groups (the outbound queue has one per channel
// family), and each redelivers independently, so a counter shared across groups
// lets one group's retries dead-letter a message another group never touched.
func attemptsKey(stream, group, id string) string {
	return fmt.Sprintf("retry:%s:%s:%s", stream, group, id)
}

// incAttemptsScript bumps the counter and refreshes its TTL in one round trip:
// an INCR followed by a separate EXPIRE leaves a counter with no expiry if the
// caller dies in between, pinning the key for as long as Redis runs.
var incAttemptsScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return n
`)

// IncAttempts records one genuine delivery failure for a message. Callers count
// only failures the message itself caused — a retry forced by a rate limit, a
// busy session or a Redis hiccup is not the message's fault, and counting those
// dead-letters replies that were always deliverable.
func (s *Stream) IncAttempts(ctx context.Context, stream, group, id string) (int64, error) {
	key := attemptsKey(stream, group, id)
	n, err := incAttemptsScript.Run(ctx, s.rdb, []string{key}, DedupTTL.Milliseconds()).Int64()
	if err != nil {
		return 0, fmt.Errorf("incr %s: %w", key, err)
	}
	return n, nil
}

// Attempts reads how many times a message has genuinely failed for this group;
// one that was never counted reads 0. The reaper compares it against
// maxAttempts to decide when to stop retrying and dead-letter.
func (s *Stream) Attempts(ctx context.Context, stream, group, id string) (int64, error) {
	key := attemptsKey(stream, group, id)
	n, err := s.rdb.Get(ctx, key).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil // never failed: nothing counted yet
	}
	if err != nil {
		return 0, fmt.Errorf("get %s: %w", key, err)
	}
	return n, nil
}

// DeadLetter moves a message to stream:deadletter (kept for manual
// intervention and alerting) and acks it in the origin group.
func (s *Stream) DeadLetter(ctx context.Context, stream, group string, m Message) error {
	payload, _ := json.Marshal(map[string]string{
		"origin_stream": stream,
		"origin_id":     m.ID,
		"payload":       string(m.Payload),
	})
	if _, err := s.Add(ctx, StreamDeadletter, payload); err != nil {
		return fmt.Errorf("deadletter %s: %w", m.ID, err)
	}
	return s.Ack(ctx, stream, group, m.ID)
}

func isBusyGroup(err error) bool {
	return err != nil && len(err.Error()) >= 9 && err.Error()[:9] == "BUSYGROUP"
}
