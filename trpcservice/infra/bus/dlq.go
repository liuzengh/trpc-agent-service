// dlq.go implements the inbound dead-letter queue: messages whose business
// processing keeps failing are bounded in retry count and, once the threshold
// is crossed, moved out of the live stream so they can never poison the
// consumer group (G2 decision).
//
// Retry counting is per business failure, not per XAUTOCLAIM claim: a long
// approval turn holds its delivery pending for minutes and gets re-claimed
// repeatedly; those claims are deduped by the worker's idempotency key and
// must not push a healthy message into the DLQ. Only an fn error (the
// consumer explicitly failing the message) increments the counter.
//
// On the threshold crossing the message is: appended to stream:inbound:dlq
// (envelope + fail_reason + attempts), removed from stream:inbound (XAck +
// XDel), counted in platform.dlq_total, logged, and — when a sink is wired —
// persisted to MySQL (dead_letters) for operator replay via the admin API.
package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/metrics"
)

// StreamDLQ is the dead-letter stream for poisoned inbound messages.
const StreamDLQ = "stream:inbound:dlq"

// DefaultMaxDeliveries is the default failure threshold before a message is
// dead-lettered (G2: 5). With the 30s XAUTOCLAIM idle window this bounds a
// poison message's lifetime to roughly 2.5 minutes of retries.
const DefaultMaxDeliveries = 5

// retryTTL bounds how long a failure counter survives; it mirrors the
// idempotency window so a redelivered message never sees a stale count.
const retryTTL = 24 * time.Hour

// DeadLetter is the dead-lettered view of one inbound message, handed to the
// persistence sink and carried on the DLQ stream.
type DeadLetter struct {
	MessageID   string // bus envelope id (idempotency key)
	StreamEntry string // redis stream entry id of the failed delivery
	TenantID    string
	AgentID     string
	SessionID   string
	Channel     string
	UserID      string
	TraceID     string
	// Payload is the JSON-encoded original envelope, the exact bytes a replay
	// re-publishes.
	Payload    string
	FailReason string
	Attempts   int64
	FailedAt   time.Time
}

// DeadLetterSink persists a dead letter out-of-band (MySQL in production). It
// must be safe for concurrent use; an error is logged, never fatal — the
// Redis DLQ stream still holds the message.
type DeadLetterSink func(ctx context.Context, e DeadLetter)

// retryKey namespaces the per-entry failure counter (see keyBuilder in bus.go).
func retryKey(streamEntryID string) string {
	return keyRetry.key(streamEntryID)
}

// SetDeadLetter arms the dead-letter policy: after maxAttempts business
// failures a message is moved to the DLQ. maxAttempts <= 0 disables the DLQ
// (failures stay pending and are retried by XAUTOCLAIM forever, the legacy
// behavior). sink may be nil (Redis-only DLQ, nothing persisted).
func (b *RedisBus) SetDeadLetter(maxAttempts int, sink DeadLetterSink) {
	b.dlqMu.Lock()
	b.maxDeliveries = maxAttempts
	b.sink = sink
	b.dlqMu.Unlock()
}

// onFailed records one business failure of msg. It returns true when the
// message was dead-lettered (the caller must NOT ack — the entry is already
// removed from the stream).
func (b *RedisBus) onFailed(ctx context.Context, group string, msg redis.XMessage, m *Message, cause error) {
	maxAttempts := b.deadLetterLimit()
	if maxAttempts <= 0 {
		return // DLQ disabled: leave pending, XAUTOCLAIM retries
	}

	attempts, err := b.client.Incr(ctx, retryKey(msg.ID)).Result()
	if err != nil {
		// Counter unavailable (transient Redis issue): keep the message
		// pending; the redelivery retries and recounts.
		slog.Warn("bus: dlq retry count failed", "entry", msg.ID, "err", err)
		return
	}
	if err := b.client.Expire(ctx, retryKey(msg.ID), retryTTL).Err(); err != nil {
		slog.Warn("bus: dlq retry ttl refresh failed", "entry", msg.ID, "err", err)
	}
	// Log the cause here, not only at the dead-letter threshold: the audit row
	// deliberately carries a coarse error class, so without this line a failing
	// turn is visible as "failed" with no readable reason anywhere until it is
	// finally parked. The attempt counter also bounds the noise.
	slog.Warn("bus: inbound handling failed",
		"id", msg.ID, "tenant", m.TenantID, "session", m.SessionID,
		"attempt", attempts, "max", maxAttempts, "err", cause)
	if attempts < int64(maxAttempts) {
		return // under the threshold: leave pending for the next retry
	}
	b.moveToDLQ(ctx, group, msg, m, cause, attempts)
}

// moveToDLQ is the threshold crossing: XAdd to the DLQ stream, best-effort
// persistence, then remove the entry from the live stream. The DLQ append
// happens first so a crash mid-move can never lose the message (it would sit
// in both streams and the replay path dedups by envelope id).
func (b *RedisBus) moveToDLQ(ctx context.Context, group string, msg redis.XMessage, m *Message, cause error, attempts int64) {
	payload, err := json.Marshal(m)
	if err != nil {
		payload = []byte("{}")
	}
	e := DeadLetter{
		MessageID:   m.ID,
		StreamEntry: msg.ID,
		TenantID:    m.TenantID,
		AgentID:     m.AgentID,
		SessionID:   m.SessionID,
		Channel:     m.Channel,
		UserID:      m.UserID,
		TraceID:     m.TraceID,
		Payload:     string(payload),
		FailReason:  fmt.Sprintf("%v", cause),
		Attempts:    attempts,
		FailedAt:    time.Now(),
	}

	// 1. Append to the DLQ stream with the original envelope fields plus the
	// failure diagnostics, so operators can XRange the stream directly.
	fields := make(map[string]interface{}, len(msg.Values)+3)
	for k, v := range msg.Values {
		fields[k] = v
	}
	fields["payload"] = e.Payload
	fields["fail_reason"] = e.FailReason
	fields["attempts"] = attempts
	if err := b.client.XAdd(ctx, &redis.XAddArgs{Stream: StreamDLQ, Values: fields}).Err(); err != nil {
		// Without the DLQ copy we must NOT remove the live entry — better an
		// over-retried message than a lost one.
		slog.Error("bus: dlq append failed, message stays in stream", "entry", msg.ID, "err", err)
		return
	}

	// 2. Persist for replay (best-effort; the stream copy above is durable).
	if sink := b.deadLetterSink(); sink != nil {
		sink(ctx, e)
	}

	// 3. Remove from the live stream: ack the pending entry, delete it so no
	// consumer ever sees it again, and drop the retry counter.
	if err := b.client.XAck(ctx, StreamInbound, group, msg.ID).Err(); err != nil {
		slog.Warn("bus: dlq ack failed", "entry", msg.ID, "err", err)
	}
	if err := b.client.XDel(ctx, StreamInbound, msg.ID).Err(); err != nil {
		slog.Warn("bus: dlq delete failed", "entry", msg.ID, "err", err)
	}
	_ = b.client.Del(ctx, retryKey(msg.ID)).Err()

	metrics.DeadLetter(ctx, m.TenantID, m.Channel)
	slog.Error("bus: message dead-lettered",
		"entry", msg.ID, "message_id", m.ID, "tenant", m.TenantID,
		"session", m.SessionID, "channel", m.Channel,
		"attempts", attempts, "reason", e.FailReason)
}

// onSucceeded clears the retry counter of a message that finally went
// through, so a later redelivery of the same envelope starts from zero.
func (b *RedisBus) onSucceeded(ctx context.Context, msg redis.XMessage) {
	if b.deadLetterLimit() <= 0 {
		return
	}
	_ = b.client.Del(ctx, retryKey(msg.ID)).Err()
}

// deadLetterLimit / deadLetterSink read the policy under the bus lock; the
// policy may be armed after consumers started (main wires it during startup).
func (b *RedisBus) deadLetterLimit() int {
	b.dlqMu.Lock()
	defer b.dlqMu.Unlock()
	return b.maxDeliveries
}

func (b *RedisBus) deadLetterSink() DeadLetterSink {
	b.dlqMu.Lock()
	defer b.dlqMu.Unlock()
	return b.sink
}
