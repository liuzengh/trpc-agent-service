package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

var senderTracer = otel.Tracer("trpc-agent-service/sender")

func sendAttr(msg OutboundMessage, result string) otelmetric.MeasurementOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
		attribute.String("result", result),
	)
}

// rateLimitedAttr tags re-queue events caused by send pacing.
func rateLimitedAttr(msg OutboundMessage) otelmetric.AddOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
	)
}

// e2eAttr tags the end-to-end latency histogram (channel + tenant).
func e2eAttr(msg OutboundMessage) otelmetric.RecordOption {
	return otelmetric.WithAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
	)
}

// DefaultSenderGroup is the consumer group a Sender reads when Group is
// empty.
const DefaultSenderGroup = "senders"

// Sender consumes the outbound stream as part of a consumer group (Group,
// default DefaultSenderGroup) and dispatches each message to the Send of its
// Channel.
//
// Per-message protocol (outbound idempotency): check the sent: key first and
// skip already-delivered replies; send; mark sent; only then Ack. A crash
// before Ack redelivers the message, but the sent: key blocks a duplicate
// push to the user. Failures stay pending for retry.
//
// Sends are paced per {channel, tenant} token bucket: when the bucket is
// exhausted the message is re-queued instead of dropped, so rate limiting
// delays but never loses.
type Sender struct {
	Stream   *storage.Stream
	Sent     *storage.SentMarker // nil disables outbound idempotency
	Channels map[string]Channel  // channel name → channel implementation
	Name     string              // consumer name

	// Group is the consumer group this sender reads; empty means
	// DefaultSenderGroup. A second group (e.g. wecomws.SenderGroup)
	// partitions stream:outbound by channel: the main group skips messages
	// it does not own via Skip, and each group carries its own pending list
	// and reaper.
	Group string
	// Skip, when set, marks messages this sender must not deliver — they
	// belong to another consumer group on the same stream. A skipped message
	// is acked and dropped here without sending or MarkSent: it must ack,
	// because an un-acked skip stays in this group's pending list forever —
	// the reaper keeps taking it over, and since this group never records a
	// delivery failure for it, it never reaches MaxAttempts and so never
	// dead-letters. It must also not consume a rate-limit token nor write the
	// sent: marker.
	Skip func(msg OutboundMessage) bool

	// Limiter paces sends per {channel}:{tenant_id}; nil disables pacing.
	// SendQPS/SendBurst are the platform-level bucket shape; SendWait bounds
	// how long a message spins for a token before being re-queued.
	Limiter   *storage.Limiter
	SendQPS   float64
	SendBurst int
	SendWait  time.Duration
	// SendPolicyFor, when set, overrides the bucket shape per tenant
	// (tenant.rate_policy send_qps/send_burst).
	SendPolicyFor func(ctx context.Context, tenantID string) (qps float64, burst int, ok bool)

	// ReapInterval is how often pending messages are scanned for takeover,
	// MaxIdle how long a pending message must sit before it counts as
	// orphaned (a crashed or wedged sender), and MaxAttempts caps redeliveries
	// before dead-lettering. Without the reaper an IM API hiccup loses the
	// reply forever (the pending entry just sits there).
	ReapInterval time.Duration
	MaxIdle      time.Duration
	MaxAttempts  int64

	InStream string // stream to consume; empty means storage.StreamOutbound
}

func (s *Sender) inStream() string {
	if s.InStream != "" {
		return s.InStream
	}
	return storage.StreamOutbound
}

func (s *Sender) group() string {
	if s.Group != "" {
		return s.Group
	}
	return DefaultSenderGroup
}

func (s *Sender) sendQPS() float64 {
	if s.SendQPS > 0 {
		return s.SendQPS
	}
	return 20
}

func (s *Sender) sendBurst() int {
	if s.SendBurst > 0 {
		return s.SendBurst
	}
	return 40
}

func (s *Sender) sendWait() time.Duration {
	if s.SendWait > 0 {
		return s.SendWait
	}
	return 30 * time.Second
}

func (s *Sender) reapInterval() time.Duration {
	if s.ReapInterval > 0 {
		return s.ReapInterval
	}
	return 30 * time.Second
}

func (s *Sender) maxIdle() time.Duration {
	if s.MaxIdle > 0 {
		return s.MaxIdle
	}
	// The takeover latency of a rate-limited or wedged send, and the safety
	// bound against claiming an in-flight send, are the same number: it must
	// exceed the worst-case send (sendWait 30s pacing + the IM API timeout
	// per segment); 2min leaves ample slack and keeps the requeue delay
	// bounded.
	return 2 * time.Minute
}

func (s *Sender) maxAttempts() int64 {
	if s.MaxAttempts > 0 {
		return s.MaxAttempts
	}
	return 5
}

// Run consumes until ctx is canceled; a nil return means a clean shutdown.
// Every reapInterval it also takes over pending messages orphaned by crashed
// senders (XCLAIM semantics via XAUTOCLAIM) — symmetric with the Worker.
func (s *Sender) Run(ctx context.Context) error {
	lastReap := time.Now()
	for {
		if ctx.Err() != nil {
			return nil
		}

		if time.Since(lastReap) >= s.reapInterval() {
			s.reap(ctx)
			lastReap = time.Now()
		}

		msgs, err := s.Stream.Read(ctx, s.inStream(), s.group(), s.Name, 10, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			plog.Warnf("sender %s read outbound: %v", s.Name, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		for _, m := range msgs {
			s.handle(ctx, m)
		}
	}
}

// reap takes over pending messages idle longer than maxIdle and resends them.
// A message that keeps failing past maxAttempts is dead-lettered so it cannot
// loop forever.
func (s *Sender) reap(ctx context.Context) {
	if err := s.Stream.EnsureGroup(ctx, s.inStream(), s.group()); err != nil {
		plog.Warnf("sender %s ensure group before reap: %v", s.Name, err)
		return
	}
	msgs, err := s.Stream.AutoClaim(ctx, s.inStream(), s.group(), s.Name, s.maxIdle(), 50)
	if err != nil {
		plog.Warnf("sender %s autoclaim: %v", s.Name, err)
		return
	}
	for _, m := range msgs {
		// Read-only: the counter tracks genuine delivery failures (recorded by
		// handle), not takeovers — a reply paced by the rate limiter or
		// delayed by a Redis hiccup is not a failure.
		attempts, err := s.Stream.Attempts(ctx, s.inStream(), s.group(), m.ID)
		if err != nil {
			plog.Warnf("sender %s count attempts %s: %v", s.Name, m.ID, err)
			continue
		}
		if attempts > s.maxAttempts() {
			plog.Errorf("sender %s dead-letters %s after %d attempts", s.Name, m.ID, attempts)
			if err := s.Stream.DeadLetter(ctx, s.inStream(), s.group(), m); err != nil {
				plog.Errorf("sender %s deadletter %s: %v", s.Name, m.ID, err)
			}
			continue
		}
		plog.Infof("sender %s takes over %s (attempt %d)", s.Name, m.ID, attempts)
		s.handle(ctx, m)
	}
}

// countFailure records one genuine delivery failure against this group's
// counter, which is what lets the reaper dead-letter a message the platform
// keeps rejecting. Requeues caused by the rate limiter, an unknown-channel
// drop or a Redis hiccup are not failures and must never come through here.
func (s *Sender) countFailure(ctx context.Context, id string) {
	if _, err := s.Stream.IncAttempts(ctx, s.inStream(), s.group(), id); err != nil {
		plog.Warnf("sender %s count failure %s: %v", s.Name, id, err)
	}
}

func (s *Sender) handle(ctx context.Context, m storage.Message) {
	var msg OutboundMessage
	if err := json.Unmarshal(m.Payload, &msg); err != nil {
		plog.Errorf("sender %s drop poison message %s: %v", s.Name, m.ID, err)
		_ = s.Stream.Ack(ctx, s.inStream(), s.group(), m.ID)
		return
	}

	// Another consumer group owns this channel (e.g. wecomws ↔ senders-ws):
	// ack and step aside before touching the sent: marker, the rate bucket
	// or the channel registry — and before any tracing setup, which only
	// matters for messages this group actually delivers.
	if s.Skip != nil && s.Skip(msg) {
		metrics.OutboundTotal.Add(ctx, 1, sendAttr(msg, "skipped_other_group"))
		_ = s.Stream.Ack(ctx, s.inStream(), s.group(), m.ID)
		return
	}

	// Continue the message trace across the outbound Stream boundary.
	ctx = metrics.ExtractTraceparent(ctx, propagation.MapCarrier{"traceparent": msg.TraceParent})
	ctx, span := senderTracer.Start(ctx, "sender.send")
	defer span.End()
	span.SetAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("session_key", msg.SessionKey),
	)

	// Already delivered (sent but un-acked in a previous life): skip the send.
	if s.Sent != nil && msg.MsgID != "" {
		sent, err := s.Sent.IsSent(ctx, msg.Channel, msg.BindingID, msg.MsgID)
		if err != nil {
			plog.Warnf("sender %s check sent %s: %v", s.Name, m.ID, err)
			return // Redis hiccup: leave pending, retry later
		}
		if sent {
			plog.Infof("sender %s skip already-sent reply for msg %s", s.Name, msg.MsgID)
			metrics.OutboundTotal.Add(ctx, 1, sendAttr(msg, "skipped_duplicate"))
			_ = s.Stream.Ack(ctx, s.inStream(), s.group(), m.ID)
			return
		}
	}

	ch, ok := s.Channels[msg.Channel]
	if !ok {
		// An unknown channel is a configuration error, not a retryable
		// failure: Ack, drop and alert.
		plog.Errorf("sender %s: no channel named %q, drop %s", s.Name, msg.Channel, m.ID)
		_ = s.Stream.Ack(ctx, s.inStream(), s.group(), m.ID)
		return
	}

	// Pace the send on the {channel, tenant} bucket; on exhaustion the
	// message stays pending for the reaper's takeover instead of being
	// re-queued under a new stream ID, which would hand it a fresh attempts
	// counter. The bucket refills in milliseconds; the reaper's maxIdle is
	// the outer bound of the delay.
	if s.Limiter != nil {
		qps, burst := s.sendQPS(), s.sendBurst()
		if s.SendPolicyFor != nil && msg.TenantID != "" {
			if q, b, ok := s.SendPolicyFor(ctx, msg.TenantID); ok && q > 0 && b > 0 {
				qps, burst = q, b
			}
		}
		scope := "send:" + msg.Channel + ":" + msg.TenantID
		ok, err := s.Limiter.WaitAllow(ctx, scope, qps, burst, s.sendWait())
		if err != nil {
			plog.Warnf("sender %s rate limit check %s: %v", s.Name, m.ID, err)
			return // Redis hiccup: leave pending, retry later
		}
		if !ok {
			metrics.SendRateLimitedTotal.Add(ctx, 1, rateLimitedAttr(msg))
			plog.Warnf("sender %s leaves %s pending: send bucket %s exhausted, reaper takes over",
				s.Name, m.ID, scope)
			return
		}
	}

	if err := ch.Send(ctx, msg); err != nil {
		// No Ack: leave it pending for retry, counted so the reaper can
		// dead-letter a message the platform keeps rejecting.
		s.countFailure(ctx, m.ID)
		metrics.OutboundTotal.Add(ctx, 1, sendAttr(msg, "error"))
		plog.Errorf("sender %s send via %s failed: %v", s.Name, msg.Channel, err)
		span.RecordError(err)
		return
	}
	metrics.OutboundTotal.Add(ctx, 1, sendAttr(msg, "ok"))
	// End-to-end latency: callback arrival → reply landed on the IM;
	// ReceivedAt rides the outbound message.
	if !msg.ReceivedAt.IsZero() {
		metrics.EndToEndDuration.Record(ctx,
			float64(time.Since(msg.ReceivedAt).Milliseconds()), e2eAttr(msg))
	}
	if s.Sent != nil && msg.MsgID != "" {
		if err := s.Sent.MarkSent(ctx, msg.Channel, msg.BindingID, msg.MsgID, ""); err != nil {
			// The user already has the reply, but the marker that suppresses
			// the duplicate did not stick: no Ack. The reaper redelivers, and
			// because IsSent will still say no, the send runs again — so this
			// counts as a failure, bounding the duplicate storm at maxAttempts.
			s.countFailure(ctx, m.ID)
			plog.Errorf("sender %s mark sent %s: %v — leaving pending for redelivery", s.Name, m.ID, err)
			return
		}
	}
	if err := s.Stream.Ack(ctx, s.inStream(), s.group(), m.ID); err != nil {
		plog.Warnf("sender %s ack %s: %v", s.Name, m.ID, err)
	}
	zap.L().Debug("outbound delivered",
		zap.String(plog.FieldChannel, msg.Channel),
		zap.String(plog.FieldSessionKey, msg.SessionKey),
		zap.String(plog.FieldTraceID, msg.TraceID))
}

// String identifies the sender in logs.
func (s *Sender) String() string { return fmt.Sprintf("sender(%s)", s.Name) }
