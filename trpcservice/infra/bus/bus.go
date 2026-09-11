// Package bus is the unified message bus and cross-node session state for
// stateless workers. Messages flow IM → Worker over Redis Streams with a
// Consumer Group; session routing, idempotency and serialization locks keep
// concurrent handling correct across nodes.
package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// Stream keys. The platform uses one shared inbound/outbound stream; tenant
// isolation comes from Message.TenantID, not from stream sharding — Consumer
// Groups work best with few streams and many groups.
const (
	StreamInbound  = "stream:inbound"
	StreamOutbound = "stream:outbound"
)

// keyBuilder is the one place the bus composes Redis key names. Each builder
// owns its namespace prefix and joins the remaining segments with the same
// separator, so a key is never assembled by ad-hoc concatenation and the wire
// format stays auditable in one file. The key tests pin the exact strings: an
// upgrade must not silently orphan live locks, routes or cursors.
type keyBuilder struct {
	prefix string
}

// key joins the builder's prefix with the given segments.
func (k keyBuilder) key(segments ...string) string {
	return k.prefix + strings.Join(segments, ":")
}

// Bus key namespaces. Session-scoped keys carry the tenant so tenants are
// isolated at the key-namespace level (detailed design §3).
var (
	keyRoute      = keyBuilder{prefix: "route:"}
	keyLock       = keyBuilder{prefix: "lock:session:"}
	keyIdem       = keyBuilder{prefix: "idem:"}
	keyApprovalRe = keyBuilder{prefix: "approval:req:"}
	keyApprovalRs = keyBuilder{prefix: "approval:res:"}
	keyOutCursor  = keyBuilder{prefix: "cursor:outbound"}
	keyIMRoute    = keyBuilder{prefix: "imroute:"}
	keyRetry      = keyBuilder{prefix: "retry:"}
)

// RouteKey returns the key binding a session to its agent.
func RouteKey(tenantID, sessionID string) string {
	return keyRoute.key(tenantID, sessionID)
}

// LockKey returns the per-session serialization lock key.
func LockKey(tenantID, sessionID string) string {
	return keyLock.key(tenantID, sessionID)
}

// IdemKey returns the message idempotency key.
func IdemKey(msgKey string) string {
	return keyIdem.key(msgKey)
}

// ApprovalReqKey returns the pending-approval key of a session. A session has
// at most one pending human approval at a time (single-pending model).
func ApprovalReqKey(tenantID, sessionID string) string {
	return keyApprovalRe.key(tenantID, sessionID)
}

// ApprovalResKey returns the human decision key of a session. The blocking
// reviewer polls it while the agent turn is suspended.
func ApprovalResKey(tenantID, sessionID string) string {
	return keyApprovalRs.key(tenantID, sessionID)
}

// OutboundCursorKey is the persisted read position of the outbound follower.
// The gateway resumes from it after a restart instead of jumping to the stream
// tail, so a reply published while the gateway was down is still delivered.
// There is one key, not one per node: the platform assumes a single gateway
// node, because two nodes would open competing IM connections for the same bot
// account (see docs/多后端适配方案.md).
func OutboundCursorKey() string {
	return keyOutCursor.key()
}

// IMRouteKey returns the persisted reply route of an IM conversation. The
// in-memory route table dies with the process; without this a restarted gateway
// could read a missed reply but would not know which chat to send it to.
func IMRouteKey(sessionID string) string {
	return keyIMRoute.key(sessionID)
}

// ---------------------------------------------------------------- envelope --

// Message is the normalized envelope flowing through the platform.
type Message struct {
	ID        string         `json:"id"` // global unique, for idempotency
	TraceID   string         `json:"trace_id"`
	TenantID  string         `json:"tenant_id"`
	AgentID   string         `json:"agent_id"`
	SessionID string         `json:"session_id"`
	Channel   string         `json:"channel"` // wecom/feishu/admin
	UserID    string         `json:"user_id"`
	Content   *model.Message `json:"content"`
	ReplyTo   string         `json:"reply_to,omitempty"` // outbound routing hint
	Kind      string         `json:"kind,omitempty"`     // text | stream | card
	Segments  []Segment      `json:"segments,omitempty"` // card segments
}

// Segment represents a rich content segment for card messages.
type Segment struct {
	Type string `json:"type"` // "text" | "markdown" | "image"
	Text string `json:"text,omitempty"`
	URL  string `json:"url,omitempty"`
	// Actions carries interactive buttons (cards). Each button's Value is the
	// callback payload the IM platform returns when it is pressed: that is how
	// an approval decision travels back without the user typing anything.
	Actions []SegmentAction `json:"actions,omitempty"`
}

// SegmentAction is one button of an interactive card.
type SegmentAction struct {
	Text  string            `json:"text"`
	Value map[string]string `json:"value,omitempty"`
}

// encode serializes a Message into Redis Stream field/value pairs.
func encode(m *Message) (map[string]interface{}, error) {
	if m == nil {
		return nil, errors.New("bus: cannot encode nil message")
	}
	fields := map[string]interface{}{
		"id":         m.ID,
		"trace_id":   m.TraceID,
		"tenant_id":  m.TenantID,
		"agent_id":   m.AgentID,
		"session_id": m.SessionID,
		"channel":    m.Channel,
		"user_id":    m.UserID,
	}
	if m.ReplyTo != "" {
		fields["reply_to"] = m.ReplyTo
	}
	if m.Content != nil {
		raw, err := json.Marshal(m.Content)
		if err != nil {
			return nil, fmt.Errorf("bus: encode content: %w", err)
		}
		fields["content"] = string(raw)
	}
	return fields, nil
}

// decode parses Redis Stream field/value pairs back into a Message.
func decode(vals map[string]interface{}) (*Message, error) {
	m := &Message{
		ID:        stringAt(vals, "id"),
		TraceID:   stringAt(vals, "trace_id"),
		TenantID:  stringAt(vals, "tenant_id"),
		AgentID:   stringAt(vals, "agent_id"),
		SessionID: stringAt(vals, "session_id"),
		Channel:   stringAt(vals, "channel"),
		UserID:    stringAt(vals, "user_id"),
		ReplyTo:   stringAt(vals, "reply_to"),
	}
	if raw := stringAt(vals, "content"); raw != "" {
		var content model.Message
		if err := json.Unmarshal([]byte(raw), &content); err != nil {
			return nil, fmt.Errorf("bus: decode content: %w", err)
		}
		m.Content = &content
	}
	return m, nil
}

func stringAt(vals map[string]interface{}, key string) string {
	if s, ok := vals[key].(string); ok {
		return s
	}
	return ""
}

// --------------------------------------------------------------------- bus --

// Bus abstracts enqueue/dequeue for inbound and outbound flows.
type Bus interface {
	PublishInbound(ctx context.Context, m *Message) error
	PublishOutbound(ctx context.Context, m *Message) error
	// ConsumeInbound joins group as consumer and invokes fn for each inbound
	// message, blocking until ctx is done.
	ConsumeInbound(ctx context.Context, group, consumer string, fn func(ctx context.Context, m *Message) error) error
}

// RedisBus implements Bus on Redis Streams with a Consumer Group. It also
// owns the cross-node session state (route / idempotency / lock / approval)
// and the inbound dead-letter policy (see dlq.go).
type RedisBus struct {
	client *redis.Client
	// consumeWorkers bounds how many messages a single consumer processes at
	// once. A bounded pool keeps one long turn (e.g. an approval wait) from
	// blocking the delivery of later messages, e.g. the human approval reply.
	consumeWorkers int

	// dlqMu guards the dead-letter policy, armed via SetDeadLetter before the
	// consumers start and read per failure from the worker pool.
	dlqMu         sync.Mutex
	maxDeliveries int
	sink          DeadLetterSink
}

// NewRedis returns a Redis-backed bus over an existing client.
func NewRedis(client *redis.Client) *RedisBus {
	return &RedisBus{client: client, consumeWorkers: defaultConsumeWorkers}
}

// NewRedisFromURL connects to the Redis at url and returns a bus over it.
func NewRedisFromURL(url string) (*RedisBus, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("bus: parse redis url: %w", err)
	}
	return &RedisBus{client: redis.NewClient(opts), consumeWorkers: defaultConsumeWorkers}, nil
}

// WithConsumeWorkers returns a copy of the bus that processes at most n
// messages concurrently per consumer. n < 1 resets to the default. The copy
// carries the same dead-letter policy (fields are copied explicitly so the
// policy mutex is never cloned).
func (b *RedisBus) WithConsumeWorkers(n int) *RedisBus {
	if n < 1 {
		n = defaultConsumeWorkers
	}
	return &RedisBus{
		client:         b.client,
		consumeWorkers: n,
		maxDeliveries:  b.deadLetterLimit(),
		sink:           b.deadLetterSink(),
	}
}

// Client exposes the underlying Redis client (for shutdown and stream
// inspection; message flow should go through the bus API).
func (b *RedisBus) Client() *redis.Client {
	return b.client
}

// PublishInbound enqueues an inbound message for workers.
func (b *RedisBus) PublishInbound(ctx context.Context, m *Message) error {
	return b.publish(ctx, StreamInbound, m)
}

// PublishOutbound enqueues a reply to be delivered back to the IM platform.
func (b *RedisBus) PublishOutbound(ctx context.Context, m *Message) error {
	return b.publish(ctx, StreamOutbound, m)
}

// ReadOutbound reads outbound messages newer than fromID without touching a
// consumer group, so admin SSE consumers can follow the stream independently
// of IM adapters (reads never ack group-delivered messages). fromID accepts
// "0" (from the beginning), "$" (only new messages), or an explicit stream id.
// An empty fromID means "$" (only new messages). The returned cursor is the
// last read stream position to pass back on the next call. Returns nil when
// nothing is available within a short bounded wait.
func (b *RedisBus) ReadOutbound(ctx context.Context, fromID string) ([]*Message, string, error) {
	if fromID == "" {
		fromID = "$"
	}
	cursor := fromID
	streams, err := b.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{StreamOutbound, fromID},
		Count:   32,
		Block:   time.Second, // bounded: SSE loops call this repeatedly
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || ctx.Err() != nil {
			return nil, cursor, nil // block window expired / caller cancelled
		}
		return nil, cursor, fmt.Errorf("bus: xread outbound: %w", err)
	}
	var out []*Message
	for _, stream := range streams {
		for _, msg := range stream.Messages {
			m, err := decode(msg.Values)
			if err != nil {
				continue // never let one bad envelope stall the stream
			}
			out = append(out, m)
			cursor = msg.ID
		}
	}
	return out, cursor, nil
}

func (b *RedisBus) publish(ctx context.Context, stream string, m *Message) error {
	if m == nil || m.ID == "" {
		return errors.New("bus: message id is required for idempotency")
	}
	fields, err := encode(m)
	if err != nil {
		return err
	}
	if err := b.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: fields}).Err(); err != nil {
		return fmt.Errorf("bus: xadd %s: %w", stream, err)
	}
	return nil
}

// ConsumeInbound reads new messages for the group, reclaiming anything left
// pending by a dead consumer first. Messages are handled with bounded
// concurrency: a long-running turn (e.g. a human approval wait) must not
// block the delivery of later messages such as the approval reply. Returns
// nil when ctx is cancelled.
func (b *RedisBus) ConsumeInbound(ctx context.Context, group, consumer string, fn func(ctx context.Context, m *Message) error) error {
	// BUSYGROUP means the group already exists; harmless on restart.
	_ = b.client.XGroupCreateMkStream(ctx, StreamInbound, group, "0").Err()

	workers := b.consumeWorkers
	if workers < 1 {
		workers = defaultConsumeWorkers
	}
	jobs := make(chan redis.XMessage, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				b.handle(ctx, group, msg, fn)
			}
		}()
	}

	enqueue := func(msg redis.XMessage) bool {
		select {
		case <-ctx.Done():
			return false
		case jobs <- msg:
			return true
		}
	}

loop:
	for {
		if ctx.Err() != nil {
			break
		}
		if err := b.reclaim(ctx, group, consumer, enqueue); err != nil {
			if ctx.Err() == nil {
				close(jobs)
				wg.Wait()
				return err
			}
			break
		}
		streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{StreamInbound, ">"},
			Count:    10,
			Block:    time.Second, // bounded so ctx is checked regularly
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // block timeout, loop and re-check ctx
			}
			if ctx.Err() != nil {
				break
			}
			close(jobs)
			wg.Wait()
			return fmt.Errorf("bus: xreadgroup: %w", err)
		}
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				if !enqueue(msg) {
					break loop
				}
			}
		}
	}
	close(jobs)
	wg.Wait()
	return nil
}

// ErrRequeue marks a handler outcome that must be retried later WITHOUT counting
// as a business failure. Transient contention (a session lock held by another
// turn) is the canonical case: the message is healthy, so counting it toward the
// dead-letter threshold would dead-letter messages merely because one session
// was busy.
var ErrRequeue = errors.New("bus: requeue without counting as a failure")

// Requeue reports whether err asks for a counted-exempt retry.
func Requeue(err error) bool { return errors.Is(err, ErrRequeue) }

// handle decodes one stream message, runs fn, and acks on success. A malformed
// envelope is acked too, so it can never become a poison pill. A business
// failure increments the DLQ retry counter (see dlq.go): under the threshold
// the message stays pending for XAUTOCLAIM; at the threshold it is moved to
// the dead-letter stream. ErrRequeue stays pending without touching the counter.
func (b *RedisBus) handle(ctx context.Context, group string, msg redis.XMessage, fn func(ctx context.Context, m *Message) error) {
	m, err := decode(msg.Values)
	if err != nil {
		// Ack the malformed envelope so it cannot become a poison pill; a
		// failed ack would leave it pending and redelivered forever.
		if ackErr := b.client.XAck(ctx, StreamInbound, group, msg.ID).Err(); ackErr != nil {
			slog.Warn("bus: ack malformed envelope failed", "id", msg.ID, "err", ackErr)
		}
		return
	}
	if err := fn(ctx, m); err != nil {
		if Requeue(err) {
			// Healthy message, transient contention: leave it pending for the
			// next XAUTOCLAIM pass without incrementing the failure counter.
			slog.Debug("bus: requeue inbound", "id", msg.ID, "reason", err)
			return
		}
		b.onFailed(ctx, group, msg, m, err)
		return // under the threshold: left pending, XAUTOCLAIM retries it
	}
	if ackErr := b.client.XAck(ctx, StreamInbound, group, msg.ID).Err(); ackErr != nil {
		// A failed ack just means redelivery (idempotency dedups it); log so
		// recurring ack failures are not invisible.
		slog.Warn("bus: ack inbound failed", "id", msg.ID, "err", ackErr)
	}
	b.onSucceeded(ctx, msg)
}

// reclaim picks up pending messages abandoned by dead consumers and enqueues
// them for the worker pool.
func (b *RedisBus) reclaim(ctx context.Context, group, consumer string, enqueue func(redis.XMessage) bool) error {
	const minIdle = 30 * time.Second
	start := "0-0"
	for {
		msgs, cursor, err := b.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   StreamInbound,
			Group:    group,
			MinIdle:  minIdle,
			Start:    start,
			Consumer: consumer,
			Count:    10,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("bus: xautoclaim: %w", err)
		}
		for _, msg := range msgs {
			if !enqueue(msg) {
				return nil
			}
		}
		if cursor == "0-0" || cursor == "" {
			return nil
		}
		start = cursor
	}
}

// ------------------------------------------------------------ session state --

const (
	routeTTL = 7 * 24 * time.Hour
	// LockTTL is the session-lock lifetime. Deliberately short: a worker that
	// dies mid-turn must not block its session forever, so liveness relies on
	// the holder refreshing the lock for as long as it works (see RefreshLock)
	// rather than on a generous TTL.
	lockTTL = 30 * time.Second
	// idemTTL is the lifetime of a *committed* idempotency marker: long enough
	// that a redelivered message never re-runs its turn.
	idemTTL = 24 * time.Hour
	// idemLeaseTTL is the lifetime of the *in-flight* marker set when a worker
	// claims a message. Two-phase idempotency: the claim only becomes durable
	// (idemTTL) once the reply is safely in the outbox. A worker that dies
	// mid-turn therefore releases the message by lease expiry instead of
	// swallowing it forever — the redelivery reprocesses the turn and the
	// durable MySQL marker keeps that reprocess from double-replying. The
	// holder refreshes the lease while it works (see ExpireIdemLease), so the
	// TTL only has to cover the gap between two heartbeats.
	idemLeaseTTL = 90 * time.Second
	// approvalResTTL keeps a resolved human decision visible long enough for
	// the waiting reviewer to pick it up (poll interval is ~1s).
	approvalResTTL = 2 * time.Minute
	// defaultConsumeWorkers is the per-consumer message concurrency used by
	// RedisBus when no explicit value is configured.
	defaultConsumeWorkers = 16
)

// SessionLockTTL exposes the session-lock lifetime so the holder can schedule
// its refresh heartbeat relative to it instead of duplicating the constant.
func SessionLockTTL() time.Duration { return lockTTL }

// SessionLockRefreshInterval is how often a lock holder should refresh to stay
// comfortably ahead of SessionLockTTL.
func SessionLockRefreshInterval() time.Duration { return lockTTL / 3 }

// IdemLeaseTTL exposes the in-flight idempotency lease so the holder can
// schedule its refresh relative to it.
func IdemLeaseTTL() time.Duration { return idemLeaseTTL }

// IdemLeaseRefreshInterval is how often the lease holder should refresh it.
func IdemLeaseRefreshInterval() time.Duration { return idemLeaseTTL / 3 }

// imRouteTTL keeps a conversation's reply route long enough to cover a gateway
// restart, an upgrade, or a weekend outage.
const imRouteTTL = 30 * 24 * time.Hour

// LoadOutboundCursor returns the persisted outbound read position, or "$" (the
// stream tail) when the gateway has never stored one.
func (b *RedisBus) LoadOutboundCursor(ctx context.Context) (string, error) {
	id, err := b.client.Get(ctx, OutboundCursorKey()).Result()
	if errors.Is(err, redis.Nil) {
		return "$", nil
	}
	if err != nil {
		return "", fmt.Errorf("bus: load outbound cursor: %w", err)
	}
	if id == "" {
		return "$", nil
	}
	return id, nil
}

// SaveOutboundCursor records the last dispatched outbound entry id. The key has
// no TTL: a stale cursor only means the follower re-reads entries it already
// delivered, which is the safe direction.
func (b *RedisBus) SaveOutboundCursor(ctx context.Context, id string) error {
	if id == "" || id == "$" {
		return nil
	}
	if err := b.client.Set(ctx, OutboundCursorKey(), id, 0).Err(); err != nil {
		return fmt.Errorf("bus: save outbound cursor: %w", err)
	}
	return nil
}

// SaveIMRoute persists a conversation's reply route (opaque JSON for the
// channels package).
func (b *RedisBus) SaveIMRoute(ctx context.Context, sessionID, payload string) error {
	if sessionID == "" || payload == "" {
		return nil
	}
	if err := b.client.Set(ctx, IMRouteKey(sessionID), payload, imRouteTTL).Err(); err != nil {
		return fmt.Errorf("bus: save im route: %w", err)
	}
	return nil
}

// LoadIMRoute returns a persisted conversation route. found=false means the
// conversation was never seen (or its route expired).
func (b *RedisBus) LoadIMRoute(ctx context.Context, sessionID string) (string, bool, error) {
	payload, err := b.client.Get(ctx, IMRouteKey(sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("bus: load im route: %w", err)
	}
	return payload, payload != "", nil
}

// SetRoute binds a session to an agent so stateless workers can resolve the
// right agent for any session.
func (b *RedisBus) SetRoute(ctx context.Context, tenantID, sessionID, agentID string) error {
	return b.client.Set(ctx, RouteKey(tenantID, sessionID), agentID, routeTTL).Err()
}

// Route resolves the agent bound to a session; empty string when unset.
func (b *RedisBus) Route(ctx context.Context, tenantID, sessionID string) (string, error) {
	v, err := b.client.Get(ctx, RouteKey(tenantID, sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("bus: get route: %w", err)
	}
	return v, nil
}

// Idempotent claims msgKey for processing. The claim is a *lease*
// (idemLeaseTTL), not a durable marker: the caller must CommitIdem once the
// work is durably recorded, or the claim expires and the message is processed
// again. That two-phase shape is what stops a crash mid-turn from dropping the
// message on the floor.
func (b *RedisBus) Idempotent(ctx context.Context, msgKey string) (bool, error) {
	ok, err := b.client.SetNX(ctx, IdemKey(msgKey), "1", idemLeaseTTL).Result()
	if err != nil {
		return false, fmt.Errorf("bus: setnx idem: %w", err)
	}
	return ok, nil
}

// CommitIdem turns the in-flight lease into a durable marker: the message is
// handled (its reply is in the outbox, or it was deliberately dropped) and must
// never be processed again within idemTTL.
func (b *RedisBus) CommitIdem(ctx context.Context, msgKey string) error {
	if err := b.client.Set(ctx, IdemKey(msgKey), "1", idemTTL).Err(); err != nil {
		return fmt.Errorf("bus: commit idem: %w", err)
	}
	return nil
}

// ExpireIdemLease extends the in-flight claim while its holder is still working.
// It reports false when the lease is gone, i.e. this worker no longer owns the
// message and another consumer may already be reprocessing it.
func (b *RedisBus) ExpireIdemLease(ctx context.Context, msgKey string) (bool, error) {
	ok, err := b.client.Expire(ctx, IdemKey(msgKey), idemLeaseTTL).Result()
	if err != nil {
		return false, fmt.Errorf("bus: expire idem lease: %w", err)
	}
	return ok, nil
}

// ClearIdem removes a msgKey marker so a failed attempt can be retried on
// redelivery (paired with Idempotent's SetNX atomic claim).
func (b *RedisBus) ClearIdem(ctx context.Context, msgKey string) error {
	if err := b.client.Del(ctx, IdemKey(msgKey)).Err(); err != nil {
		return fmt.Errorf("bus: del idem: %w", err)
	}
	return nil
}

// LockSession serializes concurrent handling of one session across nodes.
// token must be unique per attempt so UnlockSession can verify ownership.
func (b *RedisBus) LockSession(ctx context.Context, tenantID, sessionID, token string) (bool, error) {
	ok, err := b.client.SetNX(ctx, LockKey(tenantID, sessionID), token, lockTTL).Result()
	if err != nil {
		return false, fmt.Errorf("bus: lock session: %w", err)
	}
	return ok, nil
}

// UnlockSession releases a session lock only if token still owns it.
func (b *RedisBus) UnlockSession(ctx context.Context, tenantID, sessionID, token string) error {
	// ponytail: compare-and-delete via Lua so we never release another
	// holder's lock after our TTL expired.
	script := redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end`)
	return script.Run(ctx, b.client, []string{LockKey(tenantID, sessionID)}, token).Err()
}

// RefreshLock extends the session lock TTL when token still owns it. Used to
// keep the lock alive across a long turn such as a human approval wait.
func (b *RedisBus) RefreshLock(ctx context.Context, tenantID, sessionID, token string) (bool, error) {
	script := redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  redis.call("expire", KEYS[1], ARGV[2])
  return 1
end
return 0`)
	n, err := script.Run(ctx, b.client, []string{LockKey(tenantID, sessionID)},
		token, int(lockTTL/time.Second)).Int()
	if err != nil {
		return false, fmt.Errorf("bus: refresh session lock: %w", err)
	}
	return n == 1, nil
}

// ------------------------------------------------------------------ approval --

// SetPendingApproval records the single pending human approval of a session.
// The payload is an opaque JSON string owned by the caller. Re-setting
// overwrites (there is at most one pending approval per session).
func (b *RedisBus) SetPendingApproval(ctx context.Context, tenantID, sessionID, payload string, ttl time.Duration) error {
	if err := b.client.Set(ctx, ApprovalReqKey(tenantID, sessionID), payload, ttl).Err(); err != nil {
		return fmt.Errorf("bus: set pending approval: %w", err)
	}
	return nil
}

// PendingApproval returns the pending approval payload of a session ("" when
// none).
func (b *RedisBus) PendingApproval(ctx context.Context, tenantID, sessionID string) (string, error) {
	v, err := b.client.Get(ctx, ApprovalReqKey(tenantID, sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("bus: get pending approval: %w", err)
	}
	return v, nil
}

// ClearPendingApproval removes the pending approval record of a session.
func (b *RedisBus) ClearPendingApproval(ctx context.Context, tenantID, sessionID string) error {
	if err := b.client.Del(ctx, ApprovalReqKey(tenantID, sessionID)).Err(); err != nil {
		return fmt.Errorf("bus: clear pending approval: %w", err)
	}
	return nil
}

// ResolveApproval records the human decision ("approve" / "deny") for a
// session. The waiting reviewer polls ApprovalResult and wakes up on it.
func (b *RedisBus) ResolveApproval(ctx context.Context, tenantID, sessionID, decision string) error {
	if err := b.client.Set(ctx, ApprovalResKey(tenantID, sessionID), decision, approvalResTTL).Err(); err != nil {
		return fmt.Errorf("bus: resolve approval: %w", err)
	}
	return nil
}

// ApprovalResult returns the resolved human decision of a session ("" while
// still pending). The reviewer consumes it once and then clears both keys.
func (b *RedisBus) ApprovalResult(ctx context.Context, tenantID, sessionID string) (string, error) {
	v, err := b.client.Get(ctx, ApprovalResKey(tenantID, sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("bus: get approval result: %w", err)
	}
	return v, nil
}
