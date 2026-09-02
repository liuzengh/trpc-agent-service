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

// Key builders. Session-scoped keys carry the tenant so tenants are isolated
// at the key-namespace level (detailed design §3).
func RouteKey(tenantID, sessionID string) string {
	return "route:" + tenantID + ":" + sessionID
}

// LockKey returns the per-session serialization lock key.
func LockKey(tenantID, sessionID string) string {
	return "lock:session:" + tenantID + ":" + sessionID
}

// IdemKey returns the message idempotency key.
func IdemKey(msgKey string) string {
	return "idem:" + msgKey
}

// ApprovalReqKey returns the pending-approval key of a session. A session has
// at most one pending human approval at a time (single-pending model).
func ApprovalReqKey(tenantID, sessionID string) string {
	return "approval:req:" + tenantID + ":" + sessionID
}

// ApprovalResKey returns the human decision key of a session. The blocking
// reviewer polls it while the agent turn is suspended.
func ApprovalResKey(tenantID, sessionID string) string {
	return "approval:res:" + tenantID + ":" + sessionID
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
// owns the cross-node session state (route / idempotency / lock / approval).
type RedisBus struct {
	client *redis.Client
	// consumeWorkers bounds how many messages a single consumer processes at
	// once. A bounded pool keeps one long turn (e.g. an approval wait) from
	// blocking the delivery of later messages, e.g. the human approval reply.
	consumeWorkers int
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
// messages concurrently per consumer. n < 1 resets to the default.
func (b *RedisBus) WithConsumeWorkers(n int) *RedisBus {
	if n < 1 {
		n = defaultConsumeWorkers
	}
	cp := *b
	cp.consumeWorkers = n
	return &cp
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

// handle decodes one stream message, runs fn, and acks on success. A malformed
// envelope is acked too, so it can never become a poison pill.
func (b *RedisBus) handle(ctx context.Context, group string, msg redis.XMessage, fn func(ctx context.Context, m *Message) error) {
	m, err := decode(msg.Values)
	if err != nil {
		_ = b.client.XAck(ctx, StreamInbound, group, msg.ID).Err()
		return
	}
	if err := fn(ctx, m); err != nil {
		return // leave unacked: XAUTOCLAIM will retry it later
	}
	_ = b.client.XAck(ctx, StreamInbound, group, msg.ID).Err()
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
	lockTTL  = 30 * time.Second
	idemTTL  = 24 * time.Hour
	// approvalResTTL keeps a resolved human decision visible long enough for
	// the waiting reviewer to pick it up (poll interval is ~1s).
	approvalResTTL = 2 * time.Minute
	// defaultConsumeWorkers is the per-consumer message concurrency used by
	// RedisBus when no explicit value is configured.
	defaultConsumeWorkers = 16
)

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

// Idempotent reports true when msgKey is seen for the first time within the
// TTL window, i.e. the caller should process the message.
func (b *RedisBus) Idempotent(ctx context.Context, msgKey string) (bool, error) {
	ok, err := b.client.SetNX(ctx, IdemKey(msgKey), "1", idemTTL).Result()
	if err != nil {
		return false, fmt.Errorf("bus: setnx idem: %w", err)
	}
	return ok, nil
}

// SeenIdem reports whether msgKey was marked, without setting it. Check-then-
// process flows mark only after the durable work committed, so a crash between
// check and commit still reprocesses on redelivery.
func (b *RedisBus) SeenIdem(ctx context.Context, msgKey string) (bool, error) {
	_, err := b.client.Get(ctx, IdemKey(msgKey)).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("bus: get idem: %w", err)
	}
	return true, nil
}

// MarkIdem records msgKey as processed for the TTL window (the fast-path
// cache in front of the MySQL idempotency_keys table).
func (b *RedisBus) MarkIdem(ctx context.Context, msgKey string) error {
	if err := b.client.Set(ctx, IdemKey(msgKey), "1", idemTTL).Err(); err != nil {
		return fmt.Errorf("bus: set idem: %w", err)
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
