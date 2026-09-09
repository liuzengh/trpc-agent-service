package web_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// fakeStore implements tenant.Store without infrastructure.
type fakeStore struct{ data tenant.Data }

func (f fakeStore) LoadAll(context.Context) (tenant.Data, error) { return f.data, nil }

func testData() tenant.Data {
	return tenant.Data{
		Tenants:  []tenant.Tenant{{ID: "t1", Name: "demo", Status: tenant.StatusActive}},
		Apps:     []tenant.AgentApp{{ID: "a1", TenantID: "t1", Name: "assistant", Version: 1, Status: "published"}},
		Bindings: []tenant.ChannelBinding{{ID: "b1", TenantID: "t1", Channel: "mock", AppID: "a1", WebhookPath: "/mock/callback", Status: tenant.StatusActive}},
	}
}

// An unroutable callback is rejected before dedup or enqueue: no
// infrastructure is touched (Stream stays nil), the error is returned.
func TestEnqueueRejectsUnknownRoute(t *testing.T) {
	h := web.EnqueueHandler{Routes: tenant.NewResolver(fakeStore{data: testData()})}
	_, err := h.Handle(context.Background(), channels.InboundMessage{
		Channel: "mock", MsgID: "m1", WebhookPath: "/nope",
	})
	if !errors.Is(err, tenant.ErrUnknownBinding) {
		t.Fatalf("want ErrUnknownBinding, got %v", err)
	}
}

// End to end at the gateway: a routed message lands on the inbound stream
// with tenant_id and app_id stamped. Needs the Redis from compose
// (localhost:6380); skips when unreachable.
func TestEnqueueStampsTenant(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)
	defer func() { _ = rdb.Close() }()

	stream := storage.NewStream(rdb)
	inbound := "test:inbound:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound) })
	// The group must exist before the message is enqueued: entries added
	// after the group started at "$" are visible to it.
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	h := web.EnqueueHandler{
		Stream:   stream,
		Dedup:    storage.NewDeduper(rdb),
		Routes:   tenant.NewResolver(fakeStore{data: testData()}),
		InStream: inbound,
	}
	msgID := fmt.Sprintf("test-gw-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(context.Background(), "dedup:mock:b1:"+msgID) })
	if _, err := h.Handle(ctx, channels.InboundMessage{
		Channel: "mock", MsgID: msgID, SessionKey: "dm:mock:u1", UserID: "u1",
		Text: "hi", WebhookPath: "/mock/callback",
	}); err != nil {
		t.Fatal(err)
	}

	msgs, err := stream.Read(ctx, inbound, "workers", "test-gw", 1, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 enqueued message, got %d", len(msgs))
	}
	var got channels.InboundMessage
	if err := json.Unmarshal(msgs[0].Payload, &got); err != nil {
		t.Fatal(err)
	}
	if got.TenantID != "t1" || got.AppID != "a1" {
		t.Fatalf("tenant not stamped: %+v", got)
	}
	// Trace fields are intentionally not asserted: they are only stamped when
	// an OTel provider is installed.
}

// A failed enqueue must not leave the dedup key behind: the IM redelivery is
// the retry path, and a stale key would drop every retry for
// the full 24h TTL — the message would be lost instead of delayed.
func TestEnqueueRollsBackDedupOnFailure(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)
	defer func() { _ = rdb.Close() }()

	// Occupy the queue key with a plain string so XADD fails with WRONGTYPE:
	// a deterministic enqueue failure without stubbing the stream.
	inbound := "test:inbound-broken:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound) })
	if err := rdb.Set(ctx, inbound, "not-a-stream", 0).Err(); err != nil {
		t.Fatal(err)
	}

	msgID := fmt.Sprintf("test-gw-rollback-%d", time.Now().UnixNano())
	dedupKey := "dedup:mock:b1:" + msgID
	t.Cleanup(func() { rdb.Del(context.Background(), dedupKey) })

	h := web.EnqueueHandler{
		Stream:   storage.NewStream(rdb),
		Dedup:    storage.NewDeduper(rdb),
		Routes:   tenant.NewResolver(fakeStore{data: testData()}),
		InStream: inbound,
	}
	if _, err := h.Handle(ctx, channels.InboundMessage{
		Channel: "mock", MsgID: msgID, SessionKey: "dm:mock:u1", UserID: "u1",
		Text: "hi", WebhookPath: "/mock/callback",
	}); err == nil {
		t.Fatal("enqueue to a non-stream key must fail")
	}

	n, err := rdb.Exists(ctx, dedupKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("dedup key must be rolled back so the IM can redeliver")
	}
}

// A tenant over its rate limit is rejected before dedup (ErrOverloaded → the
// channel answers 5xx so the IM redelivers), and the rejection consumes no
// dedup key — the redelivery must still be admissible.
func TestEnqueueRateLimited(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)
	defer func() { _ = rdb.Close() }()
	// Start from a clean slate: bucket keys outlive a crashed test run
	// (10min TTL), and a stale empty bucket would reject even the first
	// message.
	rdb.Del(ctx, "ratelimit:tenant:t1")
	t.Cleanup(func() { rdb.Del(context.Background(), "ratelimit:tenant:t1") })

	h := web.EnqueueHandler{
		Stream: storage.NewStream(rdb), Dedup: storage.NewDeduper(rdb),
		Routes: tenant.NewResolver(fakeStore{data: testData()}),
		// burst=1 with a near-zero refill: the first message passes, the
		// second is denied.
		Limiter: storage.NewLimiter(rdb), DefaultQPS: 0.001, DefaultBurst: 1,
		InStream: "test:inbound-rl:" + t.Name(),
	}
	t.Cleanup(func() { rdb.Del(context.Background(), "test:inbound-rl:"+t.Name()) })

	mk := func(id string) channels.InboundMessage {
		return channels.InboundMessage{
			Channel: "mock", MsgID: id, SessionKey: "dm:mock:u1", UserID: "u1",
			Text: "hi", WebhookPath: "/mock/callback",
		}
	}
	id1 := fmt.Sprintf("rl-1-%d", time.Now().UnixNano())
	id2 := fmt.Sprintf("rl-2-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(context.Background(), "dedup:mock:b1:"+id1) })
	if _, err := h.Handle(ctx, mk(id1)); err != nil {
		t.Fatalf("first message must pass: %v", err)
	}
	_, err := h.Handle(ctx, mk(id2))
	if !errors.Is(err, web.ErrOverloaded) {
		t.Fatalf("want ErrOverloaded, got %v", err)
	}
	// The rejection happened before dedup: no key was consumed for id2.
	if n, _ := rdb.Exists(ctx, "dedup:mock:b1:"+id2).Result(); n != 0 {
		t.Fatal("rate-limited message must not consume its dedup key")
	}
}

// With the queue at the backpressure limit the gateway rejects and rolls back
// the dedup key: refusing beats silent MAXLEN truncation.
func TestEnqueueBackpressure(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)
	defer func() { _ = rdb.Close() }()

	inbound := "test:inbound-bp:" + t.Name()
	rdb.Del(ctx, inbound) // clean slate: a leftover entry would trip backpressure immediately
	t.Cleanup(func() { rdb.Del(context.Background(), inbound) })

	h := web.EnqueueHandler{
		Stream: storage.NewStream(rdb), Dedup: storage.NewDeduper(rdb),
		Routes:   tenant.NewResolver(fakeStore{data: testData()}),
		InStream: inbound, BackpressureLimit: 1, // reject as soon as 1 entry sits in the queue
	}
	mk := func(id string) channels.InboundMessage {
		return channels.InboundMessage{
			Channel: "mock", MsgID: id, SessionKey: "dm:mock:u1", UserID: "u1",
			Text: "hi", WebhookPath: "/mock/callback",
		}
	}
	id1 := fmt.Sprintf("bp-1-%d", time.Now().UnixNano())
	id2 := fmt.Sprintf("bp-2-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(context.Background(), "dedup:mock:b1:"+id1, "dedup:mock:b1:"+id2) })
	if _, err := h.Handle(ctx, mk(id1)); err != nil {
		t.Fatalf("empty queue must accept: %v", err)
	}
	if _, err := h.Handle(ctx, mk(id2)); !errors.Is(err, web.ErrOverloaded) {
		t.Fatalf("full queue must reject with ErrOverloaded, got %v", err)
	}
	if n, _ := rdb.Exists(ctx, "dedup:mock:b1:"+id2).Result(); n != 0 {
		t.Fatal("backpressure rejection must roll back the dedup key")
	}
}
