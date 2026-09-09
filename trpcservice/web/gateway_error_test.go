package web_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// gwTenantWithPolicy is a resolver fixture whose tenant carries an explicit
// gateway rate_policy. The ids are unique to this file so the limiter's
// bucket key never collides with other tests' fixtures.
func gwTenantWithPolicy() tenant.Data {
	return tenant.Data{
		Tenants: []tenant.Tenant{{
			ID: "t-rlpol", Name: "demo", Status: tenant.StatusActive,
			RatePolicy: []byte(`{"qps":1000,"burst":1000}`),
		}},
		Apps:     []tenant.AgentApp{{ID: "a-rlpol", TenantID: "t-rlpol", Name: "assistant", Version: 1, Status: "published"}},
		Bindings: []tenant.ChannelBinding{{ID: "b-rlpol", TenantID: "t-rlpol", Channel: "mock", AppID: "a-rlpol", WebhookPath: "/mock/callback", Status: tenant.StatusActive}},
	}
}

// Fail closed with no resolver at all (PG down at startup): the message is
// rejected before any infrastructure is touched.
func TestEnqueueNoRoutes(t *testing.T) {
	h := web.EnqueueHandler{}
	_, err := h.Handle(context.Background(), channels.InboundMessage{
		Channel: "mock", MsgID: "m1", WebhookPath: "/mock/callback",
	})
	if err == nil || !strings.Contains(err.Error(), "tenant routing unavailable") {
		t.Fatalf("nil resolver must fail closed, got %v", err)
	}
}

// The tenant rate_policy overrides the platform default QPS/burst: with a
// high-policy tenant both messages pass even though the defaults would have
// rate-limited the second (burst=1).
func TestEnqueueTenantRatePolicyOverride(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)

	stream := storage.NewStream(rdb)
	inbound := "test:inbound-rlpol:" + t.Name()
	t.Cleanup(func() { _ = rdb.Del(context.Background(), inbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	h := web.EnqueueHandler{
		Stream: stream, Dedup: storage.NewDeduper(rdb),
		Routes: tenant.NewResolver(fakeStore{data: gwTenantWithPolicy()}),
		// Defaults would allow exactly one message; the tenant policy
		// (qps=1000, burst=1000) must be picked up instead.
		Limiter: storage.NewLimiter(rdb), DefaultQPS: 0.001, DefaultBurst: 1,
		InStream: inbound,
	}
	mk := func(id string) channels.InboundMessage {
		return channels.InboundMessage{
			Channel: "mock", MsgID: id, SessionKey: "dm:mock:u1", UserID: "u1",
			Text: "hi", WebhookPath: "/mock/callback",
		}
	}
	id1 := fmt.Sprintf("rlpol-1-%d", time.Now().UnixNano())
	id2 := fmt.Sprintf("rlpol-2-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(),
			"dedup:mock:b-rlpol:"+id1, "dedup:mock:b-rlpol:"+id2,
			"ratelimit:tenant:t-rlpol")
	})

	if _, err := h.Handle(ctx, mk(id1)); err != nil {
		t.Fatalf("first message must pass: %v", err)
	}
	if _, err := h.Handle(ctx, mk(id2)); err != nil {
		t.Fatalf("tenant rate_policy must override the defaults: %v", err)
	}
}

// A Redis hiccup during the rate limit check fails open (the backpressure
// check still bounds the queue): the message is enqueued, not rejected.
func TestEnqueueLimiterFailOpen(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)
	dead := redis.NewClient(&redis.Options{Addr: testenv.RedisAddr()})
	_ = dead.Close()

	stream := storage.NewStream(rdb)
	inbound := "test:inbound-failopen:" + t.Name()
	t.Cleanup(func() { _ = rdb.Del(context.Background(), inbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	h := web.EnqueueHandler{
		Stream:  stream,
		Routes:  tenant.NewResolver(fakeStore{data: testData()}),
		Limiter: storage.NewLimiter(dead), DefaultQPS: 0.001, DefaultBurst: 1,
		InStream: inbound,
	}
	msgID := fmt.Sprintf("failopen-%d", time.Now().UnixNano())
	if _, err := h.Handle(ctx, channels.InboundMessage{
		Channel: "mock", MsgID: msgID, SessionKey: "dm:mock:u1", UserID: "u1",
		Text: "hi", WebhookPath: "/mock/callback",
	}); err != nil {
		t.Fatalf("limiter failure must fail open, got %v", err)
	}
}

// A dedup check failure is an error, not a pass-through: the message must
// not be enqueued on a maybe-duplicate.
func TestEnqueueDedupCheckError(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)
	dead := redis.NewClient(&redis.Options{Addr: testenv.RedisAddr()})
	_ = dead.Close()

	h := web.EnqueueHandler{
		Stream: storage.NewStream(rdb), Dedup: storage.NewDeduper(dead),
		Routes: tenant.NewResolver(fakeStore{data: testData()}),
	}
	_, err := h.Handle(ctx, channels.InboundMessage{
		Channel: "mock", MsgID: "m1", WebhookPath: "/mock/callback",
	})
	if err == nil || !strings.Contains(err.Error(), "dedup check") {
		t.Fatalf("dedup check failure must surface, got %v", err)
	}
}

// A message the gateway has already seen is answered with ErrDuplicate so
// the channel layer returns 200 and the IM stops redelivering.
func TestEnqueueDuplicate(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)

	stream := storage.NewStream(rdb)
	inbound := "test:inbound-dup:" + t.Name()
	t.Cleanup(func() { _ = rdb.Del(context.Background(), inbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	h := web.EnqueueHandler{
		Stream: stream, Dedup: storage.NewDeduper(rdb),
		Routes: tenant.NewResolver(fakeStore{data: testData()}),
	}
	msgID := fmt.Sprintf("dup-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = rdb.Del(context.Background(), "dedup:mock:b1:"+msgID) })
	msg := channels.InboundMessage{
		Channel: "mock", MsgID: msgID, SessionKey: "dm:mock:u1", UserID: "u1",
		Text: "hi", WebhookPath: "/mock/callback",
	}
	if _, err := h.Handle(ctx, msg); err != nil {
		t.Fatalf("first delivery must pass: %v", err)
	}
	if _, err := h.Handle(ctx, msg); !errors.Is(err, channels.ErrDuplicate) {
		t.Fatalf("second delivery must be ErrDuplicate, got %v", err)
	}
}

// The default inbound stream is stream:inbound; with no deduper the rollback
// closure is a no-op instead of a nil dereference. Both ride a dead Redis:
// the route resolves first (no infra), then the queue-length check fails.
func TestEnqueueDefaultStreamAndNilDedupRollback(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: testenv.RedisAddr()})
	_ = dead.Close()

	h := web.EnqueueHandler{
		Stream: storage.NewStream(dead),
		Routes: tenant.NewResolver(fakeStore{data: testData()}),
		// InStream empty → storage.StreamInbound; the resolver needs no
		// infrastructure, so the first Redis touch is the XLEN below.
	}
	_, err := h.Handle(context.Background(), channels.InboundMessage{
		Channel: "mock", MsgID: "m1", WebhookPath: "/mock/callback",
	})
	if err == nil || !strings.Contains(err.Error(), "queue length check") {
		t.Fatalf("dead Redis must surface at the queue check, got %v", err)
	}
}

// With backpressure disabled the enqueue itself is the failure point
// (WRONGTYPE key): the error surfaces and the dedup key is rolled back so
// the IM's redelivery is still admissible.
func TestEnqueueAddFailure(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)

	inbound := "test:inbound-addfail:" + t.Name()
	t.Cleanup(func() { _ = rdb.Del(context.Background(), inbound) })
	if err := rdb.Set(ctx, inbound, "not-a-stream", 0).Err(); err != nil {
		t.Fatal(err)
	}

	msgID := fmt.Sprintf("addfail-%d", time.Now().UnixNano())
	dedupKey := "dedup:mock:b1:" + msgID
	t.Cleanup(func() { _ = rdb.Del(context.Background(), dedupKey) })

	h := web.EnqueueHandler{
		Stream: storage.NewStream(rdb), Dedup: storage.NewDeduper(rdb),
		Routes: tenant.NewResolver(fakeStore{data: testData()}),
		// Negative limit disables the backpressure check: the failure moves
		// to the XADD.
		BackpressureLimit: -1,
		InStream:          inbound,
	}
	if _, err := h.Handle(ctx, channels.InboundMessage{
		Channel: "mock", MsgID: msgID, SessionKey: "dm:mock:u1", UserID: "u1",
		Text: "hi", WebhookPath: "/mock/callback",
	}); err == nil || !strings.Contains(err.Error(), "enqueue inbound") {
		t.Fatalf("XADD to a non-stream key must fail, got %v", err)
	}
	if n, err := rdb.Exists(ctx, dedupKey).Result(); err != nil || n != 0 {
		t.Fatalf("dedup key must be rolled back after a failed enqueue (n=%d err=%v)", n, err)
	}
}

// cancelOnFirstUse cancels the request context the first time its client runs
// a command, standing in for the platform dropping the callback connection
// while the handler is still working.
type cancelOnFirstUse struct {
	cancel context.CancelFunc
	once   sync.Once
}

func (h *cancelOnFirstUse) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *cancelOnFirstUse) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.once.Do(h.cancel)
		return next(ctx, cmd)
	}
}

func (h *cancelOnFirstUse) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// The dedup rollback must outlive the request context. The platform cancels
// the callback once its own timeout fires (WeCom: ~5s); a Forget on that dead
// context would fail and leave the key standing for its whole TTL, so every
// redelivery the platform sends to retry the message would be swallowed as
// ErrDuplicate and answered 200 — the message is lost instead of retried.
func TestEnqueueRollsBackDedupAfterRequestCancellation(t *testing.T) {
	dedupRdb := testenv.Redis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The Stream rides a second client whose first command kills the request,
	// so the dedup Check (live client) has already written the key by the time
	// the queue-length check fails.
	streamRdb := redis.NewClient(&redis.Options{Addr: testenv.RedisAddr()})
	streamRdb.AddHook(&cancelOnFirstUse{cancel: cancel})
	t.Cleanup(func() { _ = streamRdb.Close() })

	msgID := fmt.Sprintf("cancel-%d", time.Now().UnixNano())
	dedupKey := "dedup:mock:b1:" + msgID
	t.Cleanup(func() { _ = dedupRdb.Del(context.Background(), dedupKey) })

	h := web.EnqueueHandler{
		Stream: storage.NewStream(streamRdb), Dedup: storage.NewDeduper(dedupRdb),
		Routes:            tenant.NewResolver(fakeStore{data: testData()}),
		BackpressureLimit: 100,
	}
	if _, err := h.Handle(ctx, channels.InboundMessage{
		Channel: "mock", MsgID: msgID, SessionKey: "dm:mock:u1", UserID: "u1",
		Text: "hi", WebhookPath: "/mock/callback",
	}); err == nil {
		t.Fatal("the cancelled queue-length check must surface as an error")
	}
	if ctx.Err() == nil {
		t.Fatal("the test hook must have cancelled the request context")
	}
	if n, err := dedupRdb.Exists(context.Background(), dedupKey).Result(); err != nil || n != 0 {
		t.Fatalf("dedup key must be rolled back even though the request died (n=%d err=%v)", n, err)
	}
}
