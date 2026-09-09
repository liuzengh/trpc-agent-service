package channels_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// countingChannel records how many times Send was called. A non-nil gate
// blocks every Send until it is closed, so tests can freeze the sender
// mid-delivery and assert on stream state deterministically. An empty name
// means "counting".
type countingChannel struct {
	mu    sync.Mutex
	calls int
	gate  chan struct{}
	name  string
}

func (c *countingChannel) Name() string {
	if c.name == "" {
		return "counting"
	}
	return c.name
}
func (c *countingChannel) RegisterRoutes(_ *http.ServeMux, _ channels.Handler) {}
func (c *countingChannel) Send(ctx context.Context, _ channels.OutboundMessage) error {
	if c.gate != nil {
		select {
		case <-c.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return nil
}
func (c *countingChannel) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// A redelivered outbound message (sent but un-acked in a previous attempt)
// must not be sent twice.
func TestSenderOutboundIdempotency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	// Cleanups run LIFO; register Close first so the Del cleanup runs while
	// the client is still open.
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:sent:out:" + t.Name()
	// The msgID must be unique per run: the sent: marker lives 24h, so a
	// fixed ID would make the second run's FIRST delivery skip as
	// "already sent" (exposed by repeat runs against live Redis).
	msgID := fmt.Sprintf("dup-msg-%s-%d", t.Name(), time.Now().UnixNano())
	sentKey := fmt.Sprintf("sent:counting::%s", msgID)
	t.Cleanup(func() { rdb.Del(context.Background(), outbound, sentKey) })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream:   stream,
		Sent:     storage.NewSentMarker(rdb),
		Channels: map[string]channels.Channel{ch.Name(): ch},
		Name:     "test-s", InStream: outbound,
	}
	go func() { _ = sender.Run(ctx) }()

	out := channels.OutboundMessage{
		Channel: "counting", MsgID: msgID,
		SessionKey: "dm:counting:u1", UserID: "u1", Text: "reply",
	}
	payload, _ := json.Marshal(out)

	// Same payload twice: simulates "send succeeded, ack crashed" redelivery.
	if _, err := stream.Add(ctx, outbound, payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return ch.Calls() == 1 })

	// At this point the first delivery is acked. A duplicate entry (same msg)
	// arrives — the sent: key must block a second send.
	if _, err := stream.Add(ctx, outbound, payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		sent, err := storage.NewSentMarker(rdb).IsSent(ctx, "counting", "", out.MsgID)
		return err == nil && sent
	})
	time.Sleep(500 * time.Millisecond) // give a buggy duplicate a chance to fire
	if got := ch.Calls(); got != 1 {
		t.Fatalf("Send called %d times for the same message, want 1", got)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

// An exhausted send bucket re-queues the message instead of dropping it.
func TestSenderRateLimitedRequeues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:rl:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	t.Cleanup(func() { rdb.Del(context.Background(), "ratelimit:send:counting:t-rl") })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream:   stream,
		Channels: map[string]channels.Channel{"counting": ch},
		Name:     "test-rl",
		InStream: outbound,
		// burst=1 with a near-zero refill and a short wait: the first message
		// sends, the second spins 200ms and is re-queued.
		Limiter: storage.NewLimiter(rdb), SendQPS: 0.001, SendBurst: 1, SendWait: 200 * time.Millisecond,
	}
	go func() { _ = sender.Run(ctx) }()

	mk := func(id string) []byte {
		payload, _ := json.Marshal(channels.OutboundMessage{
			Channel: "counting", MsgID: id, SessionKey: "dm:counting:u1",
			UserID: "u1", Text: "hi", TenantID: "t-rl",
		})
		return payload
	}
	if _, err := stream.Add(ctx, outbound, mk("rl-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, outbound, mk("rl-b")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && ch.Calls() < 1 {
		time.Sleep(50 * time.Millisecond)
	}
	if ch.Calls() != 1 {
		t.Fatalf("want exactly 1 send (burst), got %d", ch.Calls())
	}
	// The rate-limited message must still be in the stream (re-queued copy or
	// pending original), not dropped.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		n, err := stream.Len(ctx, outbound)
		if err != nil {
			t.Fatal(err)
		}
		if n >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("rate-limited message vanished from the stream")
}

// A rate-limited requeue is a delay, not a failure. It must never reach the
// attempts counter: counting takeovers let a tenant that exhausted its send
// bucket long enough have perfectly deliverable replies dead-lettered.
func TestSenderRateLimitedRequeueIsNotAnAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	uid := fmt.Sprintf("%d", time.Now().UnixNano())
	stream := storage.NewStream(rdb)
	outbound := "test:rlcount:out:" + t.Name() + "-" + uid
	bucket := "ratelimit:send:counting:t-rlcount"
	t.Cleanup(func() { rdb.Del(context.Background(), outbound, bucket) })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream:   stream,
		Channels: map[string]channels.Channel{"counting": ch},
		Name:     "test-rlcount",
		InStream: outbound,
		// One token, no meaningful refill, a wait too short to ride it out: the
		// first reply sends and every later one is left pending for the reaper.
		Limiter: storage.NewLimiter(rdb), SendQPS: 0.001, SendBurst: 1, SendWait: 20 * time.Millisecond,
		// The strictest dead-letter bound, reaped as often as the 2s read block
		// allows: two takeovers are enough to have dead-lettered this reply when
		// a takeover counted as an attempt.
		ReapInterval: 50 * time.Millisecond, MaxIdle: time.Millisecond, MaxAttempts: 1,
	}
	go func() { _ = sender.Run(ctx) }()

	mk := func(id string) []byte {
		payload, _ := json.Marshal(channels.OutboundMessage{
			Channel: "counting", MsgID: id, SessionKey: "dm:counting:u1",
			UserID: "u1", Text: "hi", TenantID: "t-rlcount",
		})
		return payload
	}
	if _, err := stream.Add(ctx, outbound, mk("rlc-a")); err != nil { // takes the only token
		t.Fatal(err)
	}
	paced, err := stream.Add(ctx, outbound, mk("rlc-b"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), "retry:"+outbound+":senders:"+paced) })

	waitFor(t, func() bool { return ch.Calls() >= 1 })

	// Two reap cycles land at roughly 2s and 4s (the read block dominates the
	// loop), so by now the paced reply has been taken over twice.
	time.Sleep(4600 * time.Millisecond)

	if got := ch.Calls(); got != 1 {
		t.Fatalf("want exactly 1 send while the bucket is empty, got %d", got)
	}
	if got, err := stream.Attempts(ctx, outbound, "senders", paced); err != nil || got != 0 {
		t.Fatalf("a paced reply must not be counted as an attempt, got %d err=%v", got, err)
	}
	n, _, err := stream.Pending(ctx, outbound, "senders")
	if err != nil || n != 1 {
		t.Fatalf("the paced reply must still be waiting to go out, got %d pending err=%v", n, err)
	}
}

// A tenant send override (rate_policy send_qps/send_burst) replaces the
// platform default bucket shape.
func TestSenderTenantRateOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:rl-tenant:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	t.Cleanup(func() {
		rdb.Del(context.Background(), "ratelimit:send:counting:t-fast", "ratelimit:send:counting:t-slow")
	})
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream: stream, Channels: map[string]channels.Channel{"counting": ch},
		Name: "test-rl-tenant", InStream: outbound,
		// Platform default is slow (1 token / 100s); the tenant override is fast.
		Limiter: storage.NewLimiter(rdb), SendQPS: 0.01, SendBurst: 1, SendWait: 300 * time.Millisecond,
		SendPolicyFor: func(_ context.Context, tenantID string) (float64, int, bool) {
			if tenantID == "t-fast" {
				return 100, 10, true
			}
			return 0, 0, false
		},
	}
	go func() { _ = sender.Run(ctx) }()

	mk := func(id, tenant string) []byte {
		payload, _ := json.Marshal(channels.OutboundMessage{
			Channel: "counting", MsgID: id, SessionKey: "dm:counting:u1",
			UserID: "u1", Text: "hi", TenantID: tenant,
		})
		return payload
	}
	// Two sends each: t-fast's override lets both through quickly; t-slow's
	// second one waits for the default bucket and gets re-queued.
	for _, m := range [][2]string{{"fast-a", "t-fast"}, {"fast-b", "t-fast"}, {"slow-a", "t-slow"}, {"slow-b", "t-slow"}} {
		if _, err := stream.Add(ctx, outbound, mk(m[0], m[1])); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ch.Calls() >= 3 { // fast-a, fast-b, slow-a
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("want 3 sends (override tenant not throttled), got %d", ch.Calls())
}

// A message belonging to another consumer group on the same stream is
// acked and dropped here: not sent, not marked sent, no rate token spent.
// It must ack — an un-acked skip would be reaped, inflate its attempts
// counter and eventually dead-letter a deliverable message.
func TestSenderSkip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:skip:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	// The msgIDs must be unique per run: the sent: markers live 24h, so fixed
	// IDs would make a rerun skip as "already sent".
	idA := fmt.Sprintf("skip-a-%d", time.Now().UnixNano())
	idB := fmt.Sprintf("skip-b-%d", time.Now().UnixNano())
	sentA := fmt.Sprintf("sent:counting::%s", idA)
	sentB := fmt.Sprintf("sent:wecomws::%s", idB)
	t.Cleanup(func() { rdb.Del(context.Background(), sentA, sentB) })

	mk := func(id, channel string) []byte {
		payload, _ := json.Marshal(channels.OutboundMessage{
			Channel: channel, MsgID: id, SessionKey: "dm:" + channel + ":u1",
			UserID: "u1", Text: "reply",
		})
		return payload
	}
	// Both messages land before the sender's first read, so one batch covers
	// them and the PEL state is deterministic.
	if _, err := stream.Add(ctx, outbound, mk(idA, "counting")); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, outbound, mk(idB, "wecomws")); err != nil {
		t.Fatal(err)
	}

	// The gated Send freezes the first delivery while both entries sit in
	// the PEL. The wecomws spy makes the Skip branch load-bearing: without
	// it the sender would deliver skip-b through the spy, failing the
	// spy.Calls assertion below.
	ch := &countingChannel{gate: make(chan struct{})}
	ws := &countingChannel{name: "wecomws"}
	sender := &channels.Sender{
		Stream: stream, Sent: storage.NewSentMarker(rdb),
		Channels: map[string]channels.Channel{ch.Name(): ch, ws.Name(): ws},
		Name:     "test-skip", InStream: outbound,
		Skip: func(msg channels.OutboundMessage) bool { return msg.Channel == "wecomws" },
	}
	go func() { _ = sender.Run(ctx) }()
	waitFor(t, func() bool {
		n, _, err := stream.Pending(ctx, outbound, "senders")
		return err == nil && n == 2
	})

	// Release the delivery: the counting message sends and acks, the wecomws
	// message is skipped and acked — the PEL drains to zero.
	close(ch.gate)
	waitFor(t, func() bool {
		n, _, err := stream.Pending(ctx, outbound, "senders")
		return err == nil && n == 0
	})

	if got := ch.Calls(); got != 1 {
		t.Fatalf("Send called %d times, want 1 (the skipped message must not send)", got)
	}
	if got := ws.Calls(); got != 0 {
		t.Fatalf("the skipped message reached the wecomws channel %d times — the Skip branch leaked", got)
	}
	sent, err := storage.NewSentMarker(rdb).IsSent(ctx, "counting", "", idA)
	if err != nil || !sent {
		t.Fatalf("delivered message must be marked sent (sent=%v err=%v)", sent, err)
	}
	sent, err = storage.NewSentMarker(rdb).IsSent(ctx, "wecomws", "", idB)
	if err != nil || sent {
		t.Fatalf("skipped message must NOT be marked sent (sent=%v err=%v)", sent, err)
	}
}

// A sender with an explicit Group consumes its own consumer group: its acks
// (and skips) do not touch the other group's view of the stream.
func TestSenderGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:group:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	// Both groups exist before the message lands (a new group starts at $).
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}
	if err := stream.EnsureGroup(ctx, outbound, "senders-ws"); err != nil {
		t.Fatal(err)
	}

	// The msgID must be unique per run: the sent: marker lives 24h, so a
	// fixed ID would make a rerun skip as "already sent".
	msgID := fmt.Sprintf("ws-1-%d", time.Now().UnixNano())
	sentKey := fmt.Sprintf("sent:wecomws:b1:%s", msgID)
	t.Cleanup(func() { rdb.Del(context.Background(), sentKey) })

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream: stream, Sent: storage.NewSentMarker(rdb),
		Channels: map[string]channels.Channel{"wecomws": ch},
		Name:     "test-ws", InStream: outbound,
		Group: "senders-ws",
		// This sender delivers wecomws only.
		Skip: func(msg channels.OutboundMessage) bool { return msg.Channel != "wecomws" },
	}
	go func() { _ = sender.Run(ctx) }()

	payload, _ := json.Marshal(channels.OutboundMessage{
		Channel: "wecomws", MsgID: msgID, SessionKey: "dm:wecomws:u1",
		UserID: "u1", Text: "reply", BindingID: "b1",
	})
	if _, err := stream.Add(ctx, outbound, payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return ch.Calls() == 1 })

	// Delivered AND acked in its own group.
	waitFor(t, func() bool {
		n, _, err := stream.Pending(ctx, outbound, "senders-ws")
		return err == nil && n == 0
	})

	// The main group still sees the message: an ack in senders-ws does not
	// consume it for senders — that is the whole point of the partition.
	probe, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "senders",
		Consumer: "probe",
		Streams:  []string{outbound, ">"},
		Count:    10,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(probe) != 1 || len(probe[0].Messages) != 1 {
		t.Fatalf("main group must still see the message after the ws group consumed it, got %+v", probe)
	}
}

// A wedged sender's pending reply is taken over and delivered by the reaper
// (XAUTOCLAIM), symmetric with the worker's crash recovery.
func TestSenderReapsOrphanedPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	outbound := "test:reap:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	// Plant a pending message owned by a dead consumer: read it (never ack),
	// then walk away.
	payload, _ := json.Marshal(channels.OutboundMessage{
		Channel: "counting", MsgID: "orphan-1", SessionKey: "dm:counting:u1",
		UserID: "u1", Text: "孤儿回复", TenantID: "t1",
	})
	if _, err := stream.Add(ctx, outbound, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Read(ctx, outbound, "senders", "dead-sender", 1, time.Second); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream: stream, Channels: map[string]channels.Channel{"counting": ch},
		Name: "test-reaper", InStream: outbound,
		ReapInterval: 100 * time.Millisecond, MaxIdle: 100 * time.Millisecond,
	}
	go func() { _ = sender.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ch.Calls() >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("orphaned pending message was not reaped and delivered")
}

// failingChannel always fails the send: the message must stay pending (no
// Ack) so a later retry — not a drop — delivers it.
type failingChannel struct {
	mu    sync.Mutex
	calls int
}

func (c *failingChannel) Name() string                                        { return "failing" }
func (c *failingChannel) RegisterRoutes(_ *http.ServeMux, _ channels.Handler) {}
func (c *failingChannel) Send(context.Context, channels.OutboundMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return errors.New("im api down")
}
func (c *failingChannel) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// Unparseable payloads are poison: acked and dropped, never retried. An
// unknown channel name is a configuration error, likewise acked and dropped.
func TestSenderDropsPoisonAndUnknownChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	// The per-run suffix keeps concurrent runs of this suite (CI + a dev
	// shell) from sharing stream keys and interfering with each other.
	uid := fmt.Sprintf("%d", time.Now().UnixNano())

	stream := storage.NewStream(rdb)
	outbound := "test:poison:out:" + t.Name() + "-" + uid
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream: stream, Channels: map[string]channels.Channel{ch.Name(): ch},
		Name: "test-poison", InStream: outbound,
	}
	go func() { _ = sender.Run(ctx) }()

	// Not JSON at all, and JSON naming a channel this sender does not have.
	if _, err := stream.Add(ctx, outbound, []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	unknown, err := json.Marshal(channels.OutboundMessage{
		Channel: "no-such-channel", MsgID: "u-" + t.Name(),
		SessionKey: "dm:x:u1", UserID: "u1", Text: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, outbound, unknown); err != nil {
		t.Fatal(err)
	}

	// Both are acked and dropped: the PEL drains to zero and nothing was sent.
	waitFor(t, func() bool {
		n, _, err := stream.Pending(ctx, outbound, "senders")
		return err == nil && n == 0
	})
	time.Sleep(300 * time.Millisecond) // give a buggy retry a chance to fire
	if got := ch.Calls(); got != 0 {
		t.Fatalf("poison/unknown-channel messages reached the channel %d times, want 0", got)
	}
}

// A channel whose Send fails leaves the message pending (no Ack) and records the
// failure against this group's counter: the reaper redelivers it later, so a
// transient IM outage delays instead of losing, and a permanent one is
// eventually dead-lettered.
func TestSenderSendErrorLeavesPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	uid := fmt.Sprintf("%d", time.Now().UnixNano())
	stream := storage.NewStream(rdb)
	outbound := "test:senderr:out:" + t.Name() + "-" + uid
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &failingChannel{}
	sender := &channels.Sender{
		Stream: stream, Channels: map[string]channels.Channel{ch.Name(): ch},
		Name: "test-senderr", InStream: outbound,
	}
	go func() { _ = sender.Run(ctx) }()

	payload, err := json.Marshal(channels.OutboundMessage{
		Channel: "failing", MsgID: "se-" + t.Name(),
		SessionKey: "dm:failing:u1", UserID: "u1", Text: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := stream.Add(ctx, outbound, payload)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), "retry:"+outbound+":senders:"+id) })

	waitFor(t, func() bool { return ch.Calls() >= 1 })
	n, _, err := stream.Pending(ctx, outbound, "senders")
	if err != nil || n != 1 {
		t.Fatalf("failed send must stay pending for retry, got %d pending, err %v", n, err)
	}
	// The failure is counted once, and only for the group that owns it.
	waitFor(t, func() bool {
		got, err := stream.Attempts(ctx, outbound, "senders", id)
		return err == nil && got >= 1
	})
	if got, err := stream.Attempts(ctx, outbound, "senders-ws", id); err != nil || got != 0 {
		t.Fatalf("another group must not inherit this failure, got %d err=%v", got, err)
	}
}

// A message that keeps failing past MaxAttempts is dead-lettered (moved to
// stream:deadletter and acked) so it cannot loop forever — and it is never
// delivered to the channel.
func TestSenderDeadLettersAfterMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	uid := fmt.Sprintf("%d", time.Now().UnixNano())
	stream := storage.NewStream(rdb)
	outbound := "test:dl:out:" + t.Name() + "-" + uid
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	// Plant a pending message owned by a dead consumer: read it (never ack).
	payload, err := json.Marshal(channels.OutboundMessage{
		Channel: "counting", MsgID: "dl-" + t.Name(),
		SessionKey: "dm:counting:u1", UserID: "u1", Text: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	id, err := stream.Add(ctx, outbound, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Read(ctx, outbound, "senders", "dead-sender-dl", 1, time.Second); err != nil {
		t.Fatal(err)
	}
	// Two genuine delivery failures already recorded, one past MaxAttempts=1:
	// the reaper only reads the counter now, so it must dead-letter on sight
	// instead of trying again. Seeded through the production increment path so
	// the key format is not duplicated here.
	retryKey := "retry:" + outbound + ":senders:" + id
	t.Cleanup(func() { rdb.Del(context.Background(), outbound, retryKey) })
	for want := int64(1); want <= 2; want++ {
		if n, err := stream.IncAttempts(ctx, outbound, "senders", id); err != nil || n != want {
			t.Fatalf("seed retry counter: n=%d want %d err=%v", n, want, err)
		}
	}

	ch := &countingChannel{}
	sender := &channels.Sender{
		Stream: stream, Channels: map[string]channels.Channel{ch.Name(): ch},
		Name: "test-dl", InStream: outbound,
		ReapInterval: 100 * time.Millisecond, MaxIdle: 100 * time.Millisecond,
		MaxAttempts: 1,
	}
	go func() { _ = sender.Run(ctx) }()

	// The entry lands in stream:deadletter with our origin id; remove it so
	// the shared stream is left clean for other tests.
	var dlID string
	waitFor(t, func() bool {
		entries, err := rdb.XRange(ctx, storage.StreamDeadletter, "-", "+").Result()
		if err != nil {
			return false
		}
		for _, e := range entries {
			if p, _ := e.Values["payload"].(string); strings.Contains(p, id) {
				dlID = e.ID
				return true
			}
		}
		return false
	})
	t.Cleanup(func() {
		if dlID != "" {
			rdb.XDel(context.Background(), storage.StreamDeadletter, dlID)
		}
	})

	if got := ch.Calls(); got != 0 {
		t.Fatalf("dead-lettered message was delivered %d times, want 0", got)
	}
	n, _, err := stream.Pending(ctx, outbound, "senders")
	if err != nil || n != 0 {
		t.Fatalf("dead-lettered message must be acked, got %d pending, err %v", n, err)
	}
}

// A failing Redis read backs off one second and keeps consuming until the
// context is canceled; Run still shuts down cleanly with a nil error.
func TestSenderRunRetriesReadErrors(t *testing.T) {
	// Port 1 on localhost refuses connections: every Read errors.
	bad := redis.NewClient(&redis.Options{Addr: "localhost:1"})
	defer func() { _ = bad.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()

	s := &channels.Sender{
		Stream:   storage.NewStream(bad),
		Channels: map[string]channels.Channel{},
		Name:     "test-readerr",
	}
	start := time.Now()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run must return nil on shutdown, got %v", err)
	}
	if time.Since(start) < time.Second {
		t.Fatal("the read-error backoff did not engage")
	}
}
