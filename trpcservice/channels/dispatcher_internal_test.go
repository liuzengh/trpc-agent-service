package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// stubChannel counts Send calls for the internal handle tests.
type stubChannel struct {
	mu    sync.Mutex
	calls int
}

func (c *stubChannel) Name() string                               { return "counting" }
func (c *stubChannel) RegisterRoutes(_ *http.ServeMux, _ Handler) {}
func (c *stubChannel) Send(_ context.Context, _ OutboundMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return nil
}

func (c *stubChannel) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// A Sender with unset fields falls back to the platform defaults; explicit
// fields win.
func TestSenderDefaults(t *testing.T) {
	s := &Sender{Name: "s1"}
	if got := s.inStream(); got != storage.StreamOutbound {
		t.Fatalf("inStream default: got %q, want %q", got, storage.StreamOutbound)
	}
	if got := s.group(); got != "senders" {
		t.Fatalf("group default: got %q, want senders", got)
	}
	if got := s.sendQPS(); got != 20 {
		t.Fatalf("sendQPS default: got %v, want 20", got)
	}
	if got := s.sendBurst(); got != 40 {
		t.Fatalf("sendBurst default: got %v, want 40", got)
	}
	if got := s.sendWait(); got != 30*time.Second {
		t.Fatalf("sendWait default: got %v, want 30s", got)
	}
	if got := s.reapInterval(); got != 30*time.Second {
		t.Fatalf("reapInterval default: got %v, want 30s", got)
	}
	if got := s.maxIdle(); got != 2*time.Minute {
		t.Fatalf("maxIdle default: got %v, want 2m", got)
	}
	if got := s.maxAttempts(); got != 5 {
		t.Fatalf("maxAttempts default: got %v, want 5", got)
	}

	explicit := &Sender{
		InStream: "stream:x", Group: "senders-x",
		SendQPS: 1.5, SendBurst: 2, SendWait: time.Second,
		ReapInterval: time.Second, MaxIdle: time.Second, MaxAttempts: 7,
	}
	if got := explicit.inStream(); got != "stream:x" {
		t.Fatalf("inStream override lost: %q", got)
	}
	if got := explicit.group(); got != "senders-x" {
		t.Fatalf("group override lost: %q", got)
	}
	if got := explicit.sendQPS(); got != 1.5 {
		t.Fatalf("sendQPS override lost: %v", got)
	}
	if got := explicit.sendBurst(); got != 2 {
		t.Fatalf("sendBurst override lost: %v", got)
	}
	if got := explicit.sendWait(); got != time.Second {
		t.Fatalf("sendWait override lost: %v", got)
	}
	if got := explicit.reapInterval(); got != time.Second {
		t.Fatalf("reapInterval override lost: %v", got)
	}
	if got := explicit.maxIdle(); got != time.Second {
		t.Fatalf("maxIdle override lost: %v", got)
	}
	if got := explicit.maxAttempts(); got != 7 {
		t.Fatalf("maxAttempts override lost: %v", got)
	}
}

func TestSenderString(t *testing.T) {
	if got := (&Sender{Name: "s-42"}).String(); got != "sender(s-42)" {
		t.Fatalf("String() = %q, want sender(s-42)", got)
	}
}

// A rate-limited send (bucket exhausted within SendWait) is left pending for
// the reaper instead of being dropped — exercised synchronously so the wait
// cannot be cut short by test teardown.
func TestSenderHandleRateLimitedLeavesPending(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)

	stream := storage.NewStream(rdb)
	// Per-run suffixes keep concurrent runs of the suite from sharing keys.
	uid := fmt.Sprintf("%d", time.Now().UnixNano())
	outbound := "test:rlh:out:" + t.Name() + "-" + uid
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	limiter := storage.NewLimiter(rdb)
	scope := "send:counting:rlh-tenant-" + uid
	// Seed the cleanup first so the bucket key goes away even on failure.
	t.Cleanup(func() { rdb.Del(context.Background(), outbound, "ratelimit:"+scope) })

	// Drain the bucket: burst=1, near-zero refill.
	if ok, err := limiter.Allow(ctx, scope, 0.001, 1); err != nil || !ok {
		t.Fatalf("seed Allow: ok=%v err=%v", ok, err)
	}

	ch := &stubChannel{}
	s := &Sender{
		Stream: stream, Channels: map[string]Channel{ch.Name(): ch},
		Name: "t-rlh", InStream: outbound,
		Limiter: limiter, SendQPS: 0.001, SendBurst: 1, SendWait: 200 * time.Millisecond,
	}
	payload, err := json.Marshal(OutboundMessage{
		Channel: "counting", MsgID: "rlh-" + t.Name(),
		SessionKey: "dm:counting:u1", UserID: "u1", TenantID: "rlh-tenant-" + uid, Text: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, outbound, payload); err != nil {
		t.Fatal(err)
	}
	msgs, err := stream.Read(ctx, outbound, "senders", s.Name, 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: %d messages, err %v", len(msgs), err)
	}

	s.handle(ctx, msgs[0])

	if got := ch.Calls(); got != 0 {
		t.Fatalf("rate-limited send must not reach the channel, got %d sends", got)
	}
	n, _, err := stream.Pending(ctx, outbound, "senders")
	if err != nil || n != 1 {
		t.Fatalf("rate-limited message must stay pending for the reaper, got %d pending, err %v", n, err)
	}
}

// A successful send records the end-to-end latency (the inbound ReceivedAt
// rides the outbound message) and acks the entry.
func TestSenderHandleRecordsEndToEndLatency(t *testing.T) {
	ctx := context.Background()
	rdb := testenv.Redis(t)

	stream := storage.NewStream(rdb)
	uid := fmt.Sprintf("%d", time.Now().UnixNano())
	outbound := "test:e2e:out:" + t.Name() + "-" + uid
	t.Cleanup(func() { rdb.Del(context.Background(), outbound) })
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	ch := &stubChannel{}
	s := &Sender{
		Stream: stream, Channels: map[string]Channel{ch.Name(): ch},
		Name: "t-e2e", InStream: outbound,
	}
	payload, err := json.Marshal(OutboundMessage{
		Channel: "counting", MsgID: "e2e-" + t.Name(),
		SessionKey: "dm:counting:u1", UserID: "u1", Text: "hi",
		ReceivedAt: time.Now().Add(-80 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, outbound, payload); err != nil {
		t.Fatal(err)
	}
	msgs, err := stream.Read(ctx, outbound, "senders", s.Name, 1, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("read: %d messages, err %v", len(msgs), err)
	}

	s.handle(ctx, msgs[0])

	if got := ch.Calls(); got != 1 {
		t.Fatalf("want 1 send, got %d", got)
	}
	n, _, err := stream.Pending(ctx, outbound, "senders")
	if err != nil || n != 0 {
		t.Fatalf("delivered message must be acked, got %d pending, err %v", n, err)
	}
}
