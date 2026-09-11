//go:build integration

package bus

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// newBusForTest starts a throwaway Redis and returns a bus bound to it.
func newBusForTest(t *testing.T) *RedisBus {
	t.Helper()
	ctx := context.Background()

	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	url, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}
	opts, err := goredis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	return NewRedis(goredis.NewClient(opts))
}

func TestBusPublishConsumeRoundTrip(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	// Publish first: the consumer group is created with start id "0", so it
	// also picks up messages already sitting in the stream.
	in := &Message{
		ID:        "m1",
		TraceID:   "tr1",
		TenantID:  "t1",
		AgentID:   "a1",
		SessionID: "s1",
		Channel:   "wecom",
		UserID:    "u1",
		Content:   &model.Message{Role: model.RoleUser, Content: "hi"},
		ReplyTo:   "s1",
	}
	if err := b.PublishInbound(ctx, in); err != nil {
		t.Fatalf("PublishInbound: %v", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	got := make(chan *Message, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.ConsumeInbound(runCtx, "g1", "c1", func(_ context.Context, m *Message) error {
			got <- m
			return nil
		})
	}()

	select {
	case m := <-got:
		if m.ID != in.ID || m.TraceID != in.TraceID || m.TenantID != in.TenantID {
			t.Errorf("envelope corrupted: %+v", m)
		}
		if m.SessionID != in.SessionID || m.Channel != in.Channel || m.ReplyTo != in.ReplyTo {
			t.Errorf("envelope corrupted: %+v", m)
		}
		if m.Content == nil || m.Content.Content != "hi" {
			t.Errorf("Content corrupted: %+v", m.Content)
		}
	case err := <-errCh:
		t.Fatalf("ConsumeInbound exited early: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for the published message")
	}
}

func TestBusTenantIsolationOnStream(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	// Two tenants publish to the same shared stream; isolation is carried by
	// the envelope, not by stream sharding.
	if err := b.PublishInbound(ctx, &Message{ID: "m-t1", TenantID: "t1", SessionID: "s1"}); err != nil {
		t.Fatalf("publish t1: %v", err)
	}
	if err := b.PublishInbound(ctx, &Message{ID: "m-t2", TenantID: "t2", SessionID: "s1"}); err != nil {
		t.Fatalf("publish t2: %v", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	seen := make(chan string, 2)
	go func() {
		_ = b.ConsumeInbound(runCtx, "g1", "c1", func(_ context.Context, m *Message) error {
			seen <- m.TenantID + ":" + m.ID
			return nil
		})
	}()

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case v := <-seen:
			got[v] = true
		case <-time.After(15 * time.Second):
			t.Fatalf("timeout, only saw %v", got)
		}
	}
	if !got["t1:m-t1"] || !got["t2:m-t2"] {
		t.Errorf("both tenants' messages should be delivered, got %v", got)
	}
}

func TestBusIdempotency(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	first, err := b.Idempotent(ctx, "msg-42")
	if err != nil {
		t.Fatalf("Idempotent: %v", err)
	}
	if !first {
		t.Error("first sighting should be processable")
	}
	second, err := b.Idempotent(ctx, "msg-42")
	if err != nil {
		t.Fatalf("Idempotent: %v", err)
	}
	if second {
		t.Error("second sighting of the same key must be rejected")
	}
	// a different key is independent
	other, err := b.Idempotent(ctx, "msg-43")
	if err != nil {
		t.Fatalf("Idempotent: %v", err)
	}
	if !other {
		t.Error("a distinct key should be processable")
	}
}

// TestBusIdempotencyIsATwoPhaseLease pins the crash-safety contract of the
// dedup marker. A claim must start as a short lease (so a worker that dies
// mid-turn releases the message by expiry and the redelivery reprocesses it)
// and only become durable when the caller commits it after recording the
// outcome. A single long-lived claim would silently swallow every message a
// crashing worker had claimed.
func TestBusIdempotencyIsATwoPhaseLease(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	claimed, err := b.Idempotent(ctx, "msg-lease")
	if err != nil || !claimed {
		t.Fatalf("Idempotent: claimed=%v err=%v", claimed, err)
	}
	lease, err := b.client.TTL(ctx, IdemKey("msg-lease")).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if lease > IdemLeaseTTL() {
		t.Errorf("in-flight lease TTL = %v, want at most %v", lease, IdemLeaseTTL())
	}
	if lease >= time.Hour {
		t.Errorf("in-flight lease TTL = %v, want a short lease, not the durable TTL", lease)
	}

	// The holder refreshes its lease while it works; losing the key means the
	// claim is gone and another consumer may be running the message.
	held, err := b.ExpireIdemLease(ctx, "msg-lease")
	if err != nil || !held {
		t.Fatalf("ExpireIdemLease: held=%v err=%v", held, err)
	}
	if err := b.ClearIdem(ctx, "msg-lease"); err != nil {
		t.Fatalf("ClearIdem: %v", err)
	}
	if held, err = b.ExpireIdemLease(ctx, "msg-lease"); err != nil || held {
		t.Errorf("ExpireIdemLease on a released claim = %v/%v, want false", held, err)
	}

	// Phase two: committing makes the marker durable, and a committed message
	// is refused for the rest of the dedup window.
	if _, err := b.Idempotent(ctx, "msg-lease"); err != nil {
		t.Fatalf("re-claim after release: %v", err)
	}
	if err := b.CommitIdem(ctx, "msg-lease"); err != nil {
		t.Fatalf("CommitIdem: %v", err)
	}
	committed, err := b.client.TTL(ctx, IdemKey("msg-lease")).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if committed <= time.Hour {
		t.Errorf("committed TTL = %v, want the durable window so a redelivery never re-runs the turn", committed)
	}
	again, err := b.Idempotent(ctx, "msg-lease")
	if err != nil {
		t.Fatalf("Idempotent after commit: %v", err)
	}
	if again {
		t.Error("a committed message must never be reprocessed")
	}
}

func TestBusSessionLock(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	ok, err := b.LockSession(ctx, "t1", "s1", "token-A")
	if err != nil {
		t.Fatalf("LockSession: %v", err)
	}
	if !ok {
		t.Fatal("first lock acquisition should succeed")
	}
	// another node must not grab the same session
	ok, err = b.LockSession(ctx, "t1", "s1", "token-B")
	if err != nil {
		t.Fatalf("LockSession: %v", err)
	}
	if ok {
		t.Error("concurrent lock for the same session must fail")
	}
	// a different session in the same tenant is independent
	ok, err = b.LockSession(ctx, "t1", "s2", "token-A")
	if err != nil {
		t.Fatalf("LockSession: %v", err)
	}
	if !ok {
		t.Error("a different session should be lockable")
	}
	// releasing with the wrong token does not unlock
	if err := b.UnlockSession(ctx, "t1", "s1", "token-B"); err != nil {
		t.Fatalf("UnlockSession: %v", err)
	}
	if ok, _ := b.LockSession(ctx, "t1", "s1", "token-C"); ok {
		t.Error("wrong token must not release the lock")
	}
	// the owner can release and re-acquire
	if err := b.UnlockSession(ctx, "t1", "s1", "token-A"); err != nil {
		t.Fatalf("UnlockSession: %v", err)
	}
	ok, err = b.LockSession(ctx, "t1", "s1", "token-D")
	if err != nil {
		t.Fatalf("LockSession: %v", err)
	}
	if !ok {
		t.Error("lock should be acquirable after the owner released it")
	}
}

func TestBusRoute(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	if err := b.SetRoute(ctx, "t1", "s1", "agent-1"); err != nil {
		t.Fatalf("SetRoute: %v", err)
	}
	if err := b.SetRoute(ctx, "t2", "s1", "agent-2"); err != nil {
		t.Fatalf("SetRoute: %v", err)
	}

	got, err := b.Route(ctx, "t1", "s1")
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if got != "agent-1" {
		t.Errorf("Route(t1,s1) = %q, want agent-1", got)
	}
	got, err = b.Route(ctx, "t2", "s1")
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if got != "agent-2" {
		t.Errorf("Route(t2,s1) = %q, want agent-2 (same session id, different tenant)", got)
	}
	// unknown session resolves to empty, not an error
	got, err = b.Route(ctx, "t1", "nope")
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if got != "" {
		t.Errorf("Route(unset) = %q, want empty", got)
	}
}

func TestBusApprovalState(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	// unknown -> empty
	got, err := b.PendingApproval(ctx, "t1", "s1")
	if err != nil {
		t.Fatalf("PendingApproval empty: %v", err)
	}
	if got != "" {
		t.Errorf("PendingApproval = %q, want empty", got)
	}

	payload := `{"tool":"git_push","session_id":"s1"}`
	if err := b.SetPendingApproval(ctx, "t1", "s1", payload, time.Minute); err != nil {
		t.Fatalf("SetPendingApproval: %v", err)
	}
	got, err = b.PendingApproval(ctx, "t1", "s1")
	if err != nil {
		t.Fatalf("PendingApproval: %v", err)
	}
	if got != payload {
		t.Errorf("PendingApproval = %q, want %q", got, payload)
	}
	// tenant isolation on the shared session id
	if other, _ := b.PendingApproval(ctx, "t2", "s1"); other != "" {
		t.Errorf("tenant t2 must not see t1's pending approval, got %q", other)
	}

	// resolve then poll
	if err := b.ResolveApproval(ctx, "t1", "s1", "approve"); err != nil {
		t.Fatalf("ResolveApproval: %v", err)
	}
	res, err := b.ApprovalResult(ctx, "t1", "s1")
	if err != nil {
		t.Fatalf("ApprovalResult: %v", err)
	}
	if res != "approve" {
		t.Errorf("ApprovalResult = %q, want approve", res)
	}

	// clear makes both empty again
	if err := b.ClearPendingApproval(ctx, "t1", "s1"); err != nil {
		t.Fatalf("ClearPendingApproval: %v", err)
	}
	if got, _ := b.PendingApproval(ctx, "t1", "s1"); got != "" {
		t.Errorf("pending should be empty after clear, got %q", got)
	}
}

func TestBusRefreshLockOwnership(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	if _, err := b.LockSession(ctx, "t1", "s1", "token-A"); err != nil {
		t.Fatalf("LockSession: %v", err)
	}

	// a different holder cannot refresh
	refreshed, err := b.RefreshLock(ctx, "t1", "s1", "impostor")
	if err != nil {
		t.Fatalf("RefreshLock impostor: %v", err)
	}
	if refreshed {
		t.Error("impostor must not refresh someone else's lock")
	}

	// the owner can
	refreshed, err = b.RefreshLock(ctx, "t1", "s1", "token-A")
	if err != nil {
		t.Fatalf("RefreshLock owner: %v", err)
	}
	if !refreshed {
		t.Error("owner should refresh its lock")
	}

	// after release, no one refreshes
	if err := b.UnlockSession(ctx, "t1", "s1", "token-A"); err != nil {
		t.Fatalf("UnlockSession: %v", err)
	}
	refreshed, err = b.RefreshLock(ctx, "t1", "s1", "token-A")
	if err != nil {
		t.Fatalf("RefreshLock after unlock: %v", err)
	}
	if refreshed {
		t.Error("lock is gone, refresh must report false")
	}
}

func TestBusConsumeIsConcurrentWhileBlocked(t *testing.T) {
	// The whole point of bounded-concurrency consumption: while one message
	// is being processed slowly (a human approval wait), a later message
	// must still be delivered to the same consumer.
	ctx := context.Background()
	b := newBusForTest(t).WithConsumeWorkers(4)

	slowDone := make(chan string, 1)
	fastGot := make(chan string, 1)

	runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	go func() {
		_ = b.ConsumeInbound(runCtx, "g1", "c1", func(_ context.Context, m *Message) error {
			if m.ID == "slow" {
				time.Sleep(2 * time.Second) // simulate a blocking approval wait
				slowDone <- m.ID
				return nil
			}
			fastGot <- m.ID
			return nil
		})
	}()

	if err := b.PublishInbound(ctx, &Message{ID: "slow", TenantID: "t1", SessionID: "s1"}); err != nil {
		t.Fatalf("publish slow: %v", err)
	}
	time.Sleep(300 * time.Millisecond) // let the consumer pick up "slow" first
	if err := b.PublishInbound(ctx, &Message{ID: "fast", TenantID: "t1", SessionID: "s1"}); err != nil {
		t.Fatalf("publish fast: %v", err)
	}

	select {
	case id := <-fastGot:
		if id != "fast" {
			t.Errorf("fastGot = %q, want fast", id)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("later message was blocked behind the slow one; concurrency broken")
	}
	select {
	case id := <-slowDone:
		if id != "slow" {
			t.Errorf("slowDone = %q", id)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("slow message never finished")
	}
}

// TestReadOutboundDollarCursorIsResolvedOnce is the regression for a polling
// follower. XREAD re-resolves "$" on every call, so a loop that keeps passing
// "$" reads from *that moment's* tail and skips everything published between two
// calls. The admin chat SSE loop polls about once a second, which is exactly how
// a reply that had been published to stream:outbound never reached the browser.
// "$" must therefore be resolved once and handed back as a concrete cursor.
func TestReadOutboundDollarCursorIsResolvedOnce(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)
	content := model.NewUserMessage("hi")
	mk := func(id string) *Message {
		return &Message{ID: id, TenantID: "t1", SessionID: "s1", Channel: "admin", Content: &content}
	}
	if err := b.PublishOutbound(ctx, mk("d0")); err != nil {
		t.Fatal(err)
	}

	// First poll from "$": nothing new, but it must hand back a real position.
	msgs, cursor, err := b.ReadOutbound(ctx, "$")
	if err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("first poll returned %d messages, want 0", len(msgs))
	}
	if cursor == "$" || cursor == "" {
		t.Fatalf("cursor = %q, want a resolved stream id", cursor)
	}

	// Published between two polls: the next poll must see it.
	if err := b.PublishOutbound(ctx, mk("d1")); err != nil {
		t.Fatal(err)
	}
	msgs, _, err = b.ReadOutbound(ctx, cursor)
	if err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != "d1" {
		t.Fatalf("second poll: msgs=%+v, want d1 (a reply published between polls must not be skipped)", msgs)
	}
}

func TestBusReadOutboundCursor(t *testing.T) {
	ctx := context.Background()
	b := newBusForTest(t)

	mk := func(id, tenant, session string) *Message {
		content := model.NewUserMessage("hi")
		return &Message{ID: id, TenantID: tenant, SessionID: session, Channel: "admin", Content: &content}
	}
	if err := b.PublishOutbound(ctx, mk("o1", "t1", "s1")); err != nil {
		t.Fatal(err)
	}
	if err := b.PublishOutbound(ctx, mk("o2", "t1", "s1")); err != nil {
		t.Fatal(err)
	}
	if err := b.PublishOutbound(ctx, mk("o3", "t2", "s9")); err != nil {
		t.Fatal(err)
	}

	// from the start: everything, cursor = last stream id
	msgs, cursor, err := b.ReadOutbound(ctx, "0")
	if err != nil {
		t.Fatalf("read from 0: %v", err)
	}
	if len(msgs) != 3 || cursor == "" || cursor == "0" {
		t.Fatalf("from 0: msgs=%d cursor=%q, want 3 and an advancing cursor", len(msgs), cursor)
	}
	if msgs[0].ID != "o1" || msgs[2].ID != "o3" {
		t.Errorf("order corrupted: %v %v %v", msgs[0].ID, msgs[1].ID, msgs[2].ID)
	}

	// from the cursor: only the new message
	if err := b.PublishOutbound(ctx, mk("o4", "t1", "s1")); err != nil {
		t.Fatal(err)
	}
	msgs, cursor2, err := b.ReadOutbound(ctx, cursor)
	if err != nil {
		t.Fatalf("read after cursor: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != "o4" {
		t.Fatalf("after cursor: msgs=%+v, want only o4", msgs)
	}

	// "$" follows the live tail: it resolves to the newest entry at call time,
	// so it returns only what arrives while the read is blocked — which is
	// exactly the contract a live follower (the IM gateway) needs. The earlier
	// version of this test published first and then read "$", which cannot
	// return anything under Redis semantics.
	live, liveCancel := context.WithCancel(ctx)
	defer liveCancel()
	type readResult struct {
		msgs []*Message
		err  error
	}
	got := make(chan readResult, 1)
	go func() {
		msgs, _, err := b.ReadOutbound(live, "$")
		got <- readResult{msgs: msgs, err: err}
	}()
	time.Sleep(200 * time.Millisecond) // let the blocking read establish its position
	if err := b.PublishOutbound(ctx, mk("o5", "t1", "s1")); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-got:
		if res.err != nil {
			t.Fatalf("read $: %v", res.err)
		}
		if len(res.msgs) != 1 || res.msgs[0].ID != "o5" {
			t.Fatalf("dollar read: msgs=%+v, want only o5", res.msgs)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking dollar read never returned")
	}
	_ = cursor2
}
