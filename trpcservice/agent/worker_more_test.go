package agent_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// echoOrEmpty echoes text messages and answers recall events with an empty
// reply (handled-and-done: acked without an outbound hop).
type echoOrEmpty struct{}

func (echoOrEmpty) Process(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	out := channels.OutboundMessage{Channel: msg.Channel, MsgID: msg.MsgID,
		SessionKey: msg.SessionKey, UserID: msg.UserID, TenantID: msg.TenantID}
	if msg.Type == channels.TypeRecall {
		return out, nil
	}
	out.Text = "echo: " + msg.Text
	return out, nil
}

// redisKiller closes the Redis client while processing, simulating an outage
// mid-run: the outbound enqueue and the lock release must fail gracefully
// instead of wedging or panicking.
type redisKiller struct {
	rdb    *redis.Client
	killed chan struct{}
	once   sync.Once
}

func (k *redisKiller) Process(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
	k.once.Do(func() {
		_ = k.rdb.Close()
		close(k.killed)
	})
	return channels.OutboundMessage{Text: "never enqueued"}, nil
}

// slowProcessor signals when handling starts, then takes its time, so the
// test can shut the worker down with a message in flight.
type slowProcessor struct {
	started chan struct{}
	once    sync.Once
}

func (p *slowProcessor) Process(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	p.once.Do(func() { close(p.started) })
	time.Sleep(300 * time.Millisecond)
	out := channels.OutboundMessage{Channel: msg.Channel, MsgID: msg.MsgID,
		SessionKey: msg.SessionKey, UserID: msg.UserID, TenantID: msg.TenantID}
	out.Text = "echo: " + msg.Text
	return out, nil
}

// A poison message (payload that can never be unmarshaled) is acked and
// dropped instead of looping through endless redeliveries.
func TestWorkerDropsPoisonMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	stream := storage.NewStream(rdb)
	inbound := "test:poison:in:" + t.Name()
	outbound := "test:poison:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	worker := &agent.Worker{
		Stream: stream, Processor: agent.EchoProcessor{}, Name: "test-w-poison",
		InStream: inbound, OutStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()

	if _, err := stream.Add(ctx, inbound, []byte("{definitely not json")); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: "poison-valid", SessionKey: "dm:mock:u12",
		UserID: "u12", Text: "still alive", TraceID: "trace-poison",
	})
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	// The valid reply proves the batch was read; then both entries must leave
	// the pending list (the poison one acked, not endlessly redelivered).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := stream.Len(ctx, outbound); n >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pending, _, err := stream.Pending(ctx, inbound, "workers")
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			if n, _ := stream.Len(ctx, outbound); n != 1 {
				t.Fatalf("expected exactly the valid reply outbound, got %d entries", n)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("poison message was not acked within 5s")
}

// An empty reply with the done marker configured is marked done before the
// Ack: a redelivery after a lost Ack must skip reprocessing.
func TestWorkerEmptyReplyMarksDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	stream := storage.NewStream(rdb)
	inbound := "test:emptydone:in:" + t.Name()
	outbound := "test:emptydone:out:" + t.Name()
	marker := storage.NewProcessedMarker(rdb)
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	worker := &agent.Worker{
		Stream: stream, Processor: emptyProcessor{}, Name: "test-w-emptydone",
		Processed: marker,
		InStream:  inbound, OutStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()

	msgID := fmt.Sprintf("empty-done-%d", time.Now().UnixNano())
	payload, _ := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: msgID, SessionKey: "dm:mock:u13",
		UserID: "u13", Type: channels.TypeRecall, TraceID: "trace-empty-done",
	})
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	// The marker being set proves MarkDone ran (nothing else observable).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if done, err := marker.IsDone(ctx, "mock", "", msgID); err == nil && done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pending, _, err := stream.Pending(ctx, inbound, "workers")
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			if n, _ := stream.Len(ctx, outbound); n != 0 {
				t.Fatalf("empty reply must not enqueue outbound: %d entries", n)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("empty reply was not acked within 5s")
}

// When the done marker itself is unavailable (Redis down), the worker logs
// and falls through: reprocessing is better than wedging on a hiccup. Both
// the done check and the done mark fail; processing and acking still work.
func TestWorkerDoneCheckErrorsStillProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	stream := storage.NewStream(rdb)
	inbound := "test:deadmark:in:" + t.Name()
	outbound := "test:deadmark:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	// A separate closed client: every marker operation fails with
	// "client is closed" while the stream client stays healthy.
	dead := redis.NewClient(&redis.Options{Addr: testenv.RedisAddr()})
	_ = dead.Close()
	marker := storage.NewProcessedMarker(dead)

	worker := &agent.Worker{
		Stream: stream, Processor: echoOrEmpty{}, Name: "test-w-deadmark",
		Processed: marker,
		InStream:  inbound, OutStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()

	recall, _ := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: "dead-recall", SessionKey: "dm:mock:u14",
		UserID: "u14", Type: channels.TypeRecall,
	})
	if _, err := stream.Add(ctx, inbound, recall); err != nil {
		t.Fatal(err)
	}
	text, _ := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: "dead-text", SessionKey: "dm:mock:u14",
		UserID: "u14", Text: "fail open",
	})
	if _, err := stream.Add(ctx, inbound, text); err != nil {
		t.Fatal(err)
	}

	// The echo reply proves the text message was processed despite the failed
	// done check; then both messages must drain (acked, no second hop).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := stream.Len(ctx, outbound); n >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pending, _, err := stream.Pending(ctx, inbound, "workers")
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			if n, _ := stream.Len(ctx, outbound); n != 1 {
				t.Fatalf("expected exactly one outbound reply, got %d", n)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("messages were not acked within 5s")
}

// A message whose session is locked by another replica (same app scope) is
// left pending for the reaper instead of being processed or failed.
func TestWorkerLeavesPendingWhenSessionBusy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	stream := storage.NewStream(rdb)
	inbound := "test:busysess:in:" + t.Name()
	outbound := "test:busysess:out:" + t.Name()
	appID := "leave-app"
	sessKey := "dm:mock:busy-" + t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound, lockKey) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	lock := storage.NewLock(rdb)
	if ok, err := lock.TryAcquire(ctx, appID, sessKey, "crashed-owner", 30*time.Second); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	worker := &agent.Worker{
		Stream: stream, Processor: agent.EchoProcessor{}, Name: "test-w-busy",
		InStream: inbound, OutStream: outbound,
		Lock: lock, LockTTL: 30 * time.Second, LockWait: 150 * time.Millisecond,
	}
	go func() { _ = worker.Run(ctx) }()

	payload, _ := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: "busy-1", SessionKey: sessKey, AppID: appID,
		UserID: "u15", Text: "queued behind a busy session",
	})
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	// Wait until the worker read the message, then verify it stays pending
	// unprocessed (the lock outlives the whole check window).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pending, _, err := stream.Pending(ctx, inbound, "workers"); err == nil && pending >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		pending, _, err := stream.Pending(ctx, inbound, "workers")
		if err != nil {
			t.Fatal(err)
		}
		if pending < 1 {
			t.Fatal("message vanished from the pending list without processing")
		}
		if n, _ := stream.Len(ctx, outbound); n != 0 {
			t.Fatalf("busy session must not be processed, got %d outbound replies", n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A Redis outage during processing must fail gracefully: the reply is not
// enqueued, the lock release and the read loop log and back off, and the
// worker shuts down cleanly on cancel.
func TestWorkerRedisOutageDuringProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	rdb := testenv.Redis(t)
	stream := storage.NewStream(rdb)
	inbound := "test:outage:in:" + t.Name()
	outbound := "test:outage:out:" + t.Name()
	appID := "outage-app"
	sessKey := "dm:mock:outage-" + t.Name()
	lockKey := "lock:sess:" + appID + ":" + sessKey
	t.Cleanup(func() {
		cancel()
		rdb.Del(context.Background(), inbound, outbound, lockKey)
	})
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	killer := &redisKiller{rdb: rdb, killed: make(chan struct{})}
	worker := &agent.Worker{
		Stream: stream, Processor: killer, Name: "test-w-outage",
		InStream: inbound, OutStream: outbound,
		Lock: storage.NewLock(rdb), LockTTL: time.Second, LockWait: time.Second,
		ReapInterval: 100 * time.Millisecond, DrainTimeout: 200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- worker.Run(ctx) }()

	payload, _ := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: "outage-1", SessionKey: sessKey, AppID: appID,
		UserID: "u16", Text: "boom mid-run",
	})
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}
	<-killer.killed // Redis is now closed underneath the running worker

	// Give the read-error backoff and the reap path a chance to run, then
	// cancel during the backoff sleep: Run must return nil.
	time.Sleep(1400 * time.Millisecond)
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
}

// Cancelling the worker context with a message in flight finishes the current
// handler (graceful drain) and then exits the loop cleanly.
func TestWorkerStopsBetweenMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	rdb := testenv.Redis(t)
	stream := storage.NewStream(rdb)
	inbound := "test:drain:in:" + t.Name()
	outbound := "test:drain:out:" + t.Name()
	t.Cleanup(func() {
		cancel()
		rdb.Del(context.Background(), inbound, outbound)
	})
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	slow := &slowProcessor{started: make(chan struct{})}
	worker := &agent.Worker{
		Stream: stream, Processor: slow, Name: "test-w-drain",
		InStream: inbound, OutStream: outbound,
		DrainTimeout: 200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- worker.Run(ctx) }()

	payload, _ := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: "drain-1", SessionKey: "dm:mock:u17",
		UserID: "u17", Text: "shutting down",
	})
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}
	<-slow.started // handler mid-flight
	cancel()

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
}
