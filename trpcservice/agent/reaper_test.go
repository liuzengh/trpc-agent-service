package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// fastWorker builds a Worker on dedicated streams with reaping tuned for tests.
func fastWorker(stream *storage.Stream, p agent.Processor, inbound, outbound string) *agent.Worker {
	return &agent.Worker{
		Stream: stream, Processor: p, Name: "test-reaper",
		InStream: inbound, OutStream: outbound,
		ReapInterval: 100 * time.Millisecond,
		MaxIdle:      time.Millisecond,
		MaxAttempts:  3,
	}
}

// A pending message whose consumer vanished is taken over and processed.
func TestWorkerTakesOverOrphanedMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)

	stream := storage.NewStream(rdb)
	inbound := "test:reap:in:" + t.Name()
	outbound := "test:reap:out:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	in := channels.InboundMessage{
		Channel: "mock", MsgID: "orphan-1", SessionKey: "dm:mock:u9",
		UserID: "u9", Text: "rescue me", TraceID: "trace-orphan",
	}
	payload, _ := json.Marshal(in)
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	// A consumer reads the message and crashes before Ack (stays pending).
	if _, err := stream.Read(ctx, inbound, "workers", "dead-worker", 1, time.Second); err != nil {
		t.Fatal(err)
	}

	mockCh := mock.New()
	worker := fastWorker(stream, agent.EchoProcessor{}, inbound, outbound)
	sender := &channels.Sender{
		Stream:   stream,
		Channels: map[string]channels.Channel{mockCh.Name(): mockCh},
		Name:     "test-s", InStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()
	go func() { _ = sender.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range mockCh.Sent() {
			if m.TraceID == "trace-orphan" {
				return // taken over and delivered
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("orphaned pending message was not taken over within 5s")
}

// failProcessor always fails, driving the message into the dead-letter stream.
type failProcessor struct{}

func (failProcessor) Process(context.Context, channels.InboundMessage) (channels.OutboundMessage, error) {
	return channels.OutboundMessage{}, errors.New("always fails")
}

// A message that keeps failing past MaxAttempts lands in stream:deadletter
// instead of looping forever.
func TestWorkerDeadLettersPoisonMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No defer Close here: testenv.Redis registers the close as a cleanup, so it
	// runs after the Del cleanups below.
	rdb := testenv.Redis(t)
	stream := storage.NewStream(rdb)
	uid := fmt.Sprintf("%d", time.Now().UnixNano())
	inbound := "test:dlq:in:" + t.Name() + "-" + uid
	outbound := "test:dlq:out:" + t.Name() + "-" + uid
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}

	in := channels.InboundMessage{
		Channel: "mock", MsgID: "poison-1", SessionKey: "dm:mock:u10",
		UserID: "u10", Text: "boom", TraceID: "trace-poison",
	}
	payload, _ := json.Marshal(in)
	id, err := stream.Add(ctx, inbound, payload)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), "retry:"+inbound+":workers:"+id) })

	// MaxAttempts=1: the first failure counts it, one retry confirms it, and the
	// next reap dead-letters it. The loop period is the 2s blocking read, so the
	// whole path is ~4s — a tighter bound than counting four failures for the
	// same decision.
	worker := &agent.Worker{
		Stream: stream, Processor: failProcessor{}, Name: "test-reaper",
		InStream: inbound, OutStream: outbound,
		ReapInterval: 100 * time.Millisecond, MaxIdle: time.Millisecond, MaxAttempts: 1,
	}
	go func() { _ = worker.Run(ctx) }()

	// The dead-letter stream is shared, so match this run's entry by its unique
	// origin stream and remove it afterwards.
	var dlID string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && dlID == "" {
		msgs, err := rdb.XRange(ctx, storage.StreamDeadletter, "-", "+").Result()
		if err == nil {
			for _, m := range msgs {
				body, _ := m.Values["payload"].(string)
				if strings.Contains(body, "poison-1") && strings.Contains(body, inbound) {
					dlID = m.ID
				}
			}
		}
		if dlID == "" {
			time.Sleep(200 * time.Millisecond)
		}
	}
	t.Cleanup(func() {
		if dlID != "" {
			rdb.XDel(context.Background(), storage.StreamDeadletter, dlID)
		}
	})
	if dlID == "" {
		t.Fatal("poison message was not dead-lettered within 10s")
	}
}

// A message for a session locked by another worker is re-queued and processed
// after the lock expires.
func TestWorkerRequeuesWhenSessionLocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)

	stream := storage.NewStream(rdb)
	inbound := "test:lock:in:" + t.Name()
	outbound := "test:lock:out:" + t.Name()
	lockSess := "dm:mock:busy"
	t.Cleanup(func() {
		rdb.Del(context.Background(), inbound, outbound, "lock:sess:"+lockSess)
	})
	for _, gs := range [][2]string{{inbound, "workers"}, {outbound, "senders"}} {
		if err := stream.EnsureGroup(ctx, gs[0], gs[1]); err != nil {
			t.Fatal(err)
		}
	}

	// Another worker holds the session lock; it expires on its own (crash).
	lock := storage.NewLock(rdb)
	if ok, err := lock.TryAcquire(ctx, "test-app", lockSess, "crashed-worker", 1500*time.Millisecond); err != nil || !ok {
		t.Fatalf("pre-lock failed: ok=%v err=%v", ok, err)
	}

	mockCh := mock.New()
	worker := fastWorker(stream, agent.EchoProcessor{}, inbound, outbound)
	worker.Lock = lock
	worker.LockWait = 400 * time.Millisecond
	sender := &channels.Sender{
		Stream:   stream,
		Channels: map[string]channels.Channel{mockCh.Name(): mockCh},
		Name:     "test-s", InStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()
	go func() { _ = sender.Run(ctx) }()

	in := channels.InboundMessage{
		Channel: "mock", MsgID: "locked-1", SessionKey: lockSess,
		UserID: "u11", Text: "wait for the lock", TraceID: "trace-lock",
	}
	payload, _ := json.Marshal(in)
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range mockCh.Sent() {
			if m.TraceID == "trace-lock" {
				return // re-queued, then processed after the lock expired
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("message was not processed after the session lock expired")
}
