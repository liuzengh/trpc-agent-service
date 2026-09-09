package agent_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// End to end: inbound enqueue → Worker(echo) → outbound enqueue → Sender →
// mock inbox. Needs the Redis from compose (localhost:6380); skips when
// unreachable.
func TestWorkerSenderRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	defer func() { _ = rdb.Close() }()

	stream := storage.NewStream(rdb)
	// Dedicated streams per test run: a concurrently running service consumes
	// the platform streams, sharing them would race for messages.
	inbound := "test:inbound:" + t.Name()
	outbound := "test:outbound:" + t.Name()
	t.Cleanup(func() {
		rdb.Del(context.Background(), inbound, outbound)
	})
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	mockCh := mock.New()
	worker := &agent.Worker{
		Stream: stream, Processor: agent.EchoProcessor{}, Name: "test-w",
		InStream: inbound, OutStream: outbound,
	}
	sender := &channels.Sender{
		Stream:   stream,
		Channels: map[string]channels.Channel{mockCh.Name(): mockCh},
		Name:     "test-s",
		InStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()
	go func() { _ = sender.Run(ctx) }()

	in := channels.InboundMessage{
		Channel:    "mock",
		MsgID:      "test-msg-1",
		SessionKey: channels.SessionKey("mock", "u1", ""),
		UserID:     "u1",
		Text:       "hello worker",
		TraceID:    "trace-1",
	}
	payload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	// Poll the mock inbox for up to 5 seconds.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range mockCh.Sent() {
			if m.TraceID == "trace-1" {
				if m.Text != "echo: hello worker" {
					t.Fatalf("unexpected reply: %q", m.Text)
				}
				if m.SessionKey != "dm:mock:u1" {
					t.Fatalf("unexpected session key: %q", m.SessionKey)
				}
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("reply did not arrive in mock inbox within 5s")
}

// emptyProcessor replies with an empty text (recall events and the like).
type emptyProcessor struct{}

func (emptyProcessor) Process(_ context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	return channels.OutboundMessage{Channel: msg.Channel, MsgID: msg.MsgID,
		SessionKey: msg.SessionKey, UserID: msg.UserID, TenantID: msg.TenantID}, nil
}

// An empty reply is handled-and-done: the message is acked without an
// outbound hop (recall events never reply).
func TestWorkerSkipsOutboundForEmptyReply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	inbound := "test:inbound:" + t.Name()
	outbound := "test:outbound:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	worker := &agent.Worker{
		Stream: stream, Processor: emptyProcessor{}, Name: "test-w-empty",
		InStream: inbound, OutStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()

	payload, _ := json.Marshal(channels.InboundMessage{
		Channel: "mock", MsgID: "empty-1", SessionKey: "dm:mock:u9",
		UserID: "u9", Type: channels.TypeRecall, TraceID: "trace-recall",
	})
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	// The message must be acked (no pending) and nothing enqueued outbound.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pending, _, err := stream.Pending(ctx, inbound, "workers")
		if err != nil {
			t.Fatal(err)
		}
		n, _ := stream.Len(ctx, outbound)
		if pending == 0 {
			if n != 0 {
				t.Fatal("empty reply must not enqueue an outbound message")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("message was not acked within 5s")
}

// The done marker is the execution-layer idempotency: a redelivered message
// is acked without reprocessing — no second LLM run, no
// duplicate journal events.
func TestWorkerSkipsProcessedMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	t.Cleanup(func() { _ = rdb.Close() })

	stream := storage.NewStream(rdb)
	inbound := "test:inbound:" + t.Name()
	outbound := "test:outbound:" + t.Name()
	t.Cleanup(func() { rdb.Del(context.Background(), inbound, outbound) })
	if err := stream.EnsureGroup(ctx, inbound, "workers"); err != nil {
		t.Fatal(err)
	}
	if err := stream.EnsureGroup(ctx, outbound, "senders"); err != nil {
		t.Fatal(err)
	}

	worker := &agent.Worker{
		Stream: stream, Processor: agent.EchoProcessor{}, Name: "test-w-done",
		Processed: storage.NewProcessedMarker(rdb),
		InStream:  inbound, OutStream: outbound,
	}
	go func() { _ = worker.Run(ctx) }()

	msg := channels.InboundMessage{
		Channel: "mock", MsgID: fmt.Sprintf("done-%d", time.Now().UnixNano()),
		SessionKey: "dm:mock:ud", UserID: "ud", Text: "hi",
	}
	payload, _ := json.Marshal(msg)
	// Same message twice: the second delivery simulates a reaper takeover of
	// an ack that was lost after processing.
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Add(ctx, inbound, payload); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pending, _, err := stream.Pending(ctx, inbound, "workers")
		if err != nil {
			t.Fatal(err)
		}
		n, _ := stream.Len(ctx, outbound)
		if pending == 0 && n > 0 {
			if n != 1 {
				t.Fatalf("duplicate processing must not duplicate the reply: %d outbound entries", n)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("message pair not drained within 5s")
}
