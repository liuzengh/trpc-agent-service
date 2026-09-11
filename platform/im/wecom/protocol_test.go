package wecom_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
)

func TestHeartbeatMissingAckAndFiniteReconnect(t *testing.T) {
	var connections atomic.Int64
	client := fixture(t, wecom.Config{AckTimeout: 40 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond, MaxReconnects: 1, ReconnectBackoff: 5 * time.Millisecond}, func(ctx context.Context, conn *websocket.Conn) {
		connections.Add(1)
		if !auth(ctx, conn) {
			return
		}
		for {
			m, err := readFrame(ctx, conn)
			if err != nil {
				return
			}
			if m["cmd"] != "ping" {
				_ = ackFrame(ctx, conn, m, 0)
			}
		}
	})
	err := waitValue(t, runClient(client, func(context.Context, wecom.Event) error { return nil }))
	if !errors.Is(err, wecom.ErrHeartbeat) || connections.Load() != 2 {
		t.Fatalf("err=%v connections=%d", err, connections.Load())
	}
}
func TestCancelDuringBackoffIsPrompt(t *testing.T) {
	client := fixture(t, wecom.Config{MaxReconnects: 10, ReconnectBackoff: time.Second}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, func(context.Context, wecom.Event) error { return nil }) }()
	found := false
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for !found {
		select {
		case state := <-client.States():
			found = state.State == wecom.StateBackoff
		case <-deadline.C:
			t.Fatal("backoff state not reached")
		}
	}
	start := time.Now()
	cancel()
	if err := waitValue(t, done); !errors.Is(err, wecom.ErrCanceled) {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("cancellation waited for reconnect backoff")
	}
}
func TestConcurrentIdentityAndPendingLimit(t *testing.T) {
	events := make(chan wecom.Event, 1)
	written := make(chan struct{})
	release := make(chan struct{})
	client := fixture(t, wecom.Config{MaxPending: 1, AckTimeout: time.Second}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "inflight")
		m, err := readFrame(ctx, conn)
		if err != nil {
			return
		}
		close(written)
		select {
		case <-release:
		case <-ctx.Done():
			return
		}
		_ = ackFrame(ctx, conn, m, 0)
		_, _, _ = conn.Read(ctx)
	})
	runClient(client, collect(events))
	event := waitValue(t, events)
	result := make(chan error, 1)
	go func() { _, err := client.Reply(context.Background(), replyRequest(event)); result <- err }()
	waitValue(t, written)
	_, err := client.Reply(context.Background(), replyRequest(event))
	checkCommand(t, err, wecom.NotSent, wecom.CodeInFlight)
	other := event
	other.RequestID = "other"
	_, err = client.Reply(context.Background(), replyRequest(other))
	checkCommand(t, err, wecom.NotSent, wecom.CodeCapacity)
	close(release)
	if err := waitValue(t, result); err != nil {
		t.Fatal(err)
	}
	_, err = client.Reply(context.Background(), replyRequest(event))
	checkCommand(t, err, wecom.NotSent, wecom.CodeFinalAttempted)
}
func TestCancellationBeforeWriteDoesNotConsumeIdentity(t *testing.T) {
	events := make(chan wecom.Event, 1)
	var writes atomic.Int64
	client := fixture(t, wecom.Config{}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "cancel")
		for {
			m, err := readFrame(ctx, conn)
			if err != nil {
				return
			}
			writes.Add(1)
			_ = ackFrame(ctx, conn, m, 0)
		}
	})
	runClient(client, collect(events))
	event := waitValue(t, events)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Reply(ctx, replyRequest(event))
	checkCommand(t, err, wecom.NotSent, wecom.CodeCanceled)
	if writes.Load() != 0 {
		t.Fatal("canceled command reached provider")
	}
	if _, err = client.Reply(context.Background(), replyRequest(event)); err != nil {
		t.Fatal(err)
	}
	if writes.Load() != 1 {
		t.Fatal(writes.Load())
	}
}
func TestActiveHandlerFailureTerminatesAndSanitizes(t *testing.T) {
	client := fixture(t, wecom.Config{MaxReconnects: 2}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "handler-error")
		_, _, _ = conn.Read(ctx)
	})
	err := waitValue(t, runClient(client, func(context.Context, wecom.Event) error { return errors.New("caller secret data") }))
	if !errors.Is(err, wecom.ErrHandler) || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
}
func TestTypedInboundEvents(t *testing.T) {
	events := make(chan wecom.Event, 8)
	client := fixture(t, wecom.Config{}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		bodies := []map[string]any{
			{"msgid": "group-text", "aibotid": "test-bot", "chattype": "group", "chatid": "chat-1", "from": map[string]string{"userid": "u"}, "msgtype": "text", "text": map[string]string{"content": "@bot hello"}},
			{"msgid": "image", "aibotid": "test-bot", "chattype": "single", "from": map[string]string{"userid": "u"}, "msgtype": "image", "image": map[string]string{"url": "not-exposed"}},
			{"msgid": "enter", "aibotid": "test-bot", "from": map[string]string{"userid": "u"}, "msgtype": "event", "event": map[string]string{"eventtype": "enter_chat"}},
			{"msgid": "future", "aibotid": "test-bot", "from": map[string]string{"userid": "u"}, "msgtype": "event", "event": map[string]string{"eventtype": "future_event"}},
		}
		for i, body := range bodies {
			cmd := "aibot_msg_callback"
			if body["msgtype"] == "event" {
				cmd = "aibot_event_callback"
			}
			_ = sendFrame(ctx, conn, map[string]any{"cmd": cmd, "headers": map[string]string{"req_id": fmt.Sprintf("event-%d", i)}, "body": body})
		}
		_, _, _ = conn.Read(ctx)
	})
	runClient(client, collect(events))
	first := waitValue(t, events)
	if first.Kind != wecom.EventText || first.ChatID != "chat-1" || first.Text != "@bot hello" {
		t.Fatal(first)
	}
	image := waitValue(t, events)
	if image.Kind != wecom.EventUnsupported || image.Text != "" || image.EventType != "image" {
		t.Fatal(image)
	}
	enter := waitValue(t, events)
	if enter.Kind != wecom.EventNotice || enter.EventType != "enter_chat" || enter.SenderID != "u" {
		t.Fatal(enter)
	}
	future := waitValue(t, events)
	if future.Kind != wecom.EventUnsupported || future.EventType != "future_event" || future.Text != "" {
		t.Fatal(future)
	}
}
func TestMalformedCallbackAndReadLimitTerminate(t *testing.T) {
	for _, scenario := range []string{"empty_msgtype", "wrong_bot", "no_sender", "group_without_chat", "null_text", "oversized", "invalid_utf8", "duplicate_nested"} {
		t.Run(scenario, func(t *testing.T) {
			client := fixture(t, wecom.Config{ReadLimit: 512}, func(ctx context.Context, conn *websocket.Conn) {
				if !auth(ctx, conn) {
					return
				}
				body := map[string]any{"msgid": "m", "aibotid": "test-bot", "chattype": "single", "from": map[string]string{"userid": "u"}, "msgtype": "text", "text": map[string]string{"content": "text"}}
				switch scenario {
				case "empty_msgtype":
					body["msgtype"] = ""
				case "wrong_bot":
					body["aibotid"] = "another-bot"
				case "no_sender":
					delete(body, "from")
				case "group_without_chat":
					body["chattype"] = "group"
				case "null_text":
					body["text"] = nil
				case "oversized":
					body["text"] = map[string]string{"content": strings.Repeat("x", 1024)}
				}
				switch scenario {
				case "invalid_utf8":
					_ = conn.Write(ctx, websocket.MessageText, []byte{0xff})
				case "duplicate_nested":
					_ = conn.Write(ctx, websocket.MessageText, []byte(`{"cmd":"aibot_msg_callback","headers":{"req_id":"one","req_id":"two"},"body":{}}`))
				default:
					_ = sendFrame(ctx, conn, map[string]any{"cmd": "aibot_msg_callback", "headers": map[string]string{"req_id": "bad"}, "body": body})
				}
				_, _, _ = conn.Read(ctx)
			})
			err := waitValue(t, runClient(client, func(context.Context, wecom.Event) error { t.Error("malformed callback reached handler"); return nil }))
			if !errors.Is(err, wecom.ErrProtocol) {
				t.Fatalf("%s: %v", scenario, err)
			}
		})
	}
}
func TestCanceledAuthAndSecondRun(t *testing.T) {
	received := make(chan struct{})
	client := fixture(t, wecom.Config{AckTimeout: time.Second}, func(ctx context.Context, conn *websocket.Conn) {
		if _, err := readFrame(ctx, conn); err != nil {
			return
		}
		close(received)
		_, _, _ = conn.Read(ctx)
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, func(context.Context, wecom.Event) error { return nil }) }()
	waitValue(t, received)
	if err := client.Run(context.Background(), func(context.Context, wecom.Event) error { return nil }); !errors.Is(err, wecom.ErrAlreadyRun) {
		t.Fatal(err)
	}
	_, err := client.Reply(context.Background(), wecom.ReplyRequest{RequestID: "x", Generation: 1, StreamID: "s", Content: "x"})
	checkCommand(t, err, wecom.NotSent, wecom.CodeNotReady)
	cancel()
	if err := waitValue(t, done); !errors.Is(err, wecom.ErrCanceled) {
		t.Fatal(err)
	}
}

func TestHeartbeatHasReservedCapacityWhileFinalAwaitsAck(t *testing.T) {
	events := make(chan wecom.Event, 1)
	var beats atomic.Int64
	client := fixture(t, wecom.Config{MaxPending: 1, HeartbeatInterval: 10 * time.Millisecond, AckTimeout: time.Second}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "slow-final")
		for {
			m, err := readFrame(ctx, conn)
			if err != nil {
				return
			}
			if m["cmd"] == "aibot_respond_msg" {
				timer := time.NewTimer(50 * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
			} else if m["cmd"] == "ping" {
				beats.Add(1)
			}
			_ = ackFrame(ctx, conn, m, 0)
		}
	})
	runClient(client, collect(events))
	event := waitValue(t, events)
	if _, err := client.Reply(context.Background(), replyRequest(event)); err != nil {
		t.Fatalf("healthy delayed Final lost to local heartbeat capacity: %v", err)
	}
	deadline := time.After(time.Second)
	for beats.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("reserved heartbeat was not sent")
		case <-time.After(time.Millisecond):
		}
	}
	if client.State().State != wecom.StateReady {
		t.Fatal(client.State())
	}
}
