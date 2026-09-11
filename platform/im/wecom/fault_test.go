package wecom_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
)

func fixture(t *testing.T, cfg wecom.Config, serve func(context.Context, *websocket.Conn)) *wecom.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		serve(ctx, conn)
	}))
	cfg.BotID = "test-bot"
	cfg.Secret = "test-secret"
	cfg.URL = "ws" + strings.TrimPrefix(server.URL, "http")
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = 250 * time.Millisecond
	}
	if cfg.CloseTimeout == 0 {
		cfg.CloseTimeout = time.Second
	}
	client, err := wecom.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = client.Close(ctx)
		server.Close()
	})
	return client
}
func readFrame(ctx context.Context, conn *websocket.Conn) (map[string]any, error) {
	_, raw, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	err = json.Unmarshal(raw, &m)
	return m, err
}
func sendFrame(ctx context.Context, conn *websocket.Conn, m any) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, raw)
}
func ackFrame(ctx context.Context, conn *websocket.Conn, m map[string]any, code int64) error {
	return sendFrame(ctx, conn, map[string]any{"headers": m["headers"], "errcode": code, "errmsg": "provider raw secret must not escape"})
}
func auth(ctx context.Context, conn *websocket.Conn) bool {
	m, err := readFrame(ctx, conn)
	return err == nil && m["cmd"] == "aibot_subscribe" && ackFrame(ctx, conn, m, 0) == nil
}
func callback(ctx context.Context, conn *websocket.Conn, id string) error {
	return sendFrame(ctx, conn, map[string]any{"cmd": "aibot_msg_callback", "headers": map[string]string{"req_id": id}, "body": map[string]any{"msgid": "message-" + id, "aibotid": "test-bot", "chattype": "single", "from": map[string]string{"userid": "user-1"}, "msgtype": "text", "text": map[string]string{"content": "hello"}}})
}
func runClient(client *wecom.Client, handler wecom.Handler) <-chan error {
	done := make(chan error, 1)
	go func() { done <- client.Run(context.Background(), handler) }()
	return done
}
func waitValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for protocol result")
		var zero T
		return zero
	}
}
func checkCommand(t *testing.T, err error, certainty wecom.Certainty, code wecom.ErrorCode) {
	t.Helper()
	var e *wecom.CommandError
	if !errors.As(err, &e) || e.Certainty != certainty || e.Code != code {
		t.Fatalf("got %v; want %s/%s", err, certainty, code)
	}
}
func replyRequest(event wecom.Event) wecom.ReplyRequest {
	return wecom.ReplyRequest{RequestID: event.RequestID, Generation: event.Generation, StreamID: "stream-" + event.RequestID, Content: "final"}
}
func collect(ch chan<- wecom.Event) wecom.Handler {
	return func(ctx context.Context, event wecom.Event) error {
		select {
		case ch <- event:
		case <-ctx.Done():
		}
		return nil
	}
}

type countingTransport struct{ calls atomic.Int64 }

func (c *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.calls.Add(1)
	return nil, errors.New("not used")
}
func TestConstructorAndLifecycle(t *testing.T) {
	transport := &countingTransport{}
	client, err := wecom.NewClient(wecom.Config{BotID: "b", Secret: "s"}, wecom.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	if transport.calls.Load() != 0 {
		t.Fatal("constructor performed network I/O")
	}
	_, err = client.Reply(context.Background(), wecom.ReplyRequest{RequestID: "r", Generation: 1, StreamID: "s", Content: "x"})
	checkCommand(t, err, wecom.NotSent, wecom.CodeNotReady)
	if err = client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = client.Run(context.Background(), func(context.Context, wecom.Event) error { return nil }); !errors.Is(err, wecom.ErrClosed) {
		t.Fatal(err)
	}
	_, err = client.Reply(context.Background(), wecom.ReplyRequest{RequestID: "r", Generation: 1, StreamID: "s", Content: "x"})
	checkCommand(t, err, wecom.NotSent, wecom.CodeClosed)
	if got := client.State(); got.State != wecom.StateClosed {
		t.Fatal(got)
	}
	for _, cfg := range []wecom.Config{{}, {BotID: "b", Secret: "s", URL: "https://example.com"}, {BotID: "b", Secret: "s", URL: "wss://u:p@example.com"}, {BotID: "b", Secret: "s", URL: "wss://example.com?secret=x"}, {BotID: "b", Secret: "s", EventBuffer: -1}, {BotID: "b", Secret: "s", MaxReconnects: -1}, {BotID: "b", Secret: "s", AckTimeout: -1}, {BotID: "b", Secret: "s", ReadLimit: 1}, {BotID: "b", Secret: "secret\n"}} {
		if _, err := wecom.NewClient(cfg); !errors.Is(err, wecom.ErrInvalidConfig) {
			t.Fatalf("invalid config accepted: %#v", cfg)
		}
	}
}
func TestAuthRejectedIsTerminalAndSanitized(t *testing.T) {
	var connections atomic.Int64
	client := fixture(t, wecom.Config{MaxReconnects: 3}, func(ctx context.Context, conn *websocket.Conn) {
		connections.Add(1)
		m, err := readFrame(ctx, conn)
		if err == nil {
			_ = ackFrame(ctx, conn, m, 401)
		}
		_, _, _ = conn.Read(ctx)
	})
	err := waitValue(t, runClient(client, func(context.Context, wecom.Event) error { return nil }))
	if !errors.Is(err, wecom.ErrAuth) || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
	if connections.Load() != 1 || client.State().State != wecom.StateFailed {
		t.Fatalf("connections=%d state=%#v", connections.Load(), client.State())
	}
	if err := client.Run(context.Background(), func(context.Context, wecom.Event) error { return nil }); !errors.Is(err, wecom.ErrAlreadyRun) {
		t.Fatal(err)
	}
}
func TestAuthRequiresExplicitIntegerCodeAndExactID(t *testing.T) {
	for _, scenario := range []string{"missing", "null", "fraction", "string", "wrong_id", "duplicate", "array", "trailing", "binary", "cmd_type"} {
		t.Run(scenario, func(t *testing.T) {
			client := fixture(t, wecom.Config{AckTimeout: 50 * time.Millisecond}, func(ctx context.Context, conn *websocket.Conn) {
				m, err := readFrame(ctx, conn)
				if err != nil {
					return
				}
				frame := map[string]any{"headers": m["headers"]}
				switch scenario {
				case "cmd_type":
					frame["cmd"] = 7
					frame["errcode"] = 0
				case "null":
					frame["errcode"] = nil
				case "fraction":
					frame["errcode"] = 0.5
				case "string":
					frame["errcode"] = "0"
				case "wrong_id":
					frame["headers"] = map[string]string{"req_id": "wrong"}
					frame["errcode"] = 0
				}
				raw, _ := json.Marshal(frame)
				switch scenario {
				case "duplicate":
					raw = []byte(`{"headers":{"req_id":"x"},"errcode":1,"errcode":0}`)
				case "array":
					raw = []byte(`[]`)
				case "trailing":
					raw = append(raw, []byte(` {}`)...)
				}
				kind := websocket.MessageText
				if scenario == "binary" {
					kind = websocket.MessageBinary
				}
				_ = conn.Write(ctx, kind, raw)
				_, _, _ = conn.Read(ctx)
			})
			err := waitValue(t, runClient(client, func(context.Context, wecom.Event) error { t.Error("unauthenticated event delivered"); return nil }))
			if scenario == "wrong_id" {
				if !errors.Is(err, wecom.ErrAuth) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, wecom.ErrProtocol) {
				t.Fatal(err)
			}
		})
	}
}
func TestHeartbeatContinuesDuringHandlerReply(t *testing.T) {
	beats := make(chan struct{}, 4)
	result := make(chan error, 1)
	client := fixture(t, wecom.Config{HeartbeatInterval: 15 * time.Millisecond, AckTimeout: time.Second}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "inside-handler")
		for {
			m, err := readFrame(ctx, conn)
			if err != nil {
				return
			}
			if m["cmd"] == "ping" {
				beats <- struct{}{}
			}
			_ = ackFrame(ctx, conn, m, 0)
		}
	})
	runClient(client, func(ctx context.Context, event wecom.Event) error {
		for range 2 {
			select {
			case <-beats:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		_, err := client.Reply(ctx, replyRequest(event))
		result <- err
		return nil
	})
	if err := waitValue(t, result); err != nil {
		t.Fatal(err)
	}
}
func TestAckTimeoutPoisonsIdentityAndLateAckCannotAdvance(t *testing.T) {
	events := make(chan wecom.Event, 1)
	late := make(chan struct{})
	lateSent := make(chan struct{})
	client := fixture(t, wecom.Config{AckTimeout: 60 * time.Millisecond}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "late")
		m, err := readFrame(ctx, conn)
		if err != nil {
			return
		}
		select {
		case <-late:
		case <-ctx.Done():
			return
		}
		_ = ackFrame(ctx, conn, m, 0)
		close(lateSent)
		for {
			m, err = readFrame(ctx, conn)
			if err != nil {
				return
			}
			_ = ackFrame(ctx, conn, m, 0)
		}
	})
	runClient(client, collect(events))
	event := waitValue(t, events)
	_, err := client.Reply(context.Background(), replyRequest(event))
	checkCommand(t, err, wecom.Unknown, wecom.CodeAckTimeout)
	_, err = client.Reply(context.Background(), replyRequest(event))
	checkCommand(t, err, wecom.NotSent, wecom.CodePoisoned)
	close(late)
	waitValue(t, lateSent)
	_, err = client.Reply(context.Background(), replyRequest(event))
	checkCommand(t, err, wecom.NotSent, wecom.CodePoisoned)
	event.RequestID = "independent"
	if _, err := client.Reply(context.Background(), replyRequest(event)); err != nil {
		t.Fatal(err)
	}
}
func TestProviderRejectedConsumesFinalAttempt(t *testing.T) {
	events := make(chan wecom.Event, 1)
	client := fixture(t, wecom.Config{}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "rejected")
		for {
			m, err := readFrame(ctx, conn)
			if err != nil {
				return
			}
			_ = ackFrame(ctx, conn, m, 85001)
		}
	})
	runClient(client, collect(events))
	event := waitValue(t, events)
	ack, err := client.Reply(context.Background(), replyRequest(event))
	checkCommand(t, err, wecom.Rejected, wecom.CodeRejected)
	if ack.ErrCode != 85001 {
		t.Fatal(ack)
	}
	_, err = client.Reply(context.Background(), replyRequest(event))
	checkCommand(t, err, wecom.NotSent, wecom.CodeFinalAttempted)
}
func TestDisconnectAfterWriteAndOldGeneration(t *testing.T) {
	events := make(chan wecom.Event, 2)
	var connections atomic.Int64
	client := fixture(t, wecom.Config{MaxReconnects: 1, ReconnectBackoff: 5 * time.Millisecond}, func(ctx context.Context, conn *websocket.Conn) {
		n := connections.Add(1)
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, fmt.Sprintf("generation-%d", n))
		if n == 1 {
			_, _ = readFrame(ctx, conn)
			return
		}
		for {
			m, err := readFrame(ctx, conn)
			if err != nil {
				return
			}
			_ = ackFrame(ctx, conn, m, 0)
		}
	})
	runClient(client, collect(events))
	first := waitValue(t, events)
	_, err := client.Reply(context.Background(), replyRequest(first))
	var ce *wecom.CommandError
	if !errors.As(err, &ce) || ce.Certainty != wecom.Unknown {
		t.Fatal(err)
	}
	second := waitValue(t, events)
	if second.Generation != 2 {
		t.Fatal(second)
	}
	_, err = client.Reply(context.Background(), replyRequest(first))
	checkCommand(t, err, wecom.NotSent, wecom.CodeStaleGeneration)
	if _, err = client.Reply(context.Background(), replyRequest(second)); err != nil {
		t.Fatal(err)
	}
}
func TestReplacementWithoutSenderStopsReconnect(t *testing.T) {
	var connections atomic.Int64
	events := make(chan wecom.Event, 1)
	client := fixture(t, wecom.Config{MaxReconnects: 4}, func(ctx context.Context, conn *websocket.Conn) {
		connections.Add(1)
		if !auth(ctx, conn) {
			return
		}
		_ = sendFrame(ctx, conn, map[string]any{"cmd": "aibot_event_callback", "headers": map[string]string{"req_id": "replace"}, "body": map[string]any{"msgid": "replace-message", "aibotid": "test-bot", "msgtype": "event", "event": map[string]string{"eventtype": "disconnected_event"}}})
		_, _, _ = conn.Read(ctx)
	})
	done := runClient(client, func(ctx context.Context, event wecom.Event) error { events <- event; return nil })
	err := waitValue(t, done)
	if !errors.Is(err, wecom.ErrReplaced) {
		t.Fatal(err)
	}
	event := waitValue(t, events)
	if event.EventType != "disconnected_event" || event.SenderID != "" {
		t.Fatal(event)
	}
	if connections.Load() != 1 || client.State().State != wecom.StateReplaced {
		t.Fatal(client.State())
	}
	_, err = client.Reply(context.Background(), wecom.ReplyRequest{RequestID: "replace", Generation: 1, StreamID: "s", Content: "x"})
	checkCommand(t, err, wecom.NotSent, wecom.CodeNotReady)
}
func TestEventQueueFullFailsRatherThanBlockingReader(t *testing.T) {
	entered := make(chan struct{})
	flood := make(chan struct{})
	client := fixture(t, wecom.Config{EventBuffer: 1, MaxReconnects: 2}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "first")
		select {
		case <-flood:
		case <-ctx.Done():
			return
		}
		_ = callback(ctx, conn, "second")
		_ = callback(ctx, conn, "third")
		_, _, _ = conn.Read(ctx)
	})
	done := runClient(client, func(ctx context.Context, event wecom.Event) error { close(entered); <-ctx.Done(); return nil })
	waitValue(t, entered)
	close(flood)
	if err := waitValue(t, done); !errors.Is(err, wecom.ErrEventOverflow) {
		t.Fatal(err)
	}
	if client.State().Reason != wecom.CodeCapacity {
		t.Fatal(client.State())
	}
}
func TestCloseCancelsPendingAndConcurrentCallers(t *testing.T) {
	events := make(chan wecom.Event, 1)
	written := make(chan struct{})
	client := fixture(t, wecom.Config{AckTimeout: time.Second}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "pending")
		if _, err := readFrame(ctx, conn); err != nil {
			return
		}
		close(written)
		_, _, _ = conn.Read(ctx)
	})
	done := runClient(client, collect(events))
	event := waitValue(t, events)
	result := make(chan error, 1)
	go func() { _, err := client.Reply(context.Background(), replyRequest(event)); result <- err }()
	waitValue(t, written)
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- client.Close(context.Background()) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var ce *wecom.CommandError
	if err := waitValue(t, result); !errors.As(err, &ce) || ce.Certainty != wecom.Unknown {
		t.Fatal(err)
	}
	if err := waitValue(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestRequestCapacityIsBoundedAndObservable(t *testing.T) {
	events := make(chan wecom.Event, 1)
	client := fixture(t, wecom.Config{MaxRequestIDs: 1}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "one")
		for {
			m, err := readFrame(ctx, conn)
			if err != nil {
				return
			}
			_ = ackFrame(ctx, conn, m, 0)
		}
	})
	runClient(client, collect(events))
	event := waitValue(t, events)
	if _, err := client.Reply(context.Background(), replyRequest(event)); err != nil {
		t.Fatal(err)
	}
	event.RequestID = "two"
	_, err := client.Reply(context.Background(), replyRequest(event))
	checkCommand(t, err, wecom.NotSent, wecom.CodeCapacity)
	if client.State().State != wecom.StateReady {
		t.Fatal(client.State())
	}
}
func TestSlowStateReaderDoesNotBlockAndSequenceCanSkip(t *testing.T) {
	var connections atomic.Int64
	client := fixture(t, wecom.Config{StateBuffer: 1, MaxReconnects: 2, ReconnectBackoff: 5 * time.Millisecond}, func(ctx context.Context, conn *websocket.Conn) {
		connections.Add(1)
		if !auth(ctx, conn) {
			return
		}
	})
	err := waitValue(t, runClient(client, func(context.Context, wecom.Event) error { return nil }))
	if !errors.Is(err, wecom.ErrDisconnected) || connections.Load() != 3 {
		t.Fatalf("err=%v connections=%d", err, connections.Load())
	}
	last := client.State()
	if last.Sequence < 10 || last.Generation != 3 || last.State != wecom.StateFailed {
		t.Fatal(last)
	}
	snapshot, ok := <-client.States()
	if !ok || snapshot != last {
		t.Fatal(snapshot, last)
	}
	if _, ok := <-client.States(); ok {
		t.Fatal("terminal state stream not closed")
	}
}

func TestOldCanceledHandlerDoesNotKillReconnectedDispatcher(t *testing.T) {
	entered := make(chan struct{})
	secondConnected := make(chan struct{})
	delivered := make(chan wecom.Event, 1)
	var connections atomic.Int64
	client := fixture(t, wecom.Config{MaxReconnects: 1, ReconnectBackoff: 5 * time.Millisecond}, func(ctx context.Context, conn *websocket.Conn) {
		n := connections.Add(1)
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, fmt.Sprintf("callback-%d", n))
		if n == 1 {
			select {
			case <-entered:
			case <-ctx.Done():
			}
			return
		}
		close(secondConnected)
		for {
			m, err := readFrame(ctx, conn)
			if err != nil {
				return
			}
			_ = ackFrame(ctx, conn, m, 0)
		}
	})
	runClient(client, func(ctx context.Context, event wecom.Event) error {
		if event.Generation == 1 {
			close(entered)
			<-ctx.Done()
			<-secondConnected
			return ctx.Err()
		}
		delivered <- event
		return nil
	})
	if event := waitValue(t, delivered); event.Generation != 2 {
		t.Fatal(event)
	}
}
func TestCloseReportsStickyDrainTimeoutForNonCooperativeHandler(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	client := fixture(t, wecom.Config{CloseTimeout: 25 * time.Millisecond}, func(ctx context.Context, conn *websocket.Conn) {
		if !auth(ctx, conn) {
			return
		}
		_ = callback(ctx, conn, "blocked")
		_, _, _ = conn.Read(ctx)
	})
	done := runClient(client, func(context.Context, wecom.Event) error { close(entered); <-release; close(exited); return nil })
	waitValue(t, entered)
	defer func() { close(release); waitValue(t, exited) }()
	if err := client.Close(context.Background()); !errors.Is(err, wecom.ErrDrainTimeout) {
		t.Fatal(err)
	}
	if err := waitValue(t, done); !errors.Is(err, wecom.ErrDrainTimeout) {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); !errors.Is(err, wecom.ErrDrainTimeout) {
		t.Fatalf("completed Run incorrectly reported successful drain: %v", err)
	}
}
