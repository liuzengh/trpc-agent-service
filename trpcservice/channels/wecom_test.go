package channels

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

func TestWeComFakeServerContract(t *testing.T) {
	outgoing := make(chan wecomEnvelope, 8)
	server := httptest.NewServer(websocket.Handler(func(conn *websocket.Conn) {
		var subscribe wecomEnvelope
		if err := websocket.JSON.Receive(conn, &subscribe); err != nil {
			return
		}
		outgoing <- subscribe
		_ = websocket.JSON.Send(conn, wecomEnvelope{Cmd: "aibot_subscribe", Headers: subscribe.Headers, Body: mustJSON(map[string]int{"errcode": 0})})

		// Delay callbacks long enough to exercise multiple read deadlines and a
		// heartbeat before proving that the connection remains usable.
		time.Sleep(30 * time.Millisecond)
		_ = websocket.JSON.Send(conn, wecomEnvelope{
			Cmd: "aibot_msg_callback", Headers: wecomHeaders{ReqID: "direct-req"},
			Body: mustJSON(map[string]any{"msgid": "m-direct", "aibotid": "bot-account", "chattype": "single", "from": map[string]string{"userid": "user-a"}, "text": map[string]string{"content": "hello"}}),
		})
		_ = websocket.JSON.Send(conn, wecomEnvelope{
			Cmd: "aibot_msg_callback", Headers: wecomHeaders{ReqID: "group-req"},
			Body: mustJSON(map[string]any{"msgid": "m-group", "aibotid": "bot-account", "chattype": "group", "chatid": "chat-a", "from": map[string]string{"userid": "user-b"}, "text": map[string]string{"content": "group question"}}),
		})
		for {
			var current wecomEnvelope
			if err := websocket.JSON.Receive(conn, &current); err != nil {
				return
			}
			outgoing <- current
			if current.Cmd == "aibot_respond_msg" {
				code := 0
				_ = websocket.JSON.Send(conn, wecomEnvelope{Headers: current.Headers, ErrCode: &code, ErrMsg: "ok"})
			}
		}
	}))
	defer server.Close()

	adapter, err := NewWeComAdapter("wecom-a", "bot-account", "bot-id", "bot-secret", strings.Replace(server.URL, "http://", "ws://", 1))
	if err != nil {
		t.Fatal(err)
	}
	adapter.heartbeatInterval = 10 * time.Millisecond
	adapter.readPollInterval = 5 * time.Millisecond
	adapter.reconnectBackoff = 10 * time.Millisecond
	adapter.maxBackoff = 20 * time.Millisecond

	accepted := make(chan message.InboundMessage, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- adapter.Start(ctx, ingressSinkFunc(func(_ context.Context, inbound message.InboundMessage) (AcceptResult, error) {
			accepted <- inbound
			return AcceptResult{TaskID: "accepted"}, nil
		}))
	}()

	subscribe := receiveWeComEnvelope(t, outgoing)
	if subscribe.Cmd != "aibot_subscribe" || subscribe.Headers.ReqID == "" {
		t.Fatalf("subscribe = %#v", subscribe)
	}
	var credentials map[string]string
	if err := json.Unmarshal(subscribe.Body, &credentials); err != nil || credentials["bot_id"] != "bot-id" || credentials["secret"] != "bot-secret" {
		t.Fatalf("subscribe body = %s, error=%v", subscribe.Body, err)
	}

	direct := receiveWeComInbound(t, accepted)
	group := receiveWeComInbound(t, accepted)
	if direct.PlatformMessageID != "m-direct" || direct.ActorUserID != "user-a" || direct.ConversationID != "user-a" || direct.ConversationType != message.ConversationDirect || direct.PlatformRequestID != "direct-req" {
		t.Fatalf("direct mapping = %#v", direct)
	}
	if group.PlatformMessageID != "m-group" || group.ActorUserID != "user-b" || group.ConversationID != "chat-a" || group.ConversationType != message.ConversationGroup || group.Text != "group question" || group.PlatformRequestID != "group-req" {
		t.Fatalf("group mapping = %#v", group)
	}
	if err := adapter.Send(context.Background(), message.OutboundMessage{ConversationID: "chat-a", PlatformRequestID: "group-req", Text: "answer"}); err != nil {
		t.Fatal(err)
	}

	seenPing, seenResponse := false, false
	deadline := time.After(time.Second)
	for !seenPing || !seenResponse {
		select {
		case current := <-outgoing:
			switch current.Cmd {
			case "ping":
				seenPing = true
			case "aibot_respond_msg":
				seenResponse = true
				if current.Headers.ReqID != "group-req" {
					t.Fatalf("response req_id = %q", current.Headers.ReqID)
				}
				var body struct {
					MsgType string `json:"msgtype"`
					Stream  struct {
						ID      string `json:"id"`
						Finish  bool   `json:"finish"`
						Content string `json:"content"`
					} `json:"stream"`
				}
				if err := json.Unmarshal(current.Body, &body); err != nil || body.MsgType != "stream" || body.Stream.ID == "" || body.Stream.Content != "answer" || !body.Stream.Finish {
					t.Fatalf("response body = %s, error=%v", current.Body, err)
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for WeCom heartbeat and response")
		}
	}

	cancel()
	_ = adapter.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWeComDisconnectedEventReconnects(t *testing.T) {
	var connections atomic.Int32
	reconnected := make(chan struct{}, 1)
	server := httptest.NewServer(websocket.Handler(func(conn *websocket.Conn) {
		current := connections.Add(1)
		var subscribe wecomEnvelope
		if websocket.JSON.Receive(conn, &subscribe) != nil {
			return
		}
		_ = websocket.JSON.Send(conn, wecomEnvelope{Cmd: "aibot_subscribe", Headers: subscribe.Headers, Body: mustJSON(map[string]int{"errcode": 0})})
		if current == 1 {
			_ = websocket.JSON.Send(conn, wecomEnvelope{Cmd: "aibot_event_callback", Body: mustJSON(map[string]any{"event": map[string]string{"eventtype": "disconnected_event"}})})
			return
		}
		select {
		case reconnected <- struct{}{}:
		default:
		}
		var ignored wecomEnvelope
		_ = websocket.JSON.Receive(conn, &ignored)
	}))
	defer server.Close()

	adapter, err := NewWeComAdapter("wecom-a", "bot-account", "bot-id", "bot-secret", strings.Replace(server.URL, "http://", "ws://", 1))
	if err != nil {
		t.Fatal(err)
	}
	adapter.reconnectBackoff = 5 * time.Millisecond
	adapter.maxBackoff = 10 * time.Millisecond
	adapter.readPollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- adapter.Start(ctx, ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) {
			return AcceptResult{}, nil
		}))
	}()
	select {
	case <-reconnected:
	case <-time.After(time.Second):
		t.Fatal("WeCom adapter did not reconnect after disconnected_event")
	}
	cancel()
	_ = adapter.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if connections.Load() < 2 {
		t.Fatalf("connections = %d, want at least 2", connections.Load())
	}
}

func TestWeComSubscribeTopLevelErrCode(t *testing.T) {
	// The real WeCom server acknowledges aibot_subscribe with a top-level
	// errcode/errmsg and no cmd field; the adapter must treat that as success.
	accepted := make(chan message.InboundMessage, 1)
	server := httptest.NewServer(websocket.Handler(func(conn *websocket.Conn) {
		var subscribe wecomEnvelope
		if err := websocket.JSON.Receive(conn, &subscribe); err != nil {
			return
		}
		code := 0
		if err := websocket.JSON.Send(conn, wecomEnvelope{Headers: subscribe.Headers, ErrCode: &code, ErrMsg: "ok"}); err != nil {
			return
		}
		_ = websocket.JSON.Send(conn, wecomEnvelope{
			Cmd: "aibot_msg_callback", Headers: wecomHeaders{ReqID: "top-req"},
			Body: mustJSON(map[string]any{"msgid": "m-top", "aibotid": "bot-account", "chattype": "single", "from": map[string]string{"userid": "user-a"}, "text": map[string]string{"content": "top level errcode"}}),
		})
		var ignored wecomEnvelope
		_ = websocket.JSON.Receive(conn, &ignored)
	}))
	defer server.Close()

	adapter, err := NewWeComAdapter("wecom-a", "bot-account", "bot-id", "bot-secret", strings.Replace(server.URL, "http://", "ws://", 1))
	if err != nil {
		t.Fatal(err)
	}
	adapter.reconnectBackoff = 5 * time.Millisecond
	adapter.maxBackoff = 10 * time.Millisecond
	adapter.readPollInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- adapter.Start(ctx, ingressSinkFunc(func(_ context.Context, inbound message.InboundMessage) (AcceptResult, error) {
			accepted <- inbound
			return AcceptResult{TaskID: "accepted"}, nil
		}))
	}()

	inbound := receiveWeComInbound(t, accepted)
	if inbound.PlatformMessageID != "m-top" || inbound.PlatformRequestID != "top-req" || inbound.ActorUserID != "user-a" || inbound.Text != "top level errcode" {
		t.Fatalf("inbound mapping = %#v", inbound)
	}

	cancel()
	_ = adapter.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWeComReplyRequiresCallbackRequestID(t *testing.T) {
	adapter := &WeComAdapter{}
	if err := adapter.Send(context.Background(), message.OutboundMessage{ConversationID: "chat-a", Text: "answer"}); err == nil {
		t.Fatal("Send accepted a response without callback req_id")
	}
}

func TestWeComReplyRejectsProviderError(t *testing.T) {
	server := httptest.NewServer(websocket.Handler(func(conn *websocket.Conn) {
		var subscribe wecomEnvelope
		if websocket.JSON.Receive(conn, &subscribe) != nil {
			return
		}
		code := 0
		_ = websocket.JSON.Send(conn, wecomEnvelope{Headers: subscribe.Headers, ErrCode: &code, ErrMsg: "ok"})
		var response wecomEnvelope
		if websocket.JSON.Receive(conn, &response) != nil {
			return
		}
		code = 40001
		_ = websocket.JSON.Send(conn, wecomEnvelope{Headers: response.Headers, ErrCode: &code, ErrMsg: "rejected"})
	}))
	defer server.Close()

	adapter, err := NewWeComAdapter("wecom-a", "bot-account", "bot-id", "bot-secret", strings.Replace(server.URL, "http://", "ws://", 1))
	if err != nil {
		t.Fatal(err)
	}
	adapter.readPollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- adapter.Start(ctx, ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) { return AcceptResult{}, nil }))
	}()
	waitForWeComReady(t, adapter)
	if err := adapter.Send(context.Background(), message.OutboundMessage{ConversationID: "user-a", PlatformRequestID: "reply-rejected", Text: "answer"}); err == nil || !strings.Contains(err.Error(), "errcode 40001") {
		t.Fatalf("Send() error = %v, want provider rejection", err)
	}
	cancel()
	_ = adapter.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWeComReplyWaitsForProviderResponse(t *testing.T) {
	server := httptest.NewServer(websocket.Handler(func(conn *websocket.Conn) {
		var subscribe wecomEnvelope
		if websocket.JSON.Receive(conn, &subscribe) != nil {
			return
		}
		code := 0
		_ = websocket.JSON.Send(conn, wecomEnvelope{Headers: subscribe.Headers, ErrCode: &code, ErrMsg: "ok"})
		var response wecomEnvelope
		_ = websocket.JSON.Receive(conn, &response)
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	adapter, err := NewWeComAdapter("wecom-a", "bot-account", "bot-id", "bot-secret", strings.Replace(server.URL, "http://", "ws://", 1))
	if err != nil {
		t.Fatal(err)
	}
	adapter.readPollInterval = 5 * time.Millisecond
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- adapter.Start(runCtx, ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) { return AcceptResult{}, nil }))
	}()
	waitForWeComReady(t, adapter)
	sendCtx, cancelSend := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelSend()
	if err := adapter.Send(sendCtx, message.OutboundMessage{ConversationID: "user-a", PlatformRequestID: "reply-timeout", Text: "answer"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v, want deadline exceeded", err)
	}
	cancelRun()
	_ = adapter.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func waitForWeComReady(t *testing.T, adapter *WeComAdapter) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if adapter.Ready(context.Background()) == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("WeCom adapter did not become ready")
}

func receiveWeComEnvelope(t *testing.T, source <-chan wecomEnvelope) wecomEnvelope {
	t.Helper()
	select {
	case value := <-source:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom envelope")
		return wecomEnvelope{}
	}
}

func receiveWeComInbound(t *testing.T, source <-chan message.InboundMessage) message.InboundMessage {
	t.Helper()
	select {
	case value := <-source:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom inbound message")
		return message.InboundMessage{}
	}
}
