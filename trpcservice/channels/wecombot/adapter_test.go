package wecombot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Violet2314/trpc-agent-service/trpcservice/channels"
	"github.com/Violet2314/trpc-agent-service/trpcservice/gateway"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
)

var _ channels.Adapter = (*Adapter)(nil)

func TestAdapterEndToEnd(t *testing.T) {
	t.Setenv("TEST_WECOM_BOT_SECRET", "secret-1")

	const callbackReqID = "cb-1"
	inboundCh := make(chan gateway.InboundMessage, 1)
	respondCh := make(chan map[string]any, 8)
	pingCh := make(chan string, 4)

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// 1. Authenticate the subscribe request.
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var subscribe map[string]any
		if err := json.Unmarshal(data, &subscribe); err != nil {
			t.Errorf("server cannot parse subscribe: %v", err)
			return
		}
		if subscribe["cmd"] != "aibot_subscribe" {
			t.Errorf("first frame cmd = %v, want aibot_subscribe", subscribe["cmd"])
		}
		body, _ := subscribe["body"].(map[string]any)
		if body["bot_id"] != "bot-1" || body["secret"] != "secret-1" {
			t.Errorf("subscribe body = %v", body)
		}
		headers, _ := subscribe["headers"].(map[string]any)
		subscribeReqID, _ := headers["req_id"].(string)
		writeJSON(t, conn, map[string]any{
			"headers": map[string]any{"req_id": subscribeReqID},
			"errcode": 0,
			"errmsg":  "ok",
		})

		// 2. Push one p2p text callback.
		writeJSON(t, conn, map[string]any{
			"cmd":     "aibot_msg_callback",
			"headers": map[string]any{"req_id": callbackReqID},
			"body": map[string]any{
				"msgid":    "msg-1",
				"aibotid":  "bot-1",
				"chattype": "single",
				"from":     map[string]any{"userid": "user-1"},
				"msgtype":  "text",
				"text":     map[string]any{"content": "hello bot"},
			},
		})

		// 3. Consume pings and stream replies; keep reading after finish so
		// heartbeats stay observable until the client disconnects.
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame map[string]any
			if err := json.Unmarshal(data, &frame); err != nil {
				continue
			}
			if frame["cmd"] == "ping" {
				select {
				case pingCh <- "ping":
				default:
				}
				continue
			}
			if frame["cmd"] == "aibot_respond_msg" {
				respondCh <- frame
			}
		}
	}))
	defer server.Close()

	adapter, err := New(newTestCache(testSnapshot()), []string{"bot-1"}, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	adapter.heartbeat = 30 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := func(_ context.Context, message gateway.InboundMessage) (gateway.Result, error) {
		inboundCh <- message
		return gateway.Result{}, nil
	}
	if err := adapter.Run(ctx, nil, sink); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Wait for the inbound conversion.
	var inbound gateway.InboundMessage
	select {
	case inbound = <-inboundCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for inbound message")
	}
	if inbound.Channel != channelType || inbound.RouteKey != "bot-1" {
		t.Errorf("inbound channel/route = %s/%s", inbound.Channel, inbound.RouteKey)
	}
	if inbound.MsgID != "msg-1" || inbound.SenderID != "user-1" || inbound.ChatType != "p2p" {
		t.Errorf("inbound = %+v", inbound)
	}
	if inbound.Text != "hello bot" || !inbound.AddressedToBot {
		t.Errorf("inbound text/addressed = %q/%v", inbound.Text, inbound.AddressedToBot)
	}

	// Stream a reply through the replier.
	events := make(chan reply.Event, 4)
	events <- reply.Event{Type: "text_delta", Text: "hello"}
	events <- reply.Event{Type: "text_delta", Text: " world"}
	events <- reply.Event{Type: "done"}
	close(events)
	replier := adapter.NewReplier(testSnapshot())
	replyDone := make(chan error, 1)
	go func() {
		replyDone <- replier.Reply(ctx, "session-1", inbound, events)
	}()
	select {
	case err := <-replyDone:
		if err != nil {
			t.Fatalf("Reply() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reply completion")
	}

	var finished map[string]any
	deadline := time.After(5 * time.Second)
	for finished == nil {
		select {
		case frame := <-respondCh:
			frameHeaders, _ := frame["headers"].(map[string]any)
			if frameHeaders["req_id"] != callbackReqID {
				t.Errorf("respond req_id = %v, want %s", frameHeaders["req_id"], callbackReqID)
			}
			frameBody, _ := frame["body"].(map[string]any)
			stream, _ := frameBody["stream"].(map[string]any)
			if stream["finish"] == true {
				finished = frame
			}
		case <-deadline:
			t.Fatal("timed out waiting for finished stream frame")
		}
	}
	frameBody, _ := finished["body"].(map[string]any)
	stream, _ := frameBody["stream"].(map[string]any)
	if stream["content"] != "hello world" {
		t.Errorf("final content = %v, want hello world", stream["content"])
	}
	select {
	case <-pingCh:
	case <-time.After(2 * time.Second):
		t.Error("expected at least one heartbeat ping")
	}

	cancel()
	adapter.Wait()
}

func TestAdapterGroupMentionStripped(t *testing.T) {
	t.Setenv("TEST_WECOM_BOT_SECRET", "secret-1")
	inboundCh := make(chan gateway.InboundMessage, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var subscribe map[string]any
		_ = json.Unmarshal(data, &subscribe)
		headers, _ := subscribe["headers"].(map[string]any)
		writeJSON(t, conn, map[string]any{
			"headers": map[string]any{"req_id": headers["req_id"]},
			"errcode": 0,
			"errmsg":  "ok",
		})
		writeJSON(t, conn, map[string]any{
			"cmd":     "aibot_msg_callback",
			"headers": map[string]any{"req_id": "cb-2"},
			"body": map[string]any{
				"msgid":    "msg-2",
				"aibotid":  "bot-1",
				"chatid":   "chat-9",
				"chattype": "group",
				"from":     map[string]any{"userid": "user-2"},
				"msgtype":  "text",
				"text":     map[string]any{"content": "@RobotA 请帮我查天气"},
			},
		})
		// Keep the connection open until the client goes away.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	adapter, err := New(newTestCache(testSnapshot()), []string{"bot-1"}, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := adapter.Run(ctx, nil, func(_ context.Context, message gateway.InboundMessage) (gateway.Result, error) {
		inboundCh <- message
		return gateway.Result{}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case inbound := <-inboundCh:
		if inbound.ChatType != "group" || inbound.GroupID != "chat-9" {
			t.Errorf("inbound chat = %s/%s", inbound.ChatType, inbound.GroupID)
		}
		if inbound.Text != "请帮我查天气" {
			t.Errorf("inbound text = %q, want mention stripped", inbound.Text)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for inbound message")
	}
	cancel()
	adapter.Wait()
}

func TestAdapterSubscribeRejected(t *testing.T) {
	t.Setenv("TEST_WECOM_BOT_SECRET", "secret-1")
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var subscribe map[string]any
		_ = json.Unmarshal(data, &subscribe)
		headers, _ := subscribe["headers"].(map[string]any)
		writeJSON(t, conn, map[string]any{
			"headers": map[string]any{"req_id": headers["req_id"]},
			"errcode": 40001,
			"errmsg":  "invalid secret",
		})
		// Keep the socket open so the client decides to close it.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	adapter, err := New(newTestCache(testSnapshot()), []string{"bot-1"}, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := adapter.Run(ctx, nil, func(context.Context, gateway.InboundMessage) (gateway.Result, error) {
		return gateway.Result{}, nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	// The rejection triggers a backoff, so no connection should be active.
	time.Sleep(300 * time.Millisecond)
	if got := adapter.connection("bot-1"); got != nil {
		t.Error("connection registered despite rejected subscribe")
	}
	cancel()
	adapter.Wait()
}

func TestReplierErrors(t *testing.T) {
	t.Setenv("TEST_WECOM_BOT_SECRET", "secret-1")
	adapter, err := New(newTestCache(testSnapshot()), []string{"bot-1"}, "")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	replier := adapter.NewReplier(testSnapshot())

	// Missing reply target must drain events.
	events := make(chan reply.Event, 2)
	events <- reply.Event{Type: "text_delta", Text: "x"}
	close(events)
	if err := replier.Reply(context.Background(), "s", gateway.InboundMessage{Raw: "bogus"}, events); err == nil {
		t.Error("expected error for missing reply target")
	}

	// Unknown route key has no connection.
	events = make(chan reply.Event, 2)
	events <- reply.Event{Type: "text_delta", Text: "x"}
	close(events)
	message := gateway.InboundMessage{Raw: replyTarget{RouteKey: "missing", ReqID: "r"}}
	if err := replier.Reply(context.Background(), "s", message, events); err == nil {
		t.Error("expected error for missing connection")
	}
}

func TestStripGroupMention(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"mention then text", "@RobotA hello robot", "hello robot"},
		{"mention only", "@RobotA", "@RobotA"},
		{"no mention", "hello robot", "hello robot"},
		{"mention with tab", "@Bot\tquery", "query"},
		{"extra spaces", "  @RobotA   hi  ", "hi"},
	}
	for _, test := range tests {
		if got := stripGroupMention(test.input); got != test.want {
			t.Errorf("%s: stripGroupMention(%q) = %q, want %q", test.name, test.input, got, test.want)
		}
	}
}

func writeJSON(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Errorf("marshal server frame: %v", err)
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, encoded); err != nil {
		t.Errorf("write server frame: %v", err)
	}
}

func testSnapshot() tenant.Snapshot {
	return tenant.Snapshot{
		Tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", IsActive: true},
		App: tenant.AgentApp{
			ID: "app-a", TenantID: "tenant-a", AppName: "tenant-a-support",
		},
		Binding: tenant.ChannelBinding{
			ID: "binding-a", TenantID: "tenant-a", AppID: "app-a",
			Channel: channelType, RouteKey: "bot-1", IsActive: true,
			Config: map[string]string{
				"bot_id":     "bot-1",
				"bot_secret": "env:TEST_WECOM_BOT_SECRET",
			},
		},
	}
}

type wecombotCache struct {
	snapshot tenant.Snapshot
}

func (c *wecombotCache) ResolveBinding(context.Context, string, string) (tenant.Snapshot, error) {
	return c.snapshot, nil
}
func (c *wecombotCache) Invalidate(string, string) {}

func newTestCache(snapshot tenant.Snapshot) *wecombotCache {
	return &wecombotCache{snapshot: snapshot}
}
