package wecom_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
)

func TestFinalFromHandler(t *testing.T) {
	wire := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var subscribe map[string]any
		if json.Unmarshal(data, &subscribe) != nil {
			return
		}
		if subscribe["cmd"] != "aibot_subscribe" {
			return
		}
		body := subscribe["body"].(map[string]any)
		if body["bot_id"] != "test-bot" || body["secret"] != "test-secret" {
			return
		}
		headers := subscribe["headers"].(map[string]any)
		ack, _ := json.Marshal(map[string]any{"headers": headers, "errcode": 0})
		_ = conn.Write(ctx, websocket.MessageText, ack)
		callback := `{"cmd":"aibot_msg_callback","headers":{"req_id":"callback-1"},"body":{"msgid":"message-1","aibotid":"test-bot","chattype":"single","from":{"userid":"user-1"},"msgtype":"text","text":{"content":"hello"}}}`
		_ = conn.Write(ctx, websocket.MessageText, []byte(callback))
		_, data, err = conn.Read(ctx)
		if err != nil {
			return
		}
		var reply map[string]any
		if json.Unmarshal(data, &reply) != nil {
			return
		}
		wire <- reply
		ack, _ = json.Marshal(map[string]any{"headers": reply["headers"], "errcode": 0})
		_ = conn.Write(ctx, websocket.MessageText, ack)
		_, _, _ = conn.Read(ctx)
	}))
	defer server.Close()
	client, err := wecom.NewClient(wecom.Config{BotID: "test-bot", Secret: "test-secret", URL: "ws" + strings.TrimPrefix(server.URL, "http")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	acked := make(chan wecom.CommandAck, 1)
	go func() {
		done <- client.Run(ctx, func(ctx context.Context, event wecom.Event) error {
			if event.Kind != wecom.EventText || event.Text != "hello" || event.RequestID != "callback-1" || event.Generation != 1 {
				t.Errorf("unexpected event: %#v", event)
			}
			ack, err := client.Reply(ctx, wecom.ReplyRequest{RequestID: event.RequestID, Generation: event.Generation, StreamID: "stream-1", Content: "world"})
			if err != nil {
				return err
			}
			acked <- ack
			return nil
		})
	}()
	select {
	case ack := <-acked:
		if ack.RequestID != "callback-1" || ack.ErrCode != 0 {
			t.Fatal(ack)
		}
	case <-ctx.Done():
		t.Fatal("handler reply did not receive ACK")
	}
	select {
	case reply := <-wire:
		if reply["cmd"] != "aibot_respond_msg" {
			t.Fatal(reply)
		}
		body := reply["body"].(map[string]any)
		stream := body["stream"].(map[string]any)
		if body["msgtype"] != "stream" || stream["id"] != "stream-1" || stream["content"] != "world" || stream["finish"] != true {
			t.Fatal(reply)
		}
	case <-ctx.Done():
		t.Fatal("missing wire reply")
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Run did not drain")
	}
}
