package wecombot

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"golang.org/x/net/websocket"
)

func TestReconnectBackoffMatchesProviderSchedule(t *testing.T) {
	t.Parallel()
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for index, expected := range want {
		if got := reconnectBackoff(index + 1); got != expected {
			t.Fatalf("attempt %d backoff = %s, want %s", index+1, got, expected)
		}
	}
}

func TestWeComBotDoesNotReconnectAfterConnectionReplaced(t *testing.T) {
	var zero = 0
	connections := 0
	server := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		connections++
		var subscribe WireFrame
		if err := websocket.JSON.Receive(ws, &subscribe); err != nil {
			return
		}
		_ = websocket.JSON.Send(ws, WireFrame{Headers: subscribe.Headers, ErrorCode: &zero})
		_ = websocket.JSON.Send(ws, WireFrame{
			Command: "aibot_event_callback",
			Headers: Headers{RequestID: "req-disconnected"},
			Body:    json.RawMessage(`{"event":{"eventtype":"disconnected_event"}}`),
		})
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BotID: "test-bot", Secret: "test-secret",
		Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"), Origin: server.URL,
		HeartbeatTimer: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = client.Run(context.Background(), nil)
	if !errors.Is(err, ErrConnectionReplaced) {
		t.Fatalf("Run() error = %v, want ErrConnectionReplaced", err)
	}
	if connections != 1 {
		t.Fatalf("connections = %d, want no reconnect after disconnected_event", connections)
	}
}

func TestWeComBot_ClientAndSender(t *testing.T) {
	var (
		subscribed = make(chan struct{})
		pinged     = make(chan struct{})
		replyDone  = make(chan struct{})
		zero       = 0
	)

	server := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		for {
			var frame WireFrame
			if err := websocket.JSON.Receive(ws, &frame); err != nil {
				return
			}
			switch frame.Command {
			case "aibot_subscribe":
				var body map[string]string
				_ = json.Unmarshal(frame.Body, &body)
				if body["bot_id"] == "test-bot" && body["secret"] == "test-secret" {
					_ = websocket.JSON.Send(ws, WireFrame{
						Headers:      frame.Headers,
						ErrorCode:    &zero,
						ErrorMessage: "ok",
					})
					close(subscribed)

					// Push an incoming message to client
					go func() {
						time.Sleep(20 * time.Millisecond)
						// Literal provider wire shape. Do not build this fixture from
						// InboundMsgBody or the test could repeat a protocol-field bug.
						msgBody := json.RawMessage(`{"msgid":"msg-test-1","aibotid":"test-bot","chatid":"chat-group-1","chattype":"group","from":{"userid":"user-alpha"},"create_time":1710000000,"msgtype":"text","text":{"content":"Hello WeComBot"}}`)
						_ = websocket.JSON.Send(ws, WireFrame{
							Command: "aibot_msg_callback",
							Headers: Headers{RequestID: "req-cb-999"},
							Body:    msgBody,
						})
					}()
				} else {
					errCode := 40001
					_ = websocket.JSON.Send(ws, WireFrame{
						Headers:      frame.Headers,
						ErrorCode:    &errCode,
						ErrorMessage: "auth failed",
					})
				}
			case "ping":
				_ = websocket.JSON.Send(ws, WireFrame{
					Headers:   frame.Headers,
					ErrorCode: &zero,
				})
				select {
				case <-pinged:
				default:
					close(pinged)
				}
			case "aibot_send_msg":
				var body struct {
					ChatID   string `json:"chatid"`
					MsgType  string `json:"msgtype"`
					Markdown struct {
						Content string `json:"content"`
					} `json:"markdown"`
				}
				_ = json.Unmarshal(frame.Body, &body)
				if body.ChatID == "chat-group-1" && body.MsgType == "markdown" && body.Markdown.Content == "Response from agent" {
					_ = websocket.JSON.Send(ws, WireFrame{
						Headers:   frame.Headers,
						ErrorCode: &zero,
					})
					close(replyDone)
				}
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	cfg := Config{
		BotID:          "test-bot",
		Secret:         "test-secret",
		Endpoint:       wsURL,
		Origin:         server.URL,
		RequestTimeout: 2 * time.Second,
		HeartbeatTimer: 50 * time.Millisecond,
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	sender, err := NewSender(client)
	if err != nil {
		t.Fatalf("NewSender failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var (
		msgRecv channels.InboundMessage
		wg      sync.WaitGroup
	)
	wg.Add(1)

	go func() {
		_ = client.Run(ctx, func(ctx context.Context, msg channels.InboundMessage) error {
			msgRecv = msg
			wg.Done()
			return nil
		})
	}()

	select {
	case <-subscribed:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for subscribe handshake")
	}

	select {
	case <-pinged:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for ping heartbeat")
	}

	wg.Wait()

	if msgRecv.MessageID != "msg-test-1" {
		t.Fatalf("message ID = %q, want msg-test-1", msgRecv.MessageID)
	}
	if msgRecv.ProviderRequestID != "req-cb-999" {
		t.Fatalf("provider request ID = %q, want req-cb-999", msgRecv.ProviderRequestID)
	}
	if msgRecv.Channel != channels.WeCom {
		t.Fatalf("channel = %q, want %q", msgRecv.Channel, channels.WeCom)
	}
	if msgRecv.SenderID != "user-alpha" {
		t.Fatalf("sender = %q, want user-alpha", msgRecv.SenderID)
	}
	if msgRecv.Text != "Hello WeComBot" {
		t.Fatalf("text = %q, want Hello WeComBot", msgRecv.Text)
	}
	if msgRecv.ConversationScope != channels.ConversationGroup {
		t.Fatalf("scope = %q, want group", msgRecv.ConversationScope)
	}
	if msgRecv.TriggerType != channels.TriggerMention {
		t.Fatalf("trigger = %q, want mention", msgRecv.TriggerType)
	}

	// Send the asynchronous Outbox-compatible proactive reply.
	receipt, err := sender.Send(ctx, channels.ReplyTarget{
		Channel: channels.WeCom, ConversationID: "chat-group-1",
	}, channels.OutboundMessage{Text: "Response from agent"})
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if receipt.ExternalMessageID == "" {
		t.Fatal("Send receipt must contain the request ID")
	}

	select {
	case <-replyDone:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for reply ACK")
	}
}

func TestWeComBot_HandlerCanStartStreamWithoutBlockingACK(t *testing.T) {
	var zero = 0
	server := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		for {
			var frame WireFrame
			if err := websocket.JSON.Receive(ws, &frame); err != nil {
				return
			}
			switch frame.Command {
			case "aibot_subscribe":
				_ = websocket.JSON.Send(ws, WireFrame{Headers: frame.Headers, ErrorCode: &zero})
				go func() {
					time.Sleep(10 * time.Millisecond)
					_ = websocket.JSON.Send(ws, WireFrame{
						Command: "aibot_msg_callback",
						Headers: Headers{RequestID: "req-stream-1"},
						Body:    json.RawMessage(`{"msgid":"msg-stream-1","aibotid":"test-bot","chattype":"single","from":{"userid":"user-alpha"},"create_time":1710000000,"msgtype":"text","text":{"content":"请回答"}}`),
					})
				}()
			case "aibot_respond_msg":
				_ = websocket.JSON.Send(ws, WireFrame{Headers: frame.Headers, ErrorCode: &zero})
			}
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BotID:          "test-bot",
		Secret:         "test-secret",
		Endpoint:       "ws" + strings.TrimPrefix(server.URL, "http"),
		Origin:         server.URL,
		RequestTimeout: 150 * time.Millisecond,
		HeartbeatTimer: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(client)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handlerResult := make(chan error, 1)
	go func() {
		_ = client.Run(ctx, func(handlerCtx context.Context, inbound channels.InboundMessage) error {
			_, streamErr := sender.StartProgress(handlerCtx, channels.ReplyTarget{
				Channel: channels.WeCom, ConversationID: inbound.ConversationID, ProviderReplyToken: inbound.ProviderReplyToken,
			})
			handlerResult <- streamErr
			return streamErr
		})
	}()

	select {
	case err := <-handlerResult:
		if err != nil {
			t.Fatalf("StartProgress() from inbound handler error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for inbound handler")
	}
}

func TestWeComBot_CardActionBypassesBlockedMessageAndUpdatesWithinCallback(t *testing.T) {
	var zero = 0
	normalStarted := make(chan struct{})
	releaseNormal := make(chan struct{})
	updateReceived := make(chan requestCall, 1)

	server := httptest.NewServer(websocket.Handler(func(ws *websocket.Conn) {
		for {
			var frame WireFrame
			if err := websocket.JSON.Receive(ws, &frame); err != nil {
				return
			}
			switch frame.Command {
			case "aibot_subscribe":
				_ = websocket.JSON.Send(ws, WireFrame{Headers: frame.Headers, ErrorCode: &zero})
				go func() {
					_ = websocket.JSON.Send(ws, WireFrame{
						Command: "aibot_msg_callback",
						Headers: Headers{RequestID: "req-message-blocked"},
						Body:    json.RawMessage(`{"msgid":"msg-blocked","aibotid":"test-bot","chattype":"single","from":{"userid":"user-alpha"},"create_time":1710000000,"msgtype":"text","text":{"content":"慢请求"}}`),
					})
					<-normalStarted
					_ = websocket.JSON.Send(ws, WireFrame{
						Command: "aibot_event_callback",
						Headers: Headers{RequestID: "req-card-action"},
						Body:    json.RawMessage(`{"msgid":"evt-card-1","aibotid":"test-bot","chattype":"single","from":{"userid":"user-alpha"},"create_time":1710000001,"msgtype":"event","event":{"eventtype":"template_card_event","template_card_event":{"card_type":"button_interaction","event_key":"approval:approve:token-1","task_id":"token-1"}}}`),
					})
				}()
			case "aibot_respond_update_msg":
				var body map[string]any
				_ = json.Unmarshal(frame.Body, &body)
				updateReceived <- requestCall{command: frame.Command, reqID: frame.Headers.RequestID, body: body}
				_ = websocket.JSON.Send(ws, WireFrame{Headers: frame.Headers, ErrorCode: &zero})
			}
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{
		BotID:          "test-bot",
		Secret:         "test-secret",
		Endpoint:       "ws" + strings.TrimPrefix(server.URL, "http"),
		Origin:         server.URL,
		RequestTimeout: 200 * time.Millisecond,
		HeartbeatTimer: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSender(client)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		_ = client.Run(ctx, func(handlerCtx context.Context, inbound channels.InboundMessage) error {
			if inbound.Action == nil {
				close(normalStarted)
				select {
				case <-releaseNormal:
					return nil
				case <-handlerCtx.Done():
					return handlerCtx.Err()
				}
			}
			return sender.UpdateCard(handlerCtx, channels.ReplyTarget{
				Channel: channels.WeCom, ConversationID: inbound.ConversationID,
				ProviderReplyToken: inbound.ProviderReplyToken,
			}, inbound.Action.Token, channels.InteractiveCard{
				Title: "已确认", Body: "正在继续执行", State: "approved",
			})
		})
	}()

	select {
	case call := <-updateReceived:
		if call.reqID != "req-card-action" {
			t.Fatalf("update req_id = %q, want callback req_id", call.reqID)
		}
		if call.body["response_type"] != "update_template_card" {
			t.Fatalf("update body = %#v", call.body)
		}
		card, _ := call.body["template_card"].(map[string]any)
		if card["task_id"] != "token-1" || card["card_type"] != "text_notice" {
			t.Fatalf("updated card = %#v", card)
		}
		if _, exists := card["button_list"]; exists {
			t.Fatalf("resolved card retained buttons: %#v", card)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("card action was blocked behind ordinary message handling")
	}
	close(releaseNormal)
}
