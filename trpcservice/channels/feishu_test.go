package channels

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

type fakeFeishuWebSocket struct {
	started chan struct{}
	once    sync.Once
}

func (f *fakeFeishuWebSocket) Start(ctx context.Context) error {
	f.once.Do(func() { close(f.started) })
	<-ctx.Done()
	return ctx.Err()
}

func (*fakeFeishuWebSocket) Close() {}

type failingFeishuWebSocket struct{ err error }

func (f failingFeishuWebSocket) Start(context.Context) error { return f.err }
func (failingFeishuWebSocket) Close()                        {}

func TestFeishuMessageEventMapping(t *testing.T) {
	adapter := &FeishuAdapter{bindingID: "feishu-a", accountID: "cli_app", appID: "cli_app"}
	accepted := make(chan message.InboundMessage, 1)
	adapter.sink = ingressSinkFunc(func(_ context.Context, inbound message.InboundMessage) (AcceptResult, error) {
		accepted <- inbound
		return AcceptResult{TaskID: "accepted"}, nil
	})

	eventID, createTime := "event-1", "1720000000123"
	appID, senderType, openID := "cli_app", "user", "ou_sender"
	messageID, chatID, chatType, messageType := "om_message", "oc_chat", "group", "text"
	content, mentionKey := `{"text":"@_user_1 hello from Feishu"}`, "@_user_1"
	event := &larkim.P2MessageReceiveV1{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{EventID: eventID, AppID: appID}},
		Event: &larkim.P2MessageReceiveV1Data{
			Sender: &larkim.EventSender{SenderId: &larkim.UserId{OpenId: &openID}, SenderType: &senderType},
			Message: &larkim.EventMessage{
				MessageId: &messageID, ChatId: &chatID, ChatType: &chatType,
				MessageType: &messageType, Content: &content, CreateTime: &createTime,
				Mentions: []*larkim.MentionEvent{{Key: &mentionKey}},
			},
		},
	}
	if err := adapter.handleMessage(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	got := <-accepted
	if got.Channel != "feishu" || got.BindingID != "feishu-a" || got.ExternalAccountID != "cli_app" {
		t.Fatalf("binding mapping = %#v", got)
	}
	if got.PlatformMessageID != messageID || got.ReplyToMessageID != messageID || got.PlatformRequestID != eventID {
		t.Fatalf("message mapping = %#v", got)
	}
	if got.ActorUserID != openID || got.ConversationID != chatID || got.ConversationType != message.ConversationGroup || got.Text != "hello from Feishu" {
		t.Fatalf("conversation mapping = %#v", got)
	}
	if got.ReceivedAt.UnixMilli() != 1720000000123 {
		t.Fatalf("received_at = %s", got.ReceivedAt)
	}
}

func TestFeishuMessageEventWaitsForDurableIngress(t *testing.T) {
	wantErr := errors.New("redis unavailable")
	adapter := &FeishuAdapter{bindingID: "feishu-a", accountID: "cli_app", appID: "cli_app"}
	adapter.sink = ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) {
		return AcceptResult{}, wantErr
	})
	openID, senderType := "ou_sender", "user"
	messageID, chatID, chatType, messageType := "om_message", "oc_chat", "p2p", "text"
	content := `{"text":"hello"}`
	event := &larkim.P2MessageReceiveV1{Event: &larkim.P2MessageReceiveV1Data{
		Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: &openID}, SenderType: &senderType},
		Message: &larkim.EventMessage{MessageId: &messageID, ChatId: &chatID, ChatType: &chatType, MessageType: &messageType, Content: &content},
	}}
	if err := adapter.handleMessage(context.Background(), event); !errors.Is(err, wantErr) {
		t.Fatalf("handleMessage error = %v", err)
	}
}

func TestFeishuReplyOpenAPIContract(t *testing.T) {
	var mu sync.Mutex
	var replyPath, authorization string
	var replyBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_, _ = io.WriteString(w, `{"code":0,"msg":"ok","tenant_access_token":"tenant-token","expire":7200}`)
		case "/open-apis/im/v1/messages/om_source/reply":
			data, _ := io.ReadAll(r.Body)
			mu.Lock()
			replyPath, authorization = r.URL.Path, r.Header.Get("Authorization")
			_ = json.Unmarshal(data, &replyBody)
			mu.Unlock()
			_, _ = io.WriteString(w, `{"code":0,"msg":"ok","data":{"message_id":"om_reply"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := lark.NewClient("cli_app", "app-secret",
		lark.WithOpenBaseUrl(server.URL), lark.WithLogLevel(larkcore.LogLevelError), lark.WithReqTimeout(time.Second))
	adapter := &FeishuAdapter{messages: client.Im.V1.Message}
	err := adapter.Send(context.Background(), message.OutboundMessage{
		ReplyToMessageID: "om_source", RequestID: "request-1", Text: "真实模型回复",
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if replyPath != "/open-apis/im/v1/messages/om_source/reply" || authorization != "Bearer tenant-token" {
		t.Fatalf("reply target path=%q authorization=%q", replyPath, authorization)
	}
	if replyBody["msg_type"] != "text" || replyBody["uuid"] != "request-1" || replyBody["reply_in_thread"] != false {
		t.Fatalf("reply body = %#v", replyBody)
	}
	var contentBody map[string]string
	if err := json.Unmarshal([]byte(replyBody["content"].(string)), &contentBody); err != nil || contentBody["text"] != "真实模型回复" {
		t.Fatalf("reply content = %#v, error = %v", contentBody, err)
	}
}

func TestFeishuReplyRejectsBusinessError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "tenant_access_token") {
			_, _ = io.WriteString(w, `{"code":0,"msg":"ok","tenant_access_token":"tenant-token","expire":7200}`)
			return
		}
		_, _ = io.WriteString(w, `{"code":230001,"msg":"permission denied"}`)
	}))
	defer server.Close()
	client := lark.NewClient("cli_app", "app-secret",
		lark.WithOpenBaseUrl(server.URL), lark.WithLogLevel(larkcore.LogLevelError), lark.WithReqTimeout(time.Second))
	adapter := &FeishuAdapter{messages: client.Im.V1.Message}
	err := adapter.Send(context.Background(), message.OutboundMessage{ReplyToMessageID: "om_source", Text: "reply"})
	if err == nil || !strings.Contains(err.Error(), "230001") {
		t.Fatalf("business error = %v", err)
	}
}

func TestFeishuStartReadinessAndCancellation(t *testing.T) {
	ws := &fakeFeishuWebSocket{started: make(chan struct{})}
	adapter := &FeishuAdapter{bindingID: "feishu-a", accountID: "cli_app", appID: "cli_app", ws: ws, messages: fakeFeishuMessages{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- adapter.Start(ctx, ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) {
			return AcceptResult{}, nil
		}))
	}()
	select {
	case <-ws.started:
	case <-time.After(time.Second):
		t.Fatal("Feishu websocket did not start")
	}
	adapter.markFeishuReady(true)
	if err := adapter.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Feishu Start did not stop after cancellation")
	}
	if err := adapter.Ready(context.Background()); err == nil {
		t.Fatal("stopped Feishu adapter remained ready")
	}
}

func TestFeishuStartReturnsConnectionFailure(t *testing.T) {
	wantErr := errors.New("authentication rejected")
	adapter := &FeishuAdapter{
		bindingID: "feishu-a", accountID: "cli_app", appID: "cli_app",
		ws: failingFeishuWebSocket{err: wantErr}, messages: fakeFeishuMessages{},
	}
	err := adapter.Start(context.Background(), ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) {
		return AcceptResult{}, nil
	}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Start error = %v, want wrapped %v", err, wantErr)
	}
}

type fakeFeishuMessages struct{}

func (fakeFeishuMessages) Reply(context.Context, *larkim.ReplyMessageReq, ...larkcore.RequestOptionFunc) (*larkim.ReplyMessageResp, error) {
	return &larkim.ReplyMessageResp{}, nil
}

func TestFeishuMessageEventRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*larkim.P2MessageReceiveV1)
	}{
		{"missing payload", func(e *larkim.P2MessageReceiveV1) { e.Event = nil }},
		{"missing sender", func(e *larkim.P2MessageReceiveV1) { e.Event.Sender = nil }},
		{"missing message", func(e *larkim.P2MessageReceiveV1) { e.Event.Message = nil }},
		{"wrong app", func(e *larkim.P2MessageReceiveV1) { e.EventV2Base.Header.AppID = "cli_other" }},
		{"bot sender", func(e *larkim.P2MessageReceiveV1) { *e.Event.Sender.SenderType = "app" }},
		{"missing sender type", func(e *larkim.P2MessageReceiveV1) { e.Event.Sender.SenderType = nil }},
		{"missing user", func(e *larkim.P2MessageReceiveV1) { e.Event.Sender.SenderId = nil }},
		{"empty message ID", func(e *larkim.P2MessageReceiveV1) { *e.Event.Message.MessageId = " " }},
		{"empty chat ID", func(e *larkim.P2MessageReceiveV1) { *e.Event.Message.ChatId = " " }},
		{"missing chat type", func(e *larkim.P2MessageReceiveV1) { e.Event.Message.ChatType = nil }},
		{"unknown chat type", func(e *larkim.P2MessageReceiveV1) { *e.Event.Message.ChatType = "unknown" }},
		{"non-text", func(e *larkim.P2MessageReceiveV1) { *e.Event.Message.MessageType = "image" }},
		{"missing content", func(e *larkim.P2MessageReceiveV1) { e.Event.Message.Content = nil }},
		{"malformed content", func(e *larkim.P2MessageReceiveV1) { *e.Event.Message.Content = "{" }},
		{"wrong text type", func(e *larkim.P2MessageReceiveV1) { *e.Event.Message.Content = "{\"text\":123}" }},
		{"empty text", func(e *larkim.P2MessageReceiveV1) { *e.Event.Message.Content = "{\"text\":\" \"}" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			senderType, openID := "user", "ou_sender"
			messageID, chatID, chatType, messageType := "om_message", "oc_chat", "p2p", "text"
			content := "{\"text\":\"hello\"}"
			event := &larkim.P2MessageReceiveV1{
				EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{AppID: "cli_app"}},
				Event: &larkim.P2MessageReceiveV1Data{
					Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: &openID}, SenderType: &senderType},
					Message: &larkim.EventMessage{MessageId: &messageID, ChatId: &chatID, ChatType: &chatType, MessageType: &messageType, Content: &content},
				},
			}
			tt.mutate(event)
			calls := 0
			adapter := &FeishuAdapter{bindingID: "feishu-a", accountID: "account-a", appID: "cli_app"}
			adapter.sink = ingressSinkFunc(func(context.Context, message.InboundMessage) (AcceptResult, error) {
				calls++
				return AcceptResult{}, nil
			})
			if err := adapter.handleMessage(context.Background(), event); err != nil || calls != 0 {
				t.Fatalf("invalid event accepted: calls=%d, error=%v", calls, err)
			}
		})
	}
}

func TestFeishuReadinessTracksConnectionLoss(t *testing.T) {
	adapter := &FeishuAdapter{started: true, ws: &fakeFeishuWebSocket{}, messages: fakeFeishuMessages{}}
	for _, ready := range []bool{true, false, true, false} {
		adapter.markFeishuReady(ready)
		if got := adapter.Ready(context.Background()) == nil; got != ready {
			t.Fatalf("Ready() = %v, want %v", got, ready)
		}
	}
}
