package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

func TestCallbackVerifiesAndDecodesUpdate(t *testing.T) {
	adapter, _ := New(secret.StaticStore{
		"secret://bot":     "bot-token",
		"secret://webhook": "webhook-secret",
	}, nil)
	binding := testBinding(t, "")
	body := []byte(`{
        "update_id":10001,
        "message":{
            "message_id":9,"date":1700000000,"text":"hello",
            "from":{"id":42},"chat":{"id":-100,"type":"supergroup"},
            "message_thread_id":7
        }
    }`)
	request := httptest.NewRequest(http.MethodPost, "/callback", bytes.NewReader(body))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook-secret")
	result, err := adapter.Callback(context.Background(), binding, request)
	if err != nil || len(result.Messages) != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	message := result.Messages[0]
	if message.ExternalMessageID != "10001" || message.ExternalUserID != "42" ||
		message.ExternalThreadID != "7" || message.ChatType != "group" || message.ReplyTarget == "" {
		t.Fatalf("message=%+v", message)
	}
}

func TestNormalizedTelegramMediaMessage(t *testing.T) {
	messageType, text := normalizedTelegramMessage(&telegramMessage{
		Caption: "invoice",
		Document: &telegramDocument{
			FileID: "file-1", FileName: "invoice.pdf", MimeType: "application/pdf",
		},
	})
	if messageType != "file" || !strings.Contains(text, "invoice.pdf") ||
		!strings.Contains(text, "file-1") {
		t.Fatalf("type=%q text=%q", messageType, text)
	}
}

func TestCallbackRejectsWrongSecret(t *testing.T) {
	adapter, _ := New(secret.StaticStore{
		"secret://bot": "bot-token", "secret://webhook": "expected",
	}, nil)
	request := httptest.NewRequest(http.MethodPost, "/callback", bytes.NewReader([]byte(`{}`)))
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "wrong")
	if _, err := adapter.Callback(context.Background(), testBinding(t, ""), request); err == nil {
		t.Fatal("expected webhook secret error")
	}
}

func TestSendMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/botbot-token/sendMessage" {
			t.Errorf("path=%q", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if body["chat_id"] != float64(-100) || body["message_thread_id"] != float64(7) {
			t.Errorf("body=%+v", body)
		}
		_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":88}}`)
	}))
	defer server.Close()
	adapter, _ := New(secret.StaticStore{
		"secret://bot": "bot-token", "secret://webhook": "webhook-secret",
	}, server.Client())
	target, _ := json.Marshal(telegramTarget{ChatID: -100, MessageThreadID: 7})
	receipt, err := adapter.Send(context.Background(), testBinding(t, server.URL), channels.OutboundMessage{
		OutboundID: "outbound", RequestID: "request", Text: "hello", ReplyTarget: string(target),
	})
	if err != nil || receipt.ProviderMessageID != "88" {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func testBinding(t *testing.T, apiBase string) controlplane.ChannelBinding {
	t.Helper()
	configJSON, err := json.Marshal(bindingConfig{
		BotTokenRef: "secret://bot", WebhookSecretRef: "secret://webhook", APIBaseURL: apiBase,
	})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return controlplane.ChannelBinding{
		ID: "telegram-binding", TenantID: "tenant", AppID: "app",
		ChannelType: "telegram", Config: configJSON,
		Status: controlplane.StatusActive, Version: 1,
	}
}
