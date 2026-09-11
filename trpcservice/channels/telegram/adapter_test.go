package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestCapabilitiesMatchImplementedOutboundMethods(t *testing.T) {
	adapter, err := New(secret.StaticStore{}, nil)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	capabilities := adapter.Capabilities()
	if capabilities.MaxTextRunes != 4000 || capabilities.SupportsEdit ||
		capabilities.SupportsCard || capabilities.SupportsFile {
		t.Fatalf("capabilities = %+v", capabilities)
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

func TestCallbackFiltersGroupMessages(t *testing.T) {
	tests := []struct {
		name      string
		chatID    int64
		text      string
		entities  []telegramMessageEntity
		from      telegramUser
		replyFrom *telegramUser
		want      int
	}{
		{
			name: "explicit mention", chatID: -100, text: "hello @trpc_agent_test_bot",
			entities: []telegramMessageEntity{{Type: "mention", Offset: 6, Length: 20}},
			from:     telegramUser{ID: 42}, want: 1,
		},
		{
			name: "addressed command", chatID: -100, text: "/ask@trpc_agent_test_bot hello",
			entities: []telegramMessageEntity{{Type: "bot_command", Offset: 0, Length: 24}},
			from:     telegramUser{ID: 42}, want: 1,
		},
		{
			name: "reply to bot", chatID: -100, text: "hello",
			from: telegramUser{ID: 42}, replyFrom: &telegramUser{ID: 99, IsBot: true}, want: 1,
		},
		{
			name: "plain group text", chatID: -100, text: "hello",
			from: telegramUser{ID: 42}, want: 0,
		},
		{
			name: "other mention", chatID: -100, text: "hello @other_bot",
			entities: []telegramMessageEntity{{Type: "mention", Offset: 6, Length: 10}},
			from:     telegramUser{ID: 42}, want: 0,
		},
		{
			name: "disallowed group", chatID: -200, text: "/ask@trpc_agent_test_bot hello",
			entities: []telegramMessageEntity{{Type: "bot_command", Offset: 0, Length: 24}},
			from:     telegramUser{ID: 42}, want: 0,
		},
		{
			name: "other bot", chatID: -100, text: "/ask@trpc_agent_test_bot hello",
			entities: []telegramMessageEntity{{Type: "bot_command", Offset: 0, Length: 24}},
			from:     telegramUser{ID: 77, IsBot: true}, want: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter, _ := New(secret.StaticStore{
				"secret://bot": "bot-token", "secret://webhook": "webhook-secret",
			}, nil)
			binding := testBindingWithConfig(t, bindingConfig{
				BotTokenRef: "secret://bot", WebhookSecretRef: "secret://webhook",
				BotUserID: 99, BotUsername: "trpc_agent_test_bot",
				AllowedChatIDs: []int64{-100}, RequireMention: true, IgnoreBotMessages: true,
			})
			message := telegramMessage{
				MessageID: 9, Date: 1700000000, Text: test.text, Entities: test.entities,
				From: &test.from, Chat: telegramChat{ID: test.chatID, Type: "supergroup"},
			}
			if test.replyFrom != nil {
				message.ReplyToMessage = &telegramMessage{From: test.replyFrom}
			}
			body, _ := json.Marshal(telegramUpdate{UpdateID: 10001, Message: &message})
			request := httptest.NewRequest(http.MethodPost, "/callback", bytes.NewReader(body))
			request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook-secret")
			result, err := adapter.Callback(context.Background(), binding, request)
			if err != nil || len(result.Messages) != test.want {
				t.Fatalf("messages=%d err=%v", len(result.Messages), err)
			}
		})
	}
}

func TestTelegramEntityTextUsesUTF16Offsets(t *testing.T) {
	value, ok := telegramEntityText("你😀 @trpc_agent_test_bot", 4, 20)
	if !ok || value != "@trpc_agent_test_bot" {
		t.Fatalf("value=%q ok=%t", value, ok)
	}
}

func TestParseBindingValidatesGroupPolicy(t *testing.T) {
	tests := []bindingConfig{
		{
			BotTokenRef: "secret://bot", WebhookSecretRef: "secret://webhook",
			RequireMention: true,
		},
		{
			BotTokenRef: "secret://bot", WebhookSecretRef: "secret://webhook",
			AllowedChatIDs: []int64{-100, -100},
		},
	}
	for _, cfg := range tests {
		if _, err := parseBinding(testBindingWithConfig(t, cfg)); err == nil {
			t.Fatalf("config %+v was accepted", cfg)
		}
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

func TestSendMessageClassifiesRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{
            "ok":false,"error_code":429,"description":"Too Many Requests",
            "parameters":{"retry_after":7}
        }`)
	}))
	defer server.Close()
	adapter, _ := New(secret.StaticStore{
		"secret://bot": "bot-token", "secret://webhook": "webhook-secret",
	}, server.Client())
	target, _ := json.Marshal(telegramTarget{ChatID: -100})
	_, err := adapter.Send(context.Background(), testBinding(t, server.URL), channels.OutboundMessage{
		OutboundID: "outbound", RequestID: "request", Text: "hello", ReplyTarget: string(target),
	})
	var deliveryErr *channels.DeliveryError
	if !errors.As(err, &deliveryErr) || !deliveryErr.Retryable ||
		deliveryErr.RetryAfter != 7*time.Second {
		t.Fatalf("delivery error=%+v err=%v", deliveryErr, err)
	}
}

func testBinding(t *testing.T, apiBase string) controlplane.ChannelBinding {
	t.Helper()
	return testBindingWithConfig(t, bindingConfig{
		BotTokenRef: "secret://bot", WebhookSecretRef: "secret://webhook", APIBaseURL: apiBase,
	})
}

func testBindingWithConfig(t *testing.T, cfg bindingConfig) controlplane.ChannelBinding {
	t.Helper()
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return controlplane.ChannelBinding{
		ID: "telegram-binding", TenantID: "tenant", AppID: "app",
		ChannelType: "telegram", Config: configJSON,
		Status: controlplane.StatusActive, Version: 1,
	}
}
