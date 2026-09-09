package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestRealTelegramSender(t *testing.T) {
	if os.Getenv("TELEGRAM_B2_REAL") != "1" {
		t.Skip("TELEGRAM_B2_REAL is not 1; real Telegram gate is disabled")
	}
	if _, ok := os.LookupEnv("TELEGRAM_B2_BOT_TOKEN"); !ok {
		t.Skip("TELEGRAM_B2_BOT_TOKEN is not set; real Telegram evidence is skipped")
	}
	chatID, ok := os.LookupEnv("TELEGRAM_B2_CHAT_ID")
	if !ok || chatID == "" {
		t.Skip("TELEGRAM_B2_CHAT_ID is not set; real Telegram evidence is skipped")
	}
	if err := channels.ValidateDestination(Channel, channels.DestinationTypeChat, chatID); err != nil {
		t.Skip("TELEGRAM_B2_CHAT_ID is not a canonical numeric chat ID; real Telegram evidence is skipped")
	}

	binding := Binding{
		TenantID: "telegram-b2-real-tenant", BindingID: "telegram-b2-real-binding", Channel: Channel,
		BotTokenSecretRef: "env://TELEGRAM_B2_BOT_TOKEN", WebhookSecretRef: "env://TELEGRAM_B2_WEBHOOK_SECRET", Enabled: true,
	}
	resolver := EnvironmentSecretResolver{}
	sender, err := NewSender(SenderConfig{
		Bindings: []Binding{binding},
		Tokens: TokenResolverFunc(func(ctx context.Context, candidate Binding) (string, error) {
			return resolver.Resolve(ctx, candidate.BotTokenSecretRef)
		}),
		Client: &http.Client{Timeout: 15 * time.Second},
	})
	if err != nil {
		t.Fatal("real Telegram sender configuration was rejected")
	}
	if err := sender.GetMe(context.Background(), binding.BindingID); err != nil {
		t.Fatalf("Telegram getMe: FAIL %s", err)
	}
	t.Log("Telegram getMe: PASS")

	payload := channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: binding.TenantID, SessionID: "telegram-b2-real-session", JobID: "telegram-b2-real-job", ExecutionID: "telegram-b2-real-execution",
		RequestID: "telegram-b2-real-request", MessageID: "telegram-b2-real-message", TraceID: "telegram-b2-real-trace", BindingID: binding.BindingID,
		Channel: Channel, DestinationType: channels.DestinationTypeChat, DestinationID: chatID,
		ReplyText: "telegram-b2 sender boundary test", SenderRoutingVersion: channels.SenderRoutingVersion,
	}
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal("real Telegram reply payload could not be constructed")
	}
	outcome := sender.Send(context.Background(), storage.OutboxMessage{
		TenantID: binding.TenantID, ID: "reply-" + payload.ExecutionID, Kind: channels.ReplyOutboxKind,
		AggregateID: payload.ExecutionID, DedupKey: payload.TenantID + "|" + payload.ExecutionID + "|" + channels.ReplyOutboxKind, Payload: encoded,
	})
	if outcome.Class != channels.OutcomeDelivered {
		t.Fatalf("Telegram real message send: FAIL %s", outcome.Code)
	}
	t.Log("Telegram real message send: PASS")
	t.Log("Sender outcome: Delivered")

	var payloadCheck map[string]any
	if err := json.Unmarshal(encoded, &payloadCheck); err != nil || payloadCheck["chat_id"] != nil {
		t.Fatal("real test payload did not remain storage-neutral")
	}
}
