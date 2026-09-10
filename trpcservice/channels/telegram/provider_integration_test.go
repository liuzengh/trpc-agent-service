package telegram

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/go-telegram/bot"
)

// TestTelegramProviderOutboxE2E exercises a real Telegram delivery only when
// the protected E2E environment supplies both existing bot credentials. The
// receiver sends a durable reply to the sender bot, matching the live adapter
// topology used by the Telegram E2E workflow.
func TestTelegramProviderOutboxE2E(t *testing.T) {
	receiverToken, senderToken := telegramE2ECredentials(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	provider := telegramE2EProvider(t, ctx, receiverToken, senderToken)
	store := inmemory.New()
	const tenantID, sessionID, eventID, replyID = "telegram-e2e", "session", "event", "reply"
	telegramE2ESeed(t, ctx, store, tenantID, sessionID, eventID, replyID)
	worker := telegramE2EWorker(t, store, provider, tenantID)
	telegramE2EAssert(t, ctx, store, worker, tenantID, eventID, replyID)
}

func telegramE2ECredentials(t *testing.T) (string, string) {
	t.Helper()
	receiverToken := strings.TrimSpace(os.Getenv("TELEGRAM_BOT_TOKEN"))
	senderToken := strings.TrimSpace(os.Getenv("TELEGRAM_SENDER_BOT_TOKEN"))
	if receiverToken == "" || senderToken == "" {
		t.Skip("requires TELEGRAM_BOT_TOKEN and TELEGRAM_SENDER_BOT_TOKEN")
	}
	return receiverToken, senderToken
}

func telegramE2EProvider(t *testing.T, ctx context.Context, receiverToken, senderToken string) *Provider {
	t.Helper()
	receiver, err := bot.New(receiverToken, bot.WithSkipGetMe())
	if err != nil {
		t.Fatal(err)
	}
	sender, err := bot.New(senderToken, bot.WithSkipGetMe())
	if err != nil {
		t.Fatal(err)
	}
	senderIdentity, err := sender.GetMe(ctx)
	if err != nil || senderIdentity == nil || senderIdentity.ID <= 0 {
		t.Fatalf("sender identity = %#v, %v", senderIdentity, err)
	}
	provider, err := NewProvider(receiver, senderIdentity.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func telegramE2ESeed(t *testing.T, ctx context.Context, store *inmemory.Store, tenantID, sessionID, eventID, replyID string) {
	t.Helper()
	if _, err := store.CreateSession(ctx, tenantID, sessionID, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: tenantID, EventID: eventID, SessionID: sessionID, BindingID: "telegram", ExternalMessageID: "telegram-outbox-e2e"}); err != nil {
		t.Fatal(err)
	}
	event, err := store.GetMessage(ctx, tenantID, eventID)
	if err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: tenantID, EventID: eventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "runner", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: tenantID, EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "runner", FencingToken: running.FencingToken, ReplyID: replyID, SegmentCount: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: tenantID, EventID: eventID, ReplyID: replyID, SegmentCount: 1, Payload: "telegram-outbox-e2e"}); err != nil {
		t.Fatal(err)
	}
}

func telegramE2EWorker(t *testing.T, store *inmemory.Store, provider *Provider, tenantID string) *outbox.Worker {
	t.Helper()
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: tenantID, Owner: "telegram-e2e-worker", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func telegramE2EAssert(t *testing.T, ctx context.Context, store *inmemory.Store, worker *outbox.Worker, tenantID, eventID, replyID string) {
	t.Helper()
	if processed, err := worker.RunOnce(ctx); err != nil || processed != 1 {
		t.Fatalf("worker = %d, %v", processed, err)
	}
	reply, err := store.GetReply(ctx, tenantID, replyID, 0)
	if err != nil || reply.Status != runtimestorage.ReplySent || reply.ProviderMessageID == "" {
		t.Fatalf("reply = %+v, %v", reply, err)
	}
	completed, err := store.GetMessage(ctx, tenantID, eventID)
	if err != nil || completed.Status != runtimestorage.EventReplied {
		t.Fatalf("event = %+v, %v", completed, err)
	}
}
