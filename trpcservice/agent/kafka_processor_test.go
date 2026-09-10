package agent

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestKafkaProcessorAcknowledgesCompletedReplay(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	stateStore := storage.NewMemoryStateStore()
	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(testutil.NewFakeModel("runtime reply")),
		storage.NewMemoryIdempotencyStore(),
		stateStore,
		time.Minute,
		time.Hour,
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	snapshot, err := repository.GetVersion(context.Background(), "tenant-a", "support", 1)
	if err != nil {
		t.Fatalf("GetVersion() error = %v", err)
	}
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := messaging.NewInboundEnvelope(tenant.ReleaseSelection{Snapshot: snapshot, Variant: tenant.ReleaseStable}, "telegram-bot-a", "tenant-a/support/session/session-1", channels.InboundMessage{
		MessageID: "replayed-message", Channel: channels.Telegram,
		ConversationID: "chat-1", SenderID: "user-1", Text: "hello",
	}, manifests)
	if err != nil {
		t.Fatalf("NewInboundEnvelope() error = %v", err)
	}
	processor, err := NewKafkaProcessor(runtime, repository, manifests)
	if err != nil {
		t.Fatalf("NewKafkaProcessor() error = %v", err)
	}
	if err := processor.Process(context.Background(), envelope); err != nil {
		t.Fatalf("first Process() error = %v", err)
	}
	if err := processor.Process(context.Background(), envelope); err != nil {
		t.Fatalf("replayed Process() error = %v, want offset-committable success", err)
	}
	if got := pendingOutboxCount(t, stateStore, "tenant-a"); got != 1 {
		t.Fatalf("pending outbox events = %d, want one committed reply", got)
	}
}
