package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type rejectingRateLimiter struct{ err error }

func (r rejectingRateLimiter) Take(context.Context, string, string, int) error { return r.err }

type failingVersionRepository struct {
	tenant.Repository
	err error
}

type recordingWebFailurePublisher struct {
	calls     int
	tenantID  string
	ownerID   string
	requestID string
	code      string
	message   string
}

func (p *recordingWebFailurePublisher) PublishFailure(_ context.Context, tenantID, ownerID, requestID, code, message string) error {
	p.calls++
	p.tenantID = tenantID
	p.ownerID = ownerID
	p.requestID = requestID
	p.code = code
	p.message = message
	return nil
}

func (r failingVersionRepository) GetVersion(context.Context, string, string, uint64) (tenant.Snapshot, error) {
	return tenant.Snapshot{}, r.err
}

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

func TestNewKafkaProcessorRequiresDependencies(t *testing.T) {
	t.Parallel()
	repository := tenant.NewMemoryRepository()
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name           string
		runtime        *Runtime
		configurations tenant.Repository
		manifests      *messaging.ExecutionManifestCodec
	}{
		{name: "runtime", configurations: repository, manifests: manifests},
		{name: "repository", runtime: &Runtime{}, manifests: manifests},
		{name: "manifest verifier", runtime: &Runtime{}, configurations: repository},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewKafkaProcessor(test.runtime, test.configurations, test.manifests); err == nil {
				t.Fatal("NewKafkaProcessor() error = nil")
			}
		})
	}
}

func TestKafkaProcessorClassifiesInvalidQueuedMessagesAsPermanent(t *testing.T) {
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
		t.Fatal(err)
	}
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := NewKafkaProcessor(runtime, repository, manifests)
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := repository.GetVersion(context.Background(), "tenant-a", "support", 1)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := messaging.NewInboundEnvelope(tenant.ReleaseSelection{Snapshot: snapshot, Variant: tenant.ReleaseStable}, "telegram-bot-a", "tenant-a/support/session/session-invalid", channels.InboundMessage{
		MessageID: "message-valid", Channel: channels.Telegram, ConversationID: "chat-1", SenderID: "user-1", Text: "hello",
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}

	tampered := valid
	tampered.Payload = []byte(`{"changed":true}`)
	if err := processor.Process(context.Background(), tampered); err == nil || messaging.IsRetryable(err) {
		t.Fatalf("tampered envelope error = %v, want permanent", err)
	}

	invalidPayload := valid
	invalidPayload.Payload = []byte(`{"binding_id":"telegram-bot-a"}`)
	if err := manifests.SignEnvelope(&invalidPayload, "support", 1, "trace-invalid"); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), invalidPayload); err == nil || messaging.IsRetryable(err) {
		t.Fatalf("invalid payload error = %v, want permanent", err)
	}

	mismatchedManifest := valid
	if err := manifests.SignEnvelope(&mismatchedManifest, "other-app", 1, "trace-mismatch"); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), mismatchedManifest); err == nil || messaging.IsRetryable(err) {
		t.Fatalf("manifest routing mismatch error = %v, want permanent", err)
	}

	missingVersionSnapshot := snapshot
	missingVersionSnapshot.Config.ConfigVersion = 2
	missingVersion, err := messaging.NewInboundEnvelope(tenant.ReleaseSelection{Snapshot: missingVersionSnapshot, Variant: tenant.ReleaseStable}, "telegram-bot-a", "tenant-a/support/session/session-v2", channels.InboundMessage{
		MessageID: "message-v2", Channel: channels.Telegram, ConversationID: "chat-2", SenderID: "user-2", Text: "hello",
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(context.Background(), missingVersion); err == nil || messaging.IsRetryable(err) {
		t.Fatalf("missing version error = %v, want permanent", err)
	}
}

func TestKafkaProcessorClassifiesRuntimeFailureAsRetryable(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	runtimeErr := errors.New("rate limiter unavailable")
	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(testutil.NewFakeModel("runtime reply")),
		storage.NewMemoryIdempotencyStore(),
		storage.NewMemoryStateStore(),
		time.Minute,
		time.Hour,
		WithTenantRateLimiter(rejectingRateLimiter{err: runtimeErr}),
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.GetVersion(context.Background(), "tenant-a", "support", 1)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := messaging.NewInboundEnvelope(tenant.ReleaseSelection{Snapshot: snapshot, Variant: tenant.ReleaseStable}, "telegram-bot-a", "tenant-a/support/session/session-retry", channels.InboundMessage{
		MessageID: "message-retry", Channel: channels.Telegram, ConversationID: "chat-retry", SenderID: "user-retry", Text: "hello",
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := NewKafkaProcessor(runtime, repository, manifests)
	if err != nil {
		t.Fatal(err)
	}
	processErr := processor.Process(context.Background(), envelope)
	if processErr == nil || !messaging.IsRetryable(processErr) || !strings.Contains(processErr.Error(), runtimeErr.Error()) {
		t.Fatalf("Process() error = %v, want retryable runtime failure", processErr)
	}
}

func TestKafkaProcessorPublishesUserSafeTerminalWebFailure(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	snapshot, err := repository.GetVersion(context.Background(), "tenant-a", "support", 1)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := messaging.NewInboundEnvelope(tenant.ReleaseSelection{Snapshot: snapshot, Variant: tenant.ReleaseStable}, "web-console", "tenant-a/support/session/web-failed", channels.InboundMessage{
		MessageID: "web-failed", Channel: channels.Web, ConversationID: "conversation-1", SenderID: "user-1", WebOwnerID: "user-1", Text: "hello",
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingWebFailurePublisher{}
	processor, err := NewKafkaProcessor(&Runtime{}, repository, manifests, WithWebFailurePublisher(publisher))
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.NotifyTerminalFailure(context.Background(), envelope, "retry_exhausted", errors.New("model provider unavailable")); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 1 || publisher.tenantID != "tenant-a" || publisher.ownerID != "user-1" || publisher.requestID != "web-failed" ||
		publisher.code != "model_unavailable" || publisher.message != "模型服务暂时不可用，请稍后重试。" {
		t.Fatalf("published failure = %#v", publisher)
	}
}

func TestKafkaProcessorRetriesTransientConfigurationRead(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	snapshot, err := repository.GetVersion(context.Background(), "tenant-a", "support", 1)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := messaging.NewInboundEnvelope(tenant.ReleaseSelection{Snapshot: snapshot, Variant: tenant.ReleaseStable}, "telegram-bot-a", "tenant-a/support/session/session-config-retry", channels.InboundMessage{
		MessageID: "message-config-retry", Channel: channels.Telegram, ConversationID: "chat-config-retry", SenderID: "user-1", Text: "hello",
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(repository, assembly.NewFactory(testutil.NewFakeModel("reply")), storage.NewMemoryIdempotencyStore(), storage.NewMemoryStateStore(), time.Minute, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	readErr := errors.New("database temporarily unavailable")
	processor, err := NewKafkaProcessor(runtime, failingVersionRepository{Repository: repository, err: readErr}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	processErr := processor.Process(context.Background(), envelope)
	if processErr == nil || !messaging.IsRetryable(processErr) || !errors.Is(processErr, readErr) {
		t.Fatalf("Process() error = %v, want retryable configuration read failure", processErr)
	}
}

func TestKafkaProcessorDoesNotRetryDeterministicInputRejection(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	rejection := errors.New("selected model does not support image input")
	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(testutil.NewFakeModel("reply")),
		storage.NewMemoryIdempotencyStore(), storage.NewMemoryStateStore(),
		time.Minute, time.Hour,
		WithModelInputValidator(rejectingModelInputValidator{err: rejection}),
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.GetVersion(context.Background(), "tenant-a", "support", 1)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := messaging.NewExecutionManifestCodec("test-v1", []byte("platform-hmac-secret-key-at-least-32-bytes"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := messaging.NewInboundEnvelope(tenant.ReleaseSelection{Snapshot: snapshot, Variant: tenant.ReleaseStable}, "telegram-bot-a", "tenant-a/support/session/session-input-reject", channels.InboundMessage{
		MessageID: "message-input-reject", Channel: channels.Telegram, ConversationID: "chat-input-reject", SenderID: "user-1", Text: "look",
		Files: []channels.InboundFile{{Name: "photo.png", MimeType: "image/png", ArtifactName: "input:photo", Version: 0, SizeBytes: 1}},
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := NewKafkaProcessor(runtime, repository, manifests)
	if err != nil {
		t.Fatal(err)
	}
	processErr := processor.Process(context.Background(), envelope)
	if processErr == nil || messaging.IsRetryable(processErr) || !errors.Is(processErr, rejection) {
		t.Fatalf("Process() error = %v, want permanent input rejection", processErr)
	}
}
