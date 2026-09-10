package agent

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type fileCapturingRunner struct{ message model.Message }

func (r *fileCapturingRunner) Run(_ context.Context, _ string, _ string, message model.Message, _ ...agent.RunOption) (<-chan *event.Event, error) {
	r.message = message
	events := make(chan *event.Event, 1)
	events <- &event.Event{Response: &model.Response{Choices: []model.Choice{{Message: model.NewAssistantMessage("收到文件")}}}}
	close(events)
	return events, nil
}

func (*fileCapturingRunner) Close() error { return nil }

type runtimeArtifactProvider struct{ service agentartifact.Service }

func (p runtimeArtifactProvider) ArtifactService(context.Context, config.TenantConfig) (agentartifact.Service, error) {
	return p.service, nil
}

type acceptingModelInputValidator struct{}

func (acceptingModelInputValidator) ValidateInputs(config.ModelConfig, []config.ModelInputKind) error {
	return nil
}

type testDocumentInputExtractor struct{}

func (testDocumentInputExtractor) SupportsDocument(name string) bool {
	return strings.EqualFold(filepath.Ext(name), ".pdf")
}

func (testDocumentInputExtractor) ExtractDocument(context.Context, string, []byte) (string, error) {
	return "# 提取内容\n\n维修单编号 A-17", nil
}

func TestRuntimeLoadsStagedFilesIntoFrameworkUserMessage(t *testing.T) {
	t.Parallel()

	artifacts := artifactinmemory.NewService()
	snapshot := tenant.Snapshot{Config: config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
	}}
	sessionKey := "tenant-a/support/session/conversation-1"
	info := agentartifact.SessionInfo{AppName: snapshot.Config.AppName(), UserID: "console-user", SessionID: sessionKey}
	version, err := artifacts.SaveArtifact(context.Background(), info, "input/message-1/00-notes.txt", &agentartifact.Artifact{
		Data: []byte("hello from file"), MimeType: "text/plain", Name: "notes.txt",
	})
	if err != nil {
		t.Fatalf("SaveArtifact() error = %v", err)
	}

	capturing := &fileCapturingRunner{}
	runtime, err := NewRuntime(
		tenant.NewMemoryRepository(),
		staticRunnerProvider{runner: capturing},
		storage.NewMemoryIdempotencyStore(),
		storage.NewMemoryStateStore(),
		time.Minute,
		time.Hour,
		WithArtifactProvider(runtimeArtifactProvider{service: artifacts}),
		WithModelInputValidator(acceptingModelInputValidator{}),
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	runtimeContext := WithResolvedSessionKey(WithConfigurationSnapshot(context.Background(), snapshot), sessionKey)
	_, err = runtime.Handle(runtimeContext, "web-console", channels.InboundMessage{
		MessageID: "message-1", Channel: channels.Web, ConversationID: "conversation-1", SenderID: "console-user", WebOwnerID: "console-user",
		Text: "分析这个文件", Files: []channels.InboundFile{{
			Name: "notes.txt", MimeType: "text/plain", ArtifactName: "input/message-1/00-notes.txt", Version: version, SizeBytes: 15,
		}},
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got, want := capturing.message.Content, "分析这个文件\n\n附件「notes.txt」内容：\nhello from file"; got != want {
		t.Fatalf("message content = %q, want %q", got, want)
	}
	if got := len(capturing.message.ContentParts); got != 0 {
		t.Fatalf("content parts = %d, want text attachment in message content", got)
	}
}

func TestRuntimeBuildsImageAsVisionContent(t *testing.T) {
	t.Parallel()

	artifacts := artifactinmemory.NewService()
	tenantConfig := config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}
	sessionKey := "tenant-a/support/session/image"
	info := agentartifact.SessionInfo{AppName: tenantConfig.AppName(), UserID: "user-1", SessionID: sessionKey}
	version, err := artifacts.SaveArtifact(context.Background(), info, "input/image/00-defect.png", &agentartifact.Artifact{
		Data: []byte("png-bytes"), MimeType: "image/png", Name: "defect.png",
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{artifacts: runtimeArtifactProvider{service: artifacts}}
	message, err := runtime.buildUserMessage(context.Background(), tenantConfig, sessionKey, channels.InboundMessage{
		SubjectID: "user-1", Text: "看看这个缺陷", Files: []channels.InboundFile{{
			Name: "defect.png", MimeType: "image/png", ArtifactName: "input/image/00-defect.png", Version: version,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(message.ContentParts) != 1 || message.ContentParts[0].Type != model.ContentTypeImage || message.ContentParts[0].Image == nil {
		t.Fatalf("image content parts = %#v", message.ContentParts)
	}
	if got := message.ContentParts[0].Image.Format; got != "image/png" {
		t.Fatalf("image format = %q, want image/png", got)
	}
}

func TestRuntimeExtractsCommonDocumentBeforeModelInvocation(t *testing.T) {
	t.Parallel()

	artifacts := artifactinmemory.NewService()
	tenantConfig := config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}
	sessionKey := "tenant-a/support/session/document"
	info := agentartifact.SessionInfo{AppName: tenantConfig.AppName(), UserID: "user-1", SessionID: sessionKey}
	version, err := artifacts.SaveArtifact(context.Background(), info, "input/document/00-form.pdf", &agentartifact.Artifact{
		Data: []byte("pdf-bytes"), MimeType: "application/pdf", Name: "form.pdf",
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		artifacts:      runtimeArtifactProvider{service: artifacts},
		documentInputs: testDocumentInputExtractor{},
	}
	message, err := runtime.buildUserMessage(context.Background(), tenantConfig, sessionKey, channels.InboundMessage{
		SubjectID: "user-1", Text: "读取维修单", Files: []channels.InboundFile{{
			Name: "form.pdf", MimeType: "application/pdf", ArtifactName: "input/document/00-form.pdf", Version: version,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message.Content, "附件「form.pdf」内容") || !strings.Contains(message.Content, "维修单编号 A-17") {
		t.Fatalf("document content = %q", message.Content)
	}
	if len(message.ContentParts) != 0 {
		t.Fatalf("document should be extracted to text, content parts = %#v", message.ContentParts)
	}
}

func TestRequiredModelInputsOnlyContainsNativeModalities(t *testing.T) {
	t.Parallel()
	required := requiredModelInputKinds([]channels.InboundFile{
		{Name: "notes.txt", MimeType: "text/plain"},
		{Name: "manual.pdf", MimeType: "application/pdf"},
		{Name: "defect.jpg", MimeType: "image/jpeg"},
		{Name: "archive.zip", MimeType: "application/zip"},
	}, testDocumentInputExtractor{})
	want := []config.ModelInputKind{config.ModelInputImage, config.ModelInputFile}
	if len(required) != len(want) || required[0] != want[0] || required[1] != want[1] {
		t.Fatalf("required inputs = %v, want %v", required, want)
	}
}
