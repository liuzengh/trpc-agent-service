package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	frameworkmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestArtifactHydratingModelLoadsReferenceBeforeProviderCall(t *testing.T) {
	storage := artifactmemory.NewService()
	info := frameworkartifact.SessionInfo{
		AppName:   "tenant-a/app-a/runner/v1",
		UserID:    "principal-a",
		SessionID: "session-a",
	}
	data := []byte("attachment contents")
	if _, err := storage.SaveArtifact(context.Background(), info, "inbound/file", &frameworkartifact.Artifact{
		Data:     data,
		Name:     "report.docx",
		MimeType: "text/plain",
	}); err != nil {
		t.Fatalf("save artifact: %v", err)
	}

	model := &capturingModel{}
	hydrating := &artifactHydratingModel{
		Model:        model,
		artifacts:    storage,
		info:         info,
		capabilities: tenant.ModelAttachmentCapabilities{File: true},
	}
	request := &frameworkmodel.Request{Messages: []frameworkmodel.Message{{
		Role: frameworkmodel.RoleUser,
		ContentParts: []frameworkmodel.ContentPart{{
			Type: frameworkmodel.ContentTypeFile,
			ContentRef: &frameworkmodel.ContentRef{
				ArtifactRef: "artifact://inbound/file@0",
				SizeBytes:   int64(len(data)),
				MimeType:    "text/plain",
			},
		}},
	}}}

	responses, err := hydrating.GenerateContent(context.Background(), request)
	if err != nil {
		t.Fatalf("generate content: %v", err)
	}
	if responses == nil {
		t.Fatal("generate content returned nil response channel")
	}
	if model.request == nil || len(model.request.Messages) != 1 || len(model.request.Messages[0].ContentParts) != 1 {
		t.Fatalf("provider request = %#v", model.request)
	}
	part := model.request.Messages[0].ContentParts[0]
	if part.File == nil || string(part.File.Data) != string(data) || part.File.Name != "report.docx" || part.File.MimeType != "text/plain" {
		t.Fatalf("hydrated file = %#v", part.File)
	}
	originalPart := request.Messages[0].ContentParts[0]
	if originalPart.File != nil || originalPart.ContentRef == nil {
		t.Fatalf("durable request was mutated with artifact bytes: %#v", originalPart)
	}
}

func TestArtifactHydratingModelChecksHydratedBytesBeforeProvider(t *testing.T) {
	storage := artifactmemory.NewService()
	info := frameworkartifact.SessionInfo{
		AppName:   "tenant-a/app-a/runner/v1",
		UserID:    "principal-a",
		SessionID: "guardrail-attachment",
	}
	if _, err := storage.SaveArtifact(context.Background(), info, "inbound/file", &frameworkartifact.Artifact{
		Data:     []byte("password: hunter2"),
		Name:     "notes.txt",
		MimeType: "text/plain",
	}); err != nil {
		t.Fatalf("save artifact: %v", err)
	}
	provider := &capturingModel{}
	hydrating := &artifactHydratingModel{
		Model:        provider,
		artifacts:    storage,
		info:         info,
		capabilities: tenant.ModelAttachmentCapabilities{File: true},
	}
	_, err := hydrating.GenerateContent(context.Background(), &frameworkmodel.Request{Messages: []frameworkmodel.Message{{
		Role: frameworkmodel.RoleUser,
		ContentParts: []frameworkmodel.ContentPart{{
			Type:       frameworkmodel.ContentTypeFile,
			ContentRef: &frameworkmodel.ContentRef{ArtifactRef: "artifact://inbound/file@0"},
		}},
	}}})
	if !errors.Is(err, guardrail.ErrInputBlocked) {
		t.Fatalf("error = %v, want hydrated attachment guardrail rejection", err)
	}
	if !worker.IsPermanentExecutionError(err) || worker.IsRetryableExecutionError(err) {
		t.Fatalf("guardrail error classification = %v, want permanent and not retryable", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider was called with blocked hydrated attachment: %d", provider.calls)
	}
}

func TestArtifactHydratingModelRejectsUnsupportedAttachmentBeforeProvider(t *testing.T) {
	tests := []struct {
		name string
		mime string
		want frameworkmodel.ContentType
	}{
		{name: "image", mime: "image/png", want: frameworkmodel.ContentTypeImage},
		{name: "audio", mime: "audio/mpeg", want: frameworkmodel.ContentTypeAudio},
		{name: "file", mime: "application/pdf", want: frameworkmodel.ContentTypeFile},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := artifactmemory.NewService()
			info := frameworkartifact.SessionInfo{AppName: "tenant-a/app-a/runner/v1", UserID: "principal-a", SessionID: tt.name}
			if _, err := storage.SaveArtifact(context.Background(), info, "inbound/file", &frameworkartifact.Artifact{
				Data: []byte("attachment"), Name: "attachment", MimeType: tt.mime,
			}); err != nil {
				t.Fatalf("save artifact: %v", err)
			}
			provider := &capturingModel{}
			hydrating := &artifactHydratingModel{
				Model:                        provider,
				artifacts:                    storage,
				info:                         info,
				currentMessageHasAttachments: true,
				currentArtifactRefs:          []string{"artifact://inbound/file@0"},
			}
			_, err := hydrating.GenerateContent(context.Background(), &frameworkmodel.Request{Messages: []frameworkmodel.Message{{
				Role: frameworkmodel.RoleUser,
				ContentParts: []frameworkmodel.ContentPart{{
					Type:       frameworkmodel.ContentTypeFile,
					ContentRef: &frameworkmodel.ContentRef{ArtifactRef: "artifact://inbound/file@0", MimeType: tt.mime},
				}},
			}}})
			if !errors.Is(err, ErrUnsupportedModelAttachment) {
				t.Fatalf("error = %v, want unsupported attachment", err)
			}
			if !worker.IsPermanentExecutionError(err) || worker.IsRetryableExecutionError(err) {
				t.Fatalf("attachment error classification = %v, want permanent and not retryable", err)
			}
			var unsupported UnsupportedModelAttachmentError
			if !errors.As(err, &unsupported) || unsupported.ContentType != tt.want {
				t.Fatalf("attachment error = %v, want type %q", err, tt.want)
			}
			if provider.request != nil || provider.calls != 0 {
				t.Fatalf("provider was called for unsupported %s: calls=%d request=%#v", tt.name, provider.calls, provider.request)
			}
		})
	}
}

func TestArtifactHydratingModelRejectsUnknownMIMEAfterHydrateAsPermanent(t *testing.T) {
	storage := artifactmemory.NewService()
	info := frameworkartifact.SessionInfo{AppName: "tenant-a/app-a/runner/v1", UserID: "principal-a", SessionID: "hydrate-capability"}
	data := []byte("image bytes")
	if _, err := storage.SaveArtifact(context.Background(), info, "inbound/file", &frameworkartifact.Artifact{
		Data: data, Name: "image.png", MimeType: "image/png",
	}); err != nil {
		t.Fatalf("save artifact: %v", err)
	}
	provider := &capturingModel{}
	hydrating := &artifactHydratingModel{
		Model:                        provider,
		artifacts:                    storage,
		info:                         info,
		currentMessageHasAttachments: true,
		currentArtifactRefs:          []string{"artifact://inbound/file@0"},
	}
	_, err := hydrating.GenerateContent(context.Background(), &frameworkmodel.Request{Messages: []frameworkmodel.Message{{
		Role: frameworkmodel.RoleUser,
		ContentParts: []frameworkmodel.ContentPart{{
			Type:       frameworkmodel.ContentTypeFile,
			ContentRef: &frameworkmodel.ContentRef{ArtifactRef: "artifact://inbound/file@0"},
		}},
	}}})
	if !errors.Is(err, ErrUnsupportedModelAttachment) {
		t.Fatalf("error = %v, want unsupported attachment", err)
	}
	if !worker.IsPermanentExecutionError(err) || worker.IsRetryableExecutionError(err) {
		t.Fatalf("attachment error classification = %v, want permanent and not retryable", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider was called for hydrated unsupported image: %d", provider.calls)
	}
}

func TestArtifactHydratingModelOmitsUnsupportedHistoricalAttachment(t *testing.T) {
	provider := &capturingModel{}
	hydrating := &artifactHydratingModel{Model: provider}
	request := &frameworkmodel.Request{Messages: []frameworkmodel.Message{
		{
			Role:    frameworkmodel.RoleUser,
			Content: "图片消息",
			ContentParts: []frameworkmodel.ContentPart{{
				Type: frameworkmodel.ContentTypeFile,
				ContentRef: &frameworkmodel.ContentRef{
					ArtifactRef: "artifact://inbound/file@0",
					MimeType:    "image/png",
				},
			}},
		},
		{Role: frameworkmodel.RoleUser, Content: "后续文字"},
	}}

	responses, err := hydrating.GenerateContent(context.Background(), request)
	if err != nil {
		t.Fatalf("generate content: %v", err)
	}
	if responses == nil || provider.calls != 1 || provider.request == nil {
		t.Fatalf("provider invocation = responses:%v calls:%d request:%#v", responses != nil, provider.calls, provider.request)
	}
	if len(provider.request.Messages[0].ContentParts) != 0 || !strings.Contains(provider.request.Messages[0].Content, "历史附件已省略") {
		t.Fatalf("historical attachment was not sanitized: %#v", provider.request.Messages[0])
	}
	if provider.request.Messages[1].Content != "后续文字" {
		t.Fatalf("current text changed: %#v", provider.request.Messages[1])
	}
	if len(request.Messages[0].ContentParts) != 1 || request.Messages[0].Content != "图片消息" {
		t.Fatalf("durable request was mutated: %#v", request.Messages[0])
	}
}

type capturingModel struct {
	request *frameworkmodel.Request
	calls   int
}

func (m *capturingModel) GenerateContent(_ context.Context, request *frameworkmodel.Request) (<-chan *frameworkmodel.Response, error) {
	m.calls++
	m.request = request
	responses := make(chan *frameworkmodel.Response)
	close(responses)
	return responses, nil
}

func (*capturingModel) Info() frameworkmodel.Info {
	return frameworkmodel.Info{}
}

var _ frameworkmodel.Model = (*capturingModel)(nil)
