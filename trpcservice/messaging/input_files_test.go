package messaging

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
)

func TestStageInboundFilesPersistsProviderMediaAsArtifactReferences(t *testing.T) {
	service := artifactinmemory.NewService()
	info := agentartifact.SessionInfo{AppName: "support/assistant", UserID: "customer-1", SessionID: "session-1"}
	files, err := StageInboundFiles(context.Background(), service, info, "message-1", []channels.ReceivedFile{{
		Name: "../report.txt", Data: []byte("hello attachment"),
	}})
	if err != nil {
		t.Fatalf("StageInboundFiles() error = %v", err)
	}
	if len(files) != 1 || files[0].Name != "report.txt" || files[0].ArtifactName != "input/message-1/00-report.txt" {
		t.Fatalf("files = %#v", files)
	}
	version := files[0].Version
	stored, err := service.LoadArtifact(context.Background(), info, files[0].ArtifactName, &version)
	if err != nil || stored == nil || string(stored.Data) != "hello attachment" {
		t.Fatalf("stored artifact = %#v, error = %v", stored, err)
	}
}

func TestStageInboundFilesValidatesInputs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	info := agentartifact.SessionInfo{AppName: "support/assistant", UserID: "customer-1", SessionID: "session-1"}
	service := artifactinmemory.NewService()

	files, err := StageInboundFiles(ctx, nil, info, "message-1", nil)
	if err != nil || files != nil {
		t.Fatalf("empty files = %#v, %v", files, err)
	}
	if _, err := StageInboundFiles(ctx, nil, info, "message-1", []channels.ReceivedFile{{Name: "note.txt", Data: []byte("x")}}); err == nil {
		t.Fatal("missing artifact service error = nil")
	}
	if _, err := StageInboundFiles(ctx, service, info, " ", []channels.ReceivedFile{{Name: "note.txt", Data: []byte("x")}}); err == nil {
		t.Fatal("empty message ID error = nil")
	}
	if _, err := StageInboundFiles(ctx, service, info, "message-1", []channels.ReceivedFile{{Name: "empty.txt"}}); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty file error = %v", err)
	}
	tooLarge := make([]byte, storage.MaxArtifactBytes+1)
	if _, err := StageInboundFiles(ctx, service, info, "message-1", []channels.ReceivedFile{{Name: "large.bin", Data: tooLarge}}); err == nil || !strings.Contains(err.Error(), "upload limit") {
		t.Fatalf("oversized file error = %v", err)
	}
}

func TestStageInboundFilesDetectsMimeTypeAndSanitizesNames(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	service := artifactinmemory.NewService()
	info := agentartifact.SessionInfo{AppName: "support/assistant", UserID: "customer-1", SessionID: "session-1"}
	files, err := StageInboundFiles(ctx, service, info, "message-2", []channels.ReceivedFile{
		{Name: `..\\notes.txt`, MimeType: "application/octet-stream", Data: []byte("plain support note")},
		{Name: "\x00", Data: []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Name != "notes.txt" || files[0].MimeType == "application/octet-stream" {
		t.Fatalf("first file = %+v", files[0])
	}
	if files[1].Name != "upload" || files[1].MimeType != "image/png" {
		t.Fatalf("second file = %+v", files[1])
	}
}

func TestStageInboundFilesRollsBackEarlierFilesOnLaterFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	service := artifactinmemory.NewService()
	info := agentartifact.SessionInfo{AppName: "support/assistant", UserID: "customer-1", SessionID: "session-1"}
	_, err := StageInboundFiles(ctx, service, info, "message-rollback", []channels.ReceivedFile{
		{Name: "first.txt", Data: []byte("first")},
		{Name: "second.txt", Data: nil},
	})
	if err == nil {
		t.Fatal("second file failure error = nil")
	}
	version := 0
	stored, loadErr := service.LoadArtifact(ctx, info, "input/message-rollback/00-first.txt", &version)
	if loadErr != nil {
		t.Fatalf("LoadArtifact() error = %v", loadErr)
	}
	if stored != nil {
		t.Fatalf("rolled-back artifact still exists: %+v", stored)
	}
}

func TestDeleteInboundFilesIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	service := artifactinmemory.NewService()
	info := agentartifact.SessionInfo{AppName: "support/assistant", UserID: "customer-1", SessionID: "session-1"}
	version, err := service.SaveArtifact(ctx, info, "input/message-3/00-note.txt", &agentartifact.Artifact{Data: []byte("note")})
	if err != nil {
		t.Fatal(err)
	}
	file := channels.InboundFile{ArtifactName: "input/message-3/00-note.txt", Version: version}
	DeleteInboundFiles(ctx, nil, info, []channels.InboundFile{file})
	DeleteInboundFiles(ctx, service, info, []channels.InboundFile{file})
	DeleteInboundFiles(ctx, service, info, []channels.InboundFile{file})
	stored, err := service.LoadArtifact(ctx, info, file.ArtifactName, &version)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadArtifact() error = %v", err)
	}
	if stored != nil {
		t.Fatalf("deleted artifact = %+v", stored)
	}
}

func TestSafeInputName(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"../report.txt":     "report.txt",
		`..\\report.txt`:    "report.txt",
		" folder/note.txt ": "note.txt",
		".":                 "upload",
		"":                  "upload",
		"\x00":              "upload",
		"a\x00b.txt":        "ab.txt",
	}
	for input, want := range tests {
		if got := safeInputName(input); got != want {
			t.Fatalf("safeInputName(%q) = %q, want %q", input, got, want)
		}
	}
}
