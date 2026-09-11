package tool

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestSaveArtifactToolWritesFrameworkArtifact(t *testing.T) {
	service := artifactinmemory.NewService()
	tool := NewSaveArtifactTool()
	if tool.Declaration().Name != SaveArtifactToolName {
		t.Fatalf("tool name = %q", tool.Declaration().Name)
	}
	ctx := agent.NewInvocationContext(context.Background(), &agent.Invocation{
		Session:         &session.Session{AppName: "support/app", UserID: "user-1", ID: "session-1"},
		ArtifactService: service,
	})
	result, err := tool.Call(ctx, []byte(`{"filename":"report.txt","text":"hello artifact"}`))
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	payload, _ := json.Marshal(result)
	if string(payload) == "" || !containsAll(string(payload), "report.txt", `"version":0`) {
		t.Fatalf("Call() result = %s", payload)
	}
	loaded, err := service.LoadArtifact(context.Background(), agentartifact.SessionInfo{
		AppName: "support/app", UserID: "user-1", SessionID: "session-1",
	}, "report.txt", nil)
	if err != nil || loaded == nil || string(loaded.Data) != "hello artifact" {
		t.Fatalf("LoadArtifact() = %#v, %v", loaded, err)
	}
}

func TestSaveArtifactToolUserScopeAndRejectsEmptyContent(t *testing.T) {
	service := artifactinmemory.NewService()
	tool := NewSaveArtifactTool()
	ctx := agent.NewInvocationContext(context.Background(), &agent.Invocation{
		Session:         &session.Session{AppName: "support/app", UserID: "user-1", ID: "session-1"},
		ArtifactService: service,
	})
	if _, err := tool.Call(ctx, []byte(`{"filename":"empty.txt"}`)); err == nil {
		t.Fatal("Call() error = nil, want empty content rejected")
	}
	if _, err := tool.Call(ctx, []byte(`{"filename":"notes.txt","text":"keep","user_scope":true}`)); err != nil {
		t.Fatalf("Call(user_scope) error = %v", err)
	}
	loaded, err := service.LoadArtifact(context.Background(), agentartifact.SessionInfo{
		AppName: "support/app", UserID: "user-1", SessionID: "other-session",
	}, "user:notes.txt", nil)
	if err != nil || loaded == nil || string(loaded.Data) != "keep" {
		t.Fatalf("user-scoped LoadArtifact() = %#v, %v", loaded, err)
	}
}

func TestSaveArtifactToolRecordsOnlySuccessfulArtifact(t *testing.T) {
	service := artifactinmemory.NewService()
	tool := NewSaveArtifactTool()
	recorder := NewSavedArtifactRecorder()
	ctx := WithSavedArtifactRecorder(agent.NewInvocationContext(context.Background(), &agent.Invocation{
		Session:         &session.Session{AppName: "support/app", UserID: "user-1", ID: "session-1"},
		ArtifactService: service,
	}), recorder)
	if _, err := tool.Call(ctx, []byte(`{"filename":"report.txt","text":"ready","name":"report.txt"}`)); err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	items := recorder.Snapshot()
	if len(items) != 1 || items[0].Filename != "report.txt" || items[0].Version != 0 || items[0].Name != "report.txt" {
		t.Fatalf("recorded artifacts = %#v", items)
	}
	if _, err := tool.Call(ctx, []byte(`{"filename":"broken.txt"}`)); err == nil {
		t.Fatal("Call(empty) error = nil")
	}
	if got := recorder.Snapshot(); len(got) != 1 {
		t.Fatalf("failed save changed recorder: %#v", got)
	}
}

func TestSaveArtifactToolValidatesInputBeforeSideEffects(t *testing.T) {
	tool := NewSaveArtifactTool()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tool.Call(canceled, []byte(`{"filename":"report.txt","text":"ready"}`)); err == nil {
		t.Fatal("Call() accepted canceled context")
	}

	tests := []struct {
		name string
		raw  []byte
		want string
	}{
		{name: "malformed json", raw: []byte(`{`), want: "decode save artifact arguments"},
		{name: "null", raw: []byte(`null`), want: "filename is required"},
		{name: "missing filename", raw: []byte(`{"text":"ready"}`), want: "filename is required"},
		{name: "both payloads", raw: []byte(`{"filename":"report.bin","text":"ready","data_base64":"cmVhZHk="}`), want: "either text or data_base64"},
		{name: "invalid base64", raw: []byte(`{"filename":"report.bin","data_base64":"%%%"}`), want: "decode data_base64"},
		{name: "missing payload", raw: []byte(`{"filename":"report.txt"}`), want: "text or data_base64 is required"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := tool.Call(context.Background(), test.raw); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Call() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestSaveArtifactToolSupportsBinaryPayloadAndExplicitMetadata(t *testing.T) {
	service := artifactinmemory.NewService()
	tool := NewSaveArtifactTool()
	ctx := agent.NewInvocationContext(context.Background(), &agent.Invocation{
		Session:         &session.Session{AppName: "support/app", UserID: "user-1", ID: "session-1"},
		ArtifactService: service,
	})
	payload := base64.StdEncoding.EncodeToString([]byte{0, 1, 2, 3})
	result, err := tool.Call(ctx, []byte(`{"filename":"user:data.bin","mime_type":" application/x-support ","data_base64":"`+payload+`","name":" Binary data ","user_scope":true}`))
	if err != nil {
		t.Fatal(err)
	}
	resultMap := result.(map[string]any)
	if resultMap["filename"] != "user:data.bin" || resultMap["mime_type"] != "application/x-support" {
		t.Fatalf("Call() result = %#v", resultMap)
	}
	loaded, err := service.LoadArtifact(context.Background(), agentartifact.SessionInfo{
		AppName: "support/app", UserID: "user-1", SessionID: "other-session",
	}, "user:data.bin", nil)
	if err != nil || loaded == nil || !bytes.Equal(loaded.Data, []byte{0, 1, 2, 3}) || loaded.Name != "Binary data" {
		t.Fatalf("LoadArtifact() = %#v, %v", loaded, err)
	}
}

func TestSaveArtifactToolUsesBinaryDefaultMimeType(t *testing.T) {
	service := artifactinmemory.NewService()
	tool := NewSaveArtifactTool()
	ctx := agent.NewInvocationContext(context.Background(), &agent.Invocation{
		Session:         &session.Session{AppName: "support/app", UserID: "user-1", ID: "session-1"},
		ArtifactService: service,
	})
	payload := base64.StdEncoding.EncodeToString([]byte("binary"))
	result, err := tool.Call(ctx, []byte(`{"filename":"data.bin","data_base64":"`+payload+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := result.(map[string]any)["mime_type"]; got != "application/octet-stream" {
		t.Fatalf("mime_type = %v", got)
	}
}

func TestSaveArtifactToolRejectsOversizedContentAndMissingToolContext(t *testing.T) {
	tool := NewSaveArtifactTool()
	large := strings.Repeat("x", int(storage.MaxArtifactBytes)+1)
	arguments, err := json.Marshal(map[string]string{"filename": "large.txt", "text": large})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Call(context.Background(), arguments); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("oversized Call() error = %v", err)
	}
	if _, err := tool.Call(context.Background(), []byte(`{"filename":"report.txt","text":"ready"}`)); err == nil || !strings.Contains(err.Error(), "artifact tool context") {
		t.Fatalf("missing context error = %v", err)
	}
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
