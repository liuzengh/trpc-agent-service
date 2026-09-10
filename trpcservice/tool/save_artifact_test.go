package tool

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

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
		Session:         &session.Session{AppName: "acme/support", UserID: "user-1", ID: "session-1"},
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
		AppName: "acme/support", UserID: "user-1", SessionID: "session-1",
	}, "report.txt", nil)
	if err != nil || loaded == nil || string(loaded.Data) != "hello artifact" {
		t.Fatalf("LoadArtifact() = %#v, %v", loaded, err)
	}
}

func TestSaveArtifactToolUserScopeAndRejectsEmptyContent(t *testing.T) {
	service := artifactinmemory.NewService()
	tool := NewSaveArtifactTool()
	ctx := agent.NewInvocationContext(context.Background(), &agent.Invocation{
		Session:         &session.Session{AppName: "acme/support", UserID: "user-1", ID: "session-1"},
		ArtifactService: service,
	})
	if _, err := tool.Call(ctx, []byte(`{"filename":"empty.txt"}`)); err == nil {
		t.Fatal("Call() error = nil, want empty content rejected")
	}
	if _, err := tool.Call(ctx, []byte(`{"filename":"notes.txt","text":"keep","user_scope":true}`)); err != nil {
		t.Fatalf("Call(user_scope) error = %v", err)
	}
	loaded, err := service.LoadArtifact(context.Background(), agentartifact.SessionInfo{
		AppName: "acme/support", UserID: "user-1", SessionID: "other-session",
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
		Session:         &session.Session{AppName: "acme/support", UserID: "user-1", ID: "session-1"},
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

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
