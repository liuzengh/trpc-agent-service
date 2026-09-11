package toolexec

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestMemoryJournalBlocksDuplicateStart(t *testing.T) {
	journal := NewMemoryJournal()
	execution := Execution{
		TenantID: "tenant", RequestID: "request", RevisionID: "revision",
		ToolCallID: "call", ToolName: "tool", ArgumentsHash: Hash([]byte("{}")),
	}
	first, err := journal.Start(context.Background(), execution)
	if err != nil || first.Existing {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := journal.Start(context.Background(), execution)
	if err != nil || !second.Existing || second.Execution.Status != StatusRunning {
		t.Fatalf("second=%+v err=%v", second, err)
	}
}

func TestCallbacksJournalAndAuditToolExecution(t *testing.T) {
	journal := NewMemoryJournal()
	auditWriter := audit.NewMemoryWriter()
	callbacks := NewCallbacks(journal, auditWriter, "revision-a")
	invocation := &agentcore.Invocation{
		Session: &session.Session{ID: "session-a", UserID: "alice"},
		RunOptions: agentcore.RunOptions{
			AppName: "t/tenant-a/a/app-a", RequestID: "request-a",
		},
	}
	ctx := agentcore.NewInvocationContext(context.Background(), invocation)
	args := &tool.BeforeToolArgs{
		ToolCallID: "call-a", ToolName: "dangerous_demo", Arguments: []byte(`{"action":"test"}`),
	}
	if _, err := callbacks.RunBeforeTool(ctx, args); err != nil {
		t.Fatalf("before tool: %v", err)
	}
	if len(journal.records) != 0 {
		t.Fatal("BeforeTool must not record an execution before permission checks")
	}
	execution := Execution{
		RevisionID: "revision-a", ToolCallID: args.ToolCallID,
		ToolName: args.ToolName, ArgumentsHash: Hash(args.Arguments),
	}
	if err := StartAuthorized(ctx, journal, execution); err != nil {
		t.Fatalf("reserve authorized call: %v", err)
	}
	if _, err := callbacks.RunAfterTool(ctx, &tool.AfterToolArgs{
		ToolCallID: args.ToolCallID, ToolName: args.ToolName,
		Arguments: args.Arguments, Result: map[string]any{"ok": true},
	}); err != nil {
		t.Fatalf("after tool: %v", err)
	}
	if err := StartAuthorized(ctx, journal, execution); !errors.Is(err, ErrReplayBlocked) {
		t.Fatalf("replay error=%v", err)
	}
	events := auditWriter.Events()
	if len(events) != 1 || events[0].Decision != "tool_succeeded" ||
		events[0].RequestID != "request-a" {
		t.Fatalf("events=%+v", events)
	}
}
