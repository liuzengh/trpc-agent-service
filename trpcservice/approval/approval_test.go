package approval

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

func TestMemoryApprovalLifecycleAndIdentityIsolation(t *testing.T) {
	repository := NewMemoryRepository()
	request := Request{
		TenantID: "tenant-a", AppID: "app-a", RevisionID: "revision-a",
		ChannelBindingID: "binding-a", RequestID: "request-a", MessageID: "message-a",
		UserID: "user-a", SessionID: "session-a", ToolCallID: "call-a", ToolName: "dangerous",
		ArgumentsHash: ArgumentsHash([]byte(`{"value":1}`)), ResumeText: "do it",
		ReplyTarget: "user-a", ExpiresAt: time.Now().Add(time.Minute),
	}
	record, err := repository.Request(context.Background(), request)
	if err != nil || record.Status != StatusPending || record.ApprovalID == "" {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if _, err := repository.Decide(context.Background(), Decision{
		ApprovalID: record.ApprovalID, TenantID: "tenant-a", ChannelBindingID: "binding-a",
		UserID: "attacker", ExternalMessageID: "decision-1", Status: StatusApproved,
	}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("identity error=%v", err)
	}
	decided, err := repository.Decide(context.Background(), Decision{
		ApprovalID: record.ApprovalID, TenantID: "tenant-a", ChannelBindingID: "binding-a",
		UserID: "user-a", ExternalMessageID: "decision-1", Status: StatusApproved,
	})
	if err != nil || decided.Status != StatusApproved {
		t.Fatalf("decided=%+v err=%v", decided, err)
	}
	duplicate, err := repository.Decide(context.Background(), Decision{
		ApprovalID: record.ApprovalID, TenantID: "tenant-a", ChannelBindingID: "binding-a",
		UserID: "user-a", ExternalMessageID: "decision-2", Status: StatusApproved,
	})
	if err != nil || duplicate.DecisionMessageID != "decision-1" {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	if err := repository.MarkResumed(context.Background(), record.ApprovalID); err != nil {
		t.Fatalf("mark resumed: %v", err)
	}
}

func TestServiceApprovesAndEnqueuesTrustedContinuation(t *testing.T) {
	repository := NewMemoryRepository()
	journal := gateway.NewMemoryJournal()
	record, err := repository.Request(context.Background(), Request{
		TenantID: "tenant-a", AppID: "app-a", RevisionID: "revision-a",
		ChannelBindingID: "binding-a", RequestID: "request-a", MessageID: "message-a",
		UserID: "user-a", SessionID: "session-a", ToolCallID: "call-a", ToolName: "dangerous",
		ArgumentsHash: ArgumentsHash([]byte(`{"value":1}`)), ResumeText: "do it",
		ReplyTarget: "user-a", ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	service, err := NewService(repository, journal, nil)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	handled, err := service.HandleApprovalDecision(context.Background(), gateway.ApprovalDecisionInput{
		TenantID: "tenant-a", ChannelType: "telegram", ChannelBindingID: "binding-a",
		ExternalMessageID: "decision-a", UserID: "user-a", SessionID: "session-a",
		ChatType: "direct", Text: "批准 " + record.ApprovalID, ReplyTarget: "user-a",
	})
	if err != nil || !handled {
		t.Fatalf("handled=%t err=%v", handled, err)
	}
	tasks := journal.Tasks()
	if len(tasks) != 1 || len(tasks[0].ApprovedToolCalls) != 1 ||
		tasks[0].ApprovedToolCalls[0].ToolName != "dangerous" ||
		tasks[0].ApprovedToolCalls[0].ArgumentsHash != record.ArgumentsHash ||
		tasks[0].ApprovalID != record.ApprovalID {
		t.Fatalf("tasks=%+v", tasks)
	}
}

func TestParseDecisionCommand(t *testing.T) {
	id := "apr_0123456789abcdef0123456789abcdef"
	status, parsedID, ok := ParseDecisionCommand("approve " + id)
	if !ok || status != StatusApproved || parsedID != id {
		t.Fatalf("status=%q id=%q ok=%t", status, parsedID, ok)
	}
	if _, _, ok := ParseDecisionCommand("please approve " + id); ok {
		t.Fatal("free-form text must not be accepted as an approval command")
	}
}
