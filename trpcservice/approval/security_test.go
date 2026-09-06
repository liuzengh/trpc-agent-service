package approval

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

func approvalFixture() Request {
	return Request{
		TenantID: "tutorial-tenant", AppID: "tutorial-app", RevisionID: "tutorial-revision-1",
		ChannelBindingID: "tutorial-http-binding", RequestID: "test-request", MessageID: "test-message",
		UserID: "alice", SessionID: "topic-a", ToolCallID: "call-1", ToolName: "dangerous_demo",
		ArgumentsHash: ArgumentsHash([]byte(`{"action":"demo"}`)), ResumeText: "demo",
		ReplyTarget: "alice", ExpiresAt: time.Now().Add(time.Minute),
	}
}

func decisionFor(record Record) Decision {
	return Decision{
		ApprovalID: record.ApprovalID, TenantID: record.TenantID,
		ChannelBindingID: record.ChannelBindingID, UserID: record.UserID, SessionID: record.SessionID,
		ExternalMessageID: "decision-" + record.ToolCallID, Status: StatusApproved,
	}
}

func TestMemoryDecisionContract(t *testing.T) {
	repo := NewMemoryRepository()
	t.Cleanup(func() { _ = repo.Close() })
	testDecisionContract(t, repo, approvalFixture())
}

// Both backends run the same contract, including states reached by earlier
// retries and the unique decision-message constraint.
func testDecisionContract(t *testing.T, repo Repository, base Request) {
	t.Helper()
	ctx := context.Background()
	request := func(name string, expired bool) Record {
		t.Helper()
		input := base
		input.ToolCallID = name
		if expired {
			input.ExpiresAt = time.Now().Add(-time.Minute)
		}
		record, err := repo.Request(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	for _, field := range []string{"tenant", "binding", "user", "session", "missing_session"} {
		t.Run(field, func(t *testing.T) {
			record := request(field, false)
			decision := decisionFor(record)
			switch field {
			case "tenant":
				decision.TenantID = "other-tenant"
			case "binding":
				decision.ChannelBindingID = "other-binding"
			case "user":
				decision.UserID = "bob"
			case "session":
				decision.SessionID = "other-topic"
			case "missing_session":
				decision.SessionID = ""
			}
			if _, err := repo.Decide(ctx, decision); !errors.Is(err, ErrForbidden) {
				t.Fatalf("scope bypass: %v", err)
			}
			// The rejected attempt must not consume the legitimate decision.
			if _, err := repo.Decide(ctx, decisionFor(record)); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, status := range []string{StatusApproved, StatusDenied} {
		t.Run(status, func(t *testing.T) {
			record := request(status, false)
			decision := decisionFor(record)
			decision.Status = status
			first, err := repo.Decide(ctx, decision)
			if err != nil || first.Status != status {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			decision.ExternalMessageID += "-duplicate"
			second, err := repo.Decide(ctx, decision)
			if err != nil || second.DecisionMessageID != first.DecisionMessageID {
				t.Fatalf("duplicate created a new decision: %+v err=%v", second, err)
			}
			decision.Status = StatusDenied
			if status == StatusDenied {
				decision.Status = StatusApproved
			}
			if _, err := repo.Decide(ctx, decision); !errors.Is(err, ErrConflict) {
				t.Fatalf("reverse=%v", err)
			}
		})
	}
	t.Run("expired_and_replayed", func(t *testing.T) {
		record := request("expired", true)
		for i := 0; i < 2; i++ {
			if _, err := repo.Decide(ctx, decisionFor(record)); !errors.Is(err, ErrExpired) {
				t.Fatalf("expiry attempt %d: %v", i, err)
			}
		}
	})
	t.Run("decision_message_cannot_decide_two_approvals", func(t *testing.T) {
		first, second := request("unique-a", false), request("unique-b", false)
		decision := decisionFor(first)
		if _, err := repo.Decide(ctx, decision); err != nil {
			t.Fatal(err)
		}
		decision.ApprovalID = second.ApprovalID
		if _, err := repo.Decide(ctx, decision); !errors.Is(err, ErrConflict) {
			t.Fatalf("reused message: %v", err)
		}
		if _, err := repo.Decide(ctx, decisionFor(second)); err != nil {
			t.Fatalf("valid decision after conflict: %v", err)
		}
	})
	t.Run("pending_session_scope", func(t *testing.T) {
		record := request("pending-session", false)
		request("expired-session", true)
		pending, err := repo.ListPendingBySession(ctx, record.TenantID, record.ChannelBindingID, record.UserID, record.SessionID)
		if err != nil || len(pending) != 1 || pending[0].ApprovalID != record.ApprovalID {
			t.Fatalf("pending=%+v err=%v", pending, err)
		}
		for _, field := range []string{"tenant", "binding", "user", "session"} {
			identity := []string{record.TenantID, record.ChannelBindingID, record.UserID, record.SessionID}
			for i, name := range []string{"tenant", "binding", "user", "session"} {
				if name == field {
					identity[i] = "other"
				}
			}
			pending, err := repo.ListPendingBySession(ctx, identity[0], identity[1], identity[2], identity[3])
			if err != nil || len(pending) != 0 {
				t.Fatalf("pending scope leak: %s", field)
			}
		}
	})
}

func TestServiceRejectsPermanentDecisionsWithoutEnqueue(t *testing.T) {
	for _, name := range []string{"other_user", "other_session", "expired", "not_found", "conflict"} {
		t.Run(name, func(t *testing.T) {
			repo := NewMemoryRepository()
			journal := gateway.NewMemoryJournal()
			writer := audit.NewMemoryWriter()
			input := approvalFixture()
			if name == "expired" {
				input.ExpiresAt = time.Now().Add(-time.Minute)
			}
			record, err := repo.Request(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			decision := gateway.ApprovalDecisionInput{
				TenantID: input.TenantID, ChannelType: "telegram", ChannelBindingID: input.ChannelBindingID,
				UserID: input.UserID, SessionID: input.SessionID, ChatType: "group",
				ExternalMessageID: "decision", Text: "批准 " + record.ApprovalID,
			}
			decision.Scope, err = runtimecontext.NewScope(input.TenantID, input.AppID, input.RevisionID, decision.ChannelType, input.ChannelBindingID)
			if err != nil {
				t.Fatal(err)
			}
			wantError := "approval_forbidden"
			switch name {
			case "other_user":
				decision.UserID = "bob"
			case "other_session":
				decision.SessionID = "other-topic"
			case "expired":
				wantError = "approval_expired"
			case "not_found":
				decision.Text = "批准 apr_00000000000000000000000000000000"
				wantError = "approval_not_found"
			case "conflict":
				denied := decisionFor(record)
				denied.Status = StatusDenied
				if _, err := repo.Decide(context.Background(), denied); err != nil {
					t.Fatal(err)
				}
				wantError = "approval_conflict"
			}
			service, err := NewService(repo, journal, writer)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				handled, err := service.HandleApprovalDecision(context.Background(), decision)
				if !handled || err != nil || len(journal.Tasks()) != 0 {
					t.Fatalf("rejection must ACK without Agent task: handled=%t err=%v", handled, err)
				}
			}
			for _, event := range writer.Events() {
				if event.Decision != "approval_rejected" || event.ErrorType != wantError {
					t.Fatalf("missing rejection audit: %+v", event)
				}
			}
			if len(writer.Events()) != 2 {
				t.Fatal("rejection audit missing")
			}
		})
	}
}

func TestServiceContinuationIsIdempotentAndAudited(t *testing.T) {
	for _, command := range []string{"批准", "拒绝"} {
		t.Run(command, func(t *testing.T) {
			repo := NewMemoryRepository()
			journal := gateway.NewMemoryJournal()
			writer := audit.NewMemoryWriter()
			record, err := repo.Request(context.Background(), approvalFixture())
			if err != nil {
				t.Fatal(err)
			}
			service, err := NewService(repo, journal, writer)
			if err != nil {
				t.Fatal(err)
			}
			input := gateway.ApprovalDecisionInput{
				TenantID: record.TenantID, ChannelType: "telegram", ChannelBindingID: record.ChannelBindingID,
				UserID: record.UserID, SessionID: record.SessionID, ChatType: "group",
				ExternalMessageID: "decision", Text: command + " " + record.ApprovalID,
			}
			for i := 0; i < 3; i++ {
				if i == 2 {
					input.ExternalMessageID = "second-user-message"
				}
				if handled, err := service.HandleApprovalDecision(context.Background(), input); !handled || err != nil {
					t.Fatalf("handled=%t err=%v", handled, err)
				}
			}
			tasks := journal.Tasks()
			continuationID := ""
			if command == "批准" {
				if len(tasks) != 1 {
					t.Fatalf("duplicate continuation: %+v", tasks)
				}
				continuationID = tasks[0].RequestID
				if len(tasks[0].ApprovedToolCalls) != 1 || tasks[0].ApprovedToolCalls[0].ArgumentsHash != record.ArgumentsHash {
					t.Fatalf("approval scope lost: %+v", tasks[0])
				}
			} else if len(tasks) != 0 {
				t.Fatalf("denial must not invoke an Agent: %+v", tasks)
			}
			receiptID := ""
			for i, event := range writer.Events() {
				if event.Details["continuation_request_id"] != continuationID || event.Details["duplicate"] != (i > 0) {
					t.Fatalf("audit continuation link lost: %+v", event)
				}
				id, _ := event.Details["receipt_request_id"].(string)
				if id == "" || (i > 0 && id != receiptID) {
					t.Fatal("receipt is missing or not idempotent")
				}
				receiptID = id
			}
			status, reply, ok := journal.RunStatus(receiptID)
			if !ok || status != "completed" || !strings.HasPrefix(reply.Reply, "平台确认：") {
				t.Fatalf("receipt=%+v", reply)
			}
		})
	}
}

type unavailableRepository struct{ Repository }

func (r unavailableRepository) Decide(context.Context, Decision) (Record, error) {
	return Record{}, errors.New("database unavailable")
}

func TestServiceDoesNotAcknowledgeTransientFailure(t *testing.T) {
	journal := gateway.NewMemoryJournal()
	service, _ := NewService(unavailableRepository{}, journal, nil)
	handled, err := service.HandleApprovalDecision(context.Background(), gateway.ApprovalDecisionInput{
		Text: "批准 apr_00000000000000000000000000000000",
	})
	if !handled || err == nil || len(journal.Tasks()) != 0 {
		t.Fatalf("handled=%t err=%v", handled, err)
	}
}

func TestDecisionCommandMustBeExact(t *testing.T) {
	for _, text := range []string{
		"批准 apr_zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		"@some_bot 批准 apr_00000000000000000000000000000000",
		"请批准 apr_00000000000000000000000000000000",
		"批准 apr_00000000000000000000000000000000 extra",
	} {
		if _, _, ok := ParseDecisionCommand(text); ok {
			t.Fatalf("accepted %q", text)
		}
	}
}
