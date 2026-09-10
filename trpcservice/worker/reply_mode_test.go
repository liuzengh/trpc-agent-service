package worker

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestBuildReplyEventForModeIgnoresNonTerminalError(t *testing.T) {
	exec := replyModeTestExecution()
	nonTerminal := &event.Event{
		RequestID: exec.RequestID,
		Response: &model.Response{
			Object: model.ObjectTypeChatCompletionChunk,
			Error:  &model.ResponseError{Message: "recoverable graph node error"},
		},
	}
	if nonTerminal.IsTerminalError() {
		t.Fatal("test event is terminal")
	}
	replies, err := BuildReplyEventForMode(context.Background(), exec, 1, nonTerminal, ReplyModeStream)
	if err != nil {
		t.Fatalf("build non-terminal error reply: %v", err)
	}
	if len(replies) != 0 {
		t.Fatalf("non-terminal error replies = %#v, want none", replies)
	}
}

func TestBuildReplyEventForModeProjectsOrderedStreamFrames(t *testing.T) {
	exec := replyModeTestExecution()
	partial := &event.Event{
		RequestID: "request-1",
		Response: &model.Response{
			Object:    model.ObjectTypeChatCompletionChunk,
			IsPartial: true,
			Choices: []model.Choice{{Delta: model.Message{
				Role:    model.RoleAssistant,
				Content: "hello",
			}}},
		},
	}
	replies, err := BuildReplyEventForMode(context.Background(), exec, 1, partial, ReplyModeStream)
	if err != nil {
		t.Fatalf("build first stream frame: %v", err)
	}
	if len(replies) != 1 || replies[0].ReplyKind() != "stream" ||
		replies[0].StreamPhase != "update" || replies[0].Text != "hello" || replies[0].StreamSequence != 1 {
		t.Fatalf("first stream frame = %#v", replies)
	}

	partial.Response.Choices[0].Delta.Content = " world"
	replies, err = BuildReplyEventForMode(context.Background(), exec, 2, partial, ReplyModeStream)
	if err != nil {
		t.Fatalf("build second stream frame: %v", err)
	}
	if len(replies) != 1 || replies[0].Text != " world" || replies[0].StreamSequence != 2 {
		t.Fatalf("second stream frame = %#v", replies)
	}

	completion := &event.Event{
		RequestID: "request-1",
		Response: &model.Response{
			Object: model.ObjectTypeRunnerCompletion,
			Done:   true,
			Choices: []model.Choice{{Message: model.Message{
				Role:    model.RoleAssistant,
				Content: "hello world",
			}}},
		},
	}
	replies, err = BuildReplyEventForMode(context.Background(), exec, 3, completion, ReplyModeStream)
	if err != nil {
		t.Fatalf("build terminal stream frame: %v", err)
	}
	if len(replies) != 1 || replies[0].StreamPhase != "end" || replies[0].Text != "hello world" {
		t.Fatalf("terminal stream frame = %#v", replies)
	}
}

func TestBuildReplyEventForModeProjectsNativeCard(t *testing.T) {
	replies, err := BuildReplyEventForMode(
		context.Background(),
		replyModeTestExecution(),
		1,
		&event.Event{
			RequestID: "request-1",
			Response: &model.Response{
				Object: model.ObjectTypeRunnerCompletion,
				Done:   true,
				Choices: []model.Choice{{Message: model.Message{
					Role:    model.RoleAssistant,
					Content: "answer",
				}}},
			},
		},
		ReplyModeCard,
	)
	if err != nil {
		t.Fatalf("build card reply: %v", err)
	}
	if len(replies) != 1 || replies[0].ReplyKind() != "card" || replies[0].Card == nil {
		t.Fatalf("card reply = %#v", replies)
	}
	if err := replies[0].Validate(); err != nil {
		t.Fatalf("validate card reply: %v", err)
	}
}

func TestBuildReplyEventForModeSkipsEmptyOrdinaryCompletion(t *testing.T) {
	replies, err := BuildReplyEventForMode(
		context.Background(),
		replyModeTestExecution(),
		1,
		&event.Event{
			RequestID: "request-1",
			Response: &model.Response{
				Object: model.ObjectTypeRunnerCompletion,
				Done:   true,
			},
		},
		ReplyModeText,
	)
	if err != nil {
		t.Fatalf("build empty ordinary completion: %v", err)
	}
	if len(replies) != 0 {
		t.Fatalf("empty ordinary completion replies = %#v, want none", replies)
	}
}

func TestBuildReplyEventForModeSkipsOrdinaryCompletionInTextMode(t *testing.T) {
	replies, err := BuildReplyEventForMode(
		context.Background(),
		replyModeTestExecution(),
		2,
		&event.Event{
			RequestID: "request-1",
			Response: &model.Response{
				Object: model.ObjectTypeChatCompletion,
				Done:   true,
				Choices: []model.Choice{{Message: model.Message{
					Role:    model.RoleAssistant,
					Content: "answer",
				}}},
			},
		},
		ReplyModeText,
	)
	if err != nil {
		t.Fatalf("build text ordinary completion: %v", err)
	}
	if len(replies) != 0 {
		t.Fatalf("text ordinary completion replies = %#v, want none", replies)
	}
}

func replyModeTestExecution() Execution {
	return Execution{
		RequestID: "request-1",
		Tenant: tenant.RuntimeContext{
			TenantID:           "tenant-1",
			AppID:              "app-1",
			ConfigVersion:      "v1",
			Channel:            "feishu",
			BindingID:          "binding-1",
			BindingRevision:    1,
			SessionID:          "session-1",
			SessionPrincipalID: "user-1",
			UserID:             "user-1",
			TraceID:            "trace-1",
		},
	}
}
