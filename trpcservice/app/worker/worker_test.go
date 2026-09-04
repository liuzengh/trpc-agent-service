package worker

import (
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func finalTextEvents(resps ...*model.Response) <-chan *event.Event {
	ch := make(chan *event.Event, len(resps))
	for _, r := range resps {
		ch <- &event.Event{Response: r}
	}
	close(ch)
	return ch
}

func TestFinalTextPicksLastAssistantContent(t *testing.T) {
	events := finalTextEvents(
		&model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: "partial"}}}, IsPartial: true},
		&model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: "final answer"}}}},
	)
	got, _, _, _, _, err := finalTextWithUsage(events)
	if err != nil {
		t.Fatalf("finalText: %v", err)
	}
	if got != "final answer" {
		t.Errorf("finalText = %q, want %q", got, "final answer")
	}
}

func TestFinalTextIgnoresToolCallsAndUserTurns(t *testing.T) {
	events := finalTextEvents(
		&model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "t1"}}},
		}}},
	)
	got, _, _, _, _, err := finalTextWithUsage(events)
	if err != nil {
		t.Fatalf("finalText: %v", err)
	}
	if got != "" {
		t.Errorf("finalText = %q, want empty", got)
	}
}

func TestFinalTextPropagatesError(t *testing.T) {
	events := finalTextEvents(
		&model.Response{Error: &model.ResponseError{Message: "boom"}},
	)
	if _, _, _, _, _, err := finalTextWithUsage(events); err == nil {
		t.Error("finalText should return the event error")
	}
}

func TestFinalTextCollectsToolNames(t *testing.T) {
	events := finalTextEvents(
		&model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{
				{Function: model.FunctionDefinitionParam{Name: "get_current_time"}},
				{Function: model.FunctionDefinitionParam{Name: "echo"}},
				{Function: model.FunctionDefinitionParam{Name: "get_current_time"}}, // dup
			}},
		}}},
	)
	_, _, names, _, _, err := finalTextWithUsage(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "get_current_time" || names[1] != "echo" {
		t.Errorf("tool names = %v, want [get_current_time echo] (deduped, ordered)", names)
	}
}

func TestFinalTextCollectsToolNamesFromDelta(t *testing.T) {
	// Streaming adapters deliver tool calls as Delta.ToolCalls on partial
	// events. Regression: the meter only read Message.ToolCalls, so a
	// streaming model's calls never reached the tool usage dimension.
	events := finalTextEvents(
		&model.Response{IsPartial: true, Choices: []model.Choice{{
			Delta: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{
				{Function: model.FunctionDefinitionParam{Name: "execute_code"}},
			}},
		}}},
	)
	_, _, names, _, toolCalls, err := finalTextWithUsage(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "execute_code" {
		t.Errorf("tool names = %v, want [execute_code]", names)
	}
	if toolCalls["execute_code"] != 1 {
		t.Errorf("toolCalls = %v, want execute_code counted once", toolCalls)
	}
}
