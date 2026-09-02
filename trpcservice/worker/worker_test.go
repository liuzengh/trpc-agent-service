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
	got, _, err := finalTextWithUsage(events)
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
	got, _, err := finalTextWithUsage(events)
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
	if _, _, err := finalTextWithUsage(events); err == nil {
		t.Error("finalText should return the event error")
	}
}
