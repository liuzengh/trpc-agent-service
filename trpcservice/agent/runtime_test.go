package agent

import (
	"context"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestCollectChatResultAggregatesUsageOncePerModelResponse(t *testing.T) {
	events := make(chan *event.Event, 3)
	usage := &model.Usage{PromptTokens: 12, CompletionTokens: 5, TotalTokens: 17}
	response := &model.Response{
		ID: "response-1", Usage: usage,
		Choices: []model.Choice{{Message: model.Message{
			Role: model.RoleAssistant, Content: "done",
		}}},
	}
	events <- &event.Event{InvocationID: "invocation-1", Response: response}
	events <- &event.Event{InvocationID: "invocation-1", Response: response.Clone()}
	events <- &event.Event{
		InvocationID: "invocation-2",
		Response: &model.Response{
			ID: "response-2", Usage: &model.Usage{PromptTokens: 3, CompletionTokens: 2},
		},
	}
	close(events)
	result, err := collectChatResult(context.Background(), events)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if result.PromptTokens != 15 || result.CompletionTokens != 7 {
		t.Fatalf("result=%+v", result)
	}
}

func TestRuntimeRemembersNameWithinSession(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	})

	first, err := runtime.Chat(
		context.Background(),
		"alice",
		"getting-started",
		"我叫小明。",
	)
	if err != nil {
		t.Fatalf("first chat turn: %v", err)
	}
	if !strings.Contains(first.Reply, "小明") {
		t.Fatalf("first reply %q does not contain remembered name", first.Reply)
	}
	if first.RequestID == "" {
		t.Fatal("first request ID is empty")
	}
	if first.EventCount == 0 {
		t.Fatal("first event count is zero")
	}

	second, err := runtime.Chat(
		context.Background(),
		"alice",
		"getting-started",
		"我叫什么？",
	)
	if err != nil {
		t.Fatalf("second chat turn: %v", err)
	}
	if !strings.Contains(second.Reply, "你叫小明") {
		t.Fatalf("second reply %q did not use session history", second.Reply)
	}
	if second.RequestID == first.RequestID {
		t.Fatal("two turns unexpectedly share one request ID")
	}

	third, err := runtime.Chat(
		context.Background(),
		"alice",
		"getting-started",
		"我叫什么？",
	)
	if err != nil {
		t.Fatalf("third chat turn: %v", err)
	}
	if !strings.Contains(third.Reply, "你叫小明") {
		t.Fatalf("third reply %q treated the previous question as a name", third.Reply)
	}
}

func TestRuntimeSeparatesSessions(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	})

	if _, err := runtime.Chat(
		context.Background(),
		"alice",
		"session-a",
		"我叫小明。",
	); err != nil {
		t.Fatalf("store name in first session: %v", err)
	}

	result, err := runtime.Chat(
		context.Background(),
		"alice",
		"session-b",
		"我叫什么？",
	)
	if err != nil {
		t.Fatalf("chat in second session: %v", err)
	}
	if strings.Contains(result.Reply, "你叫小明") {
		t.Fatalf("reply %q leaked history from another session", result.Reply)
	}
}

func TestRuntimeValidatesInput(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	})

	tests := []struct {
		name      string
		userID    string
		sessionID string
		message   string
	}{
		{name: "missing user", sessionID: "s", message: "hello"},
		{name: "missing session", userID: "u", message: "hello"},
		{name: "missing message", userID: "u", sessionID: "s"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := runtime.Chat(
				context.Background(),
				test.userID,
				test.sessionID,
				test.message,
			); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestNewRuntimeRequiresModel(t *testing.T) {
	if _, err := NewRuntime(nil, false); err == nil {
		t.Fatal("expected nil model error")
	}
}

func TestNewRuntimeWithSessionRequiresSessionService(t *testing.T) {
	if _, err := NewRuntimeWithSession(NewTutorialModel(), nil, false); err == nil {
		t.Fatal("expected nil session service error")
	}
}

func TestRuntimeReady(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	})
	if err := runtime.Ready(context.Background()); err != nil {
		t.Fatalf("runtime readiness: %v", err)
	}

	var nilRuntime *Runtime
	if err := nilRuntime.Ready(context.Background()); err == nil {
		t.Fatal("expected nil runtime readiness error")
	}
}
