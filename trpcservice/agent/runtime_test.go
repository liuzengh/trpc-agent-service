package agent

import (
	"context"
	"strings"
	"testing"
)

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
