package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

func TestRuntimeIsolatesTenantStorageScopes(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() { _ = runtime.Close() })
	scopeA, err := runtimecontext.NewScope(
		"tenant-a", "app-a", "revision-a", "http", "binding-a",
	)
	if err != nil {
		t.Fatalf("create scope A: %v", err)
	}
	scopeB, err := runtimecontext.NewScope(
		"tenant-b", "app-b", "revision-b", "http", "binding-b",
	)
	if err != nil {
		t.Fatalf("create scope B: %v", err)
	}
	if _, err := runtime.ChatWithScope(context.Background(), ChatInput{
		Scope:     scopeA,
		MessageID: "scope-message-a1",
		UserID:    "same-user",
		SessionID: "same-session",
		Text:      "我叫小明。",
	}); err != nil {
		t.Fatalf("chat in scope A: %v", err)
	}
	result, err := runtime.ChatWithScope(context.Background(), ChatInput{
		Scope:     scopeB,
		MessageID: "scope-message-b1",
		UserID:    "same-user",
		SessionID: "same-session",
		Text:      "我叫什么？",
	})
	if err != nil {
		t.Fatalf("chat in scope B: %v", err)
	}
	if strings.Contains(result.Reply, "你叫小明") {
		t.Fatalf("cross-tenant Session leaked: %q", result.Reply)
	}
	if result.TenantID != "tenant-b" || result.AppID != "app-b" {
		t.Fatalf("result scope = %+v", result)
	}
}

func TestRuntimeRejectsForgedStorageScope(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() { _ = runtime.Close() })
	scope := runtimecontext.TutorialScope()
	scope.StorageScope = "t/other-tenant/a/tutorial-app"
	if _, err := runtime.ChatWithScope(context.Background(), ChatInput{
		Scope:     scope,
		MessageID: "forged-scope",
		UserID:    "alice",
		SessionID: "session",
		Text:      "hello",
	}); err == nil {
		t.Fatal("expected forged scope error")
	}
}
