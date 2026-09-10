package assembly

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestTenantGuardrailDoesNotConsumeCompletedToolResult(t *testing.T) {
	guard, err := tenantGuardrailPlugin(config.TenantConfig{
		Tools: config.ToolPolicy{RequireConfirmation: []string{"request_refund"}},
	}, nil)
	if err != nil {
		t.Fatalf("tenantGuardrailPlugin() error = %v", err)
	}
	manager, err := plugin.NewManager(guard)
	if err != nil {
		t.Fatalf("plugin.NewManager() error = %v", err)
	}
	callbacks := manager.ToolCallbacks()
	if callbacks == nil || len(callbacks.AfterTool) == 0 {
		t.Fatal("guardrail AfterTool callback is missing")
	}
	result, err := callbacks.RunAfterTool(context.Background(), &agenttool.AfterToolArgs{
		ToolName: "request_refund",
		Result:   map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatalf("RunAfterTool() error = %v", err)
	}
	if result == nil {
		t.Fatal("RunAfterTool() result = nil")
	}
	if result.CustomResult != nil {
		t.Fatalf("RunAfterTool() CustomResult = %#v, want nil pass-through", result.CustomResult)
	}
}
