package agent

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/secrets"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
)

func TestLiveDeepSeekToolCall(t *testing.T) {
	if os.Getenv("LIVE_E2E") != "1" {
		t.Skip("LIVE_E2E=1 is required for a billable DeepSeek test")
	}
	if os.Getenv("DEEPSEEK_KEY_FILE") == "" {
		t.Fatal("DEEPSEEK_KEY_FILE is required")
	}
	engine := NewTRPCEngine(secrets.FileEnvProvider{})
	defer engine.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result, err := engine.Run(ctx, Request{
		Tenant: tenant.Tenant{
			ID: "live_deepseek", Enabled: true,
			Agent: tenant.AgentProfile{
				ID: "assistant", Version: "live",
				Instruction:   "必须调用 get_server_time 工具获取时间，然后用一句中文回答。",
				ToolAllowlist: []string{"get_server_time"},
			},
			Model: tenant.ModelProfile{
				Provider: "deepseek", BaseURL: "https://api.deepseek.com",
				Model: "deepseek-v4-flash", APIKeyRef: "fileenv:DEEPSEEK_KEY_FILE", Timeout: "40s",
			},
			Backend: tenant.BackendProfile{Session: "memory"},
		},
		UserID: "live-user", SessionID: "live-session", Content: "请告诉我当前服务端时间。", TraceID: "123e4567-e89b-12d3-a456-426614174000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Content == "" {
		t.Fatal("DeepSeek returned an empty response")
	}
	found := false
	for _, toolName := range result.ToolCalls {
		found = found || toolName == "get_server_time"
	}
	if !found {
		t.Fatalf("expected get_server_time tool call, got %+v", result.ToolCalls)
	}
}
