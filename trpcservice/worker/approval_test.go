package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"

	"trpc.group/trpc-go/trpc-agent-go/model"
	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestClassifyApprovalReply(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"批准", "approve"},
		{" 批准 ", "approve"},
		{"批准执行", "approve"},
		{"同意", "approve"},
		{"approve", "approve"},
		{"Yes", "approve"},
		{"y", "approve"},
		{"拒绝", "deny"},
		{"拒绝执行", "deny"},
		{"不允许", "deny"},
		{"no", "deny"},
		{"DENY", "deny"},
		// ordinary messages are never decisions
		{"今天天气怎么样", ""},
		{"好的，谢谢你", ""},
		{"", ""},
		{"帮我分析一下", ""},
	}
	for _, c := range cases {
		if got := classifyApprovalReply(c.in); got != c.want {
			t.Errorf("classifyApprovalReply(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// echoSource resolves the echo tool id to a FunctionTool (its name must equal
// the definition name for approval policy matching).
func echoSource(id string) (fwtool.Tool, bool) {
	if id == "echo" {
		return tool.EchoTool(), true
	}
	return nil, false
}

func TestToolsFromProfileApprovalRails(t *testing.T) {
	ctx := context.Background()
	reg := tool.NewRegistry()
	// low risk, must be approved by the explicit rail (profile approval ids)
	if err := reg.Register(ctx, tool.Definition{ID: "echo", Name: "echo", Description: "x", RiskLevel: tool.RiskLow}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Grant(ctx, "a1", "echo"); err != nil {
		t.Fatal(err)
	}

	// rail 1: manually listed in the profile -> requires approval
	w := &Worker{tools: reg, toolSrc: echoSource}
	tools, names := w.toolsFromProfile(ctx, "a1", agent.RuntimeProfile{
		ToolIDs:         []string{"echo"},
		ApprovalToolIDs: []string{"echo"},
	})
	if len(tools) != 1 {
		t.Fatalf("tools = %d, want 1", len(tools))
	}
	if !names["echo"] {
		t.Errorf("rail 1: approved tool must be in the approval name set, got %v", names)
	}

	// rail 2: high risk triggers approval even when not listed
	if err := reg.Register(ctx, tool.Definition{ID: "echo", Name: "echo", Description: "x", RiskLevel: tool.RiskHigh}); err != nil {
		t.Fatal(err)
	}
	_, names = w.toolsFromProfile(ctx, "a1", agent.RuntimeProfile{ToolIDs: []string{"echo"}})
	if !names["echo"] {
		t.Errorf("rail 2: high-risk tool must be in the approval name set, got %v", names)
	}

	// low risk and not listed -> no approval
	if err := reg.Register(ctx, tool.Definition{ID: "echo", Name: "echo", Description: "x", RiskLevel: tool.RiskLow}); err != nil {
		t.Fatal(err)
	}
	_, names = w.toolsFromProfile(ctx, "a1", agent.RuntimeProfile{ToolIDs: []string{"echo"}})
	if len(names) != 0 {
		t.Errorf("plain low-risk tool must not require approval, got %v", names)
	}

	// an agent without grants gets no tools at all
	_, names = w.toolsFromProfile(ctx, "nobody", agent.RuntimeProfile{ToolIDs: []string{"echo"}})
	if len(names) != 0 {
		t.Errorf("ungranted agent must see no approval tools, got %v", names)
	}
}

func TestApprovalNoticeMentionsToolAndInstruction(t *testing.T) {
	content := model.NewUserMessage("hi")
	m := &bus.Message{ID: "m-1", TenantID: "t1", SessionID: "s1", UserID: "u1", Content: &content}
	notice := approvalNotice(m, "git_push", `{"remote":"origin"}`)
	if notice == nil || notice.Content == nil {
		t.Fatal("approvalNotice returned nil message")
	}
	txt := notice.Content.Content
	if !strings.Contains(txt, "git_push") {
		t.Errorf("notice should mention the tool: %q", txt)
	}
	if !strings.Contains(txt, "批准") || !strings.Contains(txt, "拒绝") {
		t.Errorf("notice should explain how to reply: %q", txt)
	}
	if notice.ReplyTo != m.ID {
		t.Errorf("notice.ReplyTo = %q, want %q", notice.ReplyTo, m.ID)
	}
}
