package governance

import (
	"context"
	"fmt"
	"sync"
)

const (
	CodeToolBudgetExceeded = "tool_budget_exceeded"
	CodeApprovalRequired   = "approval_required"
	CodeSandboxUnavailable = "sandbox_unavailable"
)

func permissionCode(reason string) string {
	switch reason {
	case "tool call budget exceeded":
		return CodeToolBudgetExceeded
	case "explicit user approval is required":
		return CodeApprovalRequired
	case "user is not allowed to call tools":
		return "tool_user_denied"
	case "tool is not allowed by the Agent revision":
		return "tool_not_allowed"
	case "invalid tool permission request":
		return "tool_request_invalid"
	default:
		return ""
	}
}

type feedbackKey struct{}

// Feedback keeps at most one deterministic platform notice per run, regardless
// of how many times a model retries. It is not a replacement for the journal.
type Feedback struct {
	mu    sync.Mutex
	first *ToolDecision
}

func (f *Feedback) Code() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.first != nil {
		return f.first.Code
	}
	return ""
}

func WithFeedback(ctx context.Context) (context.Context, *Feedback) {
	f := &Feedback{}
	return context.WithValue(ctx, feedbackKey{}, f), f
}

func recordFeedback(ctx context.Context, decision ToolDecision) {
	if decision.Code != CodeToolBudgetExceeded && decision.Code != CodeSandboxUnavailable {
		return
	}
	f, _ := ctx.Value(feedbackKey{}).(*Feedback)
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.first == nil {
		copy := decision
		f.first = &copy
	}
}

// RecordToolFailure accepts stable categories only, never raw error text.
func RecordToolFailure(ctx context.Context, toolName, code string) {
	recordFeedback(ctx, ToolDecision{ToolName: toolName, Code: code})
}

// Reply replaces ambiguous model prose for known pre-execution failures. The
// result is cached by Runtime, so duplicate deliveries get the same notice.
func (f *Feedback) Reply(original, requestID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.first == nil {
		return original
	}
	d := f.first
	if d.Code == CodeToolBudgetExceeded {
		return fmt.Sprintf("平台提示：工具 %s 的本次调用已被拒绝，未进入执行。每次 Agent 执行的工具调用上限为 %d，本次是第 %d 次尝试。这是版本配置限制，不会通过等待自动恢复。\n错误码：%s\n请求编号：%s\n请管理员检查工具预算并按需发布新版本。此前其他工具的执行不会因此回滚，请先核对执行记录，不要盲目重试。", d.ToolName, d.CallLimit, d.CallsUsed, d.Code, requestID)
	}
	return fmt.Sprintf("平台提示：沙箱执行未启用，本次工具 %s 未开始执行。\n错误码：%s\n请求编号：%s\n请部署者检查 Worker 的沙箱配置。此前其他工具的执行不会因此回滚，请先核对记录。", d.ToolName, d.Code, requestID)
}
