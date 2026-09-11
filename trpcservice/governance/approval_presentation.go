package governance

import (
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// ApprovalPromptCard is the channel-neutral confirmation UI for one pending
// side-effecting tool call.
func ApprovalPromptCard(approval PendingApproval) channels.InteractiveCard {
	description := ApprovalOperationSummary(approval.ToolDescription, approval.ToolName)
	return channels.InteractiveCard{
		Title: "确认执行操作",
		Body:  description + "\n\n确认后将执行此操作。",
		State: "pending",
		Actions: []channels.CardAction{
			{ActionID: "approval:approve:" + approval.Token, Label: "确认执行", Style: "primary"},
			{ActionID: "approval:reject:" + approval.Token, Label: "取消", Style: "danger"},
		},
	}
}

func ApprovalResultCard(approval PendingApproval, approved bool) channels.InteractiveCard {
	description := ApprovalOperationSummary(approval.ToolDescription, approval.ToolName)
	if approved {
		return channels.InteractiveCard{Title: "已确认", Body: "已确认：" + description, State: "approved"}
	}
	return channels.InteractiveCard{Title: "已取消", Body: "已取消：" + description, State: "rejected"}
}

func ApprovalOperationSummary(description, toolName string) string {
	text := strings.TrimSpace(description)
	if text == "" {
		if name := strings.TrimSpace(toolName); name != "" {
			return "执行操作：" + name
		}
		return "执行这项操作"
	}
	for _, separator := range []string{"。", "；", "\n", "，这是"} {
		if index := strings.Index(text, separator); index > 0 {
			text = strings.TrimSpace(text[:index])
		}
	}
	runes := []rune(text)
	if len(runes) > 80 {
		text = string(runes[:80]) + "…"
	}
	return text
}

func ParseApprovalAction(actionID string) (token string, approved bool, ok bool) {
	parts := strings.Split(strings.TrimSpace(actionID), ":")
	if len(parts) != 3 || parts[0] != "approval" || strings.TrimSpace(parts[2]) == "" {
		return "", false, false
	}
	switch parts[1] {
	case "approve":
		return parts[2], true, true
	case "reject":
		return parts[2], false, true
	default:
		return "", false, false
	}
}
