package tool

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

type WorkItemInput struct {
	BusinessKey string `json:"business_key" jsonschema:"description=Stable user-provided business identifier. Reuse it on retry. Never invent or change it to bypass a conflict."`
	Title       string `json:"title" jsonschema:"description=Work item title, at most 512 bytes"`
}

// managedTool marks platform code whose replay goes through business-level
// idempotency. Tenants cannot enable this marker using revision JSON.
type managedTool struct{ agenttool.CallableTool }

func (managedTool) ManagedSideEffect() bool { return true }

func NewWorkItemTool(operations *toolexec.Operations) agenttool.Tool {
	return managedTool{function.NewFunctionTool(func(ctx context.Context, input WorkItemInput) (toolexec.WorkItemReceipt, error) {
		invocation, ok := agentcore.InvocationFromContext(ctx)
		callID, hasCallID := agenttool.ToolCallIDFromContext(ctx)
		if operations == nil || !ok || invocation == nil || invocation.Session == nil || !hasCallID {
			return toolexec.WorkItemReceipt{}, toolexec.ErrOperationRejected
		}
		tenantID, appID, err := runtimecontext.ParseStorageScope(invocation.RunOptions.AppName)
		if err != nil {
			return toolexec.WorkItemReceipt{}, err
		}
		title := strings.TrimSpace(input.Title)
		if title == "" || len(title) > 512 {
			return toolexec.WorkItemReceipt{}, toolexec.ErrOperationRejected
		}
		payload, _ := json.Marshal(toolexec.WorkItemPayload{Title: title})
		op, err := operations.Execute(ctx, toolexec.OperationInput{
			TenantID: tenantID, AppID: appID, UserID: invocation.Session.UserID,
			ToolName: toolexec.WorkItemTool, BusinessKey: input.BusinessKey, Payload: payload,
			RequestID: invocation.RunOptions.RequestID, ExecutionID: toolexec.StableID(invocation.RunOptions.RequestID, callID),
		})
		if err != nil {
			return toolexec.WorkItemReceipt{OperationID: op.ID}, err
		}
		var receipt toolexec.WorkItemReceipt
		if err := json.Unmarshal(op.Result, &receipt); err != nil {
			return receipt, toolexec.ErrOperationUnknown
		}
		return receipt, nil
	}, function.WithName(toolexec.WorkItemTool), function.WithDescription("Create a platform-local work item after explicit approval. Requires a stable business_key supplied by the user; retries reuse the same key and title. Not an external ticketing integration."))}
}

func (c *Catalog) IsManagedSideEffect(name string) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	item, ok := c.tools[name].(interface{ ManagedSideEffect() bool })
	return ok && item.ManagedSideEffect()
}
