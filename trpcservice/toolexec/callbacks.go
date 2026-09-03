package toolexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func NewCallbacks(
	journal Journal,
	auditWriter audit.Writer,
	revisionID string,
) *tool.Callbacks {
	if journal == nil {
		return nil
	}
	callbacks := tool.NewCallbacks()
	callbacks.RegisterBeforeTool(func(
		ctx context.Context,
		args *tool.BeforeToolArgs,
	) (*tool.BeforeToolResult, error) {
		invocation, ok := agentcore.InvocationFromContext(ctx)
		if !ok || invocation == nil || invocation.Session == nil || args == nil {
			return nil, errors.New("tool execution context is incomplete")
		}
		tenantID, _, err := runtimecontext.ParseStorageScope(invocation.RunOptions.AppName)
		if err != nil {
			return nil, err
		}
		started, err := journal.Start(ctx, Execution{
			TenantID: tenantID, RequestID: invocation.RunOptions.RequestID,
			RevisionID: revisionID, ToolCallID: args.ToolCallID,
			ToolName: args.ToolName, ArgumentsHash: Hash(args.Arguments),
		})
		if err != nil {
			return nil, err
		}
		if started.Existing {
			return nil, fmt.Errorf("%w: status=%s", ErrReplayBlocked, started.Execution.Status)
		}
		return &tool.BeforeToolResult{}, nil
	})
	callbacks.RegisterAfterTool(func(
		ctx context.Context,
		args *tool.AfterToolArgs,
	) (*tool.AfterToolResult, error) {
		invocation, ok := agentcore.InvocationFromContext(ctx)
		if !ok || invocation == nil || invocation.Session == nil || args == nil {
			return nil, errors.New("tool execution context is incomplete")
		}
		status, errorType := StatusSucceeded, ""
		if args.Error != nil {
			status, errorType = StatusFailed, "tool_error"
		}
		resultJSON, _ := json.Marshal(args.Result)
		executionID := StableID(invocation.RunOptions.RequestID, args.ToolCallID)
		if err := journal.Complete(ctx, executionID, status, Hash(resultJSON), errorType); err != nil {
			return nil, err
		}
		if auditWriter != nil {
			tenantID, _, _ := runtimecontext.ParseStorageScope(invocation.RunOptions.AppName)
			if err := auditWriter.Record(ctx, audit.Event{
				TenantID: tenantID, UserID: invocation.Session.UserID,
				SessionID: invocation.Session.ID, RequestID: invocation.RunOptions.RequestID,
				RevisionID: revisionID, ToolName: args.ToolName,
				Decision: "tool_" + status, ErrorType: errorType,
				TraceID: audit.TraceID(ctx),
				Details: map[string]any{
					"tool_call_id":   args.ToolCallID,
					"arguments_hash": Hash(args.Arguments),
					"result_hash":    Hash(resultJSON),
				},
			}); err != nil {
				return nil, err
			}
		}
		return &tool.AfterToolResult{}, nil
	})
	return callbacks
}
