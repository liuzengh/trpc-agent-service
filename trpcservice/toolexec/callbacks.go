package toolexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// StartAuthorized reserves the execution only after the final permission policy
// allows it. In tRPC-Agent-Go v1.11.2 BeforeTool runs BEFORE permission checks,
// and denied/approval-required calls skip AfterTool entirely. Starting there
// would leave a misleading running row for a tool that never executed.
// Identity comes from the live invocation, including generated request IDs.
func StartAuthorized(ctx context.Context, journal Journal, execution Execution, managedReplay ...bool) error {
	if journal == nil {
		return nil
	}
	invocation, ok := agentcore.InvocationFromContext(ctx)
	if !ok || invocation == nil || invocation.Session == nil {
		return errors.New("tool execution context is incomplete")
	}
	tenantID, _, err := runtimecontext.ParseStorageScope(invocation.RunOptions.AppName)
	if err != nil {
		return err
	}
	execution.ID = ""
	execution.TenantID = tenantID
	execution.RequestID = invocation.RunOptions.RequestID
	started, err := journal.Start(ctx, execution)
	if err != nil {
		return err
	}
	if started.Existing && (len(managedReplay) == 0 || !managedReplay[0]) {
		return fmt.Errorf("%w: status=%s", ErrReplayBlocked, started.Execution.Status)
	}
	return nil
}

// NewCallbacks completes executions reserved by StartAuthorized in the final
// permission policy; it deliberately does not start work in BeforeTool.
func NewCallbacks(
	journal Journal,
	auditWriter audit.Writer,
	revisionID string,
) *tool.Callbacks {
	if journal == nil {
		return nil
	}
	callbacks := tool.NewCallbacks()
	callbacks.RegisterAfterTool(func(
		ctx context.Context,
		args *tool.AfterToolArgs,
	) (*tool.AfterToolResult, error) {
		invocation, ok := agentcore.InvocationFromContext(ctx)
		if !ok || invocation == nil || invocation.Session == nil || args == nil {
			return nil, errors.New("tool execution context is incomplete")
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		tenantID, _, err := runtimecontext.ParseStorageScope(invocation.RunOptions.AppName)
		if err != nil {
			return nil, err
		}
		executionID := StableID(invocation.RunOptions.RequestID, args.ToolCallID)
		stored, err := journal.Get(ctx, tenantID, executionID)
		if err != nil {
			return nil, err
		}
		status, errorType := StatusSucceeded, ""
		if args.Error != nil {
			status, errorType = StatusUnknown, "tool_outcome_unknown"
			var coded interface{ ErrorCode() string }
			if errors.As(args.Error, &coded) {
				switch coded.ErrorCode() {
				case governance.CodeSandboxUnavailable:
					status, errorType = StatusFailed, governance.CodeSandboxUnavailable
					governance.RecordToolFailure(ctx, args.ToolName, errorType)
				case "sandbox_execution_failed":
					errorType = "sandbox_execution_failed"
				}
			}
			if errors.Is(args.Error, ErrOperationRejected) || errors.Is(args.Error, ErrOperationConflict) {
				status, errorType = StatusFailed, "tool_rejected"
			}
		}
		resultJSON, _ := json.Marshal(args.Result)
		resultHash := Hash(resultJSON)
		if stored.OperationID != "" && stored.Status != StatusRunning {
			// Business facts were already committed/reconciled by Operations.
			status, errorType, resultHash = stored.Status, stored.ErrorType, stored.ResultHash
		} else {
			if stored.OperationID != "" {
				status, errorType = StatusUnknown, "business_outcome_unknown"
			}
			if err := journal.Complete(ctx, executionID, status, resultHash, errorType); err != nil {
				return nil, err
			}
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
					"result_hash":    resultHash,
					"operation_id":   stored.OperationID,
				},
			}); err != nil {
				return nil, err
			}
		}
		return &tool.AfterToolResult{}, nil
	})
	return callbacks
}
