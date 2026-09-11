package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	approvalv1 "github.com/liuzengh/trpc-agent-service/api/runtime/approval/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/toolapproval"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const approvalCapability = approvalv1.CapabilityTestTicketStatusUpdate

var ErrApproval = errors.New("tool approval failed")

type ApprovalStore interface {
	Propose(context.Context, toolapproval.Proposal) (approvalv1.Operation, error)
	AwaitAndBegin(context.Context, string, string, time.Duration) (approvalv1.Operation, []byte, error)
	Finish(context.Context, string, string, []byte, error) error
}

type approvalTool struct{ resource, capability string }
type approvalContextKey struct{}
type approvalContext struct{ tenantID, operationID string }

type approvalState struct {
	store                              ApprovalStore
	tenantID, runID, attemptID, nodeID string
	tools                              map[string]approvalTool
}

func (s *approvalState) before(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
	if args == nil {
		return nil, ErrApproval
	}
	selected, ok := s.tools[args.ToolName]
	if !ok {
		return nil, nil
	}
	invocation, ok := agent.InvocationFromContext(ctx)
	if !ok || invocation == nil || invocation.InvocationID == "" || args.ToolCallID == "" || invocation.AgentName != s.nodeID {
		return nil, ErrApproval
	}
	op, err := s.store.Propose(ctx, toolapproval.Proposal{
		TenantID: s.tenantID, RunID: s.runID, AttemptID: s.attemptID,
		InvocationID: invocation.InvocationID, ToolCallID: args.ToolCallID,
		NodeID: s.nodeID, ToolName: args.ToolName, ToolResource: selected.resource,
		Capability: selected.capability, Arguments: args.Arguments, TTL: 5 * time.Minute,
	})
	if err != nil {
		return nil, ErrApproval
	}
	_, replay, err := s.store.AwaitAndBegin(ctx, s.tenantID, op.OperationID, 200*time.Millisecond)
	if errors.Is(err, toolapproval.ErrDenied) {
		return &tool.BeforeToolResult{CustomResult: map[string]any{"approved": false, "status": "denied", "operation_id": op.OperationID}}, nil
	}
	if err != nil {
		return nil, ErrApproval
	}
	if replay != nil {
		var result any
		if json.Unmarshal(replay, &result) != nil {
			return nil, ErrApproval
		}
		return &tool.BeforeToolResult{CustomResult: result}, nil
	}
	return &tool.BeforeToolResult{Context: context.WithValue(ctx, approvalContextKey{}, approvalContext{tenantID: s.tenantID, operationID: op.OperationID})}, nil
}

func (s *approvalState) after(ctx context.Context, args *tool.AfterToolArgs) error {
	operation, ok := ctx.Value(approvalContextKey{}).(approvalContext)
	if !ok {
		return nil
	}
	var raw []byte
	var err error
	if args == nil {
		err = ErrApproval
	} else {
		err = args.Error
		if err == nil {
			raw, err = json.Marshal(args.Result)
		}
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if finishErr := s.store.Finish(finishCtx, operation.tenantID, operation.operationID, raw, err); finishErr != nil {
		return ErrApproval
	}
	// Provider/infrastructure errors are recorded UNKNOWN and retain the existing
	// action identity. Fail this Attempt non-retryably instead of letting the
	// ordinary MCP dependency policy issue the side effect again.
	if err != nil {
		return ErrApproval
	}
	return nil
}
