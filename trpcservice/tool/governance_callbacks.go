package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type cancelContextKey struct{}
type startedAtContextKey struct{}
type executionKeyContextKey struct{}
type replayedExecutionContextKey struct{}

// NewGovernanceCallbacks builds framework-native tool callbacks that enforce
// policy authorization, the request-scoped budget, digest-only audit recording,
// a per-call timeout, and tool telemetry before any tool side effect.
//
// The fail-closed guarantee comes from two places outside this file: tools are
// only ever registered at the single agent assembly site, and the agent-level
// integration test pins that a deny-all policy results in zero delegate calls.
func NewGovernanceCallbacks(policy governance.ToolPolicy, audit governance.AuditSink, ledger ExecutionLedger, timeout time.Duration) (*agenttool.Callbacks, error) {
	if policy == nil {
		return nil, fmt.Errorf("tool policy is required")
	}
	if audit == nil {
		return nil, fmt.Errorf("tool audit sink is required")
	}
	if ledger == nil {
		return nil, fmt.Errorf("tool execution ledger is required")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("tool timeout must be positive")
	}
	return &agenttool.Callbacks{
		BeforeTool: []agenttool.BeforeToolCallbackStructured{func(ctx context.Context, args *agenttool.BeforeToolArgs) (*agenttool.BeforeToolResult, error) {
			invocation, ok := governance.InvocationFromContext(ctx)
			if !ok {
				return nil, governance.ErrInvalidExecutionContext
			}
			request := governance.ToolRequest{Name: args.ToolName, Arguments: args.Arguments}
			if err := policy.Authorize(ctx, invocation.Execution, request); err != nil {
				if auditErr := recordToolAudit(ctx, audit, invocation, request, governance.ToolOutcomeDenied, 0, err); auditErr != nil {
					return nil, errors.Join(err, auditErr)
				}
				return nil, err
			}
			decision, err := ledger.Begin(ctx, ExecutionRequest{
				TenantID: invocation.Execution.TenantID, RequestID: invocation.Execution.RequestID,
				ToolCallID: args.ToolCallID, ToolName: args.ToolName, Arguments: args.Arguments,
				TraceID: invocation.Execution.TraceID, LeaseTTL: timeout + finalizationGrace,
			})
			if err != nil {
				if auditErr := recordToolAudit(ctx, audit, invocation, request, governance.ToolOutcomeFailed, 0, err); auditErr != nil {
					return nil, errors.Join(err, auditErr)
				}
				return nil, err
			}
			switch decision.Status {
			case ExecutionCompleted:
				replayContext := context.WithValue(ctx, replayedExecutionContextKey{}, true)
				if auditErr := recordToolAudit(ctx, audit, invocation, request, governance.ToolOutcomeAllowed, 0, nil); auditErr != nil {
					return nil, auditErr
				}
				return &agenttool.BeforeToolResult{Context: replayContext, CustomResult: decision.Result}, nil
			case ExecutionOutcomeUnknown:
				err := fmt.Errorf("%w: tool %q may already have produced a side effect", ErrToolOutcomeUnknown, args.ToolName)
				if auditErr := recordToolAudit(ctx, audit, invocation, request, governance.ToolOutcomeFailed, 0, err); auditErr != nil {
					return nil, errors.Join(err, auditErr)
				}
				return nil, err
			case ExecutionRunning:
				if !decision.Created {
					err := fmt.Errorf("%w: tool %q", ErrToolExecutionInProgress, args.ToolName)
					if auditErr := recordToolAudit(ctx, audit, invocation, request, governance.ToolOutcomeFailed, 0, err); auditErr != nil {
						return nil, errors.Join(err, auditErr)
					}
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unsupported tool execution decision %q", decision.Status)
			}
			if err := invocation.Budget.Consume(); err != nil {
				_ = ledger.Fail(context.WithoutCancel(ctx), invocation.Execution.TenantID, decision.IdempotencyKey, fmt.Sprintf("%T", err))
				if auditErr := recordToolAudit(ctx, audit, invocation, request, governance.ToolOutcomeDenied, 0, err); auditErr != nil {
					return nil, errors.Join(err, auditErr)
				}
				return nil, err
			}
			if invocation.Units != nil {
				if err := invocation.Units.Consume(1); err != nil {
					_ = ledger.Fail(context.WithoutCancel(ctx), invocation.Execution.TenantID, decision.IdempotencyKey, fmt.Sprintf("%T", err))
					if auditErr := recordToolAudit(ctx, audit, invocation, request, governance.ToolOutcomeDenied, 0, err); auditErr != nil {
						return nil, errors.Join(err, auditErr)
					}
					return nil, err
				}
			}
			callContext, cancel := context.WithTimeout(ctx, timeout)
			callContext = context.WithValue(callContext, cancelContextKey{}, cancel)
			callContext = context.WithValue(callContext, startedAtContextKey{}, time.Now())
			callContext = context.WithValue(callContext, executionKeyContextKey{}, decision.IdempotencyKey)
			// A non-nil Context is honored by the framework for the tool call
			// and everything downstream of it.
			return &agenttool.BeforeToolResult{Context: callContext}, nil
		}},
		AfterTool: []agenttool.AfterToolCallbackStructured{func(ctx context.Context, args *agenttool.AfterToolArgs) (*agenttool.AfterToolResult, error) {
			if replayed, _ := ctx.Value(replayedExecutionContextKey{}).(bool); replayed {
				return nil, nil
			}
			if cancel, ok := ctx.Value(cancelContextKey{}).(context.CancelFunc); ok && cancel != nil {
				defer cancel()
			}
			invocation, ok := governance.InvocationFromContext(ctx)
			if !ok {
				// The before-hook failed closed; there is nothing to audit.
				return nil, nil
			}
			request := governance.ToolRequest{Name: args.ToolName, Arguments: args.Arguments}
			outcome := governance.ToolOutcomeAllowed
			executionKey, _ := ctx.Value(executionKeyContextKey{}).(string)
			if args.Error != nil {
				outcome = governance.ToolOutcomeFailed
			}
			latency := int64(0)
			if started, ok := ctx.Value(startedAtContextKey{}).(time.Time); ok && !started.IsZero() {
				latency = time.Since(started).Milliseconds()
			}
			var ledgerErr error
			if executionKey == "" {
				ledgerErr = errors.New("tool execution key is missing after execution")
			} else if args.Error == nil {
				ledgerErr = ledger.Complete(context.WithoutCancel(ctx), invocation.Execution.TenantID, executionKey, args.Result)
			} else {
				// A tool error does not prove that the remote system rejected the
				// operation before applying it. Preserve the reservation as unknown
				// rather than turning a timeout/network failure into a duplicate.
				ledgerErr = ledger.MarkOutcomeUnknown(context.WithoutCancel(ctx), invocation.Execution.TenantID, executionKey, fmt.Sprintf("%T", args.Error))
			}
			if err := recordToolAudit(ctx, audit, invocation, request, outcome, latency, args.Error); err != nil {
				ledgerErr = errors.Join(ledgerErr, fmt.Errorf("record tool audit: %w", err))
			}
			if ledgerErr != nil {
				return nil, ledgerErr
			}
			return nil, nil
		}},
	}, nil
}

const finalizationGrace = 5 * time.Second

func recordToolAudit(ctx context.Context, audit governance.AuditSink, invocation governance.Invocation, request governance.ToolRequest, outcome governance.ToolOutcome, latencyMS int64, callErr error) error {
	sum := sha256.Sum256(request.Arguments)
	errorType := ""
	if callErr != nil {
		errorType = fmt.Sprintf("%T", callErr)
	}
	if err := audit.RecordToolAudit(ctx, governance.ToolAuditEvent{
		TenantID:        invocation.Execution.TenantID,
		TraceID:         invocation.Execution.TraceID,
		RequestID:       invocation.Execution.RequestID,
		Channel:         invocation.Execution.Channel,
		UserID:          invocation.Execution.UserID,
		SessionID:       invocation.Execution.SessionID,
		AgentName:       invocation.Execution.AgentName,
		PolicyVersion:   invocation.Execution.PolicyVersion,
		ToolName:        request.Name,
		Outcome:         outcome,
		LatencyMS:       latencyMS,
		ErrorType:       errorType,
		ArgumentsDigest: hex.EncodeToString(sum[:]),
		OccurredAt:      time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("record tool audit: %w", err)
	}
	return nil
}
