//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package governance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// Plugin enforces policy at Runner's real tool lifecycle. An Agent wrapper is
// not sufficient: LLMAgent setup can replace Invocation.Agent, and dynamic
// tools/MCP/sub-agents may never pass through a statically wrapped Tools slice.
type Plugin struct {
	name          string
	filter        *GovernanceFilter
	audit         *telemetry.Collector
	approvalStore ApprovalStore
}

type toolStartContextKey struct{}

// InvocationAuditContext carries non-secret request correlation fields from
// the Worker seam into Runner plugin callbacks. The plugin lifecycle does not
// receive the HTTP request itself, so without this explicit context tool
// audits lose channel/user/session/agent attribution.
type InvocationAuditContext struct {
	ChannelType    string
	UserID         string
	SessionOwnerID string
	SessionID      string
	AgentName      string
	TraceID        string
	InvocationID   string
	ApprovalToken  string
}

type invocationAuditContextKey struct{}

// ContextWithInvocationAudit associates request correlation metadata with a
// Runner invocation. Callers must provide identifiers only; payloads and
// credentials are intentionally not part of this type.
func ContextWithInvocationAudit(ctx context.Context, value InvocationAuditContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, invocationAuditContextKey{}, value)
}

func invocationAuditFromContext(ctx context.Context) InvocationAuditContext {
	if ctx == nil {
		return InvocationAuditContext{}
	}
	value, _ := ctx.Value(invocationAuditContextKey{}).(InvocationAuditContext)
	return value
}

type toolStart struct {
	callID string
	at     time.Time
	span   trace.Span
}

// NewPlugin builds a fail-closed governance plugin.
func NewPlugin(filter *GovernanceFilter, name string, audit ...*telemetry.Collector) *Plugin {
	return newPlugin(filter, name, nil, audit...)
}

// NewPluginWithApprovalStore creates a governance plugin with the explicit
// challenge/grant/consume capability required by confirmation-gated tools.
// A nil store remains fail-closed for dangerous tools.
func NewPluginWithApprovalStore(filter *GovernanceFilter, name string, store ApprovalStore, audit ...*telemetry.Collector) *Plugin {
	return newPlugin(filter, name, store, audit...)
}

func newPlugin(filter *GovernanceFilter, name string, store ApprovalStore, audit ...*telemetry.Collector) *Plugin {
	if name == "" {
		name = "tenant-governance"
	}
	var sink *telemetry.Collector
	if len(audit) > 0 {
		sink = audit[0]
	}
	return &Plugin{name: name, filter: filter, audit: sink, approvalStore: store}
}

func (p *Plugin) Name() string { return p.name }

// Register attaches both before and after hooks to the Runner registry.
func (p *Plugin) Register(registry *plugin.Registry) {
	registry.BeforeTool(p.beforeTool)
	registry.AfterTool(p.afterTool)
	// Mask complete model text before the Runner emits and persists its event.
	// Partial chunks are deliberately left untouched because a regular
	// expression cannot safely match a value split across chunk boundaries.
	registry.AfterModel(p.afterModel)
}

// afterModel applies tenant masking to complete textual model output at the
// framework callback seam. The response is mutated in place only after a
// detached clone has been processed, so a masking failure cannot leave a
// partially modified protocol response. Tool calls and provider signatures
// are intentionally not changed: they are protocol fields, not display text.
func (p *Plugin) afterModel(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
	if args == nil || args.Response == nil || args.Response.IsPartial || !args.Response.Done {
		return nil, nil
	}
	if p == nil || p.filter == nil {
		return nil, fmt.Errorf("governance filter is not configured")
	}

	copy := args.Response.Clone()
	for i := range copy.Choices {
		if err := p.maskModelMessage(ctx, &copy.Choices[i].Message); err != nil {
			return nil, fmt.Errorf("mask model response: %w", err)
		}
		if err := p.maskModelMessage(ctx, &copy.Choices[i].Delta); err != nil {
			return nil, fmt.Errorf("mask model response delta: %w", err)
		}
	}
	*args.Response = *copy
	return nil, nil
}

func (p *Plugin) maskModelMessage(ctx context.Context, message *model.Message) error {
	if message == nil {
		return nil
	}
	if message.Content != "" {
		masked, err := p.filter.AfterToolInvocation(ctx, "model_response", message.Content, nil)
		if err != nil {
			return err
		}
		text, ok := masked.(string)
		if !ok {
			return fmt.Errorf("masked model content has unexpected type %T", masked)
		}
		message.Content = text
	}
	// Clone only the text pointers we may modify. Response.Clone intentionally
	// does not deep-copy ContentParts, so mutating a shared pointer here would
	// bypass the callback's failure boundary.
	if len(message.ContentParts) == 0 {
		return nil
	}
	parts := append([]model.ContentPart(nil), message.ContentParts...)
	for i := range parts {
		if parts[i].Type != model.ContentTypeText || parts[i].Text == nil {
			continue
		}
		value := *parts[i].Text
		masked, err := p.filter.AfterToolInvocation(ctx, "model_response", value, nil)
		if err != nil {
			return err
		}
		text, ok := masked.(string)
		if !ok {
			return fmt.Errorf("masked model content part has unexpected type %T", masked)
		}
		parts[i].Text = &text
	}
	message.ContentParts = parts
	return nil
}

func (p *Plugin) beforeTool(ctx context.Context, args *tool.BeforeToolArgs) (result *tool.BeforeToolResult, err error) {
	ctx, span := telemetry.StartOperation(nonNilContext(ctx), telemetry.OperationToolInvoke)
	handoff := false
	defer func() {
		if !handoff {
			telemetry.EndOperation(span, err)
		}
	}()
	if p == nil || p.filter == nil {
		return nil, fmt.Errorf("governance filter is not configured")
	}
	var name string
	var input map[string]interface{}
	if args != nil {
		name = args.ToolName
		if len(args.Arguments) > 0 {
			if err := json.Unmarshal(args.Arguments, &input); err != nil {
				return nil, fmt.Errorf("governance rejected malformed tool arguments: %w", err)
			}
		}
	}
	if err := p.filter.BeforeToolInvocation(ctx, name, input); err != nil {
		if errors.Is(err, ErrToolConfirmationRequired) {
			if approvalErr := p.authorizeConfirmation(ctx, name, args); approvalErr != nil {
				_ = p.auditTool(ctx, name, "tool_denied", approvalErr, 0)
				return nil, fmt.Errorf("governance denied: %w", approvalErr)
			}
		} else {
			_ = p.auditTool(ctx, name, "tool_denied", err, 0)
			return nil, fmt.Errorf("governance denied: %w", err)
		}
	}
	if err := p.auditTool(ctx, name, "tool_allowed", nil, 0); err != nil {
		return nil, fmt.Errorf("governance audit unavailable: %w", err)
	}
	callID := ""
	if args != nil {
		callID = args.ToolCallID
	}
	toolCtx := context.WithValue(ctx, toolStartContextKey{}, toolStart{callID: callID, at: time.Now(), span: span})
	handoff = true
	return &tool.BeforeToolResult{Context: toolCtx}, nil
}

func (p *Plugin) authorizeConfirmation(ctx context.Context, name string, args *tool.BeforeToolArgs) error {
	if p == nil || p.filter == nil || p.filter.tenant == nil {
		return ErrApprovalStoreUnavailable
	}
	auditContext := invocationAuditFromContext(ctx)
	if p.approvalStore == nil {
		return ErrApprovalStoreUnavailable
	}
	var raw []byte
	if args != nil && len(args.Arguments) > 0 {
		raw = args.Arguments
	}
	argsHash, err := CanonicalArgsHash(raw)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrApprovalInvalid, err)
	}
	invocationID := auditContext.InvocationID
	if invocationID == "" {
		return fmt.Errorf("%w: invocation identity is missing", ErrApprovalInvalid)
	}
	request := ApprovalRequest{
		TenantID:       p.filter.tenant.ID,
		UserID:         auditContext.UserID,
		SessionOwnerID: auditContext.SessionOwnerID,
		SessionID:      auditContext.SessionID,
		ToolName:       name,
		ArgsHash:       argsHash,
		InvocationID:   invocationID,
	}
	// A resumed Runner was admitted against one concrete challenge row. Consume
	// that row by ID and stop on any race or capability mismatch; falling through
	// to ordinary challenge creation could silently replace a consumed grant
	// and turn a stale retry into a fresh authorization flow.
	if state := approvalStateFromContext(ctx); state != nil {
		if challengeID, resuming := state.ResumeChallengeID(); resuming {
			if auditContext.ApprovalToken != "" {
				return approvalResumeFailure()
			}
			consumer, ok := p.approvalStore.(ApprovalGrantConsumerForChallenge)
			if !ok || consumer == nil {
				return approvalResumeFailure()
			}
			if err := consumer.ConsumeGrantedForChallenge(ctx, request, challengeID); err != nil {
				return approvalResumeFailure()
			}
			return nil
		}
	}
	if auditContext.ApprovalToken != "" {
		if err := p.approvalStore.Consume(ctx, request, auditContext.ApprovalToken); err != nil {
			return fmt.Errorf("%w: %v", ErrApprovalInvalid, err)
		}
		return nil
	}
	if granted, ok := p.approvalStore.(ApprovalGrantConsumer); ok {
		switch err := granted.ConsumeGranted(ctx, request); {
		case err == nil:
			return nil
		case errors.Is(err, ErrApprovalNotGranted), errors.Is(err, ErrApprovalNotFound), errors.Is(err, ErrApprovalInvalid):
			// No active operator grant yet. Create/reuse the challenge below.
		default:
			return fmt.Errorf("consume approval grant: %w", err)
		}
	}
	challenge, err := p.approvalStore.CreateChallenge(ctx, request, defaultApprovalTTL)
	if err != nil {
		return fmt.Errorf("create approval challenge: %w", err)
	}
	if state := approvalStateFromContext(ctx); state != nil {
		state.SetChallenge(challenge)
	}
	// A confirmation pause is not a recoverable tool failure. Returning a
	// StopError prevents the flow from feeding a synthetic denial result back
	// into the model, while preserving ApprovalRequiredError for the Worker and
	// control plane through the joined error chain.
	return errors.Join(
		&ApprovalRequiredError{Challenge: challenge},
		agent.NewStopError("tool approval required"),
	)
}

// approvalResumeFailure is intentionally opaque. Store errors can contain
// provider or connection details; the Worker records a stable error code while
// the Runner receives a stop signal and cannot continue with a synthetic tool
// denial or create a replacement challenge.
func approvalResumeFailure() error {
	return errors.Join(ErrApprovalResumeInvalid, agent.NewStopError("tool approval resume is no longer valid"))
}

func (p *Plugin) afterTool(ctx context.Context, args *tool.AfterToolArgs) (result *tool.AfterToolResult, err error) {
	ctx = nonNilContext(ctx)
	started, hasStarted := ctx.Value(toolStartContextKey{}).(toolStart)
	span := started.span
	if !hasStarted || span == nil {
		ctx, span = telemetry.StartOperation(ctx, telemetry.OperationToolInvoke)
	}
	spanErr := error(nil)
	if args != nil {
		spanErr = args.Error
	}
	defer func() {
		if err != nil {
			spanErr = err
		}
		telemetry.EndOperation(span, spanErr)
	}()
	if p == nil || p.filter == nil {
		return nil, fmt.Errorf("governance filter is not configured")
	}
	var name string
	var toolResult interface{}
	var runErr error
	if args != nil {
		name, toolResult, runErr = args.ToolName, args.Result, args.Error
	}
	masked, err := p.filter.AfterToolInvocation(ctx, name, toolResult, runErr)
	if err != nil {
		_ = p.auditTool(ctx, name, "tool_failed", err, toolLatency(ctx, args))
		return nil, err
	}
	if err := p.auditTool(ctx, name, "tool_succeeded", nil, toolLatency(ctx, args)); err != nil {
		return nil, fmt.Errorf("tool outcome audit unavailable: %w", err)
	}
	return &tool.AfterToolResult{CustomResult: masked}, nil
}

func toolLatency(ctx context.Context, args *tool.AfterToolArgs) time.Duration {
	if ctx == nil {
		return 0
	}
	started, ok := ctx.Value(toolStartContextKey{}).(toolStart)
	if !ok || (args != nil && started.callID != "" && started.callID != args.ToolCallID) {
		return 0
	}
	return time.Since(started.at)
}

func (p *Plugin) auditTool(ctx context.Context, toolName, decision string, runErr error, latency time.Duration) error {
	if p == nil || p.audit == nil {
		return nil
	}
	if p.filter == nil || p.filter.tenant == nil {
		return errors.New("governance tenant policy is not configured")
	}
	auditContext := invocationAuditFromContext(ctx)
	entry := &telemetry.AuditLog{
		TenantID:    p.filter.tenant.ID,
		ChannelType: auditContext.ChannelType,
		UserID:      auditContext.UserID,
		SessionID:   auditContext.SessionID,
		AgentName:   auditContext.AgentName,
		ToolName:    toolName,
		Decision:    decision,
		LatencyMS:   int(latency.Milliseconds()),
		TraceID:     auditContext.TraceID,
	}
	if runErr != nil {
		entry.ErrorType = "tool_error"
	}
	return p.audit.LogAudit(nonNilContext(ctx), entry)
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
