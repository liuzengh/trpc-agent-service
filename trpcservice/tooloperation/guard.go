package tooloperation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	// ErrOutcomeUnknown is deliberately retry-blocking: the external side
	// effect may have happened and only an explicit resolution may release it.
	ErrOutcomeUnknown = errors.New("tooloperation: external outcome is unknown")
	// ErrOperationInProgress prevents a second worker from crossing the same
	// semantic intent fence while the first worker still owns it.
	ErrOperationInProgress = errors.New("tooloperation: operation is in progress")
	// ErrOperationRejected is a stable terminal result, never an upstream error.
	ErrOperationRejected = errors.New("tooloperation: operation was permanently rejected")
	// ErrReplayRequired asks the Worker to rerun so BeforeTool can replay a
	// confirmation won by a concurrent process without executing the tool.
	ErrReplayRequired = errors.New("tooloperation: confirmed result requires replay")
)

const (
	guardPluginName   = "tool-side-effect-ledger"
	intentKeyDomain   = "trpc-tool-intent/v1"
	defaultGuardLease = 3 * time.Minute
	finalizeTimeout   = 5 * time.Second
)

type operationContextKey struct{}

// OperationKeyFromContext exposes the opaque platform idempotency key to a
// real tool. Providers that support idempotency should receive this exact key.
func OperationKeyFromContext(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(operationContextKey{}).(*guardCall)
	if !ok || value == nil || value.operationKey == "" {
		return "", false
	}
	return value.operationKey, true
}

// GuardScope is immutable for one Runner invocation. TurnID must be derived
// from the upstream Inbox/message idempotency key, never from a random request.
// ConfigRevision is part of the intent identity so an old authorization cannot
// be replayed after a policy revision changes.
type GuardScope struct {
	TenantID       string
	AppNamespace   string
	SessionID      string
	TurnID         string
	ConfigRevision string
	SideEffects    []string
}

// GuardEvent carries only bounded labels and opaque identifiers. OperationKey
// is suitable for audit correlation but must not become a metric label.
type GuardEvent struct {
	TenantID     string
	ToolName     string
	OperationKey string
	Phase        string
	Outcome      string
}

type GuardObserver func(context.Context, GuardEvent)

// DefinitelyNotApplied may be implemented by a tool error only when the tool
// can prove that no external mutation was accepted. Ordinary timeouts and
// network errors must not implement it.
type DefinitelyNotApplied interface {
	error
	ToolSideEffectNotApplied() bool
}

type guardCall struct {
	operationKey string
	payloadHash  string
	toolName     string
	fence        Fence
	executing    bool
	replayed     bool
}

// RunGuard is both a runner-scoped plugin and a permission-policy wrapper.
// BeforeTool performs lookup/replay, while the wrapped policy crosses the
// durable executing fence only after governance has returned allow.
type RunGuard struct {
	ledger      Ledger
	scope       GuardScope
	sideEffects map[string]struct{}
	owner       string
	leaseTTL    time.Duration
	now         func() time.Time
	observer    GuardObserver

	mu      sync.Mutex
	calls   map[string]*guardCall
	callIDs map[string]*guardCall
}

type GuardOption func(*RunGuard)

func WithGuardLeaseTTL(ttl time.Duration) GuardOption {
	return func(g *RunGuard) {
		if ttl > 0 {
			g.leaseTTL = ttl
		}
	}
}

func WithGuardObserver(observer GuardObserver) GuardOption {
	return func(g *RunGuard) { g.observer = observer }
}

func NewRunGuard(ledger Ledger, scope GuardScope, opts ...GuardOption) (*RunGuard, error) {
	if !validTenant(scope.TenantID) ||
		!validRequiredOpaque(scope.AppNamespace, maxSafeLabelBytes) ||
		!validRequiredOpaque(scope.SessionID, maxSafeLabelBytes) ||
		!validRequiredOpaque(scope.TurnID, maxSafeLabelBytes) ||
		!validRequiredOpaque(scope.ConfigRevision, maxSafeLabelBytes) {
		return nil, ErrInvalidRequest
	}
	effects := make(map[string]struct{}, len(scope.SideEffects))
	for _, name := range scope.SideEffects {
		if !validSafeLabel(name, maxSafeLabelBytes) {
			return nil, ErrInvalidRequest
		}
		effects[name] = struct{}{}
	}
	if len(effects) > 0 && ledger == nil {
		return nil, ErrInvalidRequest
	}
	g := &RunGuard{
		ledger: ledger, scope: scope, sideEffects: effects,
		owner: "toolguard-" + uuid.NewString(), leaseTTL: defaultGuardLease,
		now: time.Now, calls: make(map[string]*guardCall), callIDs: make(map[string]*guardCall),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(g)
		}
	}
	return g, nil
}

func (g *RunGuard) Name() string { return guardPluginName }

func (g *RunGuard) Register(registry *plugin.Registry) {
	if g == nil || registry == nil || len(g.sideEffects) == 0 {
		return
	}
	registry.BeforeTool(g.beforeTool)
	registry.AfterTool(g.afterTool)
	registry.AfterToolMessages(g.afterToolMessages)
}

// WrapPermissionPolicy preserves the governance decision and writes nothing
// for deny/ask/error. Only an allowed side-effect call is reserved, leased,
// and marked executing immediately before the framework invokes the tool.
func (g *RunGuard) WrapPermissionPolicy(base tool.PermissionPolicy) tool.PermissionPolicy {
	return tool.PermissionPolicyFunc(func(ctx context.Context, req *tool.PermissionRequest) (tool.PermissionDecision, error) {
		decision := tool.AllowPermission()
		var err error
		if base != nil {
			decision, err = base.CheckToolPermission(ctx, req)
			if err != nil {
				return decision, err
			}
		}
		decision, err = tool.NormalizePermissionDecision(decision)
		if err != nil || decision.Action != tool.PermissionActionAllow || req == nil || !g.isSideEffect(req.ToolName) {
			return decision, err
		}

		call, err := g.callFor(ctx, req.ToolName, req.Arguments)
		if err != nil {
			return tool.PermissionDecision{}, err
		}
		reserved, err := g.ledger.Reserve(ctx, ReserveRequest{
			TenantID: g.scope.TenantID, OperationKey: call.operationKey,
			PayloadHash: call.payloadHash,
			Metadata:    Metadata{ToolName: call.toolName, OperationClass: "side_effect"},
		}, g.now().UTC())
		if err != nil {
			return tool.PermissionDecision{}, err
		}
		if reserved.Replayed || reserved.Record.State == StateConfirmed {
			return tool.PermissionDecision{}, ErrReplayRequired
		}
		switch reserved.Record.State {
		case StateExecuting, StateUnknown:
			return tool.PermissionDecision{}, ErrOutcomeUnknown
		case StatePermanentRejected:
			return tool.PermissionDecision{}, ErrOperationRejected
		}

		now := g.now().UTC()
		leased, err := g.ledger.LeaseOperation(
			ctx, g.scope.TenantID, call.operationKey, g.owner, now, g.leaseTTL,
		)
		if err != nil {
			return tool.PermissionDecision{}, mapLeaseError(err)
		}
		call.fence = leased.Fence
		if _, err := g.ledger.MarkExecuting(ctx, call.fence, g.now().UTC()); err != nil {
			// The external call has not started, but an acknowledgement-lost fence
			// cannot safely be assumed absent. Reclaim will conservatively surface it.
			return tool.PermissionDecision{}, fmt.Errorf("%w: dispatch fence unavailable", ErrOutcomeUnknown)
		}
		call.executing = true
		g.remember(call, req.ToolCallID)
		g.observe(ctx, call, "dispatch", "executing")
		return decision, nil
	})
}

func (g *RunGuard) beforeTool(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
	if args == nil || !g.isSideEffect(args.ToolName) {
		return nil, nil
	}
	if _, err := g.ledger.ReclaimExpired(ctx, g.now().UTC(), maxLeaseBatch); err != nil {
		return nil, err
	}
	call, err := g.newCall(args.ToolName, args.Arguments)
	if err != nil {
		return nil, err
	}
	record, err := g.ledger.Get(ctx, g.scope.TenantID, call.operationKey)
	switch {
	case errors.Is(err, ErrNotFound):
		g.remember(call, args.ToolCallID)
		return &tool.BeforeToolResult{Context: context.WithValue(ctx, operationContextKey{}, call)}, nil
	case err != nil:
		return nil, err
	}
	if record.PayloadHash != call.payloadHash || record.Metadata.ToolName != call.toolName {
		return nil, ErrConflict
	}
	switch record.State {
	case StateConfirmed:
		call.replayed = true
		g.remember(call, args.ToolCallID)
		g.observe(ctx, call, "replay", "confirmed")
		return &tool.BeforeToolResult{
			Context: context.WithValue(ctx, operationContextKey{}, call),
			CustomResult: map[string]any{
				"status": "already_applied", "replayed": true,
				"operation_key": call.operationKey,
			},
		}, nil
	case StateExecuting, StateUnknown:
		g.observe(ctx, call, "block", string(record.State))
		return nil, ErrOutcomeUnknown
	case StatePermanentRejected:
		return nil, ErrOperationRejected
	case StateReserved:
		if !record.LeaseExpiresAt.IsZero() && record.LeaseExpiresAt.After(g.now().UTC()) {
			return nil, ErrOperationInProgress
		}
	case StateRetryableNotApplied:
		// Explicit proof or resolution permits one newly fenced attempt.
	default:
		return nil, ErrInvalidTransition
	}
	g.remember(call, args.ToolCallID)
	return &tool.BeforeToolResult{Context: context.WithValue(ctx, operationContextKey{}, call)}, nil
}

func (g *RunGuard) afterTool(ctx context.Context, args *tool.AfterToolArgs) (*tool.AfterToolResult, error) {
	if args == nil || !g.isSideEffect(args.ToolName) {
		return nil, nil
	}
	call, err := g.callFor(ctx, args.ToolName, args.Arguments)
	if err != nil || call.replayed || !call.executing {
		return nil, err
	}
	g.remember(call, args.ToolCallID)
	if args.Error == nil {
		// The exact model-facing message is hashed in afterToolMessages. Until
		// then the record deliberately remains executing.
		g.remember(call, args.ToolCallID)
		return nil, nil
	}

	now := g.now().UTC()
	outcome := StateUnknown
	outcomeCode := "tool_execution_error"
	retryAt := time.Time{}
	var notApplied DefinitelyNotApplied
	if errors.As(args.Error, &notApplied) && notApplied.ToolSideEffectNotApplied() {
		outcome = StateRetryableNotApplied
		outcomeCode = "proved_not_applied"
		retryAt = now.Add(time.Second)
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	_, finishErr := g.ledger.Finish(finishCtx, FinishRequest{
		Fence: call.fence, Outcome: outcome, OutcomeCode: outcomeCode, RetryAt: retryAt,
	}, now)
	cancel()
	if finishErr != nil {
		return nil, fmt.Errorf("%w: operation finalization failed", ErrOutcomeUnknown)
	}
	g.observe(ctx, call, "finish", string(outcome))
	if outcome == StateUnknown {
		return nil, ErrOutcomeUnknown
	}
	return nil, args.Error
}

func (g *RunGuard) afterToolMessages(
	ctx context.Context,
	args *plugin.AfterToolMessagesArgs,
) (*plugin.AfterToolMessagesResult, error) {
	if args == nil {
		return nil, nil
	}
	messages := make(map[string]model.Message, len(args.ToolResultMessages))
	for _, message := range args.ToolResultMessages {
		messages[message.ToolID] = message
		call := g.lookupByCallID(message.ToolID)
		if call == nil || call.replayed || !call.executing {
			continue
		}
		encoded, err := json.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("%w: encode tool result", ErrOutcomeUnknown)
		}
		now := g.now().UTC()
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
		_, err = g.ledger.Finish(finishCtx, FinishRequest{
			Fence: call.fence, Outcome: StateConfirmed, OutcomeCode: "tool_succeeded",
			ResultHash: HashPayload(encoded),
		}, now)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("%w: operation confirmation failed", ErrOutcomeUnknown)
		}
		call.executing = false
		g.observe(ctx, call, "finish", "confirmed")
	}
	// Never expose an executed side-effect result to the next model call unless
	// the matching durable confirmation was written.
	for _, toolCall := range args.ToolCalls {
		call := g.lookupByCallID(toolCall.ID)
		if call == nil || call.replayed || !call.executing {
			continue
		}
		if _, ok := messages[toolCall.ID]; !ok {
			return nil, fmt.Errorf("%w: tool result message is missing", ErrOutcomeUnknown)
		}
	}
	return nil, nil
}

func (g *RunGuard) isSideEffect(name string) bool {
	if g == nil {
		return false
	}
	_, ok := g.sideEffects[name]
	return ok
}

func (g *RunGuard) newCall(toolName string, arguments []byte) (*guardCall, error) {
	payloadHash := HashPayload(canonicalJSON(arguments))
	operationKey, err := deriveIntentKey(g.scope, toolName, payloadHash)
	if err != nil {
		return nil, err
	}
	return &guardCall{operationKey: operationKey, payloadHash: payloadHash, toolName: toolName}, nil
}

func (g *RunGuard) callFor(ctx context.Context, toolName string, arguments []byte) (*guardCall, error) {
	if call, ok := ctx.Value(operationContextKey{}).(*guardCall); ok && call != nil {
		if call.toolName != toolName || call.payloadHash != HashPayload(canonicalJSON(arguments)) {
			return nil, ErrConflict
		}
		return call, nil
	}
	call, err := g.newCall(toolName, arguments)
	if err != nil {
		return nil, err
	}
	if existing := g.lookup(call.operationKey); existing != nil {
		return existing, nil
	}
	g.remember(call, "")
	return call, nil
}

func (g *RunGuard) remember(call *guardCall, toolCallID string) {
	if call == nil {
		return
	}
	g.mu.Lock()
	g.calls[call.operationKey] = call
	if toolCallID != "" {
		g.callIDs[toolCallID] = call
	}
	g.mu.Unlock()
}

func (g *RunGuard) lookup(operationKey string) *guardCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[operationKey]
}

func (g *RunGuard) lookupByCallID(toolCallID string) *guardCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.callIDs[toolCallID]
}

func deriveIntentKey(scope GuardScope, toolName, payloadHash string) (string, error) {
	if !validTenant(scope.TenantID) || !validRequiredOpaque(scope.AppNamespace, maxSafeLabelBytes) ||
		!validRequiredOpaque(scope.SessionID, maxSafeLabelBytes) ||
		!validRequiredOpaque(scope.TurnID, maxSafeLabelBytes) || !validSafeLabel(toolName, maxSafeLabelBytes) ||
		!validSHA256(payloadHash) {
		return "", ErrInvalidRequest
	}
	h := sha256.New()
	for _, value := range []string{
		intentKeyDomain, scope.TenantID, scope.AppNamespace, scope.SessionID,
		scope.TurnID, scope.ConfigRevision, toolName, payloadHash,
	} {
		writeFramed(h, value)
	}
	return "toolop_" + hex.EncodeToString(h.Sum(nil)), nil
}

func canonicalJSON(raw []byte) []byte {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) == nil {
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return append([]byte(nil), raw...)
		}
		if encoded, err := json.Marshal(value); err == nil {
			return encoded
		}
	}
	return append([]byte(nil), raw...)
}

func mapLeaseError(err error) error {
	switch {
	case errors.Is(err, ErrUnknownRequiresResolution):
		return ErrOutcomeUnknown
	case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrInvalidTransition):
		return ErrOperationInProgress
	default:
		return err
	}
}

func (g *RunGuard) observe(ctx context.Context, call *guardCall, phase, outcome string) {
	if g.observer == nil || call == nil {
		return
	}
	g.observer(ctx, GuardEvent{
		TenantID: g.scope.TenantID, ToolName: call.toolName,
		OperationKey: call.operationKey, Phase: phase, Outcome: outcome,
	})
}

// compile-time checks for the SDK surfaces used by the Worker.
var _ plugin.Plugin = (*RunGuard)(nil)
