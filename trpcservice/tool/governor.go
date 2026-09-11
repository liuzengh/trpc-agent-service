package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

// The governor is the wrapper every reliable-path tool is built through. It
// is the "actual Callable 包装器" of the approved plan, and it enforces, in
// this order, on every call:
//
//  1. a previous Unknown has blocked this execution — nothing else runs;
//  2. the per-execution call budget;
//  3. the binding's risk level;
//  4. the argument schema;
//  5. the binding is still active at the pinned version;
//  6. the intent is journaled before the callable runs (fail closed: no
//     ledger, no call);
//  7. the call runs under its own timeout;
//  8. the outcome is journaled, and an Unknown blocks the execution and
//     cancels the run, so the model cannot plan around a lie.
const (
	// DefaultMaxCallsPerExecution is the approved plan's per-execution tool
	// call ceiling. It is a budget, not a quality knob: a model that needs
	// more than this has stopped solving the user's problem.
	DefaultMaxCallsPerExecution = 8
	// DefaultCallTimeout and MaxCallTimeout bound one call. A binding may
	// lower its timeout; it may not raise it past the ceiling.
	DefaultCallTimeout = 10 * time.Second
	MaxCallTimeout     = 30 * time.Second
)

// ErrExecutionBlocked is the tool-side refusal after an Unknown outcome.
// Every later call returns it without touching the network.
var ErrExecutionBlocked = errors.New("tool: execution is blocked pending human review")

// ErrBudgetExceeded is a rejection, not a failure: the call never happened.
var ErrBudgetExceeded = errors.New("tool: per-execution call budget exceeded")

// ErrHighRisk is the plan's "高风险直接拒绝": a binding marked high risk is
// refused at call time regardless of who asks, until a human approval
// workflow exists (deferred by the plan).
var ErrHighRisk = errors.New("tool: high-risk tool calls require an approval path that does not exist yet, so the call is refused")

// ErrApprovalRequired is returned when a high-risk tool call is blocked
// waiting for human review. Unlike ErrHighRisk the intent is journaled and
// the session stopped — a human sees it on -list-blocked and resolves with
// -resolve-session.
var ErrApprovalRequired = errors.New("tool: high-risk tool call requires human approval; execution is blocked")

// BlockDecision records why an execution stopped and what a human has to
// look at.
type BlockDecision struct {
	CallID   string
	ToolName string
	Reason   string
}

// ToolMeta is the per-binding facts the governor checks. It is derived from
// the pinned tool_bindings row at assembly time and re-checked live for
// revocation before each call.
type ToolMeta struct {
	Name       string
	ToolID     int64
	Version    uint32
	Kind       string
	RiskLevel  string
	SideEffect string
	Idempotent bool
	Timeout    time.Duration
}

// GovernorOptions carries the claim facts the ledger keys on.
type GovernorOptions struct {
	Journal     *Journal
	DB          *controlplane.DB
	TenantID    string
	ExecutionID string
	SessionPK   int64
	WorkerID    string
	TraceID     string
	Fence       uint64
	// Budget defaults to DefaultMaxCallsPerExecution.
	Budget int32
}

// Governor is shared by every tool of one execution attempt.
type Governor struct {
	opts   GovernorOptions
	budget int32

	scope controlplane.Scope

	used atomic.Int32 // calls the model has attempted
	seq  atomic.Int32 // journal sequence

	mu      sync.Mutex
	blocked *BlockDecision

	runCancel atomic.Pointer[context.CancelFunc]
}

// NewGovernor builds the per-execution governor. The database is required:
// the ledger is the feature, and a governor without one would silently stop
// enforcing the recovery rule.
func NewGovernor(opts GovernorOptions) (*Governor, error) {
	if opts.Journal == nil {
		return nil, errors.New("tool: a governor needs a journal")
	}
	if opts.DB == nil || opts.TenantID == "" || opts.ExecutionID == "" {
		return nil, errors.New("tool: a governor needs a tenant and an execution")
	}
	scope, err := opts.DB.Scope(opts.TenantID)
	if err != nil {
		return nil, err
	}
	if opts.Budget <= 0 {
		opts.Budget = DefaultMaxCallsPerExecution
	}
	return &Governor{opts: opts, budget: opts.Budget, scope: scope}, nil
}

// SetRunCancel hands the governor the canceller of the run's context: an
// Unknown outcome must stop the model call in flight, not merely report an
// error to it.
func (g *Governor) SetRunCancel(cancel context.CancelFunc) {
	g.runCancel.Store(&cancel)
}

// Blocked reports the decision that stopped this execution, if any.
func (g *Governor) Blocked() *BlockDecision {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.blocked
}

func (g *Governor) block(d BlockDecision) {
	g.mu.Lock()
	if g.blocked == nil {
		g.blocked = &d
	}
	g.mu.Unlock()
	if cancel := g.runCancel.Load(); cancel != nil {
		(*cancel)()
	}
}

// CheckRecovery is the pre-run gate the approved plan demands: a claim that
// follows a dead attempt asks the ledger what that attempt may have done.
// Stale reads are abandoned (safe to re-run); a stale non-idempotent write
// blocks the execution before any new side effect can start.
func (g *Governor) CheckRecovery(ctx context.Context) (*BlockDecision, error) {
	stale, err := g.opts.Journal.StaleUnresolved(ctx, g.opts.TenantID, g.opts.ExecutionID, g.opts.Fence)
	if err != nil {
		return nil, err
	}
	var decision *BlockDecision
	for _, c := range stale {
		if c.SideEffect == "write" && !c.Idempotent {
			if decision == nil {
				// The reason must start with "tool ": that prefix is the marker
				// execution.TryUnblock checks before it will act on a blocked
				// session, and a recovery block is exactly such a block.
				decision = &BlockDecision{
					CallID:   c.CallID,
					ToolName: c.ToolName,
					Reason: fmt.Sprintf(
						"tool call %s (%s, side_effect=write) from a previous attempt is unresolved; its effect cannot be known, so this execution is parked for review",
						c.CallID, c.ToolName),
				}
			}
			continue
		}
		// No side effect (or a declared-idempotent write): re-running is the
		// documented safe action, so the stale row is concluded as failed.
		if err := g.opts.Journal.Abandon(ctx, g.opts.TenantID, c.CallID,
			"stale call from an interrupted attempt; safe to re-run because it is read-only or idempotent"); err != nil {
			return nil, err
		}
	}
	if decision != nil {
		g.block(*decision)
	}
	return decision, nil
}

// Wrap builds the governed tool. Every reliable-path tool goes through here;
// a tool that did not would be visible as an ungoverned entry in the
// revision's pinned list, which is why assembly only ever returns wrapped
// tools. The return type is CallableTool, not Tool: a governed tool that
// cannot be called would be a declaration the model could see and the
// framework could not invoke.
func (g *Governor) Wrap(inner frameworktool.CallableTool, meta ToolMeta) frameworktool.CallableTool {
	decl := inner.Declaration()
	if meta.Name == "" {
		meta.Name = decl.Name
	}
	if meta.Timeout <= 0 {
		meta.Timeout = DefaultCallTimeout
	}
	if meta.Timeout > MaxCallTimeout {
		meta.Timeout = MaxCallTimeout
	}
	if meta.Kind == "" {
		meta.Kind = "go"
	}
	if meta.SideEffect == "" {
		meta.SideEffect = "none"
	}
	wrapped := &governedTool{gov: g, inner: inner, decl: decl, meta: meta}
	// A callable that carries its own compiled schema has it validated here,
	// once, at assembly: the governor then re-validates arguments on every
	// call. Platform Go builtins have no stored schema (their typed decode is
	// the contract) and leave this nil.
	if holder, ok := inner.(interface{ inputSchema() *InputSchema }); ok {
		wrapped.schema = holder.inputSchema()
	}
	return wrapped
}

// governedTool is one wrapped callable.
type governedTool struct {
	gov    *Governor
	inner  frameworktool.CallableTool
	decl   *frameworktool.Declaration
	meta   ToolMeta
	schema *InputSchema
}

func (t *governedTool) Declaration() *frameworktool.Declaration { return t.decl }

// Call implements frameworktool.CallableTool. The ordering of checks is the
// contract: each one can only pass if every earlier one did.
func (t *governedTool) Call(ctx context.Context, jsonArgs []byte) (any, error) {
	g := t.gov

	ctx, span := otel.Tracer(metrics.ServiceName).Start(ctx, "tool.call",
		trace.WithAttributes(
			attribute.String("tool.name", t.meta.Name),
			attribute.String("tool.kind", t.meta.Kind),
			attribute.String("tool.side_effect", t.meta.SideEffect),
			attribute.String("execution.id", g.opts.ExecutionID),
		))
	defer span.End()

	if d := g.Blocked(); d != nil {
		span.SetAttributes(attribute.String("tool.outcome", "blocked"))
		return nil, fmt.Errorf("%w: %s", ErrExecutionBlocked, d.Reason)
	}

	seq := int(g.seq.Add(1))
	attempted := g.used.Add(1)
	if attempted > g.budget {
		t.journalReject(ctx, seq, jsonArgs, "budget_exceeded", ErrBudgetExceeded.Error())
		span.SetAttributes(attribute.String("tool.outcome", "rejected"))
		return nil, fmt.Errorf("%w: %s allows %d calls per execution", ErrBudgetExceeded, t.meta.Name, g.budget)
	}

	if t.meta.RiskLevel == "high" {
		// High-risk tools: journal the intent and block for human review,
		// so the session appears on -list-blocked and can be resolved with
		// -resolve-session.  The ledger holds the attempted tool and its
		// masked arguments.
		seq := int(g.seq.Add(1))
		callID := CallID(g.opts.ExecutionID, g.opts.Fence, seq)
		intent := CallIntent{
			TenantID:    g.opts.TenantID,
			ExecutionID: g.opts.ExecutionID,
			SessionPK:   g.opts.SessionPK,
			CallSeq:     seq,
			ToolID:      t.meta.ToolID,
			ToolName:    t.meta.Name,
			ToolVersion: t.meta.Version,
			ToolKind:    t.meta.Kind,
			SideEffect:  t.meta.SideEffect,
			Idempotent:  t.meta.Idempotent,
			ArgsHash:    HashArguments(jsonArgs),
			ArgsMasked:  MaskArguments(jsonArgs),
			TraceID:     g.opts.TraceID,
			WorkerID:    g.opts.WorkerID,
			Fence:       g.opts.Fence,
		}
		if jErr := g.opts.Journal.Begin(ctx, intent); jErr == nil {
			_ = g.opts.Journal.Finish(context.WithoutCancel(ctx), g.opts.TenantID, callID,
				Unknown, "approval_required", ErrApprovalRequired.Error(), 0, 0, 1)
		}
		g.block(BlockDecision{
			CallID:   callID,
			ToolName: t.meta.Name,
			Reason: fmt.Sprintf(
				"tool call %s (%s) requires human approval because it is high-risk",
				callID, t.meta.Name),
		})
		span.SetAttributes(attribute.String("tool.outcome", "awaiting_approval"),
			attribute.String("tool.call_id", callID))
		return nil, ErrApprovalRequired
	}

	if t.schema != nil {
		if err := t.schema.Validate(jsonArgs); err != nil {
			t.journalReject(ctx, seq, jsonArgs, "schema_invalid", err.Error())
			span.SetAttributes(attribute.String("tool.outcome", "rejected"))
			return nil, err
		}
	}

	if err := t.checkStillActive(ctx); err != nil {
		t.journalReject(ctx, seq, jsonArgs, "revoked", err.Error())
		span.SetAttributes(attribute.String("tool.outcome", "rejected"))
		return nil, err
	}

	callID := CallID(g.opts.ExecutionID, g.opts.Fence, seq)
	if err := g.opts.Journal.Begin(ctx, CallIntent{
		TenantID:    g.opts.TenantID,
		ExecutionID: g.opts.ExecutionID,
		SessionPK:   g.opts.SessionPK,
		CallSeq:     seq,
		ToolID:      t.meta.ToolID,
		ToolName:    t.meta.Name,
		ToolVersion: t.meta.Version,
		ToolKind:    t.meta.Kind,
		SideEffect:  t.meta.SideEffect,
		Idempotent:  t.meta.Idempotent,
		ArgsHash:    HashArguments(jsonArgs),
		ArgsMasked:  MaskArguments(jsonArgs),
		TraceID:     g.opts.TraceID,
		WorkerID:    g.opts.WorkerID,
		Fence:       g.opts.Fence,
	}); err != nil {
		// Fail closed: without the intent row the crash window is exactly
		// the one the ledger exists to close.
		span.RecordError(err)
		span.SetAttributes(attribute.String("tool.outcome", "journal_failed"))
		return nil, err
	}

	callCtx, cancel := context.WithTimeout(ctx, t.meta.Timeout)
	defer cancel()
	// The attempt recorder lets a callable that performs several physical
	// requests (the HTTP tool's one allowed retry) report each one to the
	// ledger without this package knowing which callable it wrapped.
	rec := &attemptRecorder{gov: g, callID: callID}
	callCtx = context.WithValue(callCtx, attemptCtxKey{}, rec)

	started := time.Now()
	result, callErr := t.inner.Call(callCtx, jsonArgs)
	latency := time.Since(started)

	outcome, errType, detail := classify(callErr)
	// The outcome must be written with a context that cannot be cancelled:
	// the run's cancellation (lease loss, shutdown) is precisely when the
	// journal matters most.
	jctx := context.WithoutCancel(ctx)
	resultBytes := 0
	if result != nil {
		if b, err := json.Marshal(result); err == nil {
			resultBytes = len(b)
		}
	}
	attempts := int(rec.count.Load())
	if attempts == 0 {
		attempts = 1
	}
	if err := g.opts.Journal.Finish(jctx, g.opts.TenantID, callID, outcome, errType, detail,
		resultBytes, int(latency.Milliseconds()), attempts); err != nil {
		// A journal that cannot record the outcome is a platform failure,
		// not a tool failure: block rather than let the model believe the
		// call concluded cleanly.
		g.block(BlockDecision{
			CallID:   callID,
			ToolName: t.meta.Name,
			Reason:   "tool call " + callID + " finished but the outcome could not be journaled: " + err.Error(),
		})
		span.RecordError(err)
		span.SetAttributes(attribute.String("tool.outcome", "journal_failed"))
		return nil, fmt.Errorf("tool: journal outcome for %s: %w", callID, err)
	}

	span.SetAttributes(
		attribute.String("tool.outcome", outcome.String()),
		attribute.String("tool.call_id", callID),
		attribute.Int64("tool.latency_ms", latency.Milliseconds()),
	)
	if outcome != Succeeded {
		span.SetStatus(codes.Error, errType)
	}

	if outcome == Unknown {
		g.block(BlockDecision{
			CallID:   callID,
			ToolName: t.meta.Name,
			Reason: fmt.Sprintf(
				"tool call %s (%s) outcome is unknown: %s; side effects cannot be ruled out, so the session is parked for human review",
				callID, t.meta.Name, truncate(detail, 200)),
		})
	}
	return result, callErr
}

// checkStillActive re-reads the binding before the call: a revocation that
// landed after the revision was assembled must stop the next call, not the
// next session.
func (t *governedTool) checkStillActive(ctx context.Context) error {
	if t.meta.ToolID == 0 {
		return nil
	}
	b, err := t.gov.scope.GetToolBinding(ctx, t.meta.ToolID)
	if err != nil {
		return fmt.Errorf("tool: cannot re-check binding %d for %s: %w", t.meta.ToolID, t.meta.Name, err)
	}
	if b.Status != "active" {
		return fmt.Errorf("tool: %s is %s and may not be called", t.meta.Name, b.Status)
	}
	if b.Version != t.meta.Version {
		return fmt.Errorf("tool: %s version moved from %d to %d; the pinned revision no longer matches",
			t.meta.Name, t.meta.Version, b.Version)
	}
	return nil
}

// journalReject records a call the platform refused before it ran. Rejections
// are evidence too — "the model tried and was stopped" is exactly what an
// operator auditing an incident asks first.
func (t *governedTool) journalReject(ctx context.Context, seq int, jsonArgs []byte, errType, detail string) {
	g := t.gov
	callID := CallID(g.opts.ExecutionID, g.opts.Fence, seq)
	intent := CallIntent{
		TenantID:    g.opts.TenantID,
		ExecutionID: g.opts.ExecutionID,
		SessionPK:   g.opts.SessionPK,
		CallSeq:     seq,
		ToolID:      t.meta.ToolID,
		ToolName:    t.meta.Name,
		ToolVersion: t.meta.Version,
		ToolKind:    t.meta.Kind,
		SideEffect:  t.meta.SideEffect,
		Idempotent:  t.meta.Idempotent,
		ArgsHash:    HashArguments(jsonArgs),
		ArgsMasked:  MaskArguments(jsonArgs),
		TraceID:     g.opts.TraceID,
		WorkerID:    g.opts.WorkerID,
		Fence:       g.opts.Fence,
	}
	// Best-effort by design: a rejection that cannot be journaled is still a
	// rejection, and the caller's error path is not the place to surface a
	// second, unrelated database problem.
	jctx := context.WithoutCancel(ctx)
	if err := g.opts.Journal.Begin(jctx, intent); err != nil {
		return
	}
	_ = g.opts.Journal.Reject(jctx, g.opts.TenantID, callID, errType, "rejected before calling: "+truncate(detail, 400))
}

// governorCtxKey carries the governor from the worker (which owns the claim)
// to the runner factory (which assembles tools). It is request-scoped state,
// not configuration — the same reason the framework passes its own wiring
// through contexts.
type governorCtxKey struct{}

// attemptCtxKey carries the per-call attempt recorder to a callable that
// makes several physical requests.
type attemptCtxKey struct{}

// attemptRecorder counts physical attempts and writes one ledger row each.
type attemptRecorder struct {
	gov    *Governor
	callID string
	count  atomic.Int32
}

func (r *attemptRecorder) record(ctx context.Context, httpStatus int, outcome Outcome, errType, detail string) {
	no := int(r.count.Add(1))
	jctx := context.WithoutCancel(ctx)
	_ = r.gov.opts.Journal.RecordAttempt(jctx, r.gov.opts.TenantID, r.callID, no,
		r.gov.opts.WorkerID, outcome, httpStatus, errType, detail)
}

// recordAttempt is the callable side of the recorder: a no-op outside the
// reliable path, so a tool stays usable in tests that do not wire a journal.
func recordAttempt(ctx context.Context, httpStatus int, outcome Outcome, errType, detail string) {
	if r, ok := ctx.Value(attemptCtxKey{}).(*attemptRecorder); ok {
		r.record(ctx, httpStatus, outcome, errType, detail)
	}
}

// WithGovernor attaches the per-execution governor.
func WithGovernor(ctx context.Context, g *Governor) context.Context {
	return context.WithValue(ctx, governorCtxKey{}, g)
}

// GovernorFrom returns the attached governor, or nil outside the reliable
// path. A nil governor means "this call did not come through a claim" —
// the legacy gateway path — and assembly refuses to build pinned tools
// without one.
func GovernorFrom(ctx context.Context) *Governor {
	g, _ := ctx.Value(governorCtxKey{}).(*Governor)
	return g
}
