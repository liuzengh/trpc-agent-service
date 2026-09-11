package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// RunnerFactory builds the agent runner for one claim, bound to that claim's
// private session workspace. It is a function value, not a method, so a test
// can hand the worker a fake model and a real deployment gets the real one
// without the loop knowing the difference.
type RunnerFactory func(ctx context.Context, c *Claim, ws session.Service) (runner.Runner, error)

// Worker runs claim/commit as a loop against one tenant's sessions.
type Worker struct {
	svc        *Service
	workerID   string
	makeRunner RunnerFactory
	idleWait   time.Duration
	log        *slog.Logger
}

// WorkerOptions configures NewWorker.
type WorkerOptions struct {
	// WorkerID identifies this process in leases and attempts. It must be
	// unique across replicas; the caller picks that up from hostname+pid or
	// equivalent, not from this package.
	WorkerID string
	// IdleWait is how long the loop sleeps when there is nothing to claim.
	IdleWait time.Duration
	// Log overrides the logger; nil uses slog.Default.
	Log *slog.Logger
}

// NewWorker builds a worker for one tenant's session queue.
func NewWorker(svc *Service, opts WorkerOptions, makeRunner RunnerFactory) *Worker {
	if opts.IdleWait <= 0 {
		opts.IdleWait = 250 * time.Millisecond
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &Worker{
		svc:        svc,
		workerID:   opts.WorkerID,
		makeRunner: makeRunner,
		idleWait:   opts.IdleWait,
		log:        opts.Log,
	}
}

// Run claims and executes until ctx is cancelled. It returns the context's
// own error, not a claim error: a claim that goes wrong is logged and the
// loop continues, because one bad session must not stop the worker from
// serving every other session it is idle-waiting on.
func (w *Worker) Run(ctx context.Context, tenantID string) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		claim, err := w.svc.ClaimNext(ctx, tenantID, w.workerID)
		switch {
		case errors.Is(err, ErrNothingToClaim):
			if !w.sleep(ctx) {
				return ctx.Err()
			}
			continue
		case err != nil:
			w.log.Error("execution: claim failed", "tenant", tenantID, "err", err)
			if !w.sleep(ctx) {
				return ctx.Err()
			}
			continue
		}

		if err := w.RunOne(ctx, claim); err != nil {
			w.log.Warn("execution: attempt ended without a commit",
				"tenant", tenantID, "session_pk", claim.SessionPK,
				"execution", claim.ExecutionID, "err", err)
		}
	}
}

func (w *Worker) sleep(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(w.idleWait):
		return true
	}
}

// RunOne owns one claim from renewal to commit. The renewal goroutine and the
// workspace's seal are the two things that keep a stalled or partitioned
// worker from ever believing it still owns a session it does not.
//
// Exported because the tenant sweep lives outside this package: Run's loop
// serves one tenant forever, and a process that serves several needs to
// decide for itself how to divide its attention between them.
func (w *Worker) RunOne(parent context.Context, claim *Claim) error {
	runCtx, cancel := context.WithCancel(parent)
	defer cancel()

	renewDone := make(chan struct{})
	go w.renewFor(runCtx, claim, renewDone)

	// The tool governor is bound to this claim's facts and consulted by the
	// runner factory through the context. Its recovery check runs before the
	// model does: a claim that follows a dead attempt must learn from the
	// ledger whether a side effect may already have happened, and an
	// unresolved write parks the execution instead of re-running it.
	gov, err := platformtool.NewGovernor(platformtool.GovernorOptions{
		Journal:     platformtool.NewJournal(w.svc.DB()),
		DB:          w.svc.DB(),
		TenantID:    claim.TenantID,
		ExecutionID: claim.ExecutionID,
		SessionPK:   claim.SessionPK,
		WorkerID:    claim.WorkerID,
		TraceID:     traceIDFrom(claim.Traceparent),
		Fence:       claim.FenceToken,
	})
	if err != nil {
		cancel()
		<-renewDone
		return fmt.Errorf("execution: build tool governor: %w", err)
	}
	gov.SetRunCancel(cancel)

	decision, err := gov.CheckRecovery(runCtx)
	if err != nil {
		// A ledger that cannot be read is a platform failure: running with an
		// unknown history is exactly what the recovery rule forbids.
		cancel()
		<-renewDone
		return fmt.Errorf("execution: tool recovery check: %w", err)
	}
	if decision != nil {
		cancel()
		<-renewDone
		return w.svc.BlockForReview(context.WithoutCancel(parent), claim, decision.Reason, "tool_recovery")
	}
	runCtx = platformtool.WithGovernor(runCtx, gov)

	workspace := claim.Workspace()

	runnerObj, err := w.makeRunner(runCtx, claim, workspace)
	if err != nil {
		cancel()
		<-renewDone
		return fmt.Errorf("execution: build runner: %w", err)
	}

	res, runErr := w.execute(runCtx, claim, runnerObj, workspace)

	// Whether or not the run succeeded, the lease work stops here: continuing
	// to renew a lease for a session whose outcome is already decided would
	// keep the fence alive past the point it means anything.
	cancel()
	<-renewDone

	if blocked := gov.Blocked(); blocked != nil {
		// A tool outcome went unknown mid-run, and the governor already
		// cancelled the context. The workspace is discarded: what the model
		// would have said after a maybe-happened side effect is not a fact.
		// The message waits at the head until a human disposes of the call.
		return w.svc.BlockForReview(context.WithoutCancel(parent), claim, blocked.Reason, "tool_unknown")
	}

	if runErr != nil {
		// A cancelled context with no other error is a lease-loss or a
		// shutdown, not a model failure: the correct action is to leave the
		// row alone for another worker (or this one's next attempt), not to
		// write a "the model failed" audit row that would be a lie.
		if errors.Is(runErr, ErrStaleFence) || errors.Is(runErr, sessionstore.ErrSealed) {
			return runErr
		}
		if errors.Is(runCtx.Err(), context.Canceled) && parent.Err() == nil {
			// This worker's own context (via renewFor noticing a lost lease)
			// stopped the run, not a failure inside the model.
			return runErr
		}
		res.Decision = "error"
		res.ErrorType = classify(runCtx, runErr)
		res.Detail = runErr.Error()
		res.Parts = []ReplyPart{{Text: failureText(res.ErrorType), IsDone: true}}
	}

	// Commit is attempted even when the run failed: the failure itself is a
	// real outcome worth persisting, and the reply the user should see ("服务
	// 暂时不可用") goes out through the same outbox path as a successful reply.
	if err := w.svc.Commit(context.WithoutCancel(parent), claim, res); err != nil {
		return fmt.Errorf("execution: commit: %w", err)
	}
	return nil
}

func (w *Worker) renewFor(ctx context.Context, claim *Claim, done chan<- struct{}) {
	defer close(done)
	interval := DefaultRenewInterval
	if claim.LeaseTTL > 0 && claim.LeaseTTL/3 < interval {
		interval = claim.LeaseTTL / 3
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Renew's own error is deliberately not propagated here: the only
			// way it can fail while the context is still live is a MySQL
			// blip, and the correct response is to try again next tick. The
			// authoritative check is Commit's conditional update, not this
			// loop noticing first.
			if err := w.svc.Renew(ctx, claim); err != nil && !errors.Is(err, ErrStaleFence) {
				w.log.Warn("execution: renew failed", "session_pk", claim.SessionPK, "err", err)
			}
		}
	}
}

// execute runs the agent to completion, draining the event stream and
// applying the output guardrail as text arrives, so a tripped wire changes
// what is committed rather than what has already been sent.
//
// Nothing is sent to the user from here. Every path — clean, input- or
// output-guardrail cut, or failed — ends in a Prepared workspace and a
// ReplyPart for Commit to enqueue: that is what makes "we decided the answer"
// and "we told the user" the same transaction, instead of the legacy
// behaviour where a partial stream could reach a user and then crash before
// anything was recorded.
func (w *Worker) execute(
	ctx context.Context,
	claim *Claim,
	r runner.Runner,
	ws *sessionstore.Workspace,
) (*Result, error) {
	// Input guardrails run before the model call, exactly as the legacy
	// gateway does it — same rule, same order, so a tenant cannot get
	// different protection depending on which mode their deployment runs in.
	if rule := guardrail.CheckInput(claim.Guardrails, claim.UserText); rule != "" {
		// Snapshot, not Seal: taking the snapshot is already what freezes
		// further writes (sessionstore does that itself, to stop a commit
		// retry from racing new activity). Sealing here would mean "this
		// workspace is broken", which is not what a clean guardrail rejection
		// is — it is a normal, successful outcome that simply never reached
		// the model.
		prepared, err := ws.Snapshot()
		if err != nil {
			return nil, err
		}
		return &Result{
			Prepared: prepared,
			Decision: "block",
			Detail:   "input rule: " + rule,
			Parts:    []ReplyPart{{Text: guardrail.RejectionText, IsDone: true}},
		}, nil
	}

	// calledModel is set once r.Run returns, whether or not it returned an
	// error itself: from here on, a model_call audit row is honest no matter
	// what happens next (agent error event, mid-stream cancellation, a
	// snapshot failure), and dropping it would leave an operator debugging a
	// timeout with no trace that the model was ever reached.
	calledModel := &Result{ModelCalled: true}

	events, err := r.Run(ctx, claim.ActorKey, SessionID(claim), model.NewUserMessage(claim.UserText))
	if err != nil {
		return calledModel, err
	}

	var (
		promptTokens, completionTokens int
		collected                      string
		tripped                        bool
		trippedRule                    string
		sc                             = guardrail.NewStreamChecker(claim.Guardrails)
	)
drain:
	for {
		select {
		case <-ctx.Done():
			break drain
		case ev, open := <-events:
			if !open {
				break drain
			}
			if ev.IsError() {
				return calledModel, fmt.Errorf("execution: agent error: %s", ev.Response.Error.Message)
			}
			if ev.Usage != nil {
				promptTokens += ev.Usage.PromptTokens
				completionTokens += ev.Usage.CompletionTokens
			}
			if ev.Response.Object != model.ObjectTypeChatCompletionChunk || len(ev.Response.Choices) == 0 {
				continue
			}
			text := ev.Response.Choices[0].Delta.Content
			if text == "" {
				continue
			}
			if kw := sc.Add(text); kw != "" {
				tripped = true
				trippedRule = kw
				// Stop forwarding, keep draining until the channel closes:
				// stopping early would leave the framework's own producer
				// goroutine blocked on a send nobody is reading.
				continue
			}
			collected += text
		}
	}

	if err := ctx.Err(); err != nil && !tripped {
		return calledModel, err
	}

	prepared, err := ws.Snapshot()
	if err != nil {
		return calledModel, err
	}

	notice := collected
	if tripped {
		notice = guardrail.CutoffText
	}
	res := &Result{
		Prepared:         prepared,
		Decision:         "ok",
		ModelCalled:      true,
		Latency:          time.Since(claim.claimedAt),
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
	}
	switch {
	case tripped:
		res.Decision = "block"
		res.Detail = "output keyword: " + trippedRule
		res.Parts = []ReplyPart{{Text: notice, IsDone: true}}
	case notice == "":
		// A run that produced no text at all is not a successful answer; an
		// empty reply is indistinguishable from a bug to a user, so it is
		// reported as one.
		res.Decision = "error"
		res.ErrorType = "empty_reply"
		res.Parts = []ReplyPart{{Text: failureText("empty_reply"), IsDone: true}}
	default:
		res.Parts = []ReplyPart{{Text: notice, IsDone: true}}
	}
	return res, nil
}

func classify(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	return "runner"
}

func failureText(errorType string) string {
	switch errorType {
	case "timeout":
		return "本次回复超时，请稍后重试。"
	default:
		return "服务暂时不可用，请稍后重试。"
	}
}

// ConfigForRevision resolves a published revision into the tenant.Context
// agent.NewRunner already understands, resolving every secret reference on
// the way. This is the only place in the reliable path that a plaintext
// credential exists in memory outside a resolved HTTP request, and it does
// not outlive this function's return value — callers must not log a
// tenant.Context.
func ConfigForRevision(
	ctx context.Context,
	scope controlplane.Scope,
	rev *controlplane.Revision,
	resolver *secrets.Resolver,
) (*tenant.Context, error) {
	modelProfile, err := scope.GetModelProfileByID(ctx, rev.Spec.ModelProfileID)
	if err != nil {
		return nil, fmt.Errorf("execution: model profile: %w", err)
	}
	apiKey, err := resolver.Resolve(modelProfile.APIKeyRef)
	if err != nil {
		return nil, fmt.Errorf("execution: resolve api key ref: %w", err)
	}

	t := &tenant.Context{
		ID: scope.TenantID(),
		Model: tenant.ModelConfig{
			Name:    modelProfile.ModelName,
			APIKey:  apiKey,
			BaseURL: modelProfile.BaseURL,
		},
		Instruction: rev.Spec.Instruction,
	}
	if len(rev.Spec.Guardrails) > 0 {
		if err := json.Unmarshal(rev.Spec.Guardrails, &t.Guardrails); err != nil {
			return nil, fmt.Errorf("execution: decode guardrails: %w", err)
		}
	}
	// rev.Spec.Tools is deliberately not decoded here. In reliable mode the
	// only source of tools is the revision's pinned list, resolved through
	// tool_bindings and wrapped by the governor (see DefaultRunnerFactory);
	// the legacy name-only allowlist has no path into this runner, because a
	// fallback is exactly how an ungoverned builtin would reappear.
	return t, nil
}

// runnerDeps are the optional collaborators the runner factory may need.
// They are options rather than parameters because most factories (and most
// tests) do not wire them at all, and a nil service must mean "this
// deployment does not have the capability" (a pin then fails assembly
// loudly) rather than a second code path.
type runnerDeps struct {
	knowledge *knowledge.Service
	memory    *memory.Service
	artifacts *artifact.Service
}

// RunnerOption configures DefaultRunnerFactory.
type RunnerOption func(*runnerDeps)

// WithKnowledge wires the knowledge service so a revision that pins
// knowledge_search can be assembled. Without it, such a revision fails
// assembly loudly (the pin has no implementation) instead of running with a
// silently missing tool.
func WithKnowledge(svc *knowledge.Service) RunnerOption {
	return func(d *runnerDeps) { d.knowledge = svc }
}

// WithMemory wires explicit memory (memory_write/search/delete).
func WithMemory(svc *memory.Service) RunnerOption {
	return func(d *runnerDeps) { d.memory = svc }
}

// WithArtifacts wires the artifact store (artifact_save).
func WithArtifacts(svc *artifact.Service) RunnerOption {
	return func(d *runnerDeps) { d.artifacts = svc }
}

// DefaultRunnerFactory is the real wiring: it resolves the revision this
// claim is fixed to, loads its model profile (resolving the secret
// reference), assembles the pinned tools through the governor, and hands
// the result to the same agent.NewRunner the legacy path uses. Reusing that
// function rather than a parallel one is what keeps
// "same behaviour, different source of truth" true: a change to how a Runner
// is built cannot silently apply to one path and not the other.
func DefaultRunnerFactory(cdp *controlplane.DB, resolver *secrets.Resolver, opts ...RunnerOption) RunnerFactory {
	deps := &runnerDeps{}
	for _, opt := range opts {
		opt(deps)
	}
	return func(ctx context.Context, c *Claim, ws session.Service) (runner.Runner, error) {
		scope, err := cdp.Scope(c.TenantID)
		if err != nil {
			return nil, err
		}
		rev, err := scope.GetRevisionByID(ctx, c.RevisionID)
		if err != nil {
			return nil, fmt.Errorf("execution: load fixed revision: %w", err)
		}
		t, err := ConfigForRevision(ctx, scope, rev, resolver)
		if err != nil {
			return nil, err
		}
		maxLLMCalls := rev.Spec.MaxLLMCalls
		if maxLLMCalls <= 0 {
			maxLLMCalls = config.DefaultMaxLLMCalls
		}

		// The request-scoped builtins: each exists only when the revision pins
		// it *and* this deployment configured its backing store — both halves
		// are facts of this claim, not of the binary.
		extras := map[string]frameworktool.CallableTool{}
		pinned, err := platformtool.ParseRevisionTools(rev.Spec.Tools)
		if err != nil {
			return nil, err
		}
		if pinsName(pinned, knowledge.Name) {
			if deps.knowledge == nil {
				return nil, fmt.Errorf("execution: revision %d pins %s but this deployment has knowledge disabled", rev.ID, knowledge.Name)
			}
			kbIDs, err := scope.KnowledgeBindingIDs(ctx, rev.ID)
			if err != nil {
				return nil, err
			}
			if len(kbIDs) == 0 {
				return nil, fmt.Errorf("execution: revision %d pins %s but binds no knowledge base", rev.ID, knowledge.Name)
			}
			extras[knowledge.Name] = knowledge.NewSearchTool(deps.knowledge, knowledge.Scope{
				TenantID:    c.TenantID,
				AppID:       rev.AppID,
				KBIDs:       kbIDs,
				ExecutionID: c.ExecutionID,
				SessionID:   SessionID(c),
				TraceID:     traceIDFrom(c.Traceparent),
				AgentName:   agent.AgentName,
			})
		}
		if pinsAnyMemoryTool(pinned) {
			if deps.memory == nil {
				return nil, fmt.Errorf("execution: revision %d pins a memory tool but this deployment has memory disabled", rev.ID)
			}
			for _, t := range memory.NewTools(deps.memory, memory.Scope{
				TenantID: c.TenantID,
				UserKey:  c.ActorKey,
				IsGroup:  c.IsGroup,
			}) {
				name := t.Declaration().Name
				if pinsName(pinned, name) {
					extras[name] = t
				}
			}
		}
		if pinsName(pinned, artifact.Name) {
			if deps.artifacts == nil {
				return nil, fmt.Errorf("execution: revision %d pins %s but this deployment has an artifact store disabled", rev.ID, artifact.Name)
			}
			extras[artifact.Name] = artifact.NewSaveTool(deps.artifacts, artifact.Scope{
				TenantID:    c.TenantID,
				ExecutionID: c.ExecutionID,
				SessionPK:   c.SessionPK,
			})
		}

		tools, err := platformtool.BuildPinned(ctx, scope, rev.AppID, rev.Spec.Tools, platformtool.GovernorFrom(ctx), resolver, extras)
		if err != nil {
			return nil, err
		}
		return agent.NewRunner(t, ws, maxLLMCalls, tools...)
	}
}

// pinsName reports whether a pinned list names a tool, without caring about
// the version: assembly resolves versions; this answers "did the revision
// ask for this family of capability at all".
func pinsName(pins []platformtool.PinnedTool, name string) bool {
	for _, p := range pins {
		if p.Name == name {
			return true
		}
	}
	return false
}

func pinsAnyMemoryTool(pins []platformtool.PinnedTool) bool {
	return pinsName(pins, memory.ToolWrite) || pinsName(pins, memory.ToolSearch) || pinsName(pins, memory.ToolDelete)
}
