// Package worker defines the stateless worker boundary for tenant jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"go.opentelemetry.io/otel/attribute"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	defaultEventSinkTimeout = 5 * time.Second
	defaultModelTimeout     = time.Minute
)

// ErrExecutionCanceled means a verified recall stopped the current run.
var ErrExecutionCanceled = errors.New("execution canceled by recall")

// CancellationCheck reads the authoritative canceled state for one execution.
// Recall itself owns the durable state transition; the worker only reads it.
type CancellationCheck func(context.Context, string, string, string) (bool, error)

// ExecutionLeaseValidator revalidates the durable execution lease at a
// side-effect boundary. Production implementations must check the current
// owner, run token, scope, and lease expiry in the authoritative store.
type ExecutionLeaseValidator interface {
	ValidateExecutionLease(context.Context, Execution, queue.Lease) error
}

// ExecutionUsage is the cumulative model usage known when an event is
// persisted. A terminal event can account this usage atomically with the
// execution status transition.
type ExecutionUsage struct {
	InputTokens  int
	OutputTokens int
	TotalTokens  int
	Cost         *float64
}

// Execution is the prepared context for running one tenant-scoped job.
type Execution struct {
	RequestID    string
	TenantSource gateway.TenantSource
	Tenant       tenant.RuntimeContext
	Config       tenant.AppConfig
	Message      gateway.Message
	PartitionKey string
	// TerminalStatus is set only on the terminal event passed to a durable
	// EventSink. The sink must persist it in the same transaction as that event.
	TerminalStatus queue.CompletionStatus
	// ApprovalPending marks a completion event that parks the execution for
	// human review. It must not close an IM stream as a successful answer.
	ApprovalPending bool
	Usage           ExecutionUsage
}

// SessionLock owns an acquired session partition lock. Context is canceled
// when the authoritative backend can no longer guarantee that the lock is
// held. Release must be called after the runner event channel is drained.
type SessionLock interface {
	Context() context.Context
	Release() error
}

// SessionLocker serializes runner execution for one session partition.
// Production workers must use an authoritative shared backend implementation.
type SessionLocker interface {
	Lock(ctx context.Context, partitionKey string) (SessionLock, error)
}

// EventSink receives runner events after the worker has started execution.
// Implementations must stop work and return when ctx is done.
type EventSink interface {
	HandleRunnerEvent(ctx context.Context, exec Execution, evt *event.Event) error
}

// RunResult summarizes one worker-owned runner execution.
type RunResult struct {
	Execution       Execution
	EventCount      int
	RunnerStarted   bool
	RunnerCompleted bool
	// ApprovalPending means the runner stopped at a durable human-review
	// boundary. The consumer must park the execution until the decision is
	// persisted; it must not mark this run successful.
	ApprovalPending bool
	ApprovalID      string
	InputTokens     int
	OutputTokens    int
	TotalTokens     int
	Cost            *float64
	// TerminalEventPersisted means the durable event sink committed a terminal
	// execution status. Quota cleanup may therefore happen in that transaction.
	TerminalEventPersisted bool
	// CleanupError is diagnostic only. Runner/session cleanup happens after the
	// business result has been durably projected and must not turn success into
	// a retryable execution failure.
	CleanupError error
}

type approvalContinuation struct {
	mu      sync.Mutex
	pending bool
	id      string
}

func (s *approvalContinuation) mark(id string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.pending = true
	if id != "" {
		s.id = id
	}
	s.mu.Unlock()
}

func (s *approvalContinuation) snapshot() (bool, string) {
	if s == nil {
		return false, ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending, s.id
}

// Worker prepares jobs for execution without owning session state locally.
type Worker struct {
	Config           config.Resolver
	Runner           func(context.Context, Execution) (runner.Runner, error)
	SessionLocker    SessionLocker
	Events           EventSink
	EventSinkTimeout time.Duration
	// ModelTimeout is an operator-owned upper bound for one Runner model
	// invocation path. The context is still drained and Runner.Close is called
	// after expiry.
	ModelTimeout time.Duration
	Cancellation CancellationCheck
	// LeaseValidator fences stale workers before Tool and Session side effects.
	// Production workers must attach the authoritative execution store.
	LeaseValidator ExecutionLeaseValidator
	Audit          platformaudit.Sink
	Metrics        *platformmetrics.Recorder
	// Approvals is optional for local/test runtimes. Production workers attach
	// the durable repository; without it review-required tools remain ASK.
	Approvals platformapproval.Repository
}

// New creates a Worker with all execution dependencies explicitly attached.
func New(
	configResolver config.Resolver,
	runnerBuilder func(context.Context, Execution) (runner.Runner, error),
	sessionLocker SessionLocker,
	events EventSink,
	cancellation CancellationCheck,
) *Worker {
	return &Worker{
		Config:        configResolver,
		Runner:        runnerBuilder,
		SessionLocker: sessionLocker,
		Events:        events,
		Cancellation:  cancellation,
	}
}

// Prepare validates a job and resolves the tenant backend_config.
func (w Worker) Prepare(ctx context.Context, job execution.Job) (Execution, error) {
	if err := job.Validate(); err != nil {
		return Execution{}, NewPermanentExecutionError(err)
	}
	partitionKey, err := job.PartitionKey()
	if err != nil {
		return Execution{}, NewPermanentExecutionError(err)
	}
	if w.Config == nil {
		return Execution{}, NewPermanentExecutionError(errors.New("config resolver is required"))
	}
	tenantContext := job.Tenant()
	cfg, err := w.Config.ResolveAppConfig(
		ctx,
		tenantContext.TenantID,
		tenantContext.AppID,
		tenantContext.ConfigVersion,
	)
	if err != nil {
		return Execution{}, err
	}
	if cfg.TenantID != tenantContext.TenantID ||
		cfg.AppID != tenantContext.AppID ||
		cfg.Version != tenantContext.ConfigVersion {
		return Execution{}, NewPermanentExecutionError(errors.New("resolved app config does not match job scope"))
	}
	requestID := job.RequestID()
	if w.Cancellation != nil {
		canceled, err := w.Cancellation(ctx, tenantContext.TenantID, tenantContext.AppID, requestID)
		if err != nil {
			return Execution{}, fmt.Errorf("check execution cancellation: %w", err)
		}
		if canceled {
			return Execution{}, NewPermanentExecutionError(ErrExecutionCanceled)
		}
	}
	return Execution{
		RequestID:    requestID,
		TenantSource: job.TenantSource(),
		Tenant:       tenantContext,
		Config:       cfg,
		Message:      job.Message(),
		PartitionKey: partitionKey,
	}, nil
}

// Run prepares a job, calls runner.Runner, and drains the returned event channel.
// After draining, it returns the first sink error or terminal runner error.
// It uses SessionPrincipalID as the runner user ID so group and thread messages
// share a session, while UserID remains available in runtime state as the sender.
func (w Worker) Run(ctx context.Context, job execution.Job) (result RunResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	exec, err := w.Prepare(ctx, job)
	if err != nil {
		return RunResult{}, err
	}
	result = RunResult{Execution: exec}
	approval := &approvalContinuation{}
	defer func() {
		result.ApprovalPending, result.ApprovalID = approval.snapshot()
	}()
	executionStartedAt := time.Now()
	if w.Metrics != nil {
		defer func() {
			pending, _ := approval.snapshot()
			resultLabel := "success"
			if pending {
				resultLabel = "waiting_approval"
			} else if err != nil {
				resultLabel = "failure"
			}
			w.Metrics.RecordExecution(ctx, platformmetrics.Labels{
				TenantID:      exec.Tenant.TenantID,
				AppID:         exec.Tenant.AppID,
				ConfigVersion: exec.Tenant.ConfigVersion,
				Channel:       exec.Tenant.Channel,
				Result:        resultLabel,
			}, time.Since(executionStartedAt), executionErrorType(err))
		}()
	}
	defer func() {
		if !errors.Is(err, tenant.ErrBudgetExceeded) {
			return
		}
		if exec.Config.Audit.Enabled {
			w.recordAudit(ctx, exec, platformaudit.Event{
				Decision:  "rejected",
				ErrorType: "budget_exceeded",
				EventType: platformaudit.BudgetRejected,
			})
		}
		if w.Metrics != nil {
			w.Metrics.RecordGovernanceRejected(ctx, platformmetrics.Labels{
				TenantID:      exec.Tenant.TenantID,
				AppID:         exec.Tenant.AppID,
				ConfigVersion: exec.Tenant.ConfigVersion,
				Channel:       exec.Tenant.Channel,
			}, platformaudit.BudgetRejected)
		}
	}()
	if exec.Config.Audit.Enabled && exec.Config.Audit.RecordExecutions {
		w.recordAudit(ctx, exec, platformaudit.Event{
			Decision:  "started",
			EventType: platformaudit.ExecutionStarted,
		})
		defer func() {
			eventType := platformaudit.ExecutionCompleted
			decision := "completed"
			if err != nil {
				eventType = platformaudit.ExecutionFailed
				decision = "failed"
			}
			w.recordAudit(ctx, exec, platformaudit.Event{
				Decision:     decision,
				EventType:    eventType,
				ErrorType:    executionErrorType(err),
				InputTokens:  result.InputTokens,
				OutputTokens: result.OutputTokens,
				TotalTokens:  result.TotalTokens,
				Cost:         result.Cost,
				Latency:      time.Since(executionStartedAt),
			})
		}()
	}
	if decision := guardrail.CheckInput(exec.Message.Text); decision.Blocked {
		cause := guardrail.InputBlockedError{RuleID: decision.RuleID}
		if exec.Config.Audit.Enabled {
			w.recordAudit(ctx, exec, platformaudit.Event{
				Decision:     "rejected",
				PolicyRuleID: decision.RuleID,
				PolicyReason: decision.Reason,
				ErrorType:    "guardrail_input_blocked",
				EventType:    platformaudit.ExecutionFailed,
			})
		}
		if persistErr := w.persistRunnerFailure(ctx, exec, cause, &result); persistErr != nil {
			return result, persistErr
		}
		return result, NewPermanentExecutionError(cause)
	}
	if w.Runner == nil {
		return result, NewPermanentExecutionError(errors.New("runner builder is required"))
	}
	message, err := w.runnerMessage(exec)
	if err != nil {
		return result, NewPermanentExecutionError(err)
	}
	appName, err := exec.Tenant.Scope().Key("runner")
	if err != nil {
		return result, NewPermanentExecutionError(err)
	}
	if w.SessionLocker == nil {
		return result, NewPermanentExecutionError(errors.New("session locker is required"))
	}
	lock, err := w.SessionLocker.Lock(ctx, exec.PartitionKey)
	if err != nil {
		return result, fmt.Errorf("lock session partition: %w", err)
	}
	if lock == nil {
		return result, NewPermanentExecutionError(errors.New("session lock is required"))
	}
	defer func() {
		if releaseErr := lock.Release(); releaseErr != nil {
			result.CleanupError = errors.Join(result.CleanupError, fmt.Errorf("release session partition: %w", releaseErr))
		}
	}()
	runCtx := lock.Context()
	if runCtx == nil {
		return result, NewPermanentExecutionError(errors.New("session lock context is required"))
	}
	runCtx = platformtelemetry.Extract(runCtx, map[string]string{
		"traceparent": exec.Tenant.TraceParent,
		"tracestate":  exec.Tenant.TraceState,
	})
	workerCtx, workerSpan := platformtelemetry.StartSpan(runCtx, "worker.execute",
		attribute.String("tenant_id", exec.Tenant.TenantID),
		attribute.String("app_id", exec.Tenant.AppID),
		attribute.String("config_version", exec.Tenant.ConfigVersion),
		attribute.String("request_id", exec.RequestID),
		attribute.String("channel", exec.Tenant.Channel),
	)
	defer workerSpan.End()
	modelAttempted := false
	defer func() {
		if !modelAttempted || w.Metrics == nil {
			return
		}
		result.Cost = w.Metrics.EstimateCost(exec.Config.Model.Provider, exec.Config.Model.Model, result.InputTokens, result.OutputTokens)
	}()
	r, err := w.Runner(workerCtx, exec)
	if err != nil {
		if FinalAttemptFromContext(ctx) || IsPermanentExecutionError(err) {
			if persistErr := w.persistRunnerFailure(runCtx, exec, err, &result); persistErr != nil {
				return result, persistErr
			}
		}
		return result, classifySessionLeaseFailure(runCtx, result.RunnerStarted, err)
	}
	if r == nil {
		return result, NewPermanentExecutionError(errors.New("runner is required"))
	}
	defer func() {
		if closeErr := r.Close(); closeErr != nil {
			result.CleanupError = errors.Join(result.CleanupError, fmt.Errorf("close runner: %w", closeErr))
		}
	}()
	runnerCtx, runnerSpan := platformtelemetry.StartSpan(workerCtx, "runner.run",
		attribute.String("tenant_id", exec.Tenant.TenantID),
		attribute.String("app_id", exec.Tenant.AppID),
		attribute.String("config_version", exec.Tenant.ConfigVersion),
		attribute.String("request_id", exec.RequestID),
	)
	defer runnerSpan.End()
	runnerCtx, cancelModel := context.WithTimeout(runnerCtx, w.modelTimeout())
	defer cancelModel()
	modelAttempted = true
	result.RunnerStarted = true
	// Register cancellation before Run: a managed runner may block while it
	// starts a model request, and that request must still be canceled on the
	// configured deadline.
	stopManagedCancel := cancelManagedRunnerOnContextDone(runnerCtx, r, exec.RequestID)
	defer stopManagedCancel()
	events, err := r.Run(
		runnerCtx,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		message,
		agent.WithRequestID(exec.RequestID),
		agent.WithAppName(appName),
		agent.MergeRuntimeState(runnerRuntimeState(exec)),
		agent.WithToolPermissionPolicy(w.toolPermissionPolicyWithState(exec, approval)),
		// Framework payload tracing is disabled at this boundary because this
		// service owns the safe metadata-only spans above. It prevents raw
		// prompts, tool arguments, and provider errors from entering spans.
		agent.WithDisableTracing(true),
	)
	if err != nil {
		if persistErr := w.persistRunnerFailure(runCtx, exec, err, &result); persistErr != nil {
			return result, persistErr
		}
		return result, classifySessionLeaseFailure(runCtx, result.RunnerStarted, err)
	}
	if events == nil {
		runnerErr := errors.New("runner event channel is nil")
		if persistErr := w.persistRunnerFailure(runCtx, exec, runnerErr, &result); persistErr != nil {
			return result, persistErr
		}
		return result, NewSideEffectUncertainError(runnerErr)
	}
	var sinkErr error
	var runnerErr error
	terminalEventPersisted := false
	approvalBoundary := false
	var drainTimer *time.Timer
	var drainDone <-chan time.Time
	runnerDone := runnerCtx.Done()
	beginDrain := func() {
		if drainTimer != nil {
			return
		}
		drainTimer = time.NewTimer(w.eventSinkTimeout())
		drainDone = drainTimer.C
	}
	defer func() {
		if drainTimer != nil {
			drainTimer.Stop()
		}
	}()
drainLoop:
	for {
		var evt *event.Event
		var ok bool
		select {
		case <-drainDone:
			break drainLoop
		case <-runnerDone:
			// Any runner cancellation can leave an event channel open when
			// the underlying model or tool does not cooperate. Drain only
			// for the same bounded interval used after sink failure.
			runnerDone = nil
			beginDrain()
		case evt, ok = <-events:
			if !ok {
				break drainLoop
			}
		}
		if evt == nil {
			continue
		}
		result.EventCount++
		if evt.IsRunnerCompletion() {
			result.RunnerCompleted = true
		}
		// Once a review-required tool has produced its durable tool result,
		// this attempt is parked. Do not persist any later model completion:
		// it is not a client-visible result and would be replayed as a false
		// success while the execution is WAITING_APPROVAL.
		if approvalBoundary && !evt.IsRunnerCompletion() {
			continue
		}
		if runnerErr == nil && evt.IsTerminalError() {
			runnerErr = fmt.Errorf("runner event: %w", evt.Error)
		}
		w.accumulateUsage(&result, evt)
		if modelAttempted && w.Metrics != nil {
			result.Cost = w.Metrics.EstimateCost(
				exec.Config.Model.Provider,
				exec.Config.Model.Model,
				result.InputTokens,
				result.OutputTokens,
			)
		}
		if w.Events == nil || sinkErr != nil || runnerCtx.Err() != nil {
			continue
		}
		eventExec := exec
		eventExec.Usage = executionUsage(result)
		approvalPending, _ := approval.snapshot()
		eventExec.ApprovalPending = approvalPending
		if !approvalPending {
			switch {
			case evt.IsTerminalError():
				eventExec.TerminalStatus = queue.CompletionFailed
			case evt.IsRunnerCompletion():
				eventExec.TerminalStatus = queue.CompletionSucceeded
			}
		}
		if err := w.handleRunnerEvent(runnerCtx, eventExec, evt); err != nil {
			if sinkErr == nil {
				sinkErr = err
				// Event persistence is part of the execution safety boundary.
				// Stop the runner before it can issue another model or tool call,
				// then drain already-produced events for a bounded period.
				cancelModel()
				beginDrain()
			}
		} else if eventExec.TerminalStatus != "" {
			terminalEventPersisted = true
			result.TerminalEventPersisted = true
		}
		approvalPending, _ = approval.snapshot()
		if approvalPending && evt.Response != nil && evt.Response.IsToolResultResponse() {
			approvalBoundary = true
		}
	}
	if sinkErr != nil {
		// The runner may already have executed a side-effecting tool when event
		// persistence failed. Do not let Consumer turn a projection failure into
		// a false FAILED result or blindly replay the whole execution.
		return result, NewSideEffectUncertainError(sinkErr)
	}
	if err := runnerCtx.Err(); err != nil {
		if !terminalEventPersisted {
			if persistErr := w.persistRunnerFailure(runCtx, exec, err, &result); persistErr != nil {
				return result, persistErr
			}
		}
		return result, classifySessionLeaseFailure(runCtx, result.RunnerStarted, err)
	}
	if runnerErr != nil {
		if !terminalEventPersisted {
			if persistErr := w.persistRunnerFailure(runCtx, exec, runnerErr, &result); persistErr != nil {
				return result, persistErr
			}
		}
		return result, runnerErr
	}
	approvalPending, _ := approval.snapshot()
	if !result.RunnerCompleted && !approvalPending {
		missingCompletion := errors.New("runner completed without completion event")
		if !terminalEventPersisted {
			if persistErr := w.persistRunnerFailure(runCtx, exec, missingCompletion, &result); persistErr != nil {
				return result, persistErr
			}
		}
		return result, missingCompletion
	}
	return result, nil
}

func (w Worker) persistRunnerFailure(ctx context.Context, exec Execution, cause error, result *RunResult) error {
	if w.Events == nil || cause == nil {
		return nil
	}
	if !shouldPersistRunnerFailure(cause) {
		return nil
	}
	if ctx != nil && errors.Is(context.Cause(ctx), ErrSessionLeaseLost) {
		return nil
	}
	if ctx != nil && errors.Is(context.Cause(ctx), context.Canceled) &&
		!errors.Is(cause, ErrExecutionCanceled) {
		return nil
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.eventSinkTimeout())
	defer cancel()
	errorType := "runner_failure"
	errorMessage := "runner execution failed"
	if errors.Is(cause, ErrUnsupportedAttachment) {
		errorType = unsupportedAttachmentErrorType
		errorMessage = "model attachment capability is unsupported"
	} else if errors.Is(cause, context.DeadlineExceeded) {
		errorType = "execution_timeout"
		errorMessage = "execution timed out"
	} else if errors.Is(cause, context.Canceled) || errors.Is(cause, ErrExecutionCanceled) {
		errorType = "execution_canceled"
		errorMessage = "execution canceled"
	} else if errors.Is(cause, guardrail.ErrInputBlocked) {
		errorType = "guardrail_input_blocked"
		errorMessage = "input rejected by safety policy"
	}
	synthetic := event.NewErrorEvent(exec.RequestID, "platform", errorType, errorMessage)
	synthetic.RequestID = exec.RequestID
	eventExec := exec
	if result != nil {
		eventExec.Usage = executionUsage(*result)
	}
	if IsSideEffectUncertainError(cause) {
		eventExec.TerminalStatus = queue.CompletionUncertain
	} else {
		eventExec.TerminalStatus = queue.CompletionFailed
	}
	if err := w.handleRunnerEvent(persistCtx, eventExec, synthetic); err != nil {
		return NewSideEffectUncertainError(fmt.Errorf("persist terminal runner failure: %w", err))
	}
	if result != nil {
		result.TerminalEventPersisted = true
	}
	return nil
}

func executionUsage(result RunResult) ExecutionUsage {
	return ExecutionUsage{
		InputTokens:  result.InputTokens,
		OutputTokens: result.OutputTokens,
		TotalTokens:  result.TotalTokens,
		Cost:         result.Cost,
	}
}

func shouldPersistRunnerFailure(cause error) bool {
	if cause == nil || IsSideEffectUncertainError(cause) {
		return true
	}
	if !IsRetryableExecutionError(cause) {
		return true
	}
	// Timeout/cancellation and explicit user cancellation close the current
	// stream. A separately classified retryable infrastructure error keeps the
	// execution retryable and must not be converted into a terminal event.
	return errors.Is(cause, context.DeadlineExceeded) ||
		errors.Is(cause, context.Canceled) ||
		errors.Is(cause, ErrExecutionCanceled)
}

func classifySessionLeaseFailure(ctx context.Context, runnerStarted bool, err error) error {
	if err == nil || ctx == nil || !errors.Is(context.Cause(ctx), ErrSessionLeaseLost) {
		return err
	}
	cause := fmt.Errorf("%w: %w", ErrSessionLeaseLost, err)
	if runnerStarted {
		return NewSideEffectUncertainError(cause)
	}
	return NewRetryableExecutionError(cause)
}

func (w Worker) modelTimeout() time.Duration {
	if w.ModelTimeout > 0 {
		return w.ModelTimeout
	}
	return defaultModelTimeout
}

func cancelManagedRunnerOnContextDone(
	ctx context.Context,
	r runner.Runner,
	requestID string,
) func() {
	managed, ok := r.(runner.ManagedRunner)
	if !ok || requestID == "" {
		return func() {}
	}
	stop := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(finished)
		select {
		case <-ctx.Done():
			managed.Cancel(requestID)
		case <-stop:
		}
	}()
	return func() {
		once.Do(func() {
			close(stop)
			<-finished
		})
	}
}

func (w Worker) toolPermissionPolicy(exec Execution) frameworktool.PermissionPolicy {
	return w.toolPermissionPolicyWithState(exec, nil)
}

func (w Worker) validateExecutionLease(ctx context.Context, exec Execution) error {
	if w.LeaseValidator == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	lease, ok := JobLeaseFromContext(ctx)
	if !ok {
		return fmt.Errorf("execution lease is missing: %w", queue.ErrLeaseLost)
	}
	if err := w.LeaseValidator.ValidateExecutionLease(ctx, exec, lease); err != nil {
		return fmt.Errorf("validate execution lease: %w", err)
	}
	return nil
}

func (w Worker) toolPermissionPolicyWithState(exec Execution, approval *approvalContinuation) frameworktool.PermissionPolicy {
	return frameworktool.PermissionPolicyFunc(func(
		ctx context.Context,
		request *frameworktool.PermissionRequest,
	) (frameworktool.PermissionDecision, error) {
		if err := w.validateExecutionLease(ctx, exec); err != nil {
			return frameworktool.PermissionDecision{}, err
		}
		started := time.Now()
		name := permissionToolName(request)
		if err := platformtool.AuthorizeExecution(exec.Config.Tools, name); err != nil {
			decision := frameworktool.DenyPermission("tool is not authorized")
			w.recordToolDecision(ctx, exec, name, decision, started)
			return decision, nil
		}
		if exec.Config.Tools.RequiresReview(name) {
			decision, approvalID := w.reviewDecisionWithID(ctx, exec, request, name)
			if decision.Action == frameworktool.PermissionActionAsk {
				approval.mark(approvalID)
			}
			w.recordToolDecision(ctx, exec, name, decision, started)
			return decision, nil
		}
		decision, err := frameworktool.NormalizePermissionDecision(frameworktool.AllowPermission())
		if err != nil {
			return frameworktool.PermissionDecision{}, err
		}
		w.recordToolDecision(ctx, exec, name, decision, started)
		return decision, nil
	})
}

func (w Worker) reviewDecision(
	ctx context.Context,
	exec Execution,
	request *frameworktool.PermissionRequest,
	name string,
) frameworktool.PermissionDecision {
	decision, _ := w.reviewDecisionWithID(ctx, exec, request, name)
	return decision
}

func (w Worker) reviewDecisionWithID(
	ctx context.Context,
	exec Execution,
	request *frameworktool.PermissionRequest,
	name string,
) (frameworktool.PermissionDecision, string) {
	if w.Approvals == nil {
		return frameworktool.AskPermission("human review is required"), ""
	}
	toolCallID := ""
	var arguments []byte
	if request != nil {
		toolCallID = request.ToolCallID
		arguments = request.Arguments
	}
	record, err := w.Approvals.ResolveOrCreate(ctx, platformapproval.Request{
		TenantID:       exec.Tenant.TenantID,
		AppID:          exec.Tenant.AppID,
		ConfigVersion:  exec.Tenant.ConfigVersion,
		RequestID:      exec.RequestID,
		SessionID:      exec.Tenant.SessionID,
		ToolName:       name,
		ToolCallID:     toolCallID,
		ArgumentDigest: platformapproval.DigestArguments(arguments),
		ExpiresAt:      time.Now().UTC().Add(platformapproval.DefaultTTL),
	})
	if err != nil {
		// Fail closed when the durable approval store is unavailable. Returning
		// an ASK here would allow a caller to mistake an unavailable control
		// plane for an approval.
		return frameworktool.DenyPermission("human approval is unavailable"), ""
	}
	switch record.Status {
	case platformapproval.StatusApproved:
		return frameworktool.AllowPermission(), ""
	case platformapproval.StatusDenied:
		return frameworktool.DenyPermission("human approval was denied"), ""
	case platformapproval.StatusExpired:
		return frameworktool.DenyPermission("human approval expired"), ""
	default:
		return frameworktool.AskPermission("human review is required"), record.ApprovalID
	}
}

func permissionToolName(request *frameworktool.PermissionRequest) string {
	if request == nil {
		return ""
	}
	if request.ToolName != "" {
		return request.ToolName
	}
	if request.Declaration != nil {
		return request.Declaration.Name
	}
	return ""
}

func (w Worker) handleRunnerEvent(ctx context.Context, exec Execution, evt *event.Event) error {
	sinkCtx, cancel := context.WithTimeout(ctx, w.eventSinkTimeout())
	defer cancel()

	err := w.Events.HandleRunnerEvent(sinkCtx, exec, evt)
	if sinkCtx.Err() != nil {
		return fmt.Errorf("event sink timeout: %w", sinkCtx.Err())
	}
	return err
}

func (w Worker) eventSinkTimeout() time.Duration {
	if w.EventSinkTimeout > 0 {
		return w.EventSinkTimeout
	}
	return defaultEventSinkTimeout
}

func (w Worker) runnerMessage(exec Execution) (model.Message, error) {
	message := exec.Message
	result := model.NewUserMessage(message.Text)
	if len(message.ArtifactRefs) == 0 {
		return result, nil
	}
	if exec.Config.BackendConfig.Artifact.IsZero() {
		return model.Message{}, errors.New("artifact backend is required for inbound artifacts")
	}
	for index, ref := range message.ArtifactRefs {
		_, version, err := gateway.ParseArtifactRef(ref)
		if err != nil {
			return model.Message{}, fmt.Errorf("artifact ref %d: %w", index, err)
		}
		contentRef := &model.ContentRef{ArtifactRef: ref, ArtifactVersion: version}
		result.ContentParts = append(result.ContentParts, model.ContentPart{
			Type:       model.ContentTypeFile,
			ContentRef: contentRef,
		})
	}
	return result, nil
}

func runnerRuntimeState(exec Execution) map[string]any {
	state := map[string]any{
		"tenant_id":            exec.Tenant.TenantID,
		"app_id":               exec.Tenant.AppID,
		"config_version":       exec.Tenant.ConfigVersion,
		"request_id":           exec.RequestID,
		"session_id":           exec.Tenant.SessionID,
		"session_principal_id": exec.Tenant.SessionPrincipalID,
		"user_id":              exec.Tenant.UserID,
	}
	if exec.Tenant.TraceID != "" {
		state["trace_id"] = exec.Tenant.TraceID
	}
	if exec.Tenant.Channel != "" {
		state["channel"] = exec.Tenant.Channel
	}
	if exec.Tenant.BindingID != "" {
		state["binding_id"] = exec.Tenant.BindingID
	}
	return state
}
