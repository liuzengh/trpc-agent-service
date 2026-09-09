// Package execution coordinates one session-scoped Agent execution.
package execution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrInvalidRequest       = errors.New("execution: invalid request")
	ErrMissingDependency    = errors.New("execution: missing dependency")
	ErrAgentVersionMismatch = errors.New("execution: agent version does not match tenant configuration")
	ErrExecutionFailed      = errors.New("execution failed")
)

type ExecutionState string

const (
	StateSucceeded        ExecutionState = "succeeded"
	StateCanceled         ExecutionState = "canceled"
	StateRetryableFailure ExecutionState = "retryable_failure"
	StatePermanentFailure ExecutionState = "permanent_failure"
	StateLeaseLost        ExecutionState = "lease_lost"
)

type FailureClass string

const (
	FailureNone      FailureClass = "none"
	FailureCanceled  FailureClass = "canceled"
	FailureRetryable FailureClass = "retryable_failure"
	FailurePermanent FailureClass = "permanent_failure"
	FailureLeaseLost FailureClass = "lease_lost"
)

// ExecutionError preserves the underlying Lease, Runtime, Context, Commit, or
// Release error while exposing the state classification to a caller.
type ExecutionError struct {
	Class FailureClass
	Err   error
}

func (e *ExecutionError) Error() string {
	if e == nil || e.Err == nil {
		return "execution: unknown failure"
	}
	return fmt.Sprintf("execution: %s: %v", e.Class, e.Err)
}

func (e *ExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ExecutionRequest contains the verified Job and the immutable AgentSpec
// resolved by the caller. The Queue context is deliberately absent.
type ExecutionRequest struct {
	Job            queue.AgentJob
	Agent          agent.AgentSpec
	History        []agent.Message
	OwnerID        string
	LeaseTTL       time.Duration
	RenewInterval  time.Duration
	ReleaseTimeout time.Duration
}

type ExecutionResult struct {
	State       ExecutionState
	Failure     FailureClass
	Lease       storage.Lease
	AgentResult agent.AgentResult
	CommitCount int
	Err         error
}

// ExecutionCommit is the complete fenced commit input. Implementations must
// re-check all lease fields at their own commit boundary before writing facts.
type ExecutionCommit struct {
	JobID         string
	ExecutionID   string
	TenantID      string
	SessionID     string
	OwnerID       string
	Epoch         storage.Epoch
	FenceToken    uint64
	Job           queue.AgentJob
	TenantContext tenant.TenantContext
	Agent         agent.AgentSpec
	Input         agent.AgentInput
	Result        agent.AgentResult
	Lease         storage.Lease
	Trace         ExecutionContextInfo
}

// FencedExecutionSink is the only commit boundary available to Executor.
// Implementations must atomically validate tenant, session, owner, epoch,
// fence token, and lease expiry with their durable write.
type FencedExecutionSink interface {
	Commit(context.Context, ExecutionCommit) error
}

// FencedCompletionFunc is invoked after execution fencing validation and before
// Executor reports success. The callback owns the durable completion boundary.
type FencedCompletionFunc func(context.Context, ExecutionCommit) error

type Executor struct {
	Leases storage.LeaseStore
	Agents agent.AgentFactory
	Sink   FencedExecutionSink

	// beforeCommit is a test seam for exercising the Validate/Commit window.
	beforeCommit func()
}

func NewExecutor(leases storage.LeaseStore, agents agent.AgentFactory, sink FencedExecutionSink) (*Executor, error) {
	if leases == nil || agents == nil || sink == nil {
		return nil, ErrMissingDependency
	}
	return &Executor{Leases: leases, Agents: agents, Sink: sink}, nil
}

type contextKey uint8

const executionContextKey contextKey = iota

// ExecutionContextInfo contains only correlation data and is safe to attach to
// a request-scoped context. It never carries the Queue context itself.
type ExecutionContextInfo struct {
	JobID       string
	ExecutionID string
	RequestID   string
	MessageID   string
	TraceID     string
	SessionID   string
}

func ContextInfoFrom(ctx context.Context) (ExecutionContextInfo, bool) {
	info, ok := ctx.Value(executionContextKey).(ExecutionContextInfo)
	return info, ok
}

func TenantContextFrom(ctx context.Context) (tenant.TenantContext, bool) {
	return tenant.FromContext(ctx)
}

func (e *Executor) Execute(parent context.Context, request ExecutionRequest) (ExecutionResult, error) {
	if e == nil || e.Sink == nil {
		return failureResult(StatePermanentFailure, FailurePermanent, ErrMissingDependency)
	}
	return e.execute(parent, request, e.Sink.Commit)
}

// ExecuteWithCompletion runs the same lease/runtime/fencing protocol as
// Execute, but delegates the final durable action to completion. It is used by
// the PostgreSQL Worker path and has no fallback to Execute's legacy sink.
func (e *Executor) ExecuteWithCompletion(parent context.Context, request ExecutionRequest, completion FencedCompletionFunc) (ExecutionResult, error) {
	if e == nil || completion == nil {
		return failureResult(StatePermanentFailure, FailurePermanent, ErrMissingDependency)
	}
	return e.execute(parent, request, completion)
}

func (e *Executor) execute(parent context.Context, request ExecutionRequest, completion FencedCompletionFunc) (result ExecutionResult, returnedErr error) {
	if parent == nil {
		return failureResult(StatePermanentFailure, FailurePermanent, ErrInvalidRequest)
	}
	if err := request.validate(parent); err != nil {
		return failureResult(classifyState(err), classifyFailure(err), err)
	}
	tc, err := request.Job.Tenant.Restore()
	if err != nil {
		return failureResult(StatePermanentFailure, FailurePermanent, err)
	}

	executionCtx, cancel := newExecutionContext(parent, request.Job, tc)
	defer cancel()
	lease, err := e.Leases.Acquire(executionCtx, tc, tc.SessionID, request.OwnerID, request.LeaseTTL)
	if err != nil {
		return failureResult(classifyState(err), classifyFailure(err), err)
	}
	result.Lease = lease
	acquired := true
	defer func() {
		if !acquired {
			return
		}
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(parent), request.releaseTimeout())
		defer releaseCancel()
		releaseErr := e.Leases.Release(releaseCtx, tc, result.Lease)
		if releaseErr != nil {
			if returnedErr == nil {
				returnedErr = &ExecutionError{Class: FailureRetryable, Err: errors.Join(ErrExecutionFailed, releaseErr)}
				result.State = StateRetryableFailure
				result.Failure = FailureRetryable
			} else {
				returnedErr = errors.Join(returnedErr, releaseErr)
			}
		}
		result.Err = returnedErr
	}()

	runtimeCtx, runtimeCancel := context.WithCancel(executionCtx)
	defer runtimeCancel()
	var renewMu sync.Mutex
	var renewFailure error
	renewal, err := storage.NewLeaseRenewalRunner(executionCtx, e.Leases, tc, lease, request.LeaseTTL, request.renewInterval(), func(renewErr error) {
		renewMu.Lock()
		if renewFailure == nil {
			renewFailure = renewErr
		}
		renewMu.Unlock()
		runtimeCancel()
	})
	if err != nil {
		return e.failAfterAcquire(executionCtx, parent, tc, request, result, err)
	}
	if err := renewal.Start(); err != nil {
		return e.failAfterAcquire(executionCtx, parent, tc, request, result, err)
	}

	runtime, buildErr := e.Agents.Build(runtimeCtx, tc, request.Agent)
	if buildErr != nil {
		stopErr := renewal.Stop()
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, buildErr, stopErr)
	}
	input := agent.AgentInput{
		TenantContext: tc,
		Agent:         cloneAgentSpec(request.Agent),
		History:       cloneMessages(request.History),
		Input:         messageFromDTO(request.Job.Message),
	}
	agentResult, runErr := runtime.Run(runtimeCtx, input)
	stopErr := renewal.Stop()
	renewMu.Lock()
	renewErr := renewFailure
	renewMu.Unlock()
	result.AgentResult = agentResult

	if renewErr != nil {
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, renewErr, errors.Join(runErr, stopErr))
	}
	if stopErr != nil && !errors.Is(stopErr, context.Canceled) {
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, stopErr, runErr)
	}
	if err := contextFailure(executionCtx, runErr); err != nil {
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, err, runErr)
	}
	if runErr != nil {
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, runErr, nil)
	}

	if err := validateAcquiredLease(lease, tc, request); err != nil {
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, err, nil)
	}
	if err := executionCtx.Err(); err != nil {
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, err, nil)
	}
	latest := renewal.Lease()
	result.Lease = latest
	if err := e.Leases.Validate(executionCtx, tc, latest); err != nil {
		runtimeCancel()
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, err, nil)
	}
	if err := executionCtx.Err(); err != nil {
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, err, nil)
	}
	if e.beforeCommit != nil {
		e.beforeCommit()
	}
	commit := ExecutionCommit{
		JobID: request.Job.JobID, ExecutionID: request.Job.ExecutionID,
		TenantID: tc.TenantID, SessionID: tc.SessionID, OwnerID: latest.OwnerID,
		Epoch: latest.Epoch, FenceToken: latest.FenceToken,
		Job: request.Job, TenantContext: tc, Agent: cloneAgentSpec(request.Agent),
		Input: input, Result: agentResult, Lease: latest,
		Trace: contextInfo(request.Job, tc),
	}
	if err := completion(executionCtx, commit); err != nil {
		return e.failAfterRuntime(executionCtx, parent, tc, request, result, err, nil)
	}
	result.State = StateSucceeded
	result.Failure = FailureNone
	result.CommitCount = 1
	return result, nil
}

func (e *Executor) failAfterAcquire(_ context.Context, _ context.Context, _ tenant.TenantContext, _ ExecutionRequest, result ExecutionResult, err error) (ExecutionResult, error) {
	return failureWithLease(result, err)
}

func (e *Executor) failAfterRuntime(_ context.Context, _ context.Context, _ tenant.TenantContext, _ ExecutionRequest, result ExecutionResult, primary error, secondary error) (ExecutionResult, error) {
	if secondary != nil && !errors.Is(secondary, context.Canceled) {
		primary = errors.Join(primary, secondary)
	}
	return failureWithLease(result, primary)
}

func failureWithLease(result ExecutionResult, err error) (ExecutionResult, error) {
	result.State = classifyState(err)
	result.Failure = classifyFailure(err)
	result.Err = err
	return result, &ExecutionError{Class: result.Failure, Err: err}
}

func failureResult(state ExecutionState, failure FailureClass, err error) (ExecutionResult, error) {
	result := ExecutionResult{State: state, Failure: failure, Err: err}
	return result, &ExecutionError{Class: failure, Err: err}
}

func (r ExecutionRequest) validate(parent context.Context) error {
	if err := parent.Err(); err != nil {
		return err
	}
	if r.LeaseTTL <= 0 || strings.TrimSpace(r.OwnerID) == "" {
		return fmt.Errorf("%w: owner and positive lease TTL are required", ErrInvalidRequest)
	}
	if err := r.Job.Validate(); err != nil {
		return err
	}
	tc, err := r.Job.Tenant.Restore()
	if err != nil {
		return err
	}
	if r.Agent.TenantID != tc.TenantID || r.Agent.AgentAppID != tc.AgentAppID || r.Agent.Version != r.Job.Agent.Version {
		return fmt.Errorf("%w: AgentSpec and AgentRef differ", ErrInvalidRequest)
	}
	if r.Agent.Version != tc.ConfigVersion {
		return fmt.Errorf("%w: spec=%d config=%d", ErrAgentVersionMismatch, r.Agent.Version, tc.ConfigVersion)
	}
	return nil
}

func validateAcquiredLease(lease storage.Lease, tc tenant.TenantContext, request ExecutionRequest) error {
	if lease.TenantID != tc.TenantID || lease.SessionID != tc.SessionID || lease.ResourceID != tc.SessionID || lease.OwnerID != request.OwnerID || lease.FenceToken == 0 || lease.Epoch == 0 || lease.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: acquired lease fields are invalid", ErrInvalidRequest)
	}
	return nil
}

func newExecutionContext(parent context.Context, job queue.AgentJob, tc tenant.TenantContext) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(parent, job.Deadline)
	ctx = tenant.WithContext(ctx, tc)
	ctx = context.WithValue(ctx, executionContextKey, contextInfo(job, tc))
	return ctx, cancel
}

func contextInfo(job queue.AgentJob, tc tenant.TenantContext) ExecutionContextInfo {
	return ExecutionContextInfo{JobID: job.JobID, ExecutionID: job.ExecutionID, RequestID: tc.RequestID, MessageID: tc.MessageID, TraceID: tc.TraceID, SessionID: tc.SessionID}
}

func contextFailure(ctx context.Context, runErr error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func classifyFailure(err error) FailureClass {
	if err == nil {
		return FailureNone
	}
	if errors.Is(err, storage.ErrLeaseLost) || errors.Is(err, storage.ErrFenceRejected) || errors.Is(err, storage.ErrEpochRejected) {
		return FailureLeaseLost
	}
	if errors.Is(err, context.Canceled) {
		return FailureCanceled
	}
	if errors.Is(err, queue.ErrInvalidJob) || errors.Is(err, queue.ErrInvalidEnvelope) || errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrAgentVersionMismatch) || errors.Is(err, agent.ErrInvalidSpec) || errors.Is(err, agent.ErrTenantMismatch) {
		return FailurePermanent
	}
	return FailureRetryable
}

func classifyState(err error) ExecutionState {
	switch classifyFailure(err) {
	case FailureCanceled:
		return StateCanceled
	case FailureLeaseLost:
		return StateLeaseLost
	case FailurePermanent:
		return StatePermanentFailure
	default:
		return StateRetryableFailure
	}
}

func (r ExecutionRequest) renewInterval() time.Duration {
	if r.RenewInterval > 0 {
		return r.RenewInterval
	}
	return r.LeaseTTL / 3
}

func (r ExecutionRequest) releaseTimeout() time.Duration {
	if r.ReleaseTimeout > 0 {
		return r.ReleaseTimeout
	}
	return time.Second
}

func messageFromDTO(message queue.MessageDTO) agent.Message {
	calls := make([]agent.ToolCall, len(message.ToolCalls))
	for i, call := range message.ToolCalls {
		calls[i] = agent.ToolCall{ID: call.ID, Name: call.Name, Arguments: call.Arguments}
	}
	return agent.Message{ID: message.ID, Role: message.Role, Content: message.Content, CreatedAt: message.CreatedAt, ToolID: message.ToolID, ToolName: message.ToolName, ToolCalls: calls}
}

func cloneMessages(messages []agent.Message) []agent.Message {
	out := append([]agent.Message(nil), messages...)
	for i := range out {
		out[i].ToolCalls = append([]agent.ToolCall(nil), out[i].ToolCalls...)
	}
	return out
}

func cloneAgentSpec(spec agent.AgentSpec) agent.AgentSpec {
	spec.Tools = append([]agent.ToolSpec(nil), spec.Tools...)
	for i := range spec.Tools {
		if spec.Tools[i].InputSchema != nil {
			spec.Tools[i].InputSchema = cloneArguments(spec.Tools[i].InputSchema)
		}
	}
	return spec
}

func cloneArguments(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
