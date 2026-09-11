// Package datamigration provides the reentrant control-plane executor used for
// Redis-to-SQL and vector-store migrations. Resource adapters implement one
// phase at a time; this package owns monotonic transitions and durable CAS.
package datamigration

import (
	"context"
	"errors"
	"fmt"
)

type Phase string

const (
	PhasePrepare    Phase = "prepare"
	PhaseSnapshot   Phase = "snapshot"
	PhaseCatchUp    Phase = "catch_up"
	PhaseShadowRead Phase = "shadow_read"
	PhaseCanary     Phase = "canary"
	PhaseCutover    Phase = "cutover"
	PhaseDrain      Phase = "drain"
	PhaseFinalize   Phase = "finalize"
	PhaseRollback   Phase = "rollback"
	PhaseComplete   Phase = "complete"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusRunning    Status = "running"
	StatusPaused     Status = "paused"
	StatusFailed     Status = "failed"
	StatusComplete   Status = "complete"
	StatusRolledBack Status = "rolled_back"
)

var (
	ErrNotFound             = errors.New("data migration not found")
	ErrConflict             = errors.New("data migration generation conflict")
	ErrTerminal             = errors.New("data migration is terminal")
	ErrVerificationMismatch = errors.New("data migration verification mismatch")
	ErrPhaseExecution       = errors.New("data migration phase execution failed")
)

// Job contains counters and an opaque, non-secret checkpoint. Generation is
// incremented on every write and provides fencing between executor replicas.
type Job struct {
	MigrationID  string
	TenantID     string
	AppName      string
	ResourceKind string
	Source       string
	Target       string
	Phase        Phase
	Status       Status
	Generation   int64
	Checkpoint   map[string]any
	Copied       int64
	Verified     int64
	Failed       int64
	Mismatches   int64
	LastError    string
}

type Store interface {
	Create(ctx context.Context, job Job) error
	Get(ctx context.Context, migrationID string) (Job, error)
	CompareAndSwap(ctx context.Context, expectedGeneration int64, job Job) error
}

// Result is one idempotent batch. Done means the current phase's complete
// predicate has been met, not merely that one batch returned successfully.
type Result struct {
	Done       bool
	Checkpoint map[string]any
	Copied     int64
	Verified   int64
	Failed     int64
	Mismatches int64
}

// Operator implements resource-specific work. It must make each call
// idempotent using Job.Checkpoint and stable source object/write identifiers.
type Operator interface {
	ExecutePhase(ctx context.Context, phase Phase, job Job) (Result, error)
}

type Engine struct {
	store    Store
	operator Operator
}

func New(store Store, operator Operator) (*Engine, error) {
	if store == nil || operator == nil {
		return nil, errors.New("data migration store and operator are required")
	}
	return &Engine{store: store, operator: operator}, nil
}

// Advance executes at most one batch and persists its checkpoint. Concurrent
// callers are fenced by Store.CompareAndSwap; a loser reloads on its next run.
func (e *Engine) Advance(ctx context.Context, migrationID string) (Job, error) {
	job, err := e.store.Get(ctx, migrationID)
	if err != nil {
		return Job{}, err
	}
	if job.Status == StatusComplete || job.Status == StatusRolledBack || job.Phase == PhaseComplete {
		return job, ErrTerminal
	}
	if !validPhase(job.Phase) {
		return job, errors.New("data migration has invalid phase")
	}

	result, runErr := e.operator.ExecutePhase(ctx, job.Phase, cloneJob(job))
	next := cloneJob(job)
	next.Status = StatusRunning
	if result.Checkpoint != nil {
		next.Checkpoint = cloneMap(result.Checkpoint)
	}
	next.Copied += nonNegative(result.Copied)
	next.Verified += nonNegative(result.Verified)
	next.Failed += nonNegative(result.Failed)
	next.Mismatches += nonNegative(result.Mismatches)
	next.LastError = ""
	if runErr != nil {
		// A resource adapter may fail after doing only part of a batch. Replaying
		// from the prior durable cursor is safe by contract; advancing here would
		// silently skip the remaining objects.
		next.Checkpoint = cloneMap(job.Checkpoint)
		next.Copied = job.Copied
		next.Verified = job.Verified
		next.Mismatches = job.Mismatches
		next.Status = StatusFailed
		next.LastError = phaseFailureCategory(runErr)
		if err := e.store.CompareAndSwap(ctx, job.Generation, next); err != nil {
			return job, err
		}
		next.Generation++
		return next, newPhaseExecutionError(job.Phase, runErr)
	}
	// A mismatch in an early verification batch must pause immediately. Waiting
	// until Done would lose the signal when a later batch happens to match.
	if phaseRequiresZeroMismatch(job.Phase) && result.Mismatches > 0 {
		// Keep the prior cursor so an operator repair re-verifies the mismatching
		// object instead of resuming after it.
		next.Checkpoint = cloneMap(job.Checkpoint)
		next.Status = StatusPaused
		next.LastError = "verification_mismatch"
		if err := e.store.CompareAndSwap(ctx, job.Generation, next); err != nil {
			return job, err
		}
		next.Generation++
		return next, ErrVerificationMismatch
	}

	if result.Done {
		next.Phase = nextPhase(job.Phase)
		next.Checkpoint = map[string]any{}
		if next.Phase == PhaseComplete {
			if job.Phase == PhaseRollback {
				next.Status = StatusRolledBack
			} else {
				next.Status = StatusComplete
			}
		}
	}
	if err := e.store.CompareAndSwap(ctx, job.Generation, next); err != nil {
		return job, err
	}
	next.Generation++
	return next, nil
}

type phaseExecutionError struct {
	phase Phase
	cause error
}

func (e phaseExecutionError) Error() string {
	return fmt.Sprintf("%s: phase %s", ErrPhaseExecution, e.phase)
}
func (e phaseExecutionError) Unwrap() []error { return []error{ErrPhaseExecution, e.cause} }

func newPhaseExecutionError(phase Phase, cause error) error {
	return phaseExecutionError{phase: phase, cause: cause}
}

func phaseFailureCategory(err error) string {
	switch {
	case errors.Is(err, ErrSourceNotFrozen):
		return "source_not_frozen_or_drained"
	case errors.Is(err, ErrTargetActive):
		return "target_active"
	case errors.Is(err, ErrRoutingNotReady):
		return "routing_not_ready"
	case errors.Is(err, ErrRollbackConflict):
		return "rollback_conflict"
	default:
		return "phase_execution_failed"
	}
}

// RequestRollback only changes the read/write routing phase. The resource
// operator decides how to replay newer writes; the executor never copies an
// old snapshot backwards over the current source.
func (e *Engine) RequestRollback(ctx context.Context, migrationID string) (Job, error) {
	job, err := e.store.Get(ctx, migrationID)
	if err != nil {
		return Job{}, err
	}
	if job.Status == StatusComplete || job.Status == StatusRolledBack || job.Phase == PhaseComplete {
		return job, ErrTerminal
	}
	next := cloneJob(job)
	next.Phase = PhaseRollback
	next.Status = StatusPending
	next.Checkpoint = map[string]any{}
	next.LastError = ""
	if err := e.store.CompareAndSwap(ctx, job.Generation, next); err != nil {
		return job, err
	}
	next.Generation++
	return next, nil
}

func nextPhase(phase Phase) Phase {
	switch phase {
	case PhasePrepare:
		return PhaseSnapshot
	case PhaseSnapshot:
		return PhaseCatchUp
	case PhaseCatchUp:
		return PhaseShadowRead
	case PhaseShadowRead:
		return PhaseCanary
	case PhaseCanary:
		return PhaseCutover
	case PhaseCutover:
		return PhaseDrain
	case PhaseDrain:
		return PhaseFinalize
	case PhaseFinalize, PhaseRollback:
		return PhaseComplete
	default:
		return PhaseComplete
	}
}

func validPhase(phase Phase) bool {
	return phase == PhasePrepare || phase == PhaseSnapshot || phase == PhaseCatchUp ||
		phase == PhaseShadowRead || phase == PhaseCanary || phase == PhaseCutover ||
		phase == PhaseDrain || phase == PhaseFinalize || phase == PhaseRollback
}

func phaseRequiresZeroMismatch(phase Phase) bool {
	return phase == PhaseShadowRead || phase == PhaseCanary || phase == PhaseDrain || phase == PhaseFinalize || phase == PhaseRollback
}

func nonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func cloneJob(job Job) Job {
	job.Checkpoint = cloneMap(job.Checkpoint)
	return job
}

func cloneMap(source map[string]any) map[string]any {
	if source == nil {
		return map[string]any{}
	}
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
