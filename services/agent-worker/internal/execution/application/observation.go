package application

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

// Observation deliberately cannot carry requests, credentials, provider errors,
// transcripts, or URLs. Adapters use this vocabulary for low-cardinality signals;
// telemetry is never an execution decision or a durable acceptance receipt.
type Observation struct {
	Stage                                  string
	Operation, Result                      string
	TenantID, RunID, AttemptID             string
	Duration                               time.Duration
	InputTokens, OutputTokens, TotalTokens int64
}
type Observer interface {
	Observe(context.Context, Observation)
}

func ObservationResult(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, domain.ErrFenced):
		return "fenced"
	case errors.Is(err, domain.ErrConflict):
		return "conflict"
	case errors.Is(err, domain.ErrCapacity):
		return "capacity"
	case errors.Is(err, ErrManifestMissing):
		return "manifest_wait"
	case errors.Is(err, ErrManifestContractMismatch):
		return "manifest_contract_wait"
	case errors.Is(err, domain.ErrNotReady):
		return "session_wait"
	case errors.Is(err, ErrManifestInvalid), errors.Is(err, ErrManifestUnsupported), errors.Is(err, domain.ErrInvalid):
		return "invalid"
	case errors.Is(err, ErrMemoryFinalize):
		return "memory_finalize_failed"
	case errors.Is(err, ErrMemoryApply):
		return "memory_apply_failed"
	case errors.Is(err, ErrSessionPreparation):
		return "session_preparation"
	case errors.Is(err, ErrSessionInvalid):
		return "session_invalid"
	case errors.Is(err, ErrCredentialDenied):
		return "credential_denied"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, ErrDependency):
		return "dependency"
	default:
		return "failed"
	}
}
func (p *Processor) observe(ctx context.Context, operation string, r domain.Run, attempt string, start time.Time, err error) {
	if p.observer != nil {
		p.observer.Observe(ctx, Observation{Operation: operation, Result: ObservationResult(err), TenantID: r.Request.Route.TenantID, RunID: r.Request.RunID, AttemptID: attempt, Duration: time.Since(start)})
	}
}

// StorageObservation contains only bounded, low-cardinality backlog gauges.
// Counts are per shared Execution database, not per tenant or model.
type StorageObservation struct {
	Queued, Running, RetryWait, ReplyPending, ActiveLocal int64
	OldestRunSeconds, OldestReplySeconds                  float64
}
