package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

type sessionTimeoutLedger struct {
	observationLedger
	retry bool
}

func (l *sessionTimeoutLedger) FailAttempt(ctx context.Context, g domain.Grant, reason string, retry bool) error {
	l.retry = retry
	return l.observationLedger.FailAttempt(ctx, g, reason, retry)
}

type sessionTimeoutRuntime struct {
	observationRuntime
	phase string
	cause error
}

func (r *sessionTimeoutRuntime) Prepare(context.Context, domain.Grant, domain.Plan, func(context.Context) error) (AttemptRuntime, error) {
	if r.phase == "open" {
		return nil, r.cause
	}
	return r, nil
}
func (r *sessionTimeoutRuntime) Load(ctx context.Context, head domain.Head) ([]byte, error) {
	if r.phase == "load" {
		return nil, r.cause
	}
	return r.observationRuntime.Load(ctx, head)
}
func (r *sessionTimeoutRuntime) Stage(ctx context.Context, snapshot []byte) (domain.Candidate, error) {
	if r.phase == "stage" {
		return domain.Candidate{}, r.cause
	}
	return r.observationRuntime.Stage(ctx, snapshot)
}

// The adapter classifies a backend-local timeout as a dependency failure, not
// an expired Attempt. Exercise the actual Processor decision for all Session
// I/O boundaries, including preservation of the nested context error.
func TestProcessorSessionBackendTimeoutRetries(t *testing.T) {
	for _, phase := range []string{"open", "load", "stage"} {
		t.Run(phase, func(t *testing.T) {
			parent := context.Background()
			child, cancel := context.WithDeadline(parent, time.Now().Add(-time.Second))
			defer cancel()
			cause := errors.Join(ErrDependency, child.Err())
			deadline := time.Now().Add(time.Minute)
			run := domain.Run{Request: domain.Requested{RunID: "run", Route: domain.Route{TenantID: "tenant", ManifestRef: "manifest", ManifestDigest: "digest", DeploymentRevisionID: "revision"}}, ExecutionDeadline: &deadline, Policy: domain.Policy{RenewalInterval: time.Hour}}
			ledger := &sessionTimeoutLedger{observationLedger: observationLedger{run: run}}
			runtime := &sessionTimeoutRuntime{phase: phase, cause: cause}
			processor, err := NewProcessor(ledger, observationManifest{domain.Plan{TenantID: "tenant", ManifestID: "manifest", ManifestDigest: "digest", DeploymentRevisionID: "revision"}}, runtime, "worker", 1)
			if err != nil {
				t.Fatal(err)
			}
			err = processor.Advance(parent, run)
			if !errors.Is(err, ErrDependency) || !errors.Is(err, context.DeadlineExceeded) || parent.Err() != nil {
				t.Fatalf("child deadline must preserve dependency classification with live parent: %v", err)
			}
			if ledger.failed != 1 || ledger.reason != "DEPENDENCY_UNAVAILABLE" || !ledger.retry || ledger.completed != 0 {
				t.Fatalf("backend timeout must request retry without Complete: failed=%d reason=%s retry=%t completed=%d", ledger.failed, ledger.reason, ledger.retry, ledger.completed)
			}
			wantExecutions := 0
			if phase == "stage" {
				wantExecutions = 1
			}
			if runtime.calls != wantExecutions {
				t.Fatalf("Execute calls=%d want=%d", runtime.calls, wantExecutions)
			}
		})
	}
}
