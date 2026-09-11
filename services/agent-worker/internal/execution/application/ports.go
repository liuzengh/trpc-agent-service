// Package application owns Worker use-case interfaces. SQL, NATS and SDK
// adapters depend on these interfaces, never the reverse.
package application

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/platform/tracecontext"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

// ScheduledRun carries observation metadata outside business Run/digest contracts.
type ScheduledRun struct {
	domain.Run
	Carrier tracecontext.Carrier
}

type Ledger interface {
	Accept(context.Context, domain.Requested, domain.Policy, domain.IntakeLimits) (domain.Receipt, error)
	FindRun(context.Context, string, string) (domain.Run, error)
	Ready(context.Context, int) ([]domain.Run, error)
	Claim(context.Context, domain.ClaimRequest) (domain.Grant, error)
	Renew(context.Context, domain.Grant) (domain.Grant, error)
	Check(context.Context, domain.Grant) error
	MarkExecuting(context.Context, domain.Grant) error
	Complete(context.Context, domain.Finish) (domain.Completion, error)
	FinalizeMemory(context.Context, domain.Completion, bool) error
	FailAttempt(context.Context, domain.Grant, string, bool) error
	FindCompletion(context.Context, string, string) (domain.Completion, error)
	Terminalize(context.Context, string, string, string) (bool, error)
}

// Acknowledging durable intake never waits for a model or mutable configuration.
type Acceptor struct {
	ledger   Ledger
	policy   domain.Policy
	limits   domain.IntakeLimits
	observer Observer
}

func NewAcceptor(ledger Ledger, policy domain.Policy, limits domain.IntakeLimits, observers ...Observer) (*Acceptor, error) {
	if ledger == nil || policy.Validate() != nil || limits.Validate() != nil || len(observers) > 1 {
		return nil, domain.ErrInvalid
	}
	a := &Acceptor{ledger: ledger, policy: policy, limits: limits}
	if len(observers) == 1 {
		a.observer = observers[0]
	}
	return a, nil
}
func (a *Acceptor) Accept(ctx context.Context, r domain.Requested) (receipt domain.Receipt, err error) {
	start := time.Now()
	defer func() {
		if a.observer != nil {
			a.observer.Observe(ctx, Observation{Operation: "intake", Result: ObservationResult(err), TenantID: r.Route.TenantID, RunID: r.RunID, Duration: time.Since(start)})
		}
	}()
	if err := r.Validate(); err != nil {
		return domain.Receipt{}, err
	}
	return a.ledger.Accept(ctx, r, a.policy, a.limits)
}
