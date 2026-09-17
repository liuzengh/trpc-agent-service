package governance

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

type DecisionRecorder interface {
	RecordDecision(context.Context, Decision) error
}

type RunPermit struct {
	Policy      PolicySnapshot
	Model       VersionedRef
	Decision    Decision
	Reservation Reservation
}

type RunGuard interface {
	Begin(context.Context, runtime.ExecutionEnvelope, VersionedRef, []byte) (RunPermit, error)
	Finish(context.Context, RunPermit, Usage, []byte) (Decision, error)
	Refund(context.Context, RunPermit, string) error
	Abort(context.Context, runtime.ExecutionEnvelope, VersionedRef, string) error
	Record(context.Context, Decision) error
}

// ModelCandidateGuard validates every immutable model profile which can run
// for a request before the bundle is acquired. It is optional to preserve the
// narrow RunGuard contract used by test and extension implementations.
type ModelCandidateGuard interface {
	ValidateModelCandidates(context.Context, RunPermit, []VersionedRef) (Decision, error)
}

// ModelUsage records a portion of one request's usage under its actual model
// profile. A single agent run can make several model calls around tool use.
type ModelUsage struct {
	Model VersionedRef
	Usage Usage
}

type MultiModelRunGuard interface {
	FinishModels(context.Context, RunPermit, []ModelUsage, []byte) (Decision, error)
}

type Service struct {
	Repository  Repository
	Ledger      Ledger
	Decisions   DecisionRecorder
	InputGuard  ContentGuard
	OutputGuard ContentGuard
	Telemetry   telemetry.Provider
	Now         func() time.Time
}

func (s Service) Begin(ctx context.Context, envelope runtime.ExecutionEnvelope, model VersionedRef, content []byte) (RunPermit, error) {
	if s.Repository == nil || s.Ledger == nil || s.Decisions == nil {
		return RunPermit{}, runtime.ErrCapabilityUnsupported
	}
	policy, err := s.Repository.GetPolicy(ctx, envelope.TenantID, envelope.PolicyVersion)
	if err != nil {
		return RunPermit{}, err
	}
	verdict, err := inspect(ctx, s.InputGuard, policy.Policy.InputDLP, envelope.TenantID, envelope.RequestID, "input", content)
	if err != nil {
		verdict = DLPVerdictUnknown
	}
	decision, evalErr := (Evaluator{}).Evaluate(policy, EvaluationInput{TenantID: envelope.TenantID, RequestID: envelope.RequestID, Stage: "input",
		SubjectID: envelope.UserID, PolicyVersion: envelope.PolicyVersion, Model: model, DLP: verdict})
	if evalErr != nil {
		return RunPermit{}, evalErr
	}
	permit := RunPermit{Policy: policy, Model: model, Decision: decision}
	if decision.Action != ActionAllow {
		if err := s.recordDecision(ctx, decision); err != nil {
			return RunPermit{}, err
		}
		return permit, nil
	}
	if policy.Policy.Budget.MaxCostMicrosPerRun > 0 {
		pricing, getErr := s.Repository.GetPricing(ctx, envelope.TenantID, policy.Policy.PricingVersion)
		if getErr != nil || !pricingUsable(pricing, s.now()) {
			decision.Action, decision.ReasonCode = ActionDeny, ReasonPricingUnavailable
			_ = s.recordDecision(ctx, decision)
			permit.Decision = decision
			return permit, nil
		}
	}
	maxTokens := policy.Policy.Budget.MaxInputTokens + policy.Policy.Budget.MaxOutputTokens
	reservation, err := s.Ledger.Reserve(ctx, ReserveRequest{TenantID: envelope.TenantID, RequestID: envelope.RequestID, ResourceID: model.ID,
		AttemptClass: "model", PolicyVersion: policy.Version, PricingVersion: policy.Policy.PricingVersion,
		MaxCostMicros: policy.Policy.Budget.MaxCostMicrosPerRun, MaxTokens: maxTokens})
	if err != nil {
		if errors.Is(err, runtime.ErrCapabilityUnsupported) || errors.Is(err, runtime.ErrVersionConflict) {
			decision.Action, decision.ReasonCode = ActionDeny, ReasonBudgetExceeded
			_ = s.recordDecision(ctx, decision)
			permit.Decision = decision
			return permit, nil
		}
		return RunPermit{}, err
	}
	decision.ReservationID = reservation.ReservationID
	if reservation.State != ReservationReserved {
		decision = Decision{DecisionID: StableDecisionID(envelope.TenantID, envelope.RequestID, "recovery", policy.Version), TenantID: envelope.TenantID,
			RequestID: envelope.RequestID, Stage: "recovery", Action: ActionDeny, ReasonCode: ReasonReservationClosed, PolicyVersion: policy.Version, ReservationID: reservation.ReservationID}
	}
	if err := s.recordDecision(ctx, decision); err != nil {
		return RunPermit{}, err
	}
	permit.Decision, permit.Reservation = decision, reservation
	return permit, nil
}

func (s Service) Record(ctx context.Context, decision Decision) error {
	if s.Decisions == nil {
		return runtime.ErrCapabilityUnsupported
	}
	return s.recordDecision(ctx, decision)
}

func (s Service) ValidateModelCandidates(ctx context.Context, permit RunPermit, candidates []VersionedRef) (Decision, error) {
	decision := Decision{DecisionID: StableDecisionID(permit.Policy.TenantID, permit.Decision.RequestID, "model_candidates", permit.Policy.Version),
		TenantID: permit.Policy.TenantID, RequestID: permit.Decision.RequestID, Stage: "model_candidates", Action: ActionAllow,
		ReasonCode: ReasonAllowed, PolicyVersion: permit.Policy.Version, ReservationID: permit.Reservation.ReservationID}
	if len(candidates) == 0 || candidates[0] != permit.Model {
		return Decision{}, runtime.ErrInvariantViolation
	}
	for _, candidate := range candidates {
		if !ModelAllowed(permit.Policy, candidate) {
			decision.Action, decision.ReasonCode = ActionDeny, ReasonModelDenied
			if err := s.recordDecision(ctx, decision); err != nil {
				return Decision{}, err
			}
			return decision, nil
		}
	}
	if permit.Policy.Policy.Budget.MaxCostMicrosPerRun == 0 {
		return decision, nil
	}
	pricing, err := s.Repository.GetPricing(ctx, permit.Policy.TenantID, permit.Policy.Policy.PricingVersion)
	if err != nil || !pricingUsable(pricing, s.now()) {
		decision.Action, decision.ReasonCode = ActionDeny, ReasonPricingUnavailable
		if recordErr := s.recordDecision(ctx, decision); recordErr != nil {
			return Decision{}, recordErr
		}
		return decision, nil
	}
	for _, candidate := range candidates {
		if _, err := PriceUsage(pricing, candidate, Usage{}, s.now()); err != nil {
			decision.Action, decision.ReasonCode = ActionDeny, ReasonPricingUnavailable
			if recordErr := s.recordDecision(ctx, decision); recordErr != nil {
				return Decision{}, recordErr
			}
			return decision, nil
		}
	}
	return decision, nil
}

func (s Service) Finish(ctx context.Context, permit RunPermit, usage Usage, output []byte) (Decision, error) {
	return s.FinishModels(ctx, permit, []ModelUsage{{Model: permit.Model, Usage: usage}}, output)
}

func (s Service) FinishModels(ctx context.Context, permit RunPermit, usages []ModelUsage, output []byte) (Decision, error) {
	if permit.Decision.Action != ActionAllow || permit.Reservation.ReservationID == "" {
		return Decision{}, runtime.ErrInvariantViolation
	}
	var aggregate Usage
	for _, value := range usages {
		if value.Model.ID == "" || value.Model.Version < 1 || value.Usage.InputTokens < 0 || value.Usage.OutputTokens < 0 || value.Usage.CachedInputTokens < 0 {
			return Decision{}, runtime.ErrInvariantViolation
		}
		aggregate.InputTokens += value.Usage.InputTokens
		aggregate.OutputTokens += value.Usage.OutputTokens
		aggregate.CachedInputTokens += value.Usage.CachedInputTokens
	}
	if len(usages) == 0 || ((permit.Policy.Policy.Budget.MaxInputTokens > 0 || permit.Policy.Policy.Budget.MaxOutputTokens > 0 || permit.Policy.Policy.Budget.MaxCostMicrosPerRun > 0) && aggregate.InputTokens+aggregate.OutputTokens == 0) {
		s.recordUsageMissing(ctx)
		return Decision{}, runtime.ErrCapabilityUnsupported
	}
	actualCost := int64(0)
	if permit.Policy.Policy.Budget.MaxCostMicrosPerRun > 0 {
		pricing, err := s.Repository.GetPricing(ctx, permit.Policy.TenantID, permit.Policy.Policy.PricingVersion)
		if err != nil {
			return Decision{}, err
		}
		for _, value := range usages {
			cost, priceErr := PriceUsage(pricing, value.Model, value.Usage, s.now())
			if priceErr != nil {
				return Decision{}, priceErr
			}
			if cost > int64(^uint64(0)>>1)-actualCost {
				return Decision{}, runtime.ErrInvariantViolation
			}
			actualCost += cost
		}
	}
	settled, err := s.Ledger.Settle(ctx, SettleRequest{TenantID: permit.Policy.TenantID, ReservationID: permit.Reservation.ReservationID,
		RequestID: permit.Reservation.RequestID, Stage: "model", UsageKind: "tokens", ExpectedVersion: permit.Reservation.Version, Usage: aggregate, ActualCostMicros: actualCost})
	if err != nil {
		return Decision{}, err
	}
	verdict, inspectErr := inspect(ctx, s.OutputGuard, permit.Policy.Policy.OutputDLP, permit.Policy.TenantID, permit.Reservation.RequestID, "output", output)
	decision := Decision{DecisionID: StableDecisionID(permit.Policy.TenantID, permit.Reservation.RequestID, "output", permit.Policy.Version), TenantID: permit.Policy.TenantID,
		RequestID: permit.Reservation.RequestID, Stage: "output", Action: ActionAllow, ReasonCode: ReasonAllowed, PolicyVersion: permit.Policy.Version, ReservationID: settled.ReservationID}
	if inspectErr != nil || verdict != DLPVerdictClean {
		decision.Action, decision.ReasonCode = ActionDeny, ReasonOutputRejected
	}
	if err := s.recordDecision(ctx, decision); err != nil {
		return Decision{}, err
	}
	return decision, nil
}

func (s Service) recordDecision(ctx context.Context, decision Decision) error {
	if err := s.Decisions.RecordDecision(ctx, decision); err != nil {
		return err
	}
	if decision.Action == ActionDeny && decision.ReasonCode == ReasonBudgetExceeded {
		telemetry.OrNoop(s.Telemetry).Counter(telemetry.MetricGovernanceBudgetDenied).Add(ctx, 1,
			telemetry.ComponentAttribute(telemetry.ComponentWorker))
	}
	return nil
}

func (s Service) recordUsageMissing(ctx context.Context) {
	telemetry.OrNoop(s.Telemetry).Counter(telemetry.MetricGovernanceUsageMissing).Add(ctx, 1,
		telemetry.ComponentAttribute(telemetry.ComponentWorker))
}

func (s Service) Refund(ctx context.Context, permit RunPermit, reason string) error {
	if permit.Reservation.ReservationID == "" {
		return nil
	}
	_, err := s.Ledger.Refund(ctx, permit.Policy.TenantID, permit.Reservation.ReservationID, permit.Reservation.Version, reason)
	return err
}

func (s Service) Abort(ctx context.Context, envelope runtime.ExecutionEnvelope, model VersionedRef, reason string) error {
	if s.Ledger == nil {
		return runtime.ErrCapabilityUnsupported
	}
	id, err := StableReservationID(ReserveRequest{TenantID: envelope.TenantID, RequestID: envelope.RequestID, ResourceID: model.ID, AttemptClass: "model"})
	if err != nil {
		return err
	}
	reservation, err := s.Ledger.GetReservation(ctx, envelope.TenantID, id)
	if err != nil {
		return err
	}
	if reservation.State != ReservationReserved {
		return nil
	}
	_, err = s.Ledger.Refund(ctx, envelope.TenantID, id, reservation.Version, reason)
	return err
}

func inspect(ctx context.Context, guard ContentGuard, mode DLPMode, tenantID, requestID, stage string, content []byte) (DLPVerdict, error) {
	if mode == DLPDisabled {
		return DLPVerdictClean, nil
	}
	if guard == nil {
		return DLPVerdictUnknown, runtime.ErrCapabilityUnsupported
	}
	verdict, _, err := guard.Inspect(ctx, tenantID, requestID, stage, content)
	return verdict, err
}
func (s Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}
func pricingUsable(snapshot PricingSnapshot, at time.Time) bool {
	return !at.Before(snapshot.Pricing.ValidFrom) && at.Before(snapshot.Pricing.ValidUntil)
}

var _ RunGuard = Service{}
var _ ModelCandidateGuard = Service{}
var _ MultiModelRunGuard = Service{}
