package gateway

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

// usageAccumulator is request-local state. It is deliberately separate from
// the cached Runner so concurrent executions cannot mix provider usage.
type usageAccumulator struct {
	mu    sync.Mutex
	usage budget.Usage
}

func (accumulator *usageAccumulator) Observe(_ context.Context, value budget.Usage) {
	if accumulator == nil {
		return
	}
	accumulator.mu.Lock()
	accumulator.usage.InputTokens = saturatingAdd(accumulator.usage.InputTokens, value.InputTokens)
	accumulator.usage.OutputTokens = saturatingAdd(accumulator.usage.OutputTokens, value.OutputTokens)
	accumulator.mu.Unlock()
}

func (accumulator *usageAccumulator) Snapshot() budget.Usage {
	if accumulator == nil {
		return budget.Usage{}
	}
	accumulator.mu.Lock()
	defer accumulator.mu.Unlock()
	return accumulator.usage
}

func saturatingAdd(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func (dispatcher *Dispatcher) reserveBudget(ctx context.Context, plan runtime.ExecutionPlan, requestID string) (budget.Reservation, *usageAccumulator, budget.Pricing, error) {
	if dispatcher == nil || dispatcher.budget == nil {
		return budget.Reservation{State: budget.ReservationStateDisabled}, nil, budget.Pricing{}, nil
	}
	tenantValue := plan.Tenant()
	modelValue := plan.ModelSnapshot().Profile()
	pricing, err := budget.ParsePricing(modelValue.Configuration.Options, tenantValue.BillingCurrency)
	if err != nil {
		return budget.Reservation{}, nil, budget.Pricing{}, err
	}
	limits := budget.LimitsForTenant(tenantValue)
	if limits.SpendLimitMinor != nil {
		if err := pricing.RequirePricing(); err != nil {
			return budget.Reservation{}, nil, budget.Pricing{}, err
		}
	}
	revision := plan.AgentSnapshot().Revision()
	maxLLMCalls := revision.Runtime.MaxLLMCalls
	if revision.Chain != nil {
		steps := len(revision.Chain.Steps)
		if steps < 1 || maxLLMCalls > math.MaxInt/steps {
			return budget.Reservation{}, nil, budget.Pricing{}, budget.ErrInvalid
		}
		maxLLMCalls *= steps
	}
	maxOutputTokens := int(budget.DefaultOutputTokensEstimate)
	if revision.Generation.MaxOutputTokens != nil {
		maxOutputTokens = *revision.Generation.MaxOutputTokens
	}
	estimate, err := budget.EstimateExecution(maxLLMCalls, maxOutputTokens, pricing)
	if err != nil {
		return budget.Reservation{}, nil, budget.Pricing{}, err
	}
	reservation, err := dispatcher.budget.Reserve(ctx, tenantValue, requestID, estimate)
	if err != nil {
		return budget.Reservation{}, nil, pricing, err
	}
	if reservation.State == budget.ReservationStateDisabled {
		return reservation, nil, pricing, nil
	}
	return reservation, &usageAccumulator{}, pricing, nil
}

func (dispatcher *Dispatcher) releaseBudget(ctx context.Context, reservation budget.Reservation) error {
	if dispatcher == nil || dispatcher.budget == nil || reservation.State == budget.ReservationStateDisabled {
		return nil
	}
	_, err := dispatcher.budget.Release(ctx, reservation)
	return err
}

func (dispatcher *Dispatcher) settleBudget(ctx context.Context, run *dispatchExecution, eventType audit.EventType) error {
	if dispatcher == nil || dispatcher.budget == nil || run == nil || run.budgetReservation.State == budget.ReservationStateDisabled {
		return nil
	}
	usage := budget.Usage{}
	if run.usageAccumulator != nil {
		usage = run.usageAccumulator.Snapshot()
	}
	cost, err := run.pricing.Cost(usage)
	if err != nil {
		return err
	}
	usage.SpendMinor = cost
	settled, settleErr := dispatcher.budget.Settle(ctx, run.budgetReservation, usage)
	if settled.State != "" {
		run.budgetReservation = settled
	}
	run.auditUsage = budgetAuditUsage(usage, run.pricing, eventType, run.metadata.modelProvider, run.metadata.modelName)
	return settleErr
}

func budgetAuditUsage(usage budget.Usage, pricing budget.Pricing, eventType audit.EventType, provider, model string) *audit.Usage {
	input, output, tokens, spend := usage.InputTokens, usage.OutputTokens, usage.Tokens(), usage.SpendMinor
	result := audit.ResultSuccess
	switch eventType {
	case audit.EventExecutionCanceled:
		result = audit.ResultCanceled
	case audit.EventExecutionFailed, audit.EventExecutionFallback:
		result = audit.ResultFailure
	}
	value := &audit.Usage{InputTokens: &input, OutputTokens: &output, BudgetUsedTokens: &tokens, ExecutionResult: result, Provider: provider, Model: model}
	if pricing.Configured {
		value.ModelCostMinor = &spend
		value.BudgetUsedMinor = &spend
		value.Currency = pricing.Currency
	}
	return value
}

func budgetAdmissionAudit(ctx context.Context, writer audit.Writer, tenantID, requestID, traceID string) error {
	if writer == nil {
		return nil
	}
	return audit.NewRecorder(writer, tenantID).BudgetRejected(ctx, requestID, traceID)
}

func isBudgetRejection(err error) bool {
	return errors.Is(err, budget.ErrExceeded) || errors.Is(err, budget.ErrCostUnavailable) || errors.Is(err, budget.ErrUnavailable)
}
