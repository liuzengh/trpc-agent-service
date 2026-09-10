package gateway

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	budgetmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestUsageAccumulatorAndSaturatingAdd(t *testing.T) {
	var nilAccumulator *usageAccumulator
	if got := nilAccumulator.Snapshot(); got != (budget.Usage{}) {
		t.Fatalf("nil snapshot = %+v", got)
	}
	nilAccumulator.Observe(context.Background(), budget.Usage{InputTokens: 1})
	accumulator := &usageAccumulator{}
	accumulator.Observe(context.Background(), budget.Usage{InputTokens: 2, OutputTokens: 3})
	accumulator.Observe(context.Background(), budget.Usage{InputTokens: math.MaxInt64, OutputTokens: math.MaxInt64})
	if got := accumulator.Snapshot(); got.InputTokens != math.MaxInt64 || got.OutputTokens != math.MaxInt64 {
		t.Fatalf("saturated usage = %+v", got)
	}
	if got := saturatingAdd(-2, 1); got != -1 {
		t.Fatalf("negative add = %d", got)
	}
}

func TestBudgetAuditUsageResults(t *testing.T) {
	pricing := budget.Pricing{Configured: true, Currency: "USD"}
	for _, event := range []audit.EventType{audit.EventExecutionCompleted, audit.EventExecutionCanceled, audit.EventExecutionFailed, audit.EventExecutionFallback} {
		usage := budgetAuditUsage(budget.Usage{InputTokens: 2, OutputTokens: 3, SpendMinor: 4}, pricing, event, "provider", "model")
		if usage == nil || usage.Currency != "USD" {
			t.Fatalf("audit usage for %v = %+v", event, usage)
		}
	}
	if usage := budgetAuditUsage(budget.Usage{}, budget.Pricing{}, audit.EventExecutionCompleted, "", ""); usage.ModelCostMinor != nil {
		t.Fatalf("unconfigured pricing emitted cost: %+v", usage)
	}
}

func TestBudgetHelpersHandleNilAndClassification(t *testing.T) {
	ctx := context.Background()
	var dispatcher *Dispatcher
	if err := dispatcher.releaseBudget(ctx, budget.Reservation{}); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.settleBudget(ctx, nil, audit.EventExecutionCompleted); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{budget.ErrExceeded, budget.ErrCostUnavailable, budget.ErrUnavailable} {
		if !isBudgetRejection(err) {
			t.Fatalf("%v not classified as budget rejection", err)
		}
	}
	if isBudgetRejection(errors.New("other")) {
		t.Fatal("unrelated error classified as budget rejection")
	}
	if err := budgetAdmissionAudit(ctx, nil, "tenant", "request", "trace"); err != nil {
		t.Fatal(err)
	}
	var nilDispatcher *Dispatcher
	if reservation, accumulator, pricing, err := nilDispatcher.reserveBudget(ctx, runtime.ExecutionPlan{}, "request"); err != nil || reservation.State != budget.ReservationStateDisabled || accumulator != nil || pricing.Configured {
		t.Fatalf("nil dispatcher reserve = %+v, %v, %+v, %v", reservation, accumulator, pricing, err)
	}
}

func TestReserveAndSettleBudgetFromExecutionPlan(t *testing.T) {
	fixture := newGatewayFixture(t)
	tokenLimit := int64(100_000)
	updated, err := fixture.tenants.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: fixture.tenant.TenantID, ExpectedVersion: fixture.tenant.Version, DisplayName: fixture.tenant.DisplayName,
		MonthlyTokenBudget: &tokenLimit, AuditRetentionDays: fixture.tenant.AuditRetentionDays, LogMaskingLevel: fixture.tenant.LogMaskingLevel,
		TraceSamplingRate: fixture.tenant.TraceSamplingRate, DefaultAgentAppID: fixture.tenant.DefaultAgentAppID, DefaultBackendProfileID: fixture.tenant.DefaultBackendProfileID,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.Resolve(context.Background(), mustAPIPrincipal(t, updated.TenantID, fixture.app.AppID))
	if err != nil {
		t.Fatal(err)
	}
	store := budgetmemory.New()
	dispatcher := &Dispatcher{budget: budget.NewController(store)}
	reservation, accumulator, pricing, err := dispatcher.reserveBudget(context.Background(), plan, "budget-request")
	if err != nil || reservation.State != budget.ReservationStateReserved || accumulator == nil || pricing.Configured {
		t.Fatalf("reserve = %+v, accumulator=%v, pricing=%+v, err=%v", reservation, accumulator, pricing, err)
	}
	accumulator.Observe(context.Background(), budget.Usage{InputTokens: 4, OutputTokens: 6})
	run := &dispatchExecution{budgetReservation: reservation, usageAccumulator: accumulator, pricing: pricing}
	if err := dispatcher.settleBudget(context.Background(), run, audit.EventExecutionCompleted); err != nil {
		t.Fatal(err)
	}
	if run.budgetReservation.State != budget.ReservationStateSettled || run.auditUsage == nil || run.auditUsage.BudgetUsedTokens == nil || *run.auditUsage.BudgetUsedTokens != 10 {
		t.Fatalf("settled run = %+v", run)
	}
	if err := dispatcher.releaseBudget(context.Background(), run.budgetReservation); err != nil {
		t.Fatal(err)
	}
	ledger, err := store.Snapshot(context.Background(), updated.TenantID, time.Now().UTC())
	if err != nil || ledger.UsedTokens != 10 || ledger.ReservedTokens != 0 {
		t.Fatalf("budget ledger = %+v, err=%v", ledger, err)
	}
}

func TestExecutionAuditResultMapsTerminalEvents(t *testing.T) {
	for _, test := range []struct {
		event audit.EventType
		want  audit.ExecutionResult
	}{
		{audit.EventExecutionCompleted, audit.ResultSuccess},
		{audit.EventExecutionCanceled, audit.ResultCanceled},
		{audit.EventExecutionFailed, audit.ResultFailure},
		{audit.EventExecutionFallback, audit.ResultFailure},
	} {
		if got := executionAuditResult(test.event); got != test.want {
			t.Fatalf("executionAuditResult(%q) = %q, want %q", test.event, got, test.want)
		}
	}
}

func TestFinishForwardOutputMapsTerminalFailures(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		cancel     bool
		emitted    bool
		wantError  string
		wantStatus string
	}{
		{name: "canceled", err: context.Canceled, cancel: true, wantError: ErrExecutionCanceled.Error(), wantStatus: "canceled"},
		{name: "audit", err: ErrAuditWriteFailed, wantError: ErrAuditWriteFailed.Error(), wantStatus: "error"},
		{name: "budget", err: budget.ErrExceeded, wantError: budget.ErrExceeded.Error(), wantStatus: "error"},
		{name: "pricing", err: budget.ErrCostUnavailable, wantError: budget.ErrCostUnavailable.Error(), wantStatus: "error"},
		{name: "generic", err: errors.New("provider"), wantError: ErrExecution.Error(), wantStatus: "error"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			output := make(chan DispatchEvent, 4)
			run := &dispatchExecution{metadata: dispatchMetadata{requestID: "request", traceID: "trace"}, output: output}
			ctx := context.Background()
			if test.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			run.finishForwardOutput(ctx, test.err, test.emitted)
			close(output)
			var events []DispatchEvent
			for event := range output {
				events = append(events, event)
			}
			if len(events) != 2 || events[0].Error != test.wantError || events[1].Status != test.wantStatus || !events[1].Done {
				t.Fatalf("events = %+v", events)
			}
		})
	}
	output := make(chan DispatchEvent, 2)
	run := &dispatchExecution{metadata: dispatchMetadata{requestID: "request"}, output: output}
	run.finishForwardOutput(context.Background(), context.Canceled, true)
	if len(output) != 1 {
		t.Fatalf("already emitted cancellation produced %d events", len(output))
	}
}

func TestReserveBudgetRequiresPricingForSpendLimits(t *testing.T) {
	fixture := newGatewayFixture(t)
	spendLimit := int64(100)
	updated, err := fixture.tenants.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: fixture.tenant.TenantID, ExpectedVersion: fixture.tenant.Version, DisplayName: fixture.tenant.DisplayName,
		MonthlySpendLimitMinor: &spendLimit, BillingCurrency: "USD", AuditRetentionDays: fixture.tenant.AuditRetentionDays,
		LogMaskingLevel: fixture.tenant.LogMaskingLevel, TraceSamplingRate: fixture.tenant.TraceSamplingRate,
		DefaultAgentAppID: fixture.tenant.DefaultAgentAppID, DefaultBackendProfileID: fixture.tenant.DefaultBackendProfileID,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPlanResolver(runtime.PlanResolverConfig{Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends, ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.Resolve(context.Background(), mustAPIPrincipal(t, updated.TenantID, fixture.app.AppID))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = (&Dispatcher{budget: budget.NewController(budgetmemory.New())}).reserveBudget(context.Background(), plan, "pricing-request")
	if !errors.Is(err, budget.ErrCostUnavailable) {
		t.Fatalf("reserve without pricing error = %v", err)
	}
}
