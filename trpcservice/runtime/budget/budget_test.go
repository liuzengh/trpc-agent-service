package budget_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	budgetmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestPricingAndEstimateUseProviderNeutralRates(t *testing.T) {
	pricing, err := budget.ParsePricing(map[string]string{
		budget.InputCostOption: "2", budget.OutputCostOption: "4",
	}, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if err := pricing.RequirePricing(); err != nil {
		t.Fatal(err)
	}
	cost, err := pricing.Cost(budget.Usage{InputTokens: 1_000_001, OutputTokens: 2_000_000})
	if err != nil || cost != 11 {
		t.Fatalf("cost = %d, err = %v, want 11", cost, err)
	}
	estimate, err := budget.EstimateExecution(2, 100, pricing)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.InputTokens != 2*budget.DefaultInputTokensEstimate || estimate.OutputTokens != 200 || estimate.SpendMinor != 2 {
		t.Fatalf("execution estimate = %+v, want spend 2 minor units", estimate)
	}
	if _, err := budget.ParsePricing(map[string]string{budget.InputCostOption: "2"}, "USD"); !errors.Is(err, budget.ErrCostUnavailable) {
		t.Fatalf("partial pricing error = %v", err)
	}
}

//nolint:gocyclo // Boundary table covers independent validation and overflow cases.
func TestBudgetLimitsPricingAndArithmeticBoundaries(t *testing.T) {
	negative := int64(-1)
	positive := int64(10)
	for _, test := range []struct {
		name   string
		limits budget.Limits
		valid  bool
	}{
		{name: "token limit cannot be negative", limits: budget.Limits{TokenBudget: &negative}, valid: false},
		{name: "spend limit cannot be negative", limits: budget.Limits{SpendLimitMinor: &negative, Currency: "USD"}, valid: false},
		{name: "spend limit requires ISO currency", limits: budget.Limits{SpendLimitMinor: &positive, Currency: "usd"}, valid: false},
		{name: "token limit does not require currency", limits: budget.Limits{TokenBudget: &positive}, valid: true},
		{name: "complete limits", limits: budget.Limits{TokenBudget: &positive, SpendLimitMinor: &positive, Currency: "USD"}, valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.limits.Validate()
			if test.valid && err != nil {
				t.Fatalf("limits validation error = %v", err)
			}
			if !test.valid && !errors.Is(err, budget.ErrInvalid) {
				t.Fatalf("limits validation error = %v, want ErrInvalid", err)
			}
		})
	}

	if pricing, err := budget.ParsePricing(nil, "USD"); err != nil || pricing.Configured {
		t.Fatalf("empty pricing = %+v, err = %v", pricing, err)
	}
	if err := (budget.Pricing{}).RequirePricing(); !errors.Is(err, budget.ErrCostUnavailable) {
		t.Fatalf("missing pricing requirement error = %v", err)
	}
	for _, test := range []struct {
		name     string
		options  map[string]string
		currency string
		wantErr  error
	}{
		{name: "invalid input rate", options: map[string]string{budget.InputCostOption: "wat", budget.OutputCostOption: "1"}, wantErr: budget.ErrInvalid},
		{name: "negative output rate", options: map[string]string{budget.InputCostOption: "1", budget.OutputCostOption: "-1"}, wantErr: budget.ErrInvalid},
		{name: "invalid configured currency", options: map[string]string{budget.InputCostOption: "1", budget.OutputCostOption: "1"}, currency: "usd", wantErr: budget.ErrInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := budget.ParsePricing(test.options, test.currency); !errors.Is(err, test.wantErr) {
				t.Fatalf("ParsePricing() error = %v, want %v", err, test.wantErr)
			}
		})
	}

	pricing := budget.Pricing{InputMinorPerMillion: 2, OutputMinorPerMillion: 3, Currency: "USD", Configured: true}
	if cost, err := pricing.Cost(budget.Usage{}); err != nil || cost != 0 {
		t.Fatalf("zero usage cost = %d, err = %v", cost, err)
	}
	if _, err := pricing.Cost(budget.Usage{InputTokens: -1}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("negative usage cost error = %v", err)
	}
	if _, err := (budget.Pricing{InputMinorPerMillion: -1}).Cost(budget.Usage{InputTokens: 1}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("negative pricing cost error = %v", err)
	}
	if _, err := (budget.Pricing{InputMinorPerMillion: math.MaxInt64}).Cost(budget.Usage{InputTokens: 2}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("partial cost overflow error = %v", err)
	}
	if _, err := (budget.Pricing{InputMinorPerMillion: math.MaxInt64}).Cost(budget.Usage{InputTokens: 2_000_000}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("whole cost overflow error = %v", err)
	}
	if (budget.Usage{InputTokens: math.MaxInt64, OutputTokens: 1}).Tokens() != math.MaxInt64 {
		t.Fatal("usage token overflow did not saturate")
	}
	if (budget.Estimate{InputTokens: math.MaxInt64, OutputTokens: 1}).Tokens() != math.MaxInt64 {
		t.Fatal("estimate token overflow did not saturate")
	}
	for _, test := range []struct {
		name   string
		calls  int
		output int
	}{
		{name: "zero calls", calls: 0, output: 1},
		{name: "zero output", calls: 1, output: 0},
		{name: "call multiplication overflow", calls: int(^uint(0) >> 1), output: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := budget.EstimateExecution(test.calls, test.output, pricing); !errors.Is(err, budget.ErrInvalid) {
				t.Fatalf("EstimateExecution() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestControllerReserveSettleAndReleaseIsIdempotent(t *testing.T) {
	store := budgetmemory.New()
	controller := budget.NewController(store)
	root := budgetTenant(t, 100, 100)
	ctx := context.Background()

	reservation, err := controller.Reserve(ctx, root, "request-1", budget.Estimate{InputTokens: 10, OutputTokens: 20, SpendMinor: 30})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := controller.Reserve(ctx, root, "request-1", budget.Estimate{InputTokens: 10, OutputTokens: 20, SpendMinor: 30})
	if err != nil || duplicate.State != budget.ReservationStateReserved {
		t.Fatalf("idempotent reserve = %+v, err = %v", duplicate, err)
	}
	if _, err := controller.Reserve(ctx, root, "request-1", budget.Estimate{InputTokens: 11, OutputTokens: 20, SpendMinor: 30}); !errors.Is(err, budget.ErrConflict) {
		t.Fatalf("reserve idempotency conflict = %v", err)
	}

	settled, err := controller.Settle(ctx, reservation, budget.Usage{InputTokens: 4, OutputTokens: 6, SpendMinor: 7})
	if err != nil || settled.State != budget.ReservationStateSettled || settled.ActualTokens != 10 || settled.ActualSpendMinor != 7 {
		t.Fatalf("settlement = %+v, err = %v", settled, err)
	}
	if again, err := controller.Settle(ctx, reservation, budget.Usage{InputTokens: 4, OutputTokens: 6, SpendMinor: 7}); err != nil || again.State != budget.ReservationStateSettled {
		t.Fatalf("idempotent settlement = %+v, err = %v", again, err)
	}
	ledger, err := store.Snapshot(ctx, root.TenantID, time.Now().UTC())
	if err != nil || ledger.UsedTokens != 10 || ledger.UsedMinor != 7 || ledger.ReservedTokens != 0 || ledger.ReservedMinor != 0 {
		t.Fatalf("settled ledger = %+v, err = %v", ledger, err)
	}

	released, err := controller.Reserve(ctx, root, "request-2", budget.Estimate{InputTokens: 20, OutputTokens: 10, SpendMinor: 20})
	if err != nil {
		t.Fatal(err)
	}
	if released, err = controller.Release(ctx, released); err != nil || released.State != budget.ReservationStateReleased {
		t.Fatalf("release = %+v, err = %v", released, err)
	}
	if _, err := controller.Release(ctx, released); err != nil {
		t.Fatalf("idempotent release error = %v", err)
	}
}

func TestControllerFailsClosedAndRecordsActualOverage(t *testing.T) {
	root := budgetTenant(t, 10, 10)
	if _, err := budget.NewController(nil).Reserve(context.Background(), root, "request-1", budget.Estimate{InputTokens: 1}); !errors.Is(err, budget.ErrUnavailable) {
		t.Fatalf("nil store error = %v", err)
	}

	store := budgetmemory.New()
	controller := budget.NewController(store)
	reservation, err := controller.Reserve(context.Background(), root, "request-2", budget.Estimate{InputTokens: 2, SpendMinor: 2})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := controller.Settle(context.Background(), reservation, budget.Usage{InputTokens: 20, SpendMinor: 20})
	if !errors.Is(err, budget.ErrExceeded) || settled.State != budget.ReservationStateSettled {
		t.Fatalf("overage settlement = %+v, err = %v", settled, err)
	}
	ledger, err := store.Snapshot(context.Background(), root.TenantID, time.Now().UTC())
	if err != nil || ledger.UsedTokens != 20 || ledger.UsedMinor != 20 {
		t.Fatalf("overage ledger = %+v, err = %v", ledger, err)
	}
}

func TestControllerDisablesTenantsWithoutLimits(t *testing.T) {
	root := budgetTenant(t, 0, 0)
	root.MonthlyTokenBudget = nil
	root.MonthlySpendLimitMinor = nil
	reservation, err := budget.NewController(nil).Reserve(context.Background(), root, "request-1", budget.Estimate{InputTokens: 1})
	if err != nil || reservation.State != budget.ReservationStateDisabled {
		t.Fatalf("disabled budget = %+v, err = %v", reservation, err)
	}
}

func TestControllerRejectsInvalidContextsAndReservationInputs(t *testing.T) {
	root := budgetTenant(t, 100, 100)
	controller := budget.NewController(budgetmemory.New())
	var nilContext context.Context
	if _, err := controller.Reserve(nilContext, root, "request", budget.Estimate{InputTokens: 1}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("nil context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := controller.Reserve(canceled, root, "request", budget.Estimate{InputTokens: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	for _, id := range []string{"", " leading", "trailing ", "line\nbreak", strings.Repeat("x", 257)} {
		if _, err := controller.Reserve(context.Background(), root, id, budget.Estimate{InputTokens: 1}); !errors.Is(err, budget.ErrInvalid) {
			t.Fatalf("reservation ID %q error = %v", id, err)
		}
	}
	if _, err := controller.Reserve(context.Background(), root, "negative", budget.Estimate{InputTokens: -1}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("negative estimate error = %v", err)
	}
	if _, err := budget.NewController(nil).Settle(context.Background(), budget.Reservation{State: budget.ReservationStateReserved}, budget.Usage{}); !errors.Is(err, budget.ErrUnavailable) {
		t.Fatalf("nil store settlement error = %v", err)
	}
	if _, err := controller.Settle(context.Background(), budget.Reservation{State: budget.ReservationStateReserved}, budget.Usage{InputTokens: -1}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("invalid usage error = %v", err)
	}
	if _, err := controller.Release(nil, budget.Reservation{State: budget.ReservationStateReserved}); !errors.Is(err, budget.ErrInvalid) {
		t.Fatalf("nil release context error = %v", err)
	}
	var nilController *budget.Controller
	if _, err := nilController.Reserve(context.Background(), root, "limited", budget.Estimate{InputTokens: 1}); !errors.Is(err, budget.ErrUnavailable) {
		t.Fatalf("nil controller error = %v", err)
	}
	noLimit := root.Clone()
	noLimit.MonthlyTokenBudget = nil
	noLimit.MonthlySpendLimitMinor = nil
	disabled, err := nilController.Reserve(context.Background(), noLimit, "unlimited", budget.Estimate{InputTokens: 1})
	if err != nil || disabled.State != budget.ReservationStateDisabled {
		t.Fatalf("nil controller disabled result = %+v, err = %v", disabled, err)
	}

	limits := budget.LimitsForTenant(root)
	*limits.TokenBudget = 1
	if *root.MonthlyTokenBudget == 1 {
		t.Fatal("LimitsForTenant returned aliased token budget")
	}
}

func budgetTenant(t *testing.T, tokenBudget, spendLimit int64) tenant.Tenant {
	t.Helper()
	root, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "budget-test", DisplayName: "Budget Test", MonthlyTokenBudget: &tokenBudget,
		MonthlySpendLimitMinor: &spendLimit, BillingCurrency: "USD", AuditRetentionDays: 30,
		LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return *root
}
