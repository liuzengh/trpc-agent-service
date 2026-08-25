package governance

import (
	"context"
	"errors"
	"testing"

	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
)

func TestMemoryControllerLimitsAndReconciliation(t *testing.T) {
	controller := NewMemoryController()
	profile := tenant.Tenant{ID: "tenant", Budget: tenant.BudgetPolicy{
		MaxConcurrent: 1, MaxInputTokens: 8, MaxOutputTokens: 10,
		DailyCostUSD: 0.000020, InputCostPerMillionUSD: 1, OutputCostPerMillionUSD: 1,
	}}
	ctx := context.Background()
	if err := controller.Reserve(ctx, profile, "one", "hello"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Reserve(ctx, profile, "two", "hello"); !errors.Is(err, ErrConcurrencyLimit) {
		t.Fatalf("concurrency error=%v", err)
	}
	if _, err := controller.Finalize(ctx, profile, "one", 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := controller.Reserve(ctx, profile, "two", "hello"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Cancel(ctx, profile, "two"); err != nil {
		t.Fatal(err)
	}
	if err := controller.Reserve(ctx, profile, "large", "这是一个明显超过八个字符的输入"); !errors.Is(err, ErrInputTokenLimit) {
		t.Fatalf("input error=%v", err)
	}
}

func TestCostUSD(t *testing.T) {
	policy := tenant.BudgetPolicy{InputCostPerMillionUSD: 2, OutputCostPerMillionUSD: 4}
	if got := CostUSD(policy, 1_000_000, 500_000); got != 4 {
		t.Fatalf("cost=%v", got)
	}
}
