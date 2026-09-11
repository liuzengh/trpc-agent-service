package agent

import (
	"math"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestProviderCostCatalogUsesOnlyPlatformPricing(t *testing.T) {
	cachedPromptRate := int64(100000)
	catalog, err := NewProviderCostCatalog([]config.ModelProviderConfig{{
		ID: "primary",
		Models: []config.ModelPricingConfig{{
			Name: "model-a", PromptCostMicrosPerMillionTokens: 1000000,
			CachedPromptCostMicrosPerMillionTokens: &cachedPromptRate,
			CompletionCostMicrosPerMillionTokens:   2000000,
		}},
	}})
	if err != nil {
		t.Fatalf("NewProviderCostCatalog() error = %v", err)
	}
	if got := catalog.CostMicros(config.ModelConfig{ProviderID: "primary", Name: "model-a"}, PricedTokenUsage{
		PromptTokens: 2_000_000, CachedPromptTokens: 1_000_000, CompletionTokens: 3_000_000,
	}); got != 7_100_000 {
		t.Fatalf("CostMicros() = %d, want 7100000", got)
	}
	if got := catalog.CostMicros(config.ModelConfig{ProviderID: "primary", Name: "unknown"}, PricedTokenUsage{PromptTokens: 100, CompletionTokens: 100}); got != 0 {
		t.Fatalf("unknown price cost = %d, want 0", got)
	}
	if got := catalog.CostMicros(config.ModelConfig{ProviderID: "primary", Name: "model-a"}, PricedTokenUsage{PromptTokens: 1, CachedPromptTokens: 2}); got != 0 {
		t.Fatalf("invalid cached token count cost = %d, want 0", got)
	}
}

func TestProviderCostCatalogRejectsInvalidPricingCatalog(t *testing.T) {
	negative := int64(-1)
	tests := []struct {
		name      string
		providers []config.ModelProviderConfig
	}{
		{name: "blank provider", providers: []config.ModelProviderConfig{{ID: " "}}},
		{name: "blank model", providers: []config.ModelProviderConfig{{ID: "primary", Models: []config.ModelPricingConfig{{Name: " "}}}}},
		{name: "negative prompt", providers: []config.ModelProviderConfig{{ID: "primary", Models: []config.ModelPricingConfig{{Name: "model-a", PromptCostMicrosPerMillionTokens: -1}}}}},
		{name: "negative completion", providers: []config.ModelProviderConfig{{ID: "primary", Models: []config.ModelPricingConfig{{Name: "model-a", CompletionCostMicrosPerMillionTokens: -1}}}}},
		{name: "negative cached prompt", providers: []config.ModelProviderConfig{{ID: "primary", Models: []config.ModelPricingConfig{{Name: "model-a", CachedPromptCostMicrosPerMillionTokens: &negative}}}}},
		{name: "duplicate model", providers: []config.ModelProviderConfig{{ID: "primary", Models: []config.ModelPricingConfig{{Name: "model-a"}, {Name: " model-a "}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewProviderCostCatalog(test.providers); err == nil {
				t.Fatal("NewProviderCostCatalog() error = nil")
			}
		})
	}
}

func TestProviderCostCatalogRejectsInvalidUsageAndArithmeticOverflow(t *testing.T) {
	catalog, err := NewProviderCostCatalog([]config.ModelProviderConfig{{
		ID: "primary",
		Models: []config.ModelPricingConfig{{
			Name: "model-a", PromptCostMicrosPerMillionTokens: 1_000_000,
			CompletionCostMicrosPerMillionTokens: 2_000_000,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, usage := range []PricedTokenUsage{
		{PromptTokens: -1},
		{CachedPromptTokens: -1},
		{CompletionTokens: -1},
		{PromptTokens: 1, CachedPromptTokens: 2},
	} {
		if got := catalog.CostMicros(config.ModelConfig{ProviderID: "primary", Name: "model-a"}, usage); got != 0 {
			t.Fatalf("CostMicros(%+v) = %d, want 0", usage, got)
		}
	}
	var nilCatalog *ProviderCostCatalog
	if got := nilCatalog.CostMicros(config.ModelConfig{}, PricedTokenUsage{}); got != 0 {
		t.Fatalf("nil catalog cost = %d, want 0", got)
	}
	if got := costForTokens(0, 1); got != 0 {
		t.Fatalf("zero token cost = %d, want 0", got)
	}
	if got := costForTokens(1, 0); got != 0 {
		t.Fatalf("zero rate cost = %d, want 0", got)
	}
	if got := costForTokens(1, math.MaxInt64); got != 0 {
		t.Fatalf("overflow token cost = %d, want 0", got)
	}
	if got := addCosts(math.MaxInt64, 1); got != 0 {
		t.Fatalf("overflow sum = %d, want 0", got)
	}
	if got := addCosts(1, -1); got != 0 {
		t.Fatalf("negative sum = %d, want 0", got)
	}
}

func TestProviderCostCatalogDefaultsCacheReadsToPromptRate(t *testing.T) {
	catalog, err := NewProviderCostCatalog([]config.ModelProviderConfig{{
		ID: "primary", Models: []config.ModelPricingConfig{{
			Name: "model-a", PromptCostMicrosPerMillionTokens: 1_000_000,
			CompletionCostMicrosPerMillionTokens: 2_000_000,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got := catalog.CostMicros(config.ModelConfig{ProviderID: "primary", Name: "model-a"}, PricedTokenUsage{
		PromptTokens: 2_000_000, CachedPromptTokens: 1_000_000, CompletionTokens: 3_000_000,
	})
	if got != 8_000_000 {
		t.Fatalf("CostMicros() = %d, want undiscounted 8000000", got)
	}
}
