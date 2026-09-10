package agent

import (
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
