package agent

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/modelusage"
)

func TestPriceModelUsageUsesActualFailoverCandidateBreakdown(t *testing.T) {
	primaryCached := int64(500_000)
	catalog, err := NewProviderCostCatalog([]config.ModelProviderConfig{
		{ID: "primary", Models: []config.ModelPricingConfig{{
			Name: "expensive", PromptCostMicrosPerMillionTokens: 2_000_000,
			CachedPromptCostMicrosPerMillionTokens: &primaryCached, CompletionCostMicrosPerMillionTokens: 4_000_000,
		}}},
		{ID: "fallback", Models: []config.ModelPricingConfig{{
			Name: "cheap", PromptCostMicrosPerMillionTokens: 1_000_000, CompletionCostMicrosPerMillionTokens: 2_000_000,
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{costCalculator: catalog}
	usage := runtime.priceModelUsage(config.ModelConfig{ProviderID: "primary", Name: "expensive"}, nil, []modelusage.Segment{
		{ProviderID: "primary", ModelName: "expensive", ReportedModel: "expensive-v1", PromptTokens: 1_000_000, CachedPromptTokens: 500_000, CompletionTokens: 500_000, TotalTokens: 1_500_000},
		{ProviderID: "fallback", ModelName: "cheap", ReportedModel: "cheap-v2", PromptTokens: 1_000_000, CompletionTokens: 500_000, TotalTokens: 1_500_000},
	})
	if usage.ProviderID != "mixed" || usage.ModelName != "mixed" || len(usage.Breakdown) != 2 {
		t.Fatalf("priced usage identity = %#v", usage)
	}
	// primary: 0.5M uncached * $2 + 0.5M cached * $0.5 + 0.5M completion * $4 = $3.25
	// fallback: 1M prompt * $1 + 0.5M completion * $2 = $2.00
	if usage.CostMicros != 5_250_000 {
		t.Fatalf("mixed candidate cost = %d, want 5250000", usage.CostMicros)
	}
	if usage.PromptTokens != 2_000_000 || usage.CompletionTokens != 1_000_000 || usage.TotalTokens != 3_000_000 {
		t.Fatalf("mixed candidate token totals = %#v", usage)
	}
}
