package agent

import (
	"fmt"
	"math"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// ModelCostCalculator turns provider token totals into platform account cost.
type ModelCostCalculator interface {
	CostMicros(config.ModelConfig, PricedTokenUsage) int64
}

// PricedTokenUsage is the provider-reported token breakdown used for billing.
// CachedPromptTokens is a subset of PromptTokens, never an additional charge.
type PricedTokenUsage struct {
	PromptTokens, CachedPromptTokens, CompletionTokens int
}

// ProviderCostCatalog is an immutable platform-owned price lookup table.
type ProviderCostCatalog struct {
	rates map[string]modelCostRate
}

type modelCostRate struct {
	prompt, cachedPrompt, completion int64
}

func NewProviderCostCatalog(providers []config.ModelProviderConfig) (*ProviderCostCatalog, error) {
	catalog := &ProviderCostCatalog{rates: make(map[string]modelCostRate)}
	for _, provider := range providers {
		providerID := strings.TrimSpace(provider.ID)
		if providerID == "" {
			return nil, fmt.Errorf("model provider ID is required")
		}
		for _, pricing := range provider.Models {
			name := strings.TrimSpace(pricing.Name)
			if name == "" || pricing.PromptCostMicrosPerMillionTokens < 0 || pricing.CompletionCostMicrosPerMillionTokens < 0 ||
				(pricing.CachedPromptCostMicrosPerMillionTokens != nil && *pricing.CachedPromptCostMicrosPerMillionTokens < 0) {
				return nil, fmt.Errorf("invalid pricing for provider %q", providerID)
			}
			key := providerID + "\x00" + name
			if _, exists := catalog.rates[key]; exists {
				return nil, fmt.Errorf("duplicate pricing for provider %q model %q", providerID, name)
			}
			cachedPromptRate := pricing.PromptCostMicrosPerMillionTokens
			if pricing.CachedPromptCostMicrosPerMillionTokens != nil {
				cachedPromptRate = *pricing.CachedPromptCostMicrosPerMillionTokens
			}
			catalog.rates[key] = modelCostRate{
				prompt: pricing.PromptCostMicrosPerMillionTokens, cachedPrompt: cachedPromptRate,
				completion: pricing.CompletionCostMicrosPerMillionTokens,
			}
		}
	}
	return catalog, nil
}

func (c *ProviderCostCatalog) CostMicros(model config.ModelConfig, usage PricedTokenUsage) int64 {
	if c == nil || usage.PromptTokens < 0 || usage.CachedPromptTokens < 0 || usage.CompletionTokens < 0 ||
		usage.CachedPromptTokens > usage.PromptTokens {
		return 0
	}
	rate, ok := c.rates[strings.TrimSpace(model.ProviderID)+"\x00"+strings.TrimSpace(model.Name)]
	if !ok {
		return 0
	}
	uncachedPromptTokens := usage.PromptTokens - usage.CachedPromptTokens
	promptCost := costForTokens(usage.PromptTokens, rate.prompt)
	if rate.cachedPrompt != rate.prompt {
		promptCost = addCosts(
			costForTokens(uncachedPromptTokens, rate.prompt),
			costForTokens(usage.CachedPromptTokens, rate.cachedPrompt),
		)
	}
	return addCosts(promptCost, costForTokens(usage.CompletionTokens, rate.completion))
}

func costForTokens(tokens int, rate int64) int64 {
	if tokens <= 0 || rate <= 0 || int64(tokens) > (math.MaxInt64-999999)/rate {
		return 0
	}
	return (int64(tokens)*rate + 999999) / 1000000
}

func addCosts(costs ...int64) int64 {
	var total int64
	for _, cost := range costs {
		if cost < 0 || total > math.MaxInt64-cost {
			return 0
		}
		total += cost
	}
	return total
}
