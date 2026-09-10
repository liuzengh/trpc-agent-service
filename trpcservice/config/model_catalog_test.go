package config

import "testing"

func TestModelCatalogKeepsStaticProviderConfigurationImmutable(t *testing.T) {
	cachedRate := int64(25)
	input := []ModelProviderConfig{{
		ID: "hf-main",
		Models: []ModelPricingConfig{{
			Name:                                   " org/model ",
			CachedPromptCostMicrosPerMillionTokens: &cachedRate,
			Capabilities: &ModelCapabilities{
				ReasoningEfforts: []string{"high"},
			},
		}},
	}}
	catalog, err := NewModelCatalog(input)
	if err != nil {
		t.Fatal(err)
	}

	input[0].Models[0].Name = "mutated-input"
	*input[0].Models[0].CachedPromptCostMicrosPerMillionTokens = 99
	input[0].Models[0].Capabilities.ReasoningEfforts[0] = "low"
	provider, ok := catalog.Provider("hf-main")
	if !ok {
		t.Fatal("Provider() missing hf-main")
	}
	if provider.Models[0].Name != "org/model" || *provider.Models[0].CachedPromptCostMicrosPerMillionTokens != 25 || provider.Models[0].Capabilities.ReasoningEfforts[0] != "high" {
		t.Fatalf("catalog was mutated through constructor input: %+v", provider.Models[0])
	}

	provider.Models[0].Name = "mutated-output"
	*provider.Models[0].CachedPromptCostMicrosPerMillionTokens = 77
	provider.Models[0].Capabilities.ReasoningEfforts[0] = "low"
	again, _ := catalog.Provider("hf-main")
	if again.Models[0].Name != "org/model" || *again.Models[0].CachedPromptCostMicrosPerMillionTokens != 25 || again.Models[0].Capabilities.ReasoningEfforts[0] != "high" {
		t.Fatalf("catalog was mutated through Provider() output: %+v", again.Models[0])
	}
}
