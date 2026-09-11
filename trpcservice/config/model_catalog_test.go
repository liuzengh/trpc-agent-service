package config

import (
	"reflect"
	"testing"
)

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

func TestModelCatalogValidatesProviderIdentity(t *testing.T) {
	t.Parallel()
	if _, err := NewModelCatalog([]ModelProviderConfig{{ID: " "}}); err == nil {
		t.Fatal("NewModelCatalog(empty provider ID) error = nil")
	}
	if _, err := NewModelCatalog([]ModelProviderConfig{{ID: "support-models"}, {ID: " support-models "}}); err == nil {
		t.Fatal("NewModelCatalog(duplicate provider) error = nil")
	}
	var nilCatalog *ModelCatalog
	if got := nilCatalog.Providers(); got != nil {
		t.Fatalf("nil Providers() = %+v", got)
	}
	if _, ok := nilCatalog.Provider("support-models"); ok {
		t.Fatal("nil Provider() unexpectedly succeeded")
	}
}

func TestModelCatalogAllowsConfiguredAndDiscoveredModels(t *testing.T) {
	t.Parallel()
	catalog, err := NewModelCatalog([]ModelProviderConfig{{
		ID:     "support-models",
		Models: []ModelPricingConfig{{Name: " support-primary "}, {Name: "support-backup"}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	if !catalog.AllowsModel(" support-models ", " support-primary ") {
		t.Fatal("configured model was not allowed")
	}
	if catalog.AllowsModel("support-models", "missing") {
		t.Fatal("unknown model was allowed")
	}
	if catalog.AllowsModel("missing", "support-primary") {
		t.Fatal("unknown provider model was allowed")
	}
	if catalog.AllowsModel("", "support-primary") || catalog.AllowsModel("support-models", "") {
		t.Fatal("empty model identity was allowed")
	}
	var nilCatalog *ModelCatalog
	if nilCatalog.AllowsModel("support-models", "support-primary") {
		t.Fatal("nil catalog allowed a model")
	}

	catalog.ReplaceDiscoveredModels(" support-models ", []string{" dynamic-b ", "dynamic-a", "", "dynamic-a"})
	if !catalog.AllowsModel("support-models", "dynamic-a") {
		t.Fatal("discovered model was not allowed")
	}
	want := []string{"dynamic-a", "dynamic-b"}
	if got := catalog.ListDiscoveredModels(" support-models "); !reflect.DeepEqual(got, want) {
		t.Fatalf("ListDiscoveredModels() = %+v, want %+v", got, want)
	}
	catalog.RemoveDiscoveredModel(" support-models ", " dynamic-a ")
	if catalog.AllowsModel("support-models", "dynamic-a") {
		t.Fatal("removed discovered model remained allowed")
	}
	if got := catalog.ListDiscoveredModels("support-models"); !reflect.DeepEqual(got, []string{"dynamic-b"}) {
		t.Fatalf("ListDiscoveredModels(after remove) = %+v", got)
	}

	catalog.ReplaceDiscoveredModels("missing", []string{"ignored"})
	if got := catalog.ListDiscoveredModels("missing"); len(got) != 0 {
		t.Fatalf("unknown provider discovered models = %+v", got)
	}
	nilCatalog.ReplaceDiscoveredModels("support-models", []string{"ignored"})
	nilCatalog.RemoveDiscoveredModel("support-models", "ignored")
	if got := nilCatalog.ListDiscoveredModels("support-models"); got != nil {
		t.Fatalf("nil ListDiscoveredModels() = %+v", got)
	}
}

func TestModelCatalogProvidersReturnsIndependentOrderedCopy(t *testing.T) {
	t.Parallel()
	catalog, err := NewModelCatalog([]ModelProviderConfig{
		{ID: "support-primary", Models: []ModelPricingConfig{{Name: "model-a"}}},
		{ID: "support-secondary", Models: []ModelPricingConfig{{Name: "model-b"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	providers := catalog.Providers()
	if len(providers) != 2 || providers[0].ID != "support-primary" || providers[1].ID != "support-secondary" {
		t.Fatalf("Providers() = %+v", providers)
	}
	providers[0].Models[0].Name = "mutated"
	again := catalog.Providers()
	if again[0].Models[0].Name != "model-a" {
		t.Fatalf("Providers() returned aliased data: %+v", again[0])
	}
	if _, ok := catalog.Provider("missing"); ok {
		t.Fatal("Provider(missing) unexpectedly succeeded")
	}
}
