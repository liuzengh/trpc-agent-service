package config

import "testing"

func TestModelCatalogRequiresExplicitInputCapabilitiesAcrossFailoverChain(t *testing.T) {
	catalog, err := NewModelCatalog([]ModelProviderConfig{
		{ID: "primary", Models: []ModelPricingConfig{{Name: "vision", Capabilities: &ModelCapabilities{Input: &ModelInputCapabilities{Image: true, File: true}}}}},
		{ID: "fallback", Models: []ModelPricingConfig{{Name: "text-only"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	model := ModelConfig{ProviderID: "primary", Name: "vision"}
	if err := catalog.ValidateInputs(model, []ModelInputKind{ModelInputImage, ModelInputFile}); err != nil {
		t.Fatalf("primary explicit inputs rejected: %v", err)
	}
	model.FailoverCandidates = []ModelCandidate{{ProviderID: "fallback", Name: "text-only"}}
	if err := catalog.ValidateInputs(model, []ModelInputKind{ModelInputImage}); err == nil {
		t.Fatal("ValidateInputs() error = nil, want undeclared failover capability rejected")
	}
	if err := catalog.ValidateInputs(ModelConfig{ProviderID: "primary", Name: "vision"}, []ModelInputKind{ModelInputAudio}); err == nil {
		t.Fatal("ValidateInputs(audio) error = nil, want unsupported input rejected")
	}
}

func TestModelCatalogReportsCapabilitiesGuaranteedAcrossFailover(t *testing.T) {
	catalog, err := NewModelCatalog([]ModelProviderConfig{
		{ID: "primary", Models: []ModelPricingConfig{{Name: "vision", Capabilities: &ModelCapabilities{Input: &ModelInputCapabilities{Image: true, File: true}}}}},
		{ID: "fallback", Models: []ModelPricingConfig{{Name: "vision", Capabilities: &ModelCapabilities{Input: &ModelInputCapabilities{Image: true}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, ok := catalog.GuaranteedInputCapabilities(ModelConfig{
		ProviderID: "primary", Name: "vision", FailoverCandidates: []ModelCandidate{{ProviderID: "fallback", Name: "vision"}},
	})
	if !ok || !capabilities.Image || capabilities.File || capabilities.Audio {
		t.Fatalf("guaranteed capabilities = %+v, %v", capabilities, ok)
	}
}
