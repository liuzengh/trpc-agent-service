package qdrant

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestValidateBackend(t *testing.T) {
	valid := tenant.BackendRef{
		Kind:     tenant.BackendVector,
		Provider: providerName,
		Name:     "shared-qdrant",
		Options: map[string]string{
			optionEmbeddingModel:      "text-embedding-3-small",
			optionEmbeddingDimensions: "1536",
			optionEmbeddingProfile:    "text-embedding-3-small",
			optionIndexGeneration:     "g1",
		},
	}
	if err := ValidateBackend(valid); err != nil {
		t.Fatalf("validate backend: %v", err)
	}

	for name, mutate := range map[string]func(*tenant.BackendRef){
		"kind":     func(ref *tenant.BackendRef) { ref.Kind = tenant.BackendSQL },
		"provider": func(ref *tenant.BackendRef) { ref.Provider = "other" },
		"dimensions": func(ref *tenant.BackendRef) {
			ref.Options[optionEmbeddingDimensions] = "invalid"
		},
		"profile": func(ref *tenant.BackendRef) {
			ref.Options[optionEmbeddingProfile] = "invalid profile"
		},
		"generation": func(ref *tenant.BackendRef) {
			ref.Options[optionIndexGeneration] = "-g1"
		},
	} {
		t.Run(name, func(t *testing.T) {
			ref := valid
			ref.Options = make(map[string]string, len(valid.Options))
			for key, value := range valid.Options {
				ref.Options[key] = value
			}
			mutate(&ref)
			if err := ValidateBackend(ref); err == nil {
				t.Fatal("ValidateBackend() error = nil")
			}
		})
	}
}

func TestEndpointValidate(t *testing.T) {
	if err := (Endpoint{Host: "qdrant", Port: 6334}).Validate(); err != nil {
		t.Fatalf("validate endpoint: %v", err)
	}
	for _, endpoint := range []Endpoint{
		{Port: 6334},
		{Host: "qdrant"},
		{Host: "qdrant", Port: 65536},
	} {
		if err := endpoint.Validate(); err == nil {
			t.Fatalf("Validate(%#v) error = nil", endpoint)
		}
	}
}
