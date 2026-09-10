package bootstrap

import (
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

func TestEnvironmentModelCatalogAcceptsBudgetPricingOptions(t *testing.T) {
	config := environmentConfig{
		modelProvider: defaultModelProvider,
		modelNames:    []string{"gpt-4o-mini"},
		endpointHosts: []string{"api.openai.com"},
	}
	catalog, _, err := environmentCatalogs(config)
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := catalog.NormalizeConfiguration(modelprofile.Configuration{
		Provider: defaultModelProvider, Model: "gpt-4o-mini", Endpoint: "https://api.openai.com",
		SecretRef: "env/model", Options: map[string]string{
			runtimebudget.InputCostOption: "2", runtimebudget.OutputCostOption: "4",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if configuration.Options[runtimebudget.InputCostOption] != "2" || configuration.Options[runtimebudget.OutputCostOption] != "4" {
		t.Fatalf("normalized pricing options = %+v", configuration.Options)
	}
}
