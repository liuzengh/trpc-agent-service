package deploy_test

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

// Admin authorizes a tenant-scoped model reference without resolving its
// value; only execution processes receive the actual model credential.
func TestComposeModelCredentialAndOperatorEndpointScopes(t *testing.T) {
	data, err := os.ReadFile("docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	for _, service := range []string{"worker", "summary-worker"} {
		if compose.Services[service].Environment["TRPC_SECRET_OPENAI_API_KEY"] != "${TRPC_SECRET_OPENAI_API_KEY:-}" {
			t.Errorf("%s cannot resolve the model credential used by execution", service)
		}
	}
	for service, config := range compose.Services {
		_, hasKey := config.Environment["TRPC_SECRET_OPENAI_API_KEY"]
		_, hasEndpoint := config.Environment["TRPC_OPENAI_BASE_URL"]
		wantKey := service == "worker" || service == "summary-worker"
		wantEndpoint := service == "worker" || service == "summary-worker"
		if hasKey != wantKey {
			t.Errorf("%s model credential scope differs from admission/execution requirements", service)
		}
		if hasEndpoint != wantEndpoint {
			t.Errorf("%s operator model endpoint must be restricted to execution processes", service)
		}
		if wantEndpoint && config.Environment["TRPC_OPENAI_BASE_URL"] != "${TRPC_OPENAI_BASE_URL:-}" {
			t.Errorf("%s operator model endpoint is not wired to operator configuration", service)
		}
	}
}
