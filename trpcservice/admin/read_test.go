package admin

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestSanitizeStringMapOmitsOpaqueConfigValues(t *testing.T) {
	got := sanitizeStringMap(map[string]string{
		"endpoint": "postgres://user:raw-secret@db.internal/app",
		"host":     "db.internal",
	})
	if got != nil {
		t.Fatalf("opaque config values = %#v, want omitted", got)
	}
}

func TestSanitizeAppConfigKeepsOnlyNonSecretRuntimeOptions(t *testing.T) {
	config := tenant.AppConfig{
		Model: tenant.ModelConfig{
			APIKeyRef: tenant.SecretRef{Name: "model-api-key", Version: "v3"},
			Parameters: map[string]string{
				"base_url": "https://api.example.test/v1",
				"api_key":  "should-not-return",
			},
		},
		BackendConfig: tenant.BackendConfig{
			Session: tenant.BackendRef{
				SecretRef: tenant.SecretRef{Name: "session-dsn", Version: "v2"},
				Options:   map[string]string{"schema": "agent", "dsn": "secret"},
			},
			Knowledge: tenant.BackendRef{Options: map[string]string{"index_generation": "g1", "api_key": "secret"}},
		},
		SecretRefs: []tenant.SecretRef{{Name: "model-api-key", Version: "v3"}},
	}
	got := sanitizeAppConfig(config)
	if got.Model.APIKeyRef != config.Model.APIKeyRef || got.BackendConfig.Session.SecretRef != config.BackendConfig.Session.SecretRef {
		t.Fatalf("secret references = %#v / %#v, want preserved", got.Model.APIKeyRef, got.BackendConfig.Session.SecretRef)
	}
	if len(got.SecretRefs) != 1 || got.SecretRefs[0] != config.SecretRefs[0] {
		t.Fatalf("config secret references = %#v, want preserved", got.SecretRefs)
	}
	if got.Model.Parameters["base_url"] != "https://api.example.test/v1" || got.Model.Parameters["api_key"] != "" {
		t.Fatalf("model parameters = %#v", got.Model.Parameters)
	}
	if got.BackendConfig.Session.Options["schema"] != "agent" || got.BackendConfig.Session.Options["dsn"] != "" {
		t.Fatalf("session options = %#v", got.BackendConfig.Session.Options)
	}
	if got.BackendConfig.Knowledge.Options["index_generation"] != "g1" || got.BackendConfig.Knowledge.Options["api_key"] != "" {
		t.Fatalf("knowledge options = %#v", got.BackendConfig.Knowledge.Options)
	}
}

func TestAppConfigViewKeepsSecretReferencesWithoutOpaqueValues(t *testing.T) {
	encoded, err := json.Marshal(AppConfigView{Config: tenant.AppConfig{
		Model: tenant.ModelConfig{
			APIKeyRef:  tenant.SecretRef{Name: "model-api-key", Version: "v3"},
			Parameters: map[string]string{"api_key": "raw-secret"},
		},
		BackendConfig: tenant.BackendConfig{
			Session: tenant.BackendRef{
				SecretRef: tenant.SecretRef{Name: "session-dsn", Version: "v2"},
				Options:   map[string]string{"dsn": "postgres://user:password@db/app"},
			},
		},
		SecretRefs: []tenant.SecretRef{{Name: "model-api-key", Version: "v3"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	output := string(encoded)
	for _, value := range []string{"raw-secret", "password@db"} {
		if strings.Contains(output, value) {
			t.Fatalf("wire config contains opaque secret value %q: %s", value, output)
		}
	}
	for _, ref := range []string{"\"api_key_ref\":{\"name\":\"model-api-key\",\"version\":\"v3\"}", "\"secret_ref\":{\"name\":\"session-dsn\",\"version\":\"v2\"}", "\"secret_refs\":[{\"name\":\"model-api-key\",\"version\":\"v3\"}]"} {
		if !strings.Contains(output, ref) {
			t.Fatalf("wire config omits secret reference %q: %s", ref, output)
		}
	}
}
