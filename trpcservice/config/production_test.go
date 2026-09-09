package config

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func productionFile(t *testing.T) *File {
	t.Helper()
	file, err := Load(strings.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	app := &file.Tenants[0].Apps[0]
	ref := func(name string) tenant.SecretRef {
		return tenant.SecretRef{Provider: tenant.SecretProviderVault, Key: "tenant-a/support/" + name}
	}
	app.Model.APIKey = ref("model")
	app.Channels[1].Token = ref("feishu-token")
	app.Channels[1].Secret = ref("feishu-secret")
	app.Storage.Session.Credential = ref("session")
	app.Storage.Session.MigrationTarget = &tenant.BackendConfig{
		Type:       tenant.BackendRedis,
		Endpoint:   "redis://redis.example.com:6379/0",
		Credential: ref("session-target"),
		Namespace:  "target",
	}
	return file
}

func TestValidateProductionAcceptsTenantAppSecretNamespace(t *testing.T) {
	if err := productionFile(t).ValidateProduction(); err != nil {
		t.Fatalf("ValidateProduction() error = %v", err)
	}
}

func TestValidateProductionRejectsCrossScopeAndNormalizedSecretPaths(t *testing.T) {
	tests := []string{
		"tenant-b/support/model",
		"tenant-a/support/../other",
		"tenant-a/support/path//key",
		`tenant-a/support/path\key`,
	}
	for _, key := range tests {
		t.Run(key, func(t *testing.T) {
			file := productionFile(t)
			file.Tenants[0].Apps[0].Model.APIKey.Key = key
			if err := file.ValidateProduction(); err == nil {
				t.Fatalf("ValidateProduction() accepted %q", key)
			}
		})
	}
}

func TestValidateProductionChecksMigrationTargetSecretNamespace(t *testing.T) {
	file := productionFile(t)
	file.Tenants[0].Apps[0].Storage.Session.MigrationTarget.Credential.Key = "tenant-b/support/session-target"
	if err := file.ValidateProduction(); err == nil || !strings.Contains(err.Error(), "migration_target") {
		t.Fatalf("migration target error = %v", err)
	}
}
