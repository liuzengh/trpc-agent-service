package config

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestPhase7ExampleConfigsLoad(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", "01234567890123456789012345678901")
	t.Setenv("PHASE7_MODEL_KEY", "mock-local")
	t.Setenv("PHASE7_REAL_MODEL_KEY", "test-only")
	t.Setenv("PHASE7_TELEGRAM_TOKEN", "mock-token")
	t.Setenv("PHASE7_WECOM_BOT_ID", "test-bot")
	t.Setenv("PHASE7_WECOM_BOT_SECRET", "test-secret")
	t.Setenv("PHASE7_REDIS_URL", "redis://messaging-redis:6379/0")
	t.Setenv("PHASE7_CONTROL_REDIS_URL", "redis://control-redis:6379/0")
	t.Setenv("PHASE7_ADMIN_TOKEN", "admin-token")
	t.Setenv("PHASE7_POSTGRES_DSN", "postgres://phase7:phase7@postgres:5432/phase7?sslmode=disable")
	t.Setenv("PHASE7_MYSQL_DSN", "phase7:phase7@tcp(mysql:3306)/phase7?parseTime=true&charset=utf8mb4&loc=UTC")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	for _, name := range []string{"phase7.full.example.json", "phase7.light.example.json", "phase7.real-model.example.json", "phase7.real-wecom.example.json", "phase7.real-feishu.example.json"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("PLATFORM_CONFIG_FILE", filepath.Join("..", "..", "configs", name))
			cfg, err := LoadForRole(RoleServe)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := cfg.RuntimeCatalog(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPhase7RealIMConfigsIncludeWebAndRealBindings(t *testing.T) {
	t.Setenv("IDENTITY_SECRET", "01234567890123456789012345678901")
	t.Setenv("PHASE7_REDIS_URL", "redis://messaging-redis:6379/0")
	t.Setenv("PHASE7_REAL_MODEL_KEY", "test-only")

	tests := []struct {
		name          string
		tenantID      string
		realChannel   string
		realBindingID string
		realAccountID string
	}{
		{
			name:          "phase7.real-wecom.example.json",
			tenantID:      "tenant-wecom",
			realChannel:   "wecom_aibot",
			realBindingID: "wecom-real",
			realAccountID: "wecom-real",
		},
		{
			name:          "phase7.real-feishu.example.json",
			tenantID:      "tenant-feishu",
			realChannel:   "feishu",
			realBindingID: "feishu-real",
			realAccountID: "feishu-real",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("PLATFORM_CONFIG_FILE", filepath.Join("..", "..", "configs", test.name))
			cfg, err := LoadForRole(RoleWorker)
			if err != nil {
				t.Fatal(err)
			}
			repository, err := tenant.NewPresetRepository(cfg.Catalog)
			if err != nil {
				t.Fatal(err)
			}

			assertBinding(t, repository, "demo", "demo-binding", "demo-binding", test.tenantID)
			assertBinding(t, repository, test.realChannel, test.realBindingID, test.realAccountID, test.tenantID)
		})
	}
}

func assertBinding(t *testing.T, repository tenant.Repository, channel, bindingID, accountID, tenantID string) {
	t.Helper()
	binding, err := repository.ResolveBinding(context.Background(), channel, bindingID)
	if err != nil {
		t.Fatalf("resolve binding %q for channel %q: %v", bindingID, channel, err)
	}
	if binding.ID != bindingID || binding.Channel != channel || binding.ExternalAccountID != accountID || binding.TenantID != tenantID || binding.AgentAppID != "assistant" {
		t.Fatalf("binding %q = %#v", bindingID, binding)
	}
}
