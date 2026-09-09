package tenant

import (
	"errors"
	"testing"
)

func TestResolveSecret(t *testing.T) {
	t.Setenv("MODEL_KEY_TENANT_A", "test-secret")
	value, err := ResolveSecret("env:MODEL_KEY_TENANT_A")
	if err != nil {
		t.Fatalf("ResolveSecret() error = %v", err)
	}
	if value != "test-secret" {
		t.Fatal("ResolveSecret() returned an unexpected value")
	}

	if _, err := ResolveSecret("plain-text"); err == nil {
		t.Fatal("ResolveSecret() accepted a plain-text secret")
	}
}

func TestValidationRejectsPlaintextSecrets(t *testing.T) {
	app := validApp()
	app.Model.APIKeyRef = "plain-text"
	if err := app.Validate(); err == nil {
		t.Fatal("AgentApp.Validate() accepted a plain-text API key")
	}

	binding := validBinding()
	binding.Config["app_secret"] = "plain-text"
	if err := binding.Validate(); err == nil {
		t.Fatal("ChannelBinding.Validate() accepted a plain-text secret")
	}
}

func TestValidationAcceptsSupportedConfiguration(t *testing.T) {
	tenantValue := validTenant()
	if err := tenantValue.Validate(); err != nil {
		t.Fatalf("Tenant.Validate() error = %v", err)
	}
	if err := validApp().Validate(); err != nil {
		t.Fatalf("AgentApp.Validate() error = %v", err)
	}
	if err := validBinding().Validate(); err != nil {
		t.Fatalf("ChannelBinding.Validate() error = %v", err)
	}
	if !errors.Is(ErrNotFound, ErrNotFound) {
		t.Fatal("ErrNotFound identity changed")
	}
}

func validTenant() Tenant {
	return Tenant{
		ID:       "tenant-a",
		Name:     "Tenant A",
		IsActive: true,
		Quota: Quota{
			DailyTokenLimit: 100_000,
			RatePerMinute:   60,
		},
		Policy: Policy{
			RedactPatterns: []string{"account-[0-9]+"},
			AuditLevel:     "full",
			BudgetCNY:      100,
		},
	}
}

func validApp() AgentApp {
	return AgentApp{
		ID:       "app-a",
		TenantID: "tenant-a",
		AppName:  "tenant-a-support",
		Model: ModelConfig{
			Provider:  "openai-compatible",
			Model:     "test-model",
			APIKeyRef: "env:MODEL_KEY_TENANT_A",
		},
		Tools: []string{"search"},
		Backends: BackendSelection{
			Session: "redis",
			Memory:  "pgvector",
		},
	}
}

func validBinding() ChannelBinding {
	return ChannelBinding{
		ID:       "binding-a",
		TenantID: "tenant-a",
		AppID:    "app-a",
		Channel:  "feishu",
		RouteKey: "cli-test-app",
		Config: map[string]string{
			"app_secret": "env:FEISHU_APP_SECRET_TENANT_A",
		},
		IsActive: true,
	}
}
