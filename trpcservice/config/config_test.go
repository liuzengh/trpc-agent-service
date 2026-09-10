package config_test

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestStaticResolverReturnsExactConfigAndCopiesMutableData(t *testing.T) {
	cfg := testAppConfig("tenant-a", "support", "v1")
	resolver, err := config.NewStaticResolver(cfg)
	if err != nil {
		t.Fatalf("new static resolver: %v", err)
	}

	cfg.Model.Parameters["temperature"] = "1"
	cfg.Tools.VisibleTools[0] = "mutated"
	cfg.BackendConfig.Session.Options["schema"] = "mutated"

	resolved, err := resolver.ResolveAppConfig(context.Background(), "tenant-a", "support", "v1")
	if err != nil {
		t.Fatalf("resolve app config: %v", err)
	}
	if got := resolved.Model.Parameters["temperature"]; got != "0" {
		t.Fatalf("model temperature = %q, want 0", got)
	}
	if got := resolved.Tools.VisibleTools[0]; got != "search" {
		t.Fatalf("visible tool = %q, want search", got)
	}
	if got := resolved.BackendConfig.Session.Options["schema"]; got != "agent" {
		t.Fatalf("session schema = %q, want agent", got)
	}

	resolved.Model.Parameters["temperature"] = "2"
	again, err := resolver.ResolveAppConfig(context.Background(), "tenant-a", "support", "v1")
	if err != nil {
		t.Fatalf("resolve app config again: %v", err)
	}
	if got := again.Model.Parameters["temperature"]; got != "0" {
		t.Fatalf("model temperature after caller mutation = %q, want 0", got)
	}
}

func TestStaticResolverIsolatesTenantAppAndVersion(t *testing.T) {
	resolver, err := config.NewStaticResolver(
		testAppConfig("tenant-a", "support", "v1"),
		testAppConfig("tenant-a", "support", "v2"),
		testAppConfig("tenant-b", "support", "v1"),
		testAppConfig("tenant-a", "sales", "v1"),
	)
	if err != nil {
		t.Fatalf("new static resolver: %v", err)
	}

	tests := []struct {
		name     string
		tenantID string
		appID    string
		version  string
	}{
		{name: "tenant", tenantID: "tenant-b", appID: "support", version: "v1"},
		{name: "app", tenantID: "tenant-a", appID: "sales", version: "v1"},
		{name: "version", tenantID: "tenant-a", appID: "support", version: "v2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := resolver.ResolveAppConfig(
				context.Background(),
				tt.tenantID,
				tt.appID,
				tt.version,
			)
			if err != nil {
				t.Fatalf("resolve app config: %v", err)
			}
			if cfg.TenantID != tt.tenantID || cfg.AppID != tt.appID || cfg.Version != tt.version {
				t.Fatalf(
					"resolved scope = %q/%q/%q, want %q/%q/%q",
					cfg.TenantID,
					cfg.AppID,
					cfg.Version,
					tt.tenantID,
					tt.appID,
					tt.version,
				)
			}
		})
	}
}

func TestNewStaticResolverRejectsInvalidOrDuplicateConfig(t *testing.T) {
	invalid := testAppConfig("tenant-a", "support", "v1")
	invalid.Model.Model = ""
	if _, err := config.NewStaticResolver(invalid); err == nil {
		t.Fatal("new static resolver succeeded with invalid config")
	}

	cfg := testAppConfig("tenant-a", "support", "v1")
	if _, err := config.NewStaticResolver(cfg, cfg); err == nil {
		t.Fatal("new static resolver succeeded with duplicate config")
	}
}

func TestStaticResolverRejectsIncompleteOrUnknownScope(t *testing.T) {
	resolver, err := config.NewStaticResolver(testAppConfig("tenant-a", "support", "v1"))
	if err != nil {
		t.Fatalf("new static resolver: %v", err)
	}

	tests := []struct {
		name     string
		tenantID string
		appID    string
		version  string
	}{
		{name: "missing tenant", appID: "support", version: "v1"},
		{name: "missing app", tenantID: "tenant-a", version: "v1"},
		{name: "missing version", tenantID: "tenant-a", appID: "support"},
		{name: "unknown config", tenantID: "tenant-b", appID: "support", version: "v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := resolver.ResolveAppConfig(
				context.Background(),
				tt.tenantID,
				tt.appID,
				tt.version,
			); err == nil {
				t.Fatal("resolve app config succeeded with invalid scope")
			}
		})
	}
}

func TestStaticResolverWithBindingsValidatesConfigReferences(t *testing.T) {
	cfg := testAppConfig("tenant-a", "support", "v1")
	cfg.ChannelBinding = []string{"binding-1"}
	binding := testBinding("tenant-a", "support", "binding-1")

	resolver, err := config.NewStaticResolverWithBindings([]channels.Binding{binding}, cfg)
	if err != nil {
		t.Fatalf("new static resolver with bindings: %v", err)
	}
	resolved, err := resolver.ResolveAppConfig(context.Background(), "tenant-a", "support", "v1")
	if err != nil {
		t.Fatalf("resolve app config: %v", err)
	}
	if got := resolved.ChannelBinding[0]; got != "binding-1" {
		t.Fatalf("channel binding = %q, want binding-1", got)
	}
}

func TestStaticResolverRejectsMissingBindingResolver(t *testing.T) {
	cfg := testAppConfig("tenant-a", "support", "v1")
	cfg.ChannelBinding = []string{"binding-1"}

	if _, err := config.NewStaticResolver(cfg); err == nil {
		t.Fatal("new static resolver succeeded with unresolved channel binding")
	}
}

func TestValidateAppConfigBindingsRejectsWrongBindingScope(t *testing.T) {
	cfg := testAppConfig("tenant-a", "support", "v1")
	cfg.ChannelBinding = []string{"binding-1"}
	bindings := wrongScopeBindingResolver{
		binding: testBinding("tenant-b", "support", "binding-1"),
	}

	if err := config.ValidateAppConfigBindings(context.Background(), cfg, bindings); err == nil {
		t.Fatal("validate app config bindings succeeded with mismatched binding scope")
	}
}

func TestStaticBindingResolverRejectsDuplicateBinding(t *testing.T) {
	binding := testBinding("tenant-a", "support", "binding-1")
	if _, err := config.NewStaticBindingResolver(binding, binding); err == nil {
		t.Fatal("new static binding resolver succeeded with duplicate binding")
	}
}

func TestStaticBindingResolverResolvesPublicRouteInChannelScope(t *testing.T) {
	binding := testBinding("tenant-a", "support", "binding-1")
	resolver, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatalf("new static binding resolver: %v", err)
	}

	snapshot, err := resolver.ResolveBindingByPublicRoute(
		context.Background(),
		channels.ChannelWeCom,
		binding.PublicRouteID,
	)
	if err != nil {
		t.Fatalf("resolve public route: %v", err)
	}
	if snapshot.Binding != binding {
		t.Fatalf("resolved snapshot = %#v, want %#v", snapshot.Binding, binding)
	}

	if _, err := resolver.ResolveBindingByPublicRoute(
		context.Background(),
		channels.ChannelFeishu,
		binding.PublicRouteID,
	); !errors.Is(err, channels.ErrBindingChannelMismatch) {
		t.Fatalf("channel mismatch error = %v, want %v", err, channels.ErrBindingChannelMismatch)
	}
	if _, err := resolver.ResolveBindingByPublicRoute(
		context.Background(),
		channels.ChannelWeCom,
		"unknown-route",
	); !errors.Is(err, channels.ErrBindingNotFound) {
		t.Fatalf("unknown route error = %v, want %v", err, channels.ErrBindingNotFound)
	}
}

func testAppConfig(tenantID, appID, version string) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:   "openai",
			APIKeyRef:  tenant.SecretRef{Name: "model-key"},
			Model:      "gpt-4.1-mini",
			Parameters: map[string]string{"temperature": "0"},
		},
		Tools: tenant.ToolPolicy{
			VisibleTools:    []string{"search"},
			ExecutableTools: []string{"search"},
		},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "session-sql",
				Options:  map[string]string{"schema": "agent"},
			},
		},
	}
}

type wrongScopeBindingResolver struct {
	binding channels.Binding
}

func (r wrongScopeBindingResolver) ResolveBinding(
	_ context.Context,
	_,
	_,
	_ string,
) (channels.Binding, error) {
	return r.binding, nil
}

func testBinding(tenantID, appID, bindingID string) channels.Binding {
	return channels.Binding{
		TenantID:        tenantID,
		AppID:           appID,
		BindingID:       bindingID,
		Channel:         channels.ChannelWeCom,
		ExternalAccount: "corp-agent-1",
		Secret: tenant.SecretRef{
			Name:    "wecom-bot-secret",
			Version: "v1",
		},
		PublicRouteID:   "route-" + bindingID,
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}
