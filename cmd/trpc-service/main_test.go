package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestParseServeArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantAddr string
		wantHelp bool
		wantErr  bool
	}{
		{name: "default address", wantAddr: "127.0.0.1:8080"},
		{name: "override address", args: []string{"-addr", "127.0.0.1:9090"}, wantAddr: "127.0.0.1:9090"},
		{name: "help", args: []string{"-h"}, wantHelp: true},
		{name: "unexpected argument", args: []string{"extra"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			addr, help, err := parseServeArgs(tt.args, &output)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseServeArgs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if addr != tt.wantAddr || help != tt.wantHelp {
				t.Fatalf("parseServeArgs() = (%q, %v), want (%q, %v)", addr, help, tt.wantAddr, tt.wantHelp)
			}
		})
	}
}

func TestSQLCommandRejectsInvalidKindBeforeLoadingConfig(t *testing.T) {
	if err := runSQLInit([]string{"-kind", "sqlite"}, false); err == nil {
		t.Fatal("expected invalid kind error")
	}
}

func TestNewAdaptersKeepsMissingCredentialNotReady(t *testing.T) {
	cfg := adapterTestConfig(t, []tenant.ChannelBinding{{
		ID: "telegram-a", Channel: "telegram", ExternalAccountID: "123", CredentialRef: "env:TELEGRAM_A",
		TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
	}}, map[string]string{"env:MODEL": "model-key"})
	adapters, err := newAdapters(cfg)
	if err != nil || len(adapters) != 1 {
		t.Fatalf("newAdapters() = (%#v, %v)", adapters, err)
	}
	if err := adapters[0].Ready(context.Background()); err == nil {
		t.Fatal("adapter with a missing credential reported ready")
	}
}

func TestNewAdaptersRejectsDuplicateResolvedBot(t *testing.T) {
	cfg := adapterTestConfig(t, []tenant.ChannelBinding{
		{ID: "telegram-a", Channel: "telegram", ExternalAccountID: "123", CredentialRef: "env:TELEGRAM_A", TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true},
		{ID: "telegram-b", Channel: "telegram", ExternalAccountID: "456", CredentialRef: "env:TELEGRAM_B", TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true},
	}, map[string]string{"env:MODEL": "model-key", "env:TELEGRAM_A": "same-token", "env:TELEGRAM_B": "same-token"})
	if _, err := newAdapters(cfg); err == nil {
		t.Fatal("duplicate resolved Telegram bot was accepted")
	}
}

func TestNewAdaptersCreatesFeishuAdapter(t *testing.T) {
	cfg := adapterTestConfig(t, []tenant.ChannelBinding{{
		ID: "feishu-a", Channel: "feishu", ExternalAccountID: "cli_app",
		BotIDRef: "env:FEISHU_APP_ID", BotSecretRef: "env:FEISHU_APP_SECRET",
		TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
	}}, map[string]string{
		"env:MODEL":             "model-key",
		"env:FEISHU_APP_ID":     "cli_app",
		"env:FEISHU_APP_SECRET": "app-secret",
	})
	adapters, err := newAdapters(cfg)
	if err != nil || len(adapters) != 1 {
		t.Fatalf("newAdapters() = (%#v, %v)", adapters, err)
	}
	if _, ok := adapters[0].(*channels.FeishuAdapter); !ok {
		t.Fatalf("adapter type = %T, want *channels.FeishuAdapter", adapters[0])
	}
}

func TestNewAdaptersKeepsMissingFeishuCredentialNotReady(t *testing.T) {
	cfg := adapterTestConfig(t, []tenant.ChannelBinding{{
		ID: "feishu-a", Channel: "feishu", ExternalAccountID: "cli_app",
		BotIDRef: "env:FEISHU_APP_ID", BotSecretRef: "env:FEISHU_APP_SECRET",
		TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
	}}, map[string]string{"env:MODEL": "model-key", "env:FEISHU_APP_ID": "cli_app"})
	adapters, err := newAdapters(cfg)
	if err != nil || len(adapters) != 1 {
		t.Fatalf("newAdapters() = (%#v, %v)", adapters, err)
	}
	if _, ok := adapters[0].(*channels.UnavailableAdapter); !ok {
		t.Fatalf("adapter type = %T, want *channels.UnavailableAdapter", adapters[0])
	}
}

func TestNewAdaptersRejectsDuplicateResolvedFeishuApp(t *testing.T) {
	bindings := []tenant.ChannelBinding{
		{ID: "feishu-a", Channel: "feishu", ExternalAccountID: "account-a", BotIDRef: "env:FEISHU_A", BotSecretRef: "env:FEISHU_SECRET_A", TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true},
		{ID: "feishu-b", Channel: "feishu", ExternalAccountID: "account-b", BotIDRef: "env:FEISHU_B", BotSecretRef: "env:FEISHU_SECRET_B", TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true},
	}
	credentials := map[string]string{
		"env:MODEL": "model-key", "env:FEISHU_A": "cli_same", "env:FEISHU_B": "cli_same",
		"env:FEISHU_SECRET_A": "secret-a", "env:FEISHU_SECRET_B": "secret-b",
	}
	if _, err := newAdapters(adapterTestConfig(t, bindings, credentials)); err == nil {
		t.Fatal("duplicate resolved Feishu app was accepted")
	}
}

func adapterTestConfig(t *testing.T, bindings []tenant.ChannelBinding, credentials map[string]string) config.Config {
	t.Helper()
	catalog := tenant.Catalog{
		Tenants:         []tenant.Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{TenantID: "tenant-a", ID: "memory", Kind: tenant.StorageKindInMemory}},
		AgentApps:       []tenant.AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []tenant.ConfigVersion{{
			TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory", Instruction: "test",
			Model: tenant.ModelConfig{Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL", RequestTimeout: time.Second, MaxOutputTokens: 32},
		}},
		ChannelBindings: bindings,
	}
	cfg, err := config.NewCatalogConfig(catalog, []byte("01234567890123456789012345678901"), config.NewStaticCredentialResolver(credentials))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestParseGatewayAndWorkerArgs(t *testing.T) {
	var output bytes.Buffer
	addr, consumer, help, err := parseGatewayArgs([]string{"-addr", "127.0.0.1:9090", "-consumer", "gateway-fixed"}, &output)
	if err != nil || help || addr != "127.0.0.1:9090" || consumer != "gateway-fixed" {
		t.Fatalf("parseGatewayArgs() = (%q, %q, %v, %v)", addr, consumer, help, err)
	}
	healthAddr, consumer, help, err := parseWorkerArgs([]string{"-health-addr", "127.0.0.1:9091", "-consumer", "worker-fixed"}, &output)
	if err != nil || help || healthAddr != "127.0.0.1:9091" || consumer != "worker-fixed" {
		t.Fatalf("parseWorkerArgs() = (%q, %q, %v, %v)", healthAddr, consumer, help, err)
	}
	if _, _, _, err := parseGatewayArgs([]string{"extra"}, &output); err == nil {
		t.Fatal("parseGatewayArgs() unexpectedly accepted a positional argument")
	}
	if _, _, _, err := parseWorkerArgs([]string{"extra"}, &output); err == nil {
		t.Fatal("parseWorkerArgs() unexpectedly accepted a positional argument")
	}
}
