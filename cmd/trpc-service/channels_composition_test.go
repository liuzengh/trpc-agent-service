package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

type staticSecretResolver string

func (r staticSecretResolver) Resolve(context.Context, string) (string, error) {
	return string(r), nil
}

func TestComposeTelegramRejectsTrailingCredentialJSON(t *testing.T) {
	tenants := []config.TenantConfig{{
		TenantID: "example", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type: config.ChannelTelegram, BindingID: "bot-1", CredentialRef: "env:BOT_1_CONFIG",
		}},
	}}
	valid := `{"bot_token":"bot-token"}`
	for _, trailing := range []string{
		` {"secret_token":"other","bot_token":"other"}`,
		` trailing-garbage`,
	} {
		_, err := decodeTelegramCredential(context.Background(), staticSecretResolver(valid+trailing), tenants[0].Channels[0])
		if err == nil || !strings.Contains(err.Error(), "exactly one JSON value") {
			t.Fatalf("decodeTelegramCredential() trailing %q error = %v, want exactly one JSON value", trailing, err)
		}
	}
}

func TestDecodeChannelCredentials(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	telegramBinding := config.ChannelBinding{Type: config.ChannelTelegram, BindingID: "support-telegram", CredentialRef: "env:TELEGRAM_CONFIG"}
	telegramCredential, err := decodeTelegramCredential(ctx, staticSecretResolver(`{"bot_token":"12345:test-token","api_base_url":" https://telegram.example.test "}`), telegramBinding)
	if err != nil {
		t.Fatalf("decodeTelegramCredential() error = %v", err)
	}
	if telegramCredential.BotToken != "12345:test-token" || telegramCredential.APIBaseURL != " https://telegram.example.test " {
		t.Fatalf("telegram credential = %+v", telegramCredential)
	}

	wecomBinding := config.ChannelBinding{Type: config.ChannelWeCom, BindingID: "support-wecom", CredentialRef: "env:WECOM_CONFIG"}
	wecomCredential, err := decodeWeComCredential(ctx, staticSecretResolver(`{"bot_id":"bot-1","secret":"test-secret","endpoint":"wss://example.test/ws","origin":"https://example.test"}`), wecomBinding)
	if err != nil {
		t.Fatalf("decodeWeComCredential() error = %v", err)
	}
	if wecomCredential.BotID != "bot-1" || wecomCredential.Secret != "test-secret" {
		t.Fatalf("wecom credential = %+v", wecomCredential)
	}

	feishuBinding := config.ChannelBinding{Type: config.ChannelFeishu, BindingID: "support-feishu", CredentialRef: "env:FEISHU_CONFIG"}
	feishuCredential, err := decodeFeishuCredential(ctx, staticSecretResolver(`{"app_id":"app-1","app_secret":"test-secret","api_base_url":"https://open.example.test"}`), feishuBinding)
	if err != nil {
		t.Fatalf("decodeFeishuCredential() error = %v", err)
	}
	if feishuCredential.AppID != "app-1" || feishuCredential.AppSecret != "test-secret" {
		t.Fatalf("feishu credential = %+v", feishuCredential)
	}
}

func TestDecodeChannelCredentialsRejectInvalidProviderConfiguration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		binding config.ChannelBinding
		value   string
		decode  func(context.Context, staticSecretResolver, config.ChannelBinding) error
	}{
		{
			name:    "telegram missing token",
			binding: config.ChannelBinding{Type: config.ChannelTelegram, BindingID: "support-telegram", CredentialRef: "env:CONFIG"},
			value:   `{}`,
			decode: func(ctx context.Context, resolver staticSecretResolver, binding config.ChannelBinding) error {
				_, err := decodeTelegramCredential(ctx, resolver, binding)
				return err
			},
		},
		{
			name:    "wecom missing secret",
			binding: config.ChannelBinding{Type: config.ChannelWeCom, BindingID: "support-wecom", CredentialRef: "env:CONFIG"},
			value:   `{"bot_id":"bot-1"}`,
			decode: func(ctx context.Context, resolver staticSecretResolver, binding config.ChannelBinding) error {
				_, err := decodeWeComCredential(ctx, resolver, binding)
				return err
			},
		},
		{
			name:    "feishu missing secret",
			binding: config.ChannelBinding{Type: config.ChannelFeishu, BindingID: "support-feishu", CredentialRef: "env:CONFIG"},
			value:   `{"app_id":"app-1"}`,
			decode: func(ctx context.Context, resolver staticSecretResolver, binding config.ChannelBinding) error {
				_, err := decodeFeishuCredential(ctx, resolver, binding)
				return err
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.decode(context.Background(), staticSecretResolver(tt.value), tt.binding); err == nil {
				t.Fatal("invalid provider configuration error = nil")
			}
		})
	}
}

type failingSecretResolver struct{ err error }

func (r failingSecretResolver) Resolve(context.Context, string) (string, error) { return "", r.err }

func TestDecodeBindingCredentialValidation(t *testing.T) {
	t.Parallel()
	binding := config.ChannelBinding{BindingID: "support", CredentialRef: "env:SUPPORT_CONFIG"}
	var target map[string]any
	if err := decodeBindingCredential(context.Background(), nil, binding, &target); err == nil {
		t.Fatal("nil secret resolver error = nil")
	}
	wantErr := errors.New("secret unavailable")
	if err := decodeBindingCredential(context.Background(), failingSecretResolver{err: wantErr}, binding, &target); !errors.Is(err, wantErr) {
		t.Fatalf("secret resolution error = %v", err)
	}
	if err := decodeBindingCredential(context.Background(), staticSecretResolver(`{"known":`), binding, &target); err == nil {
		t.Fatal("invalid JSON error = nil")
	}
	type strictCredential struct {
		Known string `json:"known"`
	}
	var strict strictCredential
	if err := decodeBindingCredential(context.Background(), staticSecretResolver(`{"known":"ok","unknown":true}`), binding, &strict); err == nil {
		t.Fatal("unknown credential field error = nil")
	}
	if err := decodeBindingCredential(context.Background(), staticSecretResolver(`{"known":"ok"}`), binding, &strict); err != nil || strict.Known != "ok" {
		t.Fatalf("valid credential = %+v, %v", strict, err)
	}
}

func TestBindingsForFiltersByChannel(t *testing.T) {
	t.Parallel()
	tenants := []config.TenantConfig{
		{TenantID: "support-a", Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "tg-a"}, {Type: config.ChannelFeishu, BindingID: "fs-a"}}},
		{TenantID: "support-b", Channels: []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "tg-b"}}},
	}
	got := bindingsFor(tenants, config.ChannelTelegram)
	want := []config.ChannelBinding{{Type: config.ChannelTelegram, BindingID: "tg-a"}, {Type: config.ChannelTelegram, BindingID: "tg-b"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bindingsFor() = %+v, want %+v", got, want)
	}
	if got := bindingsFor(tenants, "missing"); len(got) != 0 {
		t.Fatalf("bindingsFor(missing) = %+v", got)
	}
}

func TestChannelConfigurationHelpers(t *testing.T) {
	t.Parallel()
	if got := fallback("  configured  ", "default"); got != "configured" {
		t.Fatalf("fallback(configured) = %q", got)
	}
	if got := fallback("  ", "default"); got != "default" {
		t.Fatalf("fallback(empty) = %q", got)
	}

	values := map[string]string{
		"BROKERS":  " first.example.test:9092, , second.example.test:9092 ",
		"OPTIONAL": " configured ",
	}
	getenv := environment(func(name string) string { return values[name] })
	brokers, err := csvRequired(getenv, "BROKERS")
	if err != nil || !reflect.DeepEqual(brokers, []string{"first.example.test:9092", "second.example.test:9092"}) {
		t.Fatalf("csvRequired() = %+v, %v", brokers, err)
	}
	if _, err := csvRequired(getenv, "MISSING"); err == nil {
		t.Fatal("csvRequired(missing) error = nil")
	}
	values["EMPTY_BROKERS"] = ", ,"
	if _, err := csvRequired(getenv, "EMPTY_BROKERS"); err == nil {
		t.Fatal("csvRequired(empty list) error = nil")
	}
	if got := optional(getenv, "OPTIONAL", "fallback"); got != "configured" {
		t.Fatalf("optional(configured) = %q", got)
	}
	if got := optional(getenv, "MISSING", "fallback"); got != "fallback" {
		t.Fatalf("optional(missing) = %q", got)
	}
}
