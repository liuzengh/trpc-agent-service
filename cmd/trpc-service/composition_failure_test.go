package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
)

func mapEnvironment(values map[string]string) environment {
	return func(name string) string { return values[name] }
}

func TestComposeIdentityProvidersRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name, raw, want string
	}{
		{name: "missing", want: "LOGIN_PROVIDER or LOGIN_PROVIDERS_FILE/LOGIN_PROVIDERS_JSON"},
		{name: "malformed", raw: `{`, want: "decode login provider configuration"},
		{name: "empty", raw: `{"providers":[]}`, want: "at least one provider"},
		{name: "missing id", raw: `{"providers":[{"type":"oidc","display_name":"SSO","issuer":"https://sso.example","client_id":"c","client_secret_ref":"env:OIDC"}]}`, want: "requires id"},
		{name: "unsupported", raw: `{"providers":[{"id":"p","type":"password","display_name":"Password"}]}`, want: "unsupported type"},
		{name: "secret", raw: `{"providers":[{"id":"p","type":"wecom","display_name":"企业微信","corp_id":"corp","agent_id":1,"secret_ref":"env:WECOM_LOGIN_SECRET"}]}`, want: "WECOM_LOGIN_SECRET"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := map[string]string{}
			if test.raw != "" {
				values["LOGIN_PROVIDERS_JSON"] = test.raw
				values["LOGIN_CALLBACK_URL"] = "https://app.example/api/v1/auth/callback"
			}
			secrets, secretErr := credential.NewEnvironmentSecretResolver(mapEnvironment(values))
			if secretErr != nil {
				t.Fatal(secretErr)
			}
			_, err := composeIdentityProviders(context.Background(), mapEnvironment(values), identity.NewMemoryIdentityStore(), http.DefaultClient, credential.NewMemoryTokenCache(), secrets, feishuLoginOptions{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestComposeIdentityProvidersKeepsDevelopmentMockShortcut(t *testing.T) {
	values := map[string]string{"LOGIN_PROVIDER": "mock"}
	secrets, err := credential.NewEnvironmentSecretResolver(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	providers, err := composeIdentityProviders(context.Background(), mapEnvironment(values), identity.NewMemoryIdentityStore(), http.DefaultClient, credential.NewMemoryTokenCache(), secrets, feishuLoginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	provider, ok := providers["mock"]
	if !ok || provider.Descriptor().Type != identity.ProviderMock {
		t.Fatalf("providers = %+v, want mock development provider", providers)
	}
}

func TestComposeIdentityProvidersUsesSimpleFeishuModeAndKeepsMock(t *testing.T) {
	values := map[string]string{
		"LOGIN_PROVIDER":                "feishu",
		"LOGIN_MOCK_ENABLED":            "true",
		"LOGIN_CALLBACK_URL":            "https://app.example/api/v1/auth/callback",
		"FEISHU_TRAILFORGE_CONFIG_JSON": `{"app_id":"cli_test","app_secret":"feishu-secret"}`,
	}
	secrets, err := credential.NewEnvironmentSecretResolver(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	providers, err := composeIdentityProviders(context.Background(), mapEnvironment(values), identity.NewMemoryIdentityStore(), http.DefaultClient, credential.NewMemoryTokenCache(), secrets, feishuLoginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 2 || providers["feishu"] == nil || providers["mock"] == nil {
		t.Fatalf("providers = %+v, want feishu + mock", providers)
	}
	metadata := providers["feishu"].(identity.IdentityProviderConfiguration).ConfigurationMetadata()
	if metadata["app_id"] != "cli_test" {
		t.Fatalf("feishu metadata = %+v", metadata)
	}
}

func TestComposeIdentityProvidersUsesSimpleWeComMode(t *testing.T) {
	values := map[string]string{
		"LOGIN_PROVIDER":       "wecom",
		"LOGIN_CALLBACK_URL":   "https://app.example/api/v1/auth/callback",
		"LOGIN_STATE_SECRET":   "01234567890123456789012345678901",
		"WECOM_LOGIN_CORP_ID":  "wx-corp",
		"WECOM_LOGIN_AGENT_ID": "1000001",
		"WECOM_LOGIN_SECRET":   "wecom-secret",
	}
	secrets, err := credential.NewEnvironmentSecretResolver(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	providers, err := composeIdentityProviders(context.Background(), mapEnvironment(values), identity.NewMemoryIdentityStore(), http.DefaultClient, credential.NewMemoryTokenCache(), secrets, feishuLoginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	provider := providers["wecom"]
	if provider == nil || provider.Descriptor().Type != identity.ProviderWeCom {
		t.Fatalf("providers = %+v, want wecom provider", providers)
	}
	metadata := provider.(identity.IdentityProviderConfiguration).ConfigurationMetadata()
	if metadata["corp_id"] != "wx-corp" || metadata["agent_id"] != "1000001" {
		t.Fatalf("wecom metadata = %+v", metadata)
	}
}

func TestLoginStateSecretAllowsEphemeralDevelopmentSecretOnlyWithMock(t *testing.T) {
	if got, err := loginStateSecret(mapEnvironment(map[string]string{"LOGIN_MOCK_ENABLED": "true"})); err != nil || len(got) < 32 {
		t.Fatalf("development state secret = %q, %v", got, err)
	}
	if _, err := loginStateSecret(mapEnvironment(map[string]string{"LOGIN_PROVIDER": "feishu"})); err == nil || !strings.Contains(err.Error(), "LOGIN_STATE_SECRET") {
		t.Fatalf("production missing state secret error = %v", err)
	}
}

func TestComposeIdentityProvidersExposesAllConfiguredEnterpriseProviders(t *testing.T) {
	values := map[string]string{
		"LOGIN_PROVIDERS_JSON": `{"providers":[
			{"id":"wecom-main","type":"wecom","display_name":"企业微信","corp_id":"corp","agent_id":1,"secret_ref":"env:WECOM_LOGIN_SECRET"},
			{"id":"feishu-main","type":"feishu","display_name":"飞书","app_id":"cli_test","tenant_key":"tenant","secret_ref":"env:FEISHU_LOGIN_SECRET"}
		]}`,
		"LOGIN_CALLBACK_URL":  "https://app.example/api/v1/auth/callback",
		"WECOM_LOGIN_SECRET":  "wecom-secret",
		"FEISHU_LOGIN_SECRET": "feishu-secret",
	}
	secrets, err := credential.NewEnvironmentSecretResolver(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	providers, err := composeIdentityProviders(context.Background(), mapEnvironment(values), identity.NewMemoryIdentityStore(), http.DefaultClient, credential.NewMemoryTokenCache(), secrets, feishuLoginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 2 || providers["wecom-main"] == nil || providers["feishu-main"] == nil {
		t.Fatalf("providers = %+v, want both configured enterprise providers", providers)
	}
}

func TestComposeIdentityProvidersAddsOptionalMockWithoutHidingEnterpriseProviders(t *testing.T) {
	values := map[string]string{
		"LOGIN_PROVIDERS_JSON": `{"providers":[
			{"id":"wecom-main","type":"wecom","display_name":"企业微信","corp_id":"corp","agent_id":1,"secret_ref":"env:WECOM_LOGIN_SECRET"},
			{"id":"feishu-main","type":"feishu","display_name":"飞书","app_id":"cli_test","tenant_key":"tenant","secret_ref":"env:FEISHU_LOGIN_SECRET"}
		]}`,
		"LOGIN_MOCK_ENABLED":  "true",
		"LOGIN_CALLBACK_URL":  "https://app.example/api/v1/auth/callback",
		"WECOM_LOGIN_SECRET":  "wecom-secret",
		"FEISHU_LOGIN_SECRET": "feishu-secret",
	}
	secrets, err := credential.NewEnvironmentSecretResolver(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	providers, err := composeIdentityProviders(context.Background(), mapEnvironment(values), identity.NewMemoryIdentityStore(), http.DefaultClient, credential.NewMemoryTokenCache(), secrets, feishuLoginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 3 || providers["wecom-main"] == nil || providers["feishu-main"] == nil || providers["mock"] == nil {
		t.Fatalf("providers = %+v, want all enterprise providers + mock", providers)
	}
}

func TestComposeIdentityProvidersTreatsMockShortcutAsFallbackWhenEnterpriseConfigExists(t *testing.T) {
	values := map[string]string{
		"LOGIN_PROVIDER":       "mock",
		"LOGIN_PROVIDERS_JSON": `{"providers":[{"id":"wecom-main","type":"wecom","display_name":"企业微信","corp_id":"corp","agent_id":1,"secret_ref":"env:WECOM_LOGIN_SECRET"}]}`,
		"LOGIN_CALLBACK_URL":   "https://app.example/api/v1/auth/callback",
		"WECOM_LOGIN_SECRET":   "wecom-secret",
	}
	secrets, err := credential.NewEnvironmentSecretResolver(mapEnvironment(values))
	if err != nil {
		t.Fatal(err)
	}
	providers, err := composeIdentityProviders(context.Background(), mapEnvironment(values), identity.NewMemoryIdentityStore(), http.DefaultClient, credential.NewMemoryTokenCache(), secrets, feishuLoginOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 2 || providers["wecom-main"] == nil || providers["mock"] == nil {
		t.Fatalf("providers = %+v, want configured enterprise provider + development mock", providers)
	}
}

func TestLoginSessionTTLValidation(t *testing.T) {
	tests := []struct {
		name, raw, wantError string
		want                 time.Duration
	}{
		{name: "default", want: 8 * time.Hour},
		{name: "configured", raw: "45m", want: 45 * time.Minute},
		{name: "malformed", raw: "tomorrow", wantError: "Go duration"},
		{name: "zero", raw: "0s", wantError: "positive"},
		{name: "negative", raw: "-1m", wantError: "positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := loginSessionTTL(mapEnvironment(map[string]string{"LOGIN_SESSION_TTL": test.raw}))
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error = %v, want containing %q", err, test.wantError)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("got %v, %v; want %v, nil", got, err, test.want)
			}
		})
	}
}

func TestLoginCallbackURLValidation(t *testing.T) {
	tests := []struct {
		name, raw, want string
	}{
		{name: "valid https", raw: "https://agent.example.com/api/v1/auth/callback", want: "https://agent.example.com/api/v1/auth/callback"},
		{name: "valid local http", raw: "http://127.0.0.1:8080/api/v1/auth/callback", want: "http://127.0.0.1:8080/api/v1/auth/callback"},
		{name: "missing", want: "error"},
		{name: "relative", raw: "/api/v1/auth/callback", want: "error"},
		{name: "wrong path", raw: "https://agent.example.com/callback", want: "error"},
		{name: "query", raw: "https://agent.example.com/api/v1/auth/callback?x=1", want: "error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := loginCallbackURL(mapEnvironment(map[string]string{"LOGIN_CALLBACK_URL": test.raw}))
			if test.want == "error" {
				if err == nil {
					t.Fatalf("loginCallbackURL(%q) = %q, nil; want error", test.raw, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("loginCallbackURL(%q) = %q, %v; want %q, nil", test.raw, got, err, test.want)
			}
		})
	}
}

func TestBuildApplicationFailsWithoutConfigPath(t *testing.T) {
	if _, err := buildApplication(context.Background(), mapEnvironment(nil)); err == nil || !strings.Contains(err.Error(), "CONFIG_PATH") {
		t.Fatalf("error = %v, want missing CONFIG_PATH", err)
	}
}

func TestLoadServiceConfigReportsMissingFile(t *testing.T) {
	_, err := loadServiceConfig(mapEnvironment(map[string]string{"CONFIG_PATH": "/path/that/does/not/exist.yaml"}))
	if err == nil || !strings.Contains(err.Error(), "open CONFIG_PATH") {
		t.Fatalf("error = %v, want open CONFIG_PATH", err)
	}
}

func TestApplicationRunRejectsIncompleteComposition(t *testing.T) {
	tests := []*application{
		nil,
		{listenAddress: "127.0.0.1:0", workers: []*messaging.Worker{{}}},
		{listenAddress: "127.0.0.1:0", handler: http.NotFoundHandler()},
		{handler: http.NotFoundHandler(), workers: []*messaging.Worker{{}}},
	}
	for index, app := range tests {
		if err := app.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "not fully configured") {
			t.Fatalf("case %d error = %v, want incomplete composition", index, err)
		}
	}
}

func TestApplicationRunReportsListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	app := &application{listenAddress: listener.Addr().String(), handler: http.NotFoundHandler(), workers: []*messaging.Worker{{}}}
	if err := app.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "listen HTTP") {
		t.Fatalf("error = %v, want listen HTTP", err)
	}
}
