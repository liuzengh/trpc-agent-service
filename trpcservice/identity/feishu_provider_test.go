package identity

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

const testFeishuRedirectURI = "https://console.example.com/api/v1/auth/callback"

func TestFeishuBeginUsesConfiguredRedirectURIExactly(t *testing.T) {
	provider, err := NewFeishuProvider(FeishuConfig{
		ProviderID: "feishu-main", AppID: "cli_test", AppSecret: "secret", TenantKey: "tenant",
		RedirectURI: testFeishuRedirectURI,
	})
	if err != nil {
		t.Fatal(err)
	}
	authURL, err := provider.Begin(AuthRequest{State: "state-1"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.Query().Get("redirect_uri"); got != testFeishuRedirectURI {
		t.Fatalf("redirect_uri = %q, want %q", got, testFeishuRedirectURI)
	}
	if got := parsed.Query().Get("app_id"); got != "cli_test" {
		t.Fatalf("app_id = %q, want cli_test", got)
	}
}

func TestFeishuExchangeReusesAuthorizationRedirectURI(t *testing.T) {
	var tokenRedirectURI string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/open-apis/authen/v2/oauth/token":
			var payload map[string]string
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatalf("decode token request: %v", err)
			}
			tokenRedirectURI = payload["redirect_uri"]
			_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "access_token": "user-token"})
		case "/open-apis/authen/v1/user_info":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{"tenant_key": "tenant", "open_id": "ou_test", "name": "测试用户"},
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	provider, err := NewFeishuProvider(FeishuConfig{
		ProviderID: "feishu-main", AppID: "cli_test", AppSecret: "secret", TenantKey: "tenant",
		RedirectURI: testFeishuRedirectURI, APIBaseURL: server.URL, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.Exchange(context.Background(), AuthExchange{Code: "code-1"})
	if err != nil {
		t.Fatal(err)
	}
	if tokenRedirectURI != testFeishuRedirectURI {
		t.Fatalf("token redirect_uri = %q, want %q", tokenRedirectURI, testFeishuRedirectURI)
	}
	if got.ProviderID != "feishu-main" || got.SubjectID != "ou_test" || got.EnterpriseID != "tenant" {
		t.Fatalf("identity = %+v", got)
	}
}

func TestFeishuQRExchangeUsesLegacyEndpointsAndCachesAppToken(t *testing.T) {
	appTokenRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/open-apis/auth/v3/app_access_token/internal":
			appTokenRequests++
			_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "app_access_token": "app-token", "expire": 7200})
		case "/open-apis/authen/v1/access_token":
			_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "data": map[string]any{"access_token": "user-token"}})
		case "/open-apis/authen/v1/user_info":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{"tenant_key": "tenant", "open_id": "ou_qr", "name": "扫码用户"},
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	provider, err := NewFeishuProvider(FeishuConfig{
		ProviderID: "feishu-main", AppID: "cli_test", AppSecret: "secret", TenantKey: "tenant",
		RedirectURI: testFeishuRedirectURI, APIBaseURL: server.URL, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"qr-code-1", "qr-code-2"} {
		got, exchangeErr := provider.ExchangeQR(context.Background(), AuthExchange{Code: code})
		if exchangeErr != nil {
			t.Fatal(exchangeErr)
		}
		if got.SubjectID != "ou_qr" || got.EnterpriseID != "tenant" {
			t.Fatalf("identity = %+v", got)
		}
	}
	if appTokenRequests != 1 {
		t.Fatalf("app token requests = %d, want 1 cached request", appTokenRequests)
	}
}
