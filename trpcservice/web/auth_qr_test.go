package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

// qrTestProvider is a provider that supports embedded QR login.
type qrTestProvider struct{ id string }

func (p *qrTestProvider) Descriptor() identity.ProviderDescriptor {
	return identity.ProviderDescriptor{ProviderID: p.id, Type: identity.ProviderFeishu, DisplayName: "飞书"}
}

func (p *qrTestProvider) Begin(identity.AuthRequest) (string, error) {
	return "https://accounts.feishu.cn/open-apis/authen/v1/authorize", nil
}

func (p *qrTestProvider) BeginQR(request identity.AuthRequest) (string, error) {
	return identity.FeishuQRGoto(identity.DefaultFeishuQRAuthorizeURL, "cli_test", "https://app.example/api/v1/auth/callback", request.State)
}

func (p *qrTestProvider) Exchange(context.Context, identity.AuthExchange) (identity.Identity, error) {
	return identity.Identity{}, errors.New("not used")
}

func (p *qrTestProvider) ExchangeQR(context.Context, identity.AuthExchange) (identity.Identity, error) {
	return identity.Identity{}, errors.New("not used")
}

// plainTestProvider deliberately does not implement identity.QRLoginProvider.
type plainTestProvider struct{ id string }

func (p *plainTestProvider) Descriptor() identity.ProviderDescriptor {
	return identity.ProviderDescriptor{ProviderID: p.id, Type: identity.ProviderOIDC, DisplayName: "企业 SSO"}
}

func (p *plainTestProvider) Begin(identity.AuthRequest) (string, error) {
	return "https://sso.example/authorize", nil
}

func (p *plainTestProvider) Exchange(context.Context, identity.AuthExchange) (identity.Identity, error) {
	return identity.Identity{}, errors.New("not used")
}

func TestQRBeginReturnsGotoAndTransactionCookie(t *testing.T) {
	handler := newQRTestHandler(t, QRModeSDKRedirect, &qrTestProvider{id: "feishu-test"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/auth/qr/begin?provider=feishu-test", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Goto      string `json:"goto"`
		State     string `json:"state"`
		ExpiresIn int    `json:"expires_in"`
		SDKURL    string `json:"sdk_url"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !strings.HasPrefix(body.Goto, identity.DefaultFeishuQRAuthorizeURL+"?") {
		t.Errorf("goto = %q, want the legacy authorize endpoint", body.Goto)
	}
	if !strings.Contains(body.Goto, "client_id=cli_test") || !strings.Contains(body.Goto, "response_type=code") {
		t.Errorf("goto is missing required parameters: %q", body.Goto)
	}
	if body.State == "" {
		t.Error("state must not be empty")
	}
	if body.ExpiresIn != identity.FeishuQRTTLSeconds {
		t.Errorf("expires_in = %d, want %d", body.ExpiresIn, identity.FeishuQRTTLSeconds)
	}
	if body.SDKURL != identity.DefaultFeishuQRSDKURL {
		t.Errorf("sdk_url = %q, want the official SDK", body.SDKURL)
	}
	// The login transaction cookie must be issued so the shared callback keeps working.
	found := false
	for _, cookie := range recorder.Result().Cookies() {
		if cookie.Name == "dsh_login_tx" && cookie.Value != "" {
			found = true
		}
	}
	if !found {
		t.Error("login transaction cookie was not issued")
	}
	transactionRequest := httptest.NewRequest(http.MethodGet, "/api/v1/auth/callback", nil)
	for _, cookie := range recorder.Result().Cookies() {
		transactionRequest.AddCookie(cookie)
	}
	transaction, err := identity.ReadLoginTransactionCookie(transactionRequest, "test-state-secret")
	if err != nil {
		t.Fatalf("read login transaction: %v", err)
	}
	if transaction.Flow != identity.LoginFlowQRLegacy {
		t.Fatalf("transaction flow = %q, want %q", transaction.Flow, identity.LoginFlowQRLegacy)
	}
}

func TestQRBeginDisabledReturnsNotFound(t *testing.T) {
	handler := newQRTestHandler(t, QRModeOff, &qrTestProvider{id: "feishu-test"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/auth/qr/begin?provider=feishu-test", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 when QR mode is off", recorder.Code)
	}
}

func TestQRBeginRejectsProviderWithoutQRSupport(t *testing.T) {
	handler := newQRTestHandler(t, QRModeSDKRedirect, &plainTestProvider{id: "oidc-test"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/auth/qr/begin?provider=oidc-test", nil))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a provider without QR support", recorder.Code)
	}
}

func TestProvidersExposeQRCapability(t *testing.T) {
	handler := newQRTestHandler(t, QRModeSDKRedirect, &qrTestProvider{id: "feishu-test"}, &plainTestProvider{id: "oidc-test"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/auth/providers", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body struct {
		Providers []authProviderConfiguration `json:"providers"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	var feishuSeen, oidcSeen bool
	for _, provider := range body.Providers {
		switch provider.ProviderID {
		case "feishu-test":
			feishuSeen = true
			if !provider.QRSupported || !provider.QREnabled {
				t.Errorf("feishu provider must expose QR support, got %+v", provider)
			}
		case "oidc-test":
			oidcSeen = true
			if provider.QRSupported || provider.QREnabled {
				t.Errorf("non-QR provider must not advertise QR support, got %+v", provider)
			}
		}
	}
	if !feishuSeen || !oidcSeen {
		t.Fatalf("provider list is incomplete: %+v", body.Providers)
	}
}

func TestCallbackTreatsProviderErrorAsCancellation(t *testing.T) {
	handler := newQRTestHandler(t, QRModeSDKRedirect, &qrTestProvider{id: "feishu-test"})
	begin := httptest.NewRecorder()
	handler.ServeHTTP(begin, httptest.NewRequest(http.MethodGet, "/api/v1/auth/qr/begin?provider=feishu-test", nil))
	var login struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(begin.Body.Bytes(), &login); err != nil {
		t.Fatalf("decode QR begin: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/callback?error=access_denied&state="+login.State, nil)
	for _, cookie := range begin.Result().Cookies() {
		request.AddCookie(cookie)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", recorder.Code)
	}
	if location := recorder.Header().Get("Location"); location != "/console/?login_error=access_denied" {
		t.Errorf("Location = %q, want access_denied redirect", location)
	}
}

func TestCallbackRejectsProviderErrorWithoutValidTransaction(t *testing.T) {
	handler := newQRTestHandler(t, QRModeSDKRedirect, &qrTestProvider{id: "feishu-test"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/auth/callback?error=access_denied&state=forged", nil))
	if recorder.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", recorder.Code)
	}
	if location := recorder.Header().Get("Location"); location != "/console/?login_error=invalid_state" {
		t.Errorf("Location = %q, want invalid_state redirect", location)
	}
}

func TestAuthHandlerRejectsUnknownQRMode(t *testing.T) {
	_, err := NewAuthHandler(AuthDependencies{
		Providers:   map[string]identity.IdentityProvider{"feishu-test": &qrTestProvider{id: "feishu-test"}},
		Sessions:    identity.NewMemorySessionStore(),
		Users:       identity.NewMemoryIdentityStore(),
		SessionTTL:  time.Hour,
		StateSecret: "test-state-secret",
		QRMode:      "sdk_iframe",
	})
	if err == nil {
		t.Fatal("expected an error for an unsupported QR mode")
	}
}

func newQRTestHandler(t *testing.T, qrMode string, providers ...identity.IdentityProvider) http.Handler {
	t.Helper()
	registry := make(map[string]identity.IdentityProvider, len(providers))
	for _, provider := range providers {
		registry[provider.Descriptor().ProviderID] = provider
	}
	handler, err := NewAuthHandler(AuthDependencies{
		Providers:   registry,
		Sessions:    identity.NewMemorySessionStore(),
		Users:       identity.NewMemoryIdentityStore(),
		SessionTTL:  time.Hour,
		StateSecret: "test-state-secret",
		QRMode:      qrMode,
		CallbackURL: "https://app.example/api/v1/auth/callback",
	})
	if err != nil {
		t.Fatalf("NewAuthHandler() error = %v", err)
	}
	return handler
}
