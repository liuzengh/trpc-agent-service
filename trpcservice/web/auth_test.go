package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

const testStateSecret = "test-state-secret"

type fakeEnterpriseProvider struct {
	descriptor identity.ProviderDescriptor
	enterprise string
	subject    string
	name       string
	email      string
}

func (p *fakeEnterpriseProvider) Descriptor() identity.ProviderDescriptor { return p.descriptor }

func (p *fakeEnterpriseProvider) Begin(request identity.AuthRequest) (string, error) {
	query := url.Values{}
	query.Set("state", request.State)
	query.Set("nonce", request.Nonce)
	query.Set("challenge", request.PKCEChallenge)
	return "https://idp.example/authorize?" + query.Encode(), nil
}

func (p *fakeEnterpriseProvider) Exchange(_ context.Context, exchange identity.AuthExchange) (identity.Identity, error) {
	if exchange.Code != "good-code" {
		return identity.Identity{}, context.Canceled
	}
	return identity.Identity{
		ProviderID: p.descriptor.ProviderID, ProviderType: p.descriptor.Type,
		EnterpriseID: p.enterprise, SubjectID: p.subject, DisplayName: p.name, Email: p.email,
	}, nil
}

type authFixture struct {
	handler  http.Handler
	users    *identity.MemoryIdentityStore
	sessions *identity.MemorySessionStore
	provider *fakeEnterpriseProvider
}

func testAuthFixture(t *testing.T, mutate func(*AuthDependencies, *identity.MemoryIdentityStore, *fakeEnterpriseProvider)) authFixture {
	t.Helper()
	provider := &fakeEnterpriseProvider{
		descriptor: identity.ProviderDescriptor{ProviderID: "corp-wecom", Type: identity.ProviderWeCom, DisplayName: "企业微信"},
		enterprise: "ww-corp", subject: "employee-1", name: "张三", email: "zhangsan@example.com",
	}
	users := identity.NewMemoryIdentityStore()
	if err := users.UpsertLoginProvider(context.Background(), provider.descriptor, provider.enterprise); err != nil {
		t.Fatal(err)
	}
	sessions := identity.NewMemorySessionStore()
	deps := AuthDependencies{
		Providers: map[string]identity.IdentityProvider{provider.descriptor.ProviderID: provider},
		Sessions:  sessions, Users: users, Audits: users, SessionTTL: 2 * time.Hour, StateSecret: testStateSecret,
	}
	if mutate != nil {
		mutate(&deps, users, provider)
	}
	handler, err := NewAuthHandler(deps)
	if err != nil {
		t.Fatalf("NewAuthHandler() error = %v", err)
	}
	return authFixture{handler: handler, users: users, sessions: sessions, provider: provider}
}

func beginAuth(t *testing.T, handler http.Handler, providerID string) (string, *http.Cookie) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/login?provider="+url.QueryEscape(providerID), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login status = %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		AuthURL string `json:"auth_url"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(body.AuthURL)
	if err != nil {
		t.Fatal(err)
	}
	var transaction *http.Cookie
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == "dsh_login_tx" {
			transaction = cookie
			break
		}
	}
	if transaction == nil {
		t.Fatal("login transaction cookie missing")
	}
	return parsed.Query().Get("state"), transaction
}

func completeAuth(t *testing.T, handler http.Handler, providerID string) []*http.Cookie {
	t.Helper()
	state, transaction := beginAuth(t, handler, providerID)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/callback?code=good-code&state="+url.QueryEscape(state), nil)
	req.AddCookie(transaction)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/console/" {
		t.Fatalf("callback = %d location=%q body=%s", rec.Code, rec.Header().Get("Location"), rec.Body.String())
	}
	return rec.Result().Cookies()
}

func cookieNamed(cookies []*http.Cookie, name string) *http.Cookie {
	for _, cookie := range cookies {
		if cookie.Name == name && cookie.Value != "" {
			return cookie
		}
	}
	return nil
}

func TestAuthListsConfiguredProviders(t *testing.T) {
	fixture := testAuthFixture(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/providers", nil)
	rec := httptest.NewRecorder()
	fixture.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("providers status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"provider_id":"corp-wecom"`) || !strings.Contains(rec.Body.String(), `"display_name":"企业微信"`) {
		t.Fatalf("providers = %s", rec.Body.String())
	}
	for _, providerType := range []string{`"type":"wecom"`} {
		if !strings.Contains(rec.Body.String(), providerType) {
			t.Fatalf("providers must expose enabled type %s: %s", providerType, rec.Body.String())
		}
	}
	for _, providerType := range []string{`"type":"feishu"`, `"type":"oidc"`, `"type":"mock"`} {
		if strings.Contains(rec.Body.String(), providerType) {
			t.Fatalf("public providers must hide unavailable type %s: %s", providerType, rec.Body.String())
		}
	}
	if !strings.Contains(rec.Body.String(), `"provider_id":"corp-wecom"`) || !strings.Contains(rec.Body.String(), `"enabled":true`) {
		t.Fatalf("configured provider status missing: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "ww-corp") {
		t.Fatal("public provider metadata leaked enterprise credential detail")
	}
}

func TestAuthConfigurationShowsSupportedAndEnabledProvidersToSystemAdmin(t *testing.T) {
	fixture := testAuthFixture(t, func(deps *AuthDependencies, users *identity.MemoryIdentityStore, provider *fakeEnterpriseProvider) {
		user, err := users.ResolveLoginIdentity(context.Background(), identity.Identity{ProviderID: provider.descriptor.ProviderID, ProviderType: provider.descriptor.Type, EnterpriseID: provider.enterprise, SubjectID: provider.subject, DisplayName: provider.name, Email: provider.email})
		if err != nil {
			t.Fatal(err)
		}
		if err := users.SetSystemAdmin(context.Background(), user.PlatformUserID, true); err != nil {
			t.Fatal(err)
		}
		deps.CallbackURL = "https://console.example.com/api/v1/auth/callback"
	})
	cookies := completeAuth(t, fixture.handler, fixture.provider.descriptor.ProviderID)
	session := cookieNamed(cookies, "dsh_session")
	if session == nil {
		t.Fatal("session cookie missing")
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/configuration", nil)
	request.AddCookie(session)
	recorder := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("configuration = %d %s", recorder.Code, recorder.Body.String())
	}
	var body struct {
		CallbackURL string `json:"callback_url"`
		Providers   []struct {
			Type                string     `json:"type"`
			Configured          bool       `json:"configured"`
			Enabled             bool       `json:"enabled"`
			LastSuccessfulLogin *time.Time `json:"last_successful_login_at"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.CallbackURL != "https://console.example.com/api/v1/auth/callback" || len(body.Providers) != 5 {
		t.Fatalf("configuration = %+v", body)
	}
	configured := map[string]bool{}
	for _, provider := range body.Providers {
		configured[provider.Type] = provider.Configured && provider.Enabled
		if provider.Type == "wecom" && provider.LastSuccessfulLogin == nil {
			t.Fatal("configured provider with completed OAuth must expose latest successful login")
		}
	}
	if !configured["wecom"] || configured["feishu"] || configured["oidc"] || configured["mock"] {
		t.Fatalf("provider state = %+v", configured)
	}
}

func TestAuthProviderVerificationUsesCurrentAdminAccount(t *testing.T) {
	fixture := testAuthFixture(t, func(_ *AuthDependencies, users *identity.MemoryIdentityStore, provider *fakeEnterpriseProvider) {
		user, err := users.ResolveLoginIdentity(context.Background(), identity.Identity{
			ProviderID: provider.descriptor.ProviderID, ProviderType: provider.descriptor.Type,
			EnterpriseID: provider.enterprise, SubjectID: provider.subject, DisplayName: provider.name, Email: provider.email,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := users.SetSystemAdmin(context.Background(), user.PlatformUserID, true); err != nil {
			t.Fatal(err)
		}
	})
	cookies := completeAuth(t, fixture.handler, fixture.provider.descriptor.ProviderID)
	session, csrf := cookieNamed(cookies, "dsh_session"), cookieNamed(cookies, "csrf_token")
	if session == nil || csrf == nil {
		t.Fatalf("login cookies = %#v", cookies)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/verify-provider", strings.NewReader(`{"provider_id":"corp-wecom"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CSRF-Token", csrf.Value)
	req.AddCookie(session)
	req.AddCookie(csrf)
	rec := httptest.NewRecorder()
	fixture.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("verification start = %d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		AuthURL string `json:"auth_url"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(body.AuthURL)
	if err != nil {
		t.Fatal(err)
	}
	transaction := cookieNamed(rec.Result().Cookies(), "dsh_login_tx")
	if transaction == nil {
		t.Fatal("verification transaction cookie missing")
	}

	callback := httptest.NewRequest(http.MethodGet, "/api/v1/auth/callback?code=good-code&state="+url.QueryEscape(parsed.Query().Get("state")), nil)
	callback.AddCookie(transaction)
	callbackRecorder := httptest.NewRecorder()
	fixture.handler.ServeHTTP(callbackRecorder, callback)
	if callbackRecorder.Code != http.StatusFound || callbackRecorder.Header().Get("Location") != "/console/?tab=login-settings&provider_verified=corp-wecom" {
		t.Fatalf("verification callback = %d location=%q", callbackRecorder.Code, callbackRecorder.Header().Get("Location"))
	}
	if cookieNamed(callbackRecorder.Result().Cookies(), "dsh_session") != nil {
		t.Fatal("provider verification must not replace the current admin login session")
	}
	if latest, found, err := fixture.users.LatestLoginAtForProvider(context.Background(), "corp-wecom"); err != nil || !found || latest.IsZero() {
		t.Fatalf("provider verification timestamp = %v, %v, %v", latest, found, err)
	}
}

func TestAuthLoginRejectsUnknownProvider(t *testing.T) {
	fixture := testAuthFixture(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/login?provider=missing", nil)
	rec := httptest.NewRecorder()
	fixture.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestAuthCallbackCreatesCookieSessionWithoutTenantMembership(t *testing.T) {
	fixture := testAuthFixture(t, nil)
	cookies := completeAuth(t, fixture.handler, fixture.provider.descriptor.ProviderID)
	session := cookieNamed(cookies, "dsh_session")
	csrf := cookieNamed(cookies, "csrf_token")
	if session == nil || csrf == nil {
		t.Fatalf("callback cookies = %#v", cookies)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	request.AddCookie(session)
	recorder := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("me = %d %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"display_name":"张三"`) || !strings.Contains(recorder.Body.String(), `"is_system_admin":false`) {
		t.Fatalf("me = %s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"tenants":[]`) {
		t.Fatalf("zero-tenant user must still login: %s", recorder.Body.String())
	}
}

func TestAuthCallbackDoesNotBootstrapSystemAdminFromExternalIdentity(t *testing.T) {
	fixture := testAuthFixture(t, nil)
	completeAuth(t, fixture.handler, fixture.provider.descriptor.ProviderID)
	login, err := fixture.users.LookupLoginIdentity(context.Background(), fixture.provider.descriptor.ProviderID, fixture.provider.subject)
	if err != nil {
		t.Fatal(err)
	}
	isAdmin, err := fixture.users.IsSystemAdmin(context.Background(), login.PlatformUserID)
	if err != nil || isAdmin {
		t.Fatalf("external login system admin = %v, %v; want false", isAdmin, err)
	}
}

func TestAuthLocalOnlyLoginRequiresPasswordChangeThenRotatesSession(t *testing.T) {
	users := identity.NewMemoryIdentityStore()
	hash, err := identity.HashLocalPassword("temporary-password-123")
	if err != nil {
		t.Fatal(err)
	}
	user, err := users.CreateLocalUser(context.Background(), "local.user", "本地用户", "", hash, true)
	if err != nil {
		t.Fatal(err)
	}
	sessions := identity.NewMemorySessionStore()
	handler, err := NewAuthHandler(AuthDependencies{LocalEnabled: true, Sessions: sessions, Users: users, Audits: users, SessionTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	bad := httptest.NewRequest(http.MethodPost, "/api/v1/auth/local/login", strings.NewReader(`{"username":"local.user","password":"wrong-password-123"}`))
	bad.Header.Set("Content-Type", "application/json")
	badRecorder := httptest.NewRecorder()
	handler.ServeHTTP(badRecorder, bad)
	if badRecorder.Code != http.StatusUnauthorized || strings.Contains(badRecorder.Body.String(), "local.user") {
		t.Fatalf("bad local login = %d %s", badRecorder.Code, badRecorder.Body.String())
	}

	login := httptest.NewRequest(http.MethodPost, "/api/v1/auth/local/login", strings.NewReader(`{"username":"local.user","password":"temporary-password-123"}`))
	login.Header.Set("Content-Type", "application/json")
	loginRecorder := httptest.NewRecorder()
	handler.ServeHTTP(loginRecorder, login)
	if loginRecorder.Code != http.StatusOK || !strings.Contains(loginRecorder.Body.String(), `"must_change_password":true`) {
		t.Fatalf("local login = %d %s", loginRecorder.Code, loginRecorder.Body.String())
	}
	session := cookieNamed(loginRecorder.Result().Cookies(), "dsh_session")
	csrf := cookieNamed(loginRecorder.Result().Cookies(), "csrf_token")
	if session == nil || csrf == nil {
		t.Fatal("local login cookies missing")
	}

	change := httptest.NewRequest(http.MethodPost, "/api/v1/auth/local/password", strings.NewReader(`{"current_password":"temporary-password-123","new_password":"permanent-password-456"}`))
	change.Header.Set("Content-Type", "application/json")
	change.AddCookie(session)
	change.AddCookie(csrf)
	change.Header.Set("X-CSRF-Token", csrf.Value)
	changeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(changeRecorder, change)
	if changeRecorder.Code != http.StatusNoContent {
		t.Fatalf("password change = %d %s", changeRecorder.Code, changeRecorder.Body.String())
	}
	if _, err := sessions.Get(context.Background(), session.Value); err == nil {
		t.Fatal("temporary-password session must be revoked after password change")
	}
	credential, _, err := users.LookupLocalCredential(context.Background(), "local.user")
	if err != nil || credential.MustChangePassword || !identity.VerifyLocalPassword(credential.PasswordHash, "permanent-password-456") {
		t.Fatalf("rotated credential = %#v err=%v", credential, err)
	}
	principal, err := users.ResolveSessionUser(context.Background(), user.PlatformUserID)
	if err != nil || principal.MustChangePassword {
		t.Fatalf("resolved principal after change = %#v err=%v", principal, err)
	}
}

func TestAuthLocalRegistrationCreatesPlatformIdentityWithoutTenantAccess(t *testing.T) {
	users := identity.NewMemoryIdentityStore()
	sessions := identity.NewMemorySessionStore()
	handler, err := NewAuthHandler(AuthDependencies{
		LocalEnabled: true, LocalRegistrationEnabled: true,
		Sessions: sessions, Users: users, Audits: users, SessionTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/local/register", strings.NewReader(`{"username":"new.user","display_name":"新用户","email":"new@example.com","password":"password-12345"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("register status = %d: %s", recorder.Code, recorder.Body.String())
	}
	sessionCookie := cookieNamed(recorder.Result().Cookies(), "dsh_session")
	if sessionCookie == nil {
		t.Fatal("registration did not issue login session")
	}
	principal, err := sessions.Get(context.Background(), sessionCookie.Value)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := users.ResolveSessionUser(context.Background(), principal.PlatformUserID)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.IsSystemAdmin || len(resolved.Tenants) != 0 {
		t.Fatalf("self-registered identity unexpectedly has access: %+v", resolved)
	}
}

func TestAuthLocalRegistrationIsClosedByDefault(t *testing.T) {
	handler, err := NewAuthHandler(AuthDependencies{
		LocalEnabled: true, Sessions: identity.NewMemorySessionStore(), Users: identity.NewMemoryIdentityStore(), SessionTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/local/register", strings.NewReader(`{"username":"new.user","password":"password-12345"}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("register status = %d, want 404", recorder.Code)
	}
}

func TestAuthCallbackRejectsTamperedState(t *testing.T) {
	fixture := testAuthFixture(t, nil)
	_, transaction := beginAuth(t, fixture.handler, fixture.provider.descriptor.ProviderID)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/callback?code=good-code&state=tampered", nil)
	req.AddCookie(transaction)
	rec := httptest.NewRecorder()
	fixture.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound || !strings.Contains(rec.Header().Get("Location"), "invalid_state") {
		t.Fatalf("callback = %d location=%q", rec.Code, rec.Header().Get("Location"))
	}
}

func TestAuthLogoutRequiresCSRFAndInvalidatesCookieSession(t *testing.T) {
	fixture := testAuthFixture(t, nil)
	cookies := completeAuth(t, fixture.handler, fixture.provider.descriptor.ProviderID)
	session, csrf := cookieNamed(cookies, "dsh_session"), cookieNamed(cookies, "csrf_token")

	missing := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	missing.AddCookie(session)
	missingRec := httptest.NewRecorder()
	fixture.handler.ServeHTTP(missingRec, missing)
	if missingRec.Code != http.StatusForbidden {
		t.Fatalf("logout without CSRF = %d", missingRec.Code)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/logout", nil)
	request.AddCookie(session)
	request.AddCookie(csrf)
	request.Header.Set("X-CSRF-Token", csrf.Value)
	recorder := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("logout = %d", recorder.Code)
	}

	me := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	me.AddCookie(session)
	meRec := httptest.NewRecorder()
	fixture.handler.ServeHTTP(meRec, me)
	if meRec.Code != http.StatusUnauthorized {
		t.Fatalf("old session after logout = %d", meRec.Code)
	}
}

func TestAuthSessionCannotBeUsedAsBearerCredential(t *testing.T) {
	fixture := testAuthFixture(t, nil)
	cookies := completeAuth(t, fixture.handler, fixture.provider.descriptor.ProviderID)
	session := cookieNamed(cookies, "dsh_session")
	request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", nil)
	request.Header.Set("Authorization", "Bearer "+session.Value)
	recorder := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("Bearer browser session = %d, want 401", recorder.Code)
	}
}
