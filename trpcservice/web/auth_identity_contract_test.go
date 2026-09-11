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

type secondaryLoginProvider struct {
	descriptor identity.ProviderDescriptor
	boundaryID string
	subjectID  string
}

func (p *secondaryLoginProvider) Descriptor() identity.ProviderDescriptor { return p.descriptor }

func (p *secondaryLoginProvider) Begin(request identity.AuthRequest) (string, error) {
	query := url.Values{}
	query.Set("state", request.State)
	query.Set("nonce", request.Nonce)
	query.Set("challenge", request.PKCEChallenge)
	return "https://login.example.test/authorize?" + query.Encode(), nil
}

func (p *secondaryLoginProvider) Exchange(context.Context, identity.AuthExchange) (identity.Identity, error) {
	return identity.Identity{
		ProviderID:   p.descriptor.ProviderID,
		ProviderType: p.descriptor.Type,
		EnterpriseID: p.boundaryID,
		SubjectID:    p.subjectID,
		DisplayName:  "客服用户",
		Email:        "support.user@example.test",
	}, nil
}

func newLocalAndSecondaryAuthHandler(t *testing.T) (http.Handler, *identity.MemoryIdentityStore) {
	t.Helper()
	provider := &secondaryLoginProvider{
		descriptor: identity.ProviderDescriptor{
			ProviderID:  "secondary-login",
			Type:        identity.ProviderOIDC,
			DisplayName: "备用登录",
		},
		boundaryID: "directory-test",
		subjectID:  "support-user-1",
	}
	users := identity.NewMemoryIdentityStore()
	if err := users.UpsertLoginProvider(context.Background(), provider.descriptor, provider.boundaryID); err != nil {
		t.Fatalf("UpsertLoginProvider() error = %v", err)
	}
	handler, err := NewAuthHandler(AuthDependencies{
		Providers:                map[string]identity.IdentityProvider{provider.descriptor.ProviderID: provider},
		Sessions:                 identity.NewMemorySessionStore(),
		Users:                    users,
		Audits:                   users,
		LocalEnabled:             true,
		LocalRegistrationEnabled: true,
		SessionTTL:               time.Hour,
		StateSecret:              testStateSecret,
	})
	if err != nil {
		t.Fatalf("NewAuthHandler() error = %v", err)
	}
	return handler, users
}

func registerLocalAccount(t *testing.T, handler http.Handler) (*http.Cookie, *http.Cookie) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/local/register", strings.NewReader(`{
		"username":"support.user",
		"display_name":"客服用户",
		"email":"support.user@example.test",
		"password":"local-password-12345"
	}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("local register = %d %s", recorder.Code, recorder.Body.String())
	}
	session := cookieNamed(recorder.Result().Cookies(), "dsh_session")
	csrf := cookieNamed(recorder.Result().Cookies(), "csrf_token")
	if session == nil || csrf == nil {
		t.Fatalf("registration cookies = %#v", recorder.Result().Cookies())
	}
	return session, csrf
}

func TestAuthLoginIdentityLifecycleThroughHTTP(t *testing.T) {
	handler, _ := newLocalAndSecondaryAuthHandler(t)
	session, csrf := registerLocalAccount(t, handler)

	link := httptest.NewRequest(http.MethodPost, "/api/v1/auth/link", strings.NewReader(`{"provider_id":"secondary-login"}`))
	link.Header.Set("Content-Type", "application/json")
	link.Header.Set("X-CSRF-Token", csrf.Value)
	link.AddCookie(session)
	link.AddCookie(csrf)
	linkRecorder := httptest.NewRecorder()
	handler.ServeHTTP(linkRecorder, link)
	if linkRecorder.Code != http.StatusOK {
		t.Fatalf("link start = %d %s", linkRecorder.Code, linkRecorder.Body.String())
	}
	var linkBody struct {
		AuthURL string `json:"auth_url"`
	}
	if err := json.Unmarshal(linkRecorder.Body.Bytes(), &linkBody); err != nil {
		t.Fatal(err)
	}
	authURL, err := url.Parse(linkBody.AuthURL)
	if err != nil {
		t.Fatal(err)
	}
	transaction := cookieNamed(linkRecorder.Result().Cookies(), "dsh_login_tx")
	if transaction == nil {
		t.Fatal("login-link transaction cookie missing")
	}

	callback := httptest.NewRequest(http.MethodGet, "/api/v1/auth/callback?code=linked&state="+url.QueryEscape(authURL.Query().Get("state")), nil)
	callback.AddCookie(transaction)
	callbackRecorder := httptest.NewRecorder()
	handler.ServeHTTP(callbackRecorder, callback)
	if callbackRecorder.Code != http.StatusFound || callbackRecorder.Header().Get("Location") != "/console/?tab=account&identity_linked=1" {
		t.Fatalf("link callback = %d location=%q body=%s", callbackRecorder.Code, callbackRecorder.Header().Get("Location"), callbackRecorder.Body.String())
	}

	list := httptest.NewRequest(http.MethodGet, "/api/v1/auth/login-identities", nil)
	list.AddCookie(session)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, list)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("list login identities = %d %s", listRecorder.Code, listRecorder.Body.String())
	}
	if !strings.Contains(listRecorder.Body.String(), `"provider_id":"local"`) || !strings.Contains(listRecorder.Body.String(), `"provider_id":"secondary-login"`) {
		t.Fatalf("login identities = %s", listRecorder.Body.String())
	}

	remove := httptest.NewRequest(http.MethodDelete, "/api/v1/auth/login-identities", strings.NewReader(`{"provider_id":"secondary-login","subject_id":"support-user-1"}`))
	remove.Header.Set("Content-Type", "application/json")
	remove.Header.Set("X-CSRF-Token", csrf.Value)
	remove.AddCookie(session)
	remove.AddCookie(csrf)
	removeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(removeRecorder, remove)
	if removeRecorder.Code != http.StatusNoContent {
		t.Fatalf("remove login identity = %d %s", removeRecorder.Code, removeRecorder.Body.String())
	}

	removeLast := httptest.NewRequest(http.MethodDelete, "/api/v1/auth/login-identities", strings.NewReader(`{"provider_id":"local","subject_id":"support.user"}`))
	removeLast.Header.Set("Content-Type", "application/json")
	removeLast.Header.Set("X-CSRF-Token", csrf.Value)
	removeLast.AddCookie(session)
	removeLast.AddCookie(csrf)
	removeLastRecorder := httptest.NewRecorder()
	handler.ServeHTTP(removeLastRecorder, removeLast)
	if removeLastRecorder.Code != http.StatusConflict {
		t.Fatalf("remove last login identity = %d, want 409", removeLastRecorder.Code)
	}
}

func TestAuthRejectsInvalidConstructionAndRequestMethods(t *testing.T) {
	t.Parallel()
	validUsers := identity.NewMemoryIdentityStore()
	validSessions := identity.NewMemorySessionStore()
	provider := &secondaryLoginProvider{
		descriptor: identity.ProviderDescriptor{ProviderID: "secondary-login", Type: identity.ProviderOIDC},
		boundaryID: "directory-test",
		subjectID:  "support-user-1",
	}

	tests := []struct {
		name string
		deps AuthDependencies
	}{
		{name: "no provider", deps: AuthDependencies{Sessions: validSessions, Users: validUsers, SessionTTL: time.Hour}},
		{name: "nil provider", deps: AuthDependencies{Providers: map[string]identity.IdentityProvider{"secondary-login": nil}, Sessions: validSessions, Users: validUsers, SessionTTL: time.Hour, StateSecret: testStateSecret}},
		{name: "missing sessions", deps: AuthDependencies{Providers: map[string]identity.IdentityProvider{"secondary-login": provider}, Users: validUsers, SessionTTL: time.Hour, StateSecret: testStateSecret}},
		{name: "missing users", deps: AuthDependencies{Providers: map[string]identity.IdentityProvider{"secondary-login": provider}, Sessions: validSessions, SessionTTL: time.Hour, StateSecret: testStateSecret}},
		{name: "invalid ttl", deps: AuthDependencies{Providers: map[string]identity.IdentityProvider{"secondary-login": provider}, Sessions: validSessions, Users: validUsers, StateSecret: testStateSecret}},
		{name: "missing state secret", deps: AuthDependencies{Providers: map[string]identity.IdentityProvider{"secondary-login": provider}, Sessions: validSessions, Users: validUsers, SessionTTL: time.Hour}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewAuthHandler(test.deps); err == nil {
				t.Fatal("NewAuthHandler() error = nil")
			}
		})
	}

	handler, _ := newLocalAndSecondaryAuthHandler(t)
	for _, endpoint := range []string{"/api/v1/auth/providers", "/api/v1/auth/login?provider=secondary-login", "/api/v1/auth/local/login", "/api/v1/auth/local/register"} {
		request := httptest.NewRequest(http.MethodPatch, endpoint, nil)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("PATCH %s status = %d, want 405", endpoint, recorder.Code)
		}
	}
}

func TestAuthLocalPasswordValidationErrors(t *testing.T) {
	handler, _ := newLocalAndSecondaryAuthHandler(t)
	session, csrf := registerLocalAccount(t, handler)

	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "bad json", body: `{`, want: http.StatusBadRequest},
		{name: "wrong current password", body: `{"current_password":"wrong-password","new_password":"another-password-12345"}`, want: http.StatusBadRequest},
		{name: "same password", body: `{"current_password":"local-password-12345","new_password":"local-password-12345"}`, want: http.StatusBadRequest},
		{name: "weak new password", body: `{"current_password":"local-password-12345","new_password":"short"}`, want: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/local/password", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-CSRF-Token", csrf.Value)
			request.AddCookie(session)
			request.AddCookie(csrf)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, test.want, recorder.Body.String())
			}
		})
	}
}
