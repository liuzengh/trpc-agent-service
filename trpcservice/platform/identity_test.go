package platform

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProductionIdentityLoginUsesBoundedHTTPOnlySession(t *testing.T) {
	provider := NewJWTIdentityProvider(JWTIdentityConfig{Issuer: "issuer", Audience: "audience", HMACSecret: []byte("secret")}, map[string]DevelopmentIdentity{
		"user": {ID: "user", Name: "User", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RoleOperator}, {TenantID: "tenant-b", Role: RoleViewer}}},
	})
	handler := NewAdminHandler(nil, DevelopmentIdentity{})
	handler.ConfigureIdentityProvider(provider)
	server := httptest.NewServer(handler)
	defer server.Close()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	token := signTestJWT(t, []byte("secret"), map[string]any{"iss": "issuer", "aud": "audience", "sub": "user", "exp": time.Now().Add(time.Hour).Unix()})
	response, err := client.Post(server.URL+"/api/v1/auth/login", "application/json", bytes.NewBufferString(`{"token":"`+token+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	var identity identityResponse
	decodeResponse(t, response, http.StatusOK, &identity)
	if identity.ID != "user" {
		t.Fatalf("identity = %#v", identity)
	}
	response, _ = client.Get(server.URL + "/api/v1/auth/me")
	decodeResponse(t, response, http.StatusOK, &identity)
	requireJSONResponse(t, client, server.URL+"/api/v1/auth/switch-tenant", `{"tenant_id":"tenant-b"}`, "", http.StatusOK, &identity)
	if identity.ActiveTenantID != "tenant-b" || identity.ActiveRole != RoleViewer {
		t.Fatalf("switched identity = %#v", identity)
	}
	response, _ = client.Post(server.URL+"/api/v1/auth/logout", "application/json", bytes.NewBuffer(nil))
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("logout = %d", response.StatusCode)
	}
	response, _ = client.Get(server.URL + "/api/v1/auth/me")
	assertAPIError(t, response, http.StatusUnauthorized, "identity_required")
}

func TestProductionIdentityValidatesJWTAndTenantAssignment(t *testing.T) {
	provider := NewJWTIdentityProvider(JWTIdentityConfig{
		Issuer: "https://identity.example", Audience: "agent-platform", HMACSecret: []byte("test-signing-secret"),
	}, map[string]DevelopmentIdentity{
		"operator-1": {ID: "operator-1", Name: "Operator", Assignments: []TenantAssignment{
			{TenantID: "tenant-a", TenantName: "A", Role: RoleOperator},
			{TenantID: "tenant-b", TenantName: "B", Role: RoleViewer},
		}},
	})
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{})
	handler.ConfigureIdentityProvider(provider)
	server := httptest.NewServer(handler)
	defer server.Close()

	token := signTestJWT(t, []byte("test-signing-secret"), map[string]any{
		"iss": "https://identity.example", "aud": "agent-platform", "sub": "operator-1", "exp": time.Now().Add(time.Hour).Unix(),
	})
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/auth/me", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Active-Tenant", "tenant-b")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var identity identityResponse
	decodeResponse(t, response, http.StatusOK, &identity)
	if identity.ID != "operator-1" || identity.ActiveTenantID != "tenant-b" || identity.ActiveRole != RoleViewer {
		t.Fatalf("identity = %#v", identity)
	}

	forged, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/admin/agent-apps", nil)
	forged.Header.Set("Authorization", "Bearer "+token)
	forged.Header.Set("X-Active-Tenant", "tenant-forged")
	assertAPIError(t, mustDo(t, forged), http.StatusForbidden, "tenant_not_assigned")
}

func TestProductionIdentityRejectsInvalidCredentialsAndDisablesDevelopmentSession(t *testing.T) {
	provider := NewJWTIdentityProvider(JWTIdentityConfig{
		Issuer: "issuer", Audience: "audience", HMACSecret: []byte("correct-secret"),
	}, map[string]DevelopmentIdentity{"user": {ID: "user", Assignments: []TenantAssignment{{TenantID: "tenant-a", Role: RoleViewer}}}})
	handler := NewAdminHandler(NewInMemoryControlPlane(), DevelopmentIdentity{ID: "developer", Assignments: []TenantAssignment{{TenantID: "tenant-dev", Role: RolePlatformAdmin}}})
	handler.ConfigureIdentityProvider(provider)
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/v1/auth/me")
	if err != nil {
		t.Fatal(err)
	}
	assertAPIError(t, response, http.StatusUnauthorized, "identity_required")

	bad := signTestJWT(t, []byte("wrong-secret"), map[string]any{
		"iss": "issuer", "aud": "audience", "sub": "user", "exp": time.Now().Add(time.Hour).Unix(),
	})
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/auth/me", nil)
	request.Header.Set("Authorization", "Bearer "+bad)
	assertAPIError(t, mustDo(t, request), http.StatusUnauthorized, "invalid_identity_token")

	expired := signTestJWT(t, []byte("correct-secret"), map[string]any{
		"iss": "issuer", "aud": "audience", "sub": "user", "exp": time.Now().Add(-time.Minute).Unix(),
	})
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/v1/auth/me", nil)
	request.Header.Set("Authorization", "Bearer "+expired)
	assertAPIError(t, mustDo(t, request), http.StatusUnauthorized, "invalid_identity_token")
}

func mustDo(t *testing.T, request *http.Request) *http.Response {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func signTestJWT(t *testing.T, secret []byte, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	payload, _ := json.Marshal(claims)
	encoded := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(encoded))
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestProductionIdentityRejectsMalformedBearerWithoutLeakingToken(t *testing.T) {
	handler := NewAdminHandler(nil, DevelopmentIdentity{})
	handler.ConfigureIdentityProvider(NewJWTIdentityProvider(JWTIdentityConfig{Issuer: "issuer", Audience: "aud", HMACSecret: []byte("secret")}, nil))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/me", bytes.NewBuffer(nil))
	request.Header.Set("Authorization", "Bearer stage5-secret-canary")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || bytes.Contains(response.Body.Bytes(), []byte("stage5-secret-canary")) {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}
