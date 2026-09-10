package admin

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestNewSessionAuthenticatorRejectsInvalidConfiguration(t *testing.T) {
	static, err := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, username, password string
		static                   *StaticAuthenticator
	}{
		{name: "empty username", username: "", password: "secret", static: static},
		{name: "empty password", username: "operator", password: "", static: static},
		{name: "username newline", username: "oper\nator", password: "secret", static: static},
		{name: "password newline", username: "operator", password: "secret\r", static: static},
		{name: "missing static authenticator", username: "operator", password: "secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSessionAuthenticator(tc.username, tc.password, tc.static); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSessionAuthenticatorAuthenticateCookieWriteAndContextBranches(t *testing.T) {
	static, err := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewSessionAuthenticator("operator", "secret", static)
	if err != nil {
		t.Fatal(err)
	}
	token, err := auth.newSession()
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://admin.test/admin/v1", nil)
	request.AddCookie(&http.Cookie{Name: AdminSessionCookie, Value: token})
	if _, err := auth.Authenticate(context.Background(), request); !errors.Is(err, ErrForbidden) {
		t.Fatalf("cookie write without same origin error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := auth.Authenticate(canceled, httptest.NewRequest(http.MethodGet, "http://admin.test/admin/v1", nil)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled authentication error = %v", err)
	}

	auth.rand = nil
	if _, err := auth.newSession(); err != nil {
		t.Fatalf("nil random source fallback error = %v", err)
	}
}

func TestSessionPrincipalRejectsMalformedPayloads(t *testing.T) {
	static, err := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewSessionAuthenticator("operator", "secret", static)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := auth.newSession()
	if err != nil {
		t.Fatal(err)
	}
	_, _, ok := strings.Cut(valid, ".")
	if !ok {
		t.Fatal("session token has no signature")
	}
	for _, tc := range []struct {
		name, token string
	}{
		{name: "missing separator", token: "invalid"},
		{name: "invalid payload encoding", token: "%%%." + auth.signSessionPayload("%%%")},
		{name: "invalid payload json", token: "e30." + auth.signSessionPayload("e30")},
		{name: "trailing payload data", token: signedAuthPayload(t, auth, []byte(`{"e":4102444800,"n":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","v":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}{}`))},
		{name: "invalid nonce encoding", token: signedAuthPayload(t, auth, authPayloadBytes(t, sessionPayload{ExpiresAtUnix: 4102444800, Nonce: "%%", CredentialVersion: base64.RawURLEncoding.EncodeToString(auth.credentialVersion[:])}))},
		{name: "wrong nonce length", token: signedAuthPayload(t, auth, authPayloadBytes(t, sessionPayload{ExpiresAtUnix: 4102444800, Nonce: base64.RawURLEncoding.EncodeToString([]byte("short")), CredentialVersion: base64.RawURLEncoding.EncodeToString(auth.credentialVersion[:])}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := auth.sessionPrincipal(tc.token); ok {
				t.Fatalf("malformed token accepted: %q", tc.token)
			}
		})
	}

	// A valid payload with an expired timestamp exercises the time boundary
	// after all structural and credential checks pass.
	auth.now = func() time.Time { return time.Now().Add(defaultSessionTTL + time.Hour) }
	if _, ok := auth.sessionPrincipal(valid); ok {
		t.Fatal("expired token accepted")
	}
}

func authPayloadBytes(t *testing.T, payload sessionPayload) []byte {
	t.Helper()
	value, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func signedAuthPayload(t *testing.T, auth *SessionAuthenticator, payload []byte) string {
	t.Helper()
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	return encoded + "." + auth.signSessionPayload(encoded)
}

func TestAuthOriginAndBodyHelpersRejectInvalidInputs(t *testing.T) {
	if sameOrigin(nil) || sameOriginOrNoOrigin(nil) {
		t.Fatal("nil request unexpectedly matched same origin")
	}
	if sameOriginOrNoOrigin(httptest.NewRequest(http.MethodGet, "http://admin.test/admin/v1", nil)) != true {
		t.Fatal("request without origin should be allowed for non-browser clients")
	}
	invalid := httptest.NewRequest(http.MethodGet, "http://admin.test/admin/v1", nil)
	invalid.Header.Set("Origin", "not a URL")
	if sameOrigin(invalid) {
		t.Fatal("invalid origin unexpectedly matched")
	}
	missingHost := &http.Request{Method: http.MethodGet, URL: &url.URL{Host: "admin.test", Scheme: "http"}, Header: make(http.Header)}
	missingHost.Header.Set("Origin", "http://admin.test")
	if !sameOrigin(missingHost) {
		t.Fatal("request URL host was not used when Host header was empty")
	}
	tlsRequest := httptest.NewRequest(http.MethodGet, "https://admin.test/admin/v1", nil)
	tlsRequest.TLS = &tls.ConnectionState{}
	if requestScheme(tlsRequest) != "https" {
		t.Fatal("TLS request did not select HTTPS scheme")
	}
	if err := decodeAuthBody(nil, &struct{}{}); err == nil {
		t.Fatal("nil auth request was accepted")
	}
	request := httptest.NewRequest(http.MethodPost, "/admin/auth/login", nil)
	if err := decodeAuthBody(request, &struct{}{}); err == nil {
		t.Fatal("missing auth body was accepted")
	}
}
