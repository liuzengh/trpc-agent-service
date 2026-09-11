package adminauth

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPrincipalTenantScopedActions(t *testing.T) {
	p := Principal{Subject: "viewer", Roles: []string{"viewer"}, Tenants: []string{"acme"}}
	if !p.Allows(ActionView, "acme") || p.Allows(ActionOperate, "acme") || p.Allows(ActionView, "globex") {
		t.Fatal("tenant-scoped viewer permissions are incorrect")
	}
	if !p.AllowsGlobal(ActionView) || p.AllowsGlobal(ActionOperate) {
		t.Fatal("scoped viewer global read/write permissions are incorrect")
	}
}

func TestOIDCVerifiesClaimsAndRejectsWrongAudience(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJWKS(w, &key.PublicKey, "key-1")
	}))
	defer server.Close()
	oidc, err := NewOIDC(OIDCOptions{Issuer: "https://issuer.test", Audience: "trpc-admin", JWKSURL: server.URL, CacheTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	token := signedToken(t, key, map[string]any{"iss": "https://issuer.test", "aud": "trpc-admin", "sub": "operator", "roles": []string{"operator"}, "tenants": []string{"acme"}, "exp": time.Now().Add(time.Hour).Unix()})
	principal, err := oidc.Authenticate(t.Context(), token)
	if err != nil || !principal.Allows(ActionOperate, "acme") {
		t.Fatalf("valid token rejected: %+v %v", principal, err)
	}
	wrong := signedToken(t, key, map[string]any{"iss": "https://issuer.test", "aud": "other", "sub": "operator", "roles": []string{"operator"}, "exp": time.Now().Add(time.Hour).Unix()})
	if _, err := oidc.Authenticate(t.Context(), wrong); err == nil {
		t.Fatal("wrong audience was accepted")
	}
}

func signedToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(data)
	}
	header := encode(map[string]string{"alg": "RS256", "kid": "key-1", "typ": "JWT"})
	body := encode(claims)
	digest := sha256.Sum256([]byte(header + "." + body))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return header + "." + body + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func writeJWKS(w http.ResponseWriter, key *rsa.PublicKey, kid string) {
	encode := func(value []byte) string { return base64.RawURLEncoding.EncodeToString(value) }
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{"kty": "RSA", "kid": kid, "alg": "RS256", "n": encode(key.N.Bytes()), "e": encode([]byte{1, 0, 1})}}})
}
