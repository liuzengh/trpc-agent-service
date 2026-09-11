package identity

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

func TestOIDCBeginUsesDiscoveryPKCEAndConfiguredRedirectURI(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(writer, request)
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"issuer":                                server.URL,
			"authorization_endpoint":                server.URL + "/authorize",
			"token_endpoint":                        server.URL + "/token",
			"jwks_uri":                              server.URL + "/jwks",
			"userinfo_endpoint":                     server.URL + "/userinfo",
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	}))
	defer server.Close()

	provider, err := NewOIDCProvider(context.Background(), OIDCConfig{
		ProviderID: "corp-oidc", Issuer: server.URL, ClientID: "client", ClientSecret: "secret",
		RedirectURI: "https://console.example.com/api/v1/auth/callback", HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	authURL, err := provider.Begin(AuthRequest{State: "state-1", Nonce: "nonce-1", PKCEChallenge: "challenge-1"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if parsed.Path != "/authorize" || query.Get("redirect_uri") != "https://console.example.com/api/v1/auth/callback" {
		t.Fatalf("authorization URL = %s", authURL)
	}
	if query.Get("state") != "state-1" || query.Get("nonce") != "nonce-1" || query.Get("code_challenge") != "challenge-1" || query.Get("code_challenge_method") != "S256" {
		t.Fatalf("authorization query = %v", query)
	}
}

func TestOIDCExchangeVerifiesIDTokenNonceAndUserInfo(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: privateKey},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "test-key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"issuer":                                server.URL,
				"authorization_endpoint":                server.URL + "/authorize",
				"token_endpoint":                        server.URL + "/token",
				"jwks_uri":                              server.URL + "/jwks",
				"userinfo_endpoint":                     server.URL + "/userinfo",
				"response_types_supported":              []string{"code"},
				"subject_types_supported":               []string{"public"},
				"id_token_signing_alg_values_supported": []string{"RS256"},
			})
		case "/jwks":
			_ = json.NewEncoder(writer).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
				Key: &privateKey.PublicKey, KeyID: "test-key", Algorithm: string(jose.RS256), Use: "sig",
			}}})
		case "/token":
			if err := request.ParseForm(); err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			if request.Form.Get("grant_type") != "authorization_code" || request.Form.Get("code") != "good-code" {
				http.Error(writer, "invalid authorization code exchange", http.StatusBadRequest)
				return
			}
			if request.Form.Get("redirect_uri") != "https://console.example.com/api/v1/auth/callback" || request.Form.Get("code_verifier") != "verifier-1" {
				http.Error(writer, "redirect URI or PKCE verifier mismatch", http.StatusBadRequest)
				return
			}
			now := time.Now()
			idToken, signErr := jwt.Signed(signer).Claims(jwt.Claims{
				Issuer: server.URL, Subject: "employee-42", Audience: jwt.Audience{"client"},
				IssuedAt: jwt.NewNumericDate(now), Expiry: jwt.NewNumericDate(now.Add(time.Hour)),
			}).Claims(map[string]any{
				"nonce": "nonce-1",
				"name":  "Token Name",
				"email": "token@example.com",
			}).Serialize()
			if signErr != nil {
				http.Error(writer, signErr.Error(), http.StatusInternalServerError)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"access_token": "access-1", "token_type": "Bearer", "expires_in": 3600, "id_token": idToken,
			})
		case "/userinfo":
			if request.Header.Get("Authorization") != "Bearer access-1" {
				http.Error(writer, "missing bearer token", http.StatusUnauthorized)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"sub": "employee-42", "name": "企业用户", "email": "employee@example.com",
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	provider, err := NewOIDCProvider(context.Background(), OIDCConfig{
		ProviderID: "corp-oidc", Issuer: server.URL, ClientID: "client", ClientSecret: "secret",
		RedirectURI: "https://console.example.com/api/v1/auth/callback", HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := provider.Exchange(context.Background(), AuthExchange{
		Code: "good-code", Nonce: "nonce-1", PKCEVerifier: "verifier-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if identity.ProviderID != "corp-oidc" || identity.ProviderType != ProviderOIDC || identity.EnterpriseID != server.URL {
		t.Fatalf("identity provider boundary = %+v", identity)
	}
	if identity.SubjectID != "employee-42" || identity.DisplayName != "企业用户" || identity.Email != "employee@example.com" {
		t.Fatalf("identity = %+v", identity)
	}
}
