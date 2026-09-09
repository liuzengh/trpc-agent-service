package secret

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPProviderResolvesVaultWithoutLeakingMetadata(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/secret/data/model" || request.Header.Get("X-Vault-Token") != "vault-token" {
			t.Fatalf("unexpected request path=%q", request.URL.Path)
		}
		_, _ = response.Write([]byte(`{"data":{"data":{"value":"resolved"}}}`))
	}))
	defer server.Close()
	provider := &HTTPProvider{Endpoint: server.URL, Token: "vault-token", Mode: HTTPProviderVault, Client: server.Client()}
	value, err := provider.Resolve(context.Background(), "secret/data/model")
	if err != nil || value != "resolved" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}

func TestHTTPProviderDoesNotFollowRedirects(t *testing.T) {
	var targetCalls int
	target := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		targetCalls++
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte(`{"value":"unexpected"}`))
	}))
	defer target.Close()
	origin := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, target.URL, http.StatusFound)
	}))
	defer origin.Close()

	provider := &HTTPProvider{
		Endpoint: origin.URL,
		Token:    "vault-token",
		Mode:     HTTPProviderVault,
		Client:   origin.Client(),
	}
	if _, err := provider.Resolve(context.Background(), "secret/data/model"); err == nil {
		t.Fatal("redirect response was accepted")
	}
	if targetCalls != 0 {
		t.Fatalf("redirect target received %d request(s)", targetCalls)
	}
}
