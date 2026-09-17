package vault

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

func TestProviderResolvesExactVersionWithScopedToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tenant/data/model" || r.Header.Get("X-Vault-Token") != "vault-token" || r.Header.Get("X-Vault-Namespace") != "platform" {
			t.Fatalf("request path=%q token=%q namespace=%q", r.URL.Path, r.Header.Get("X-Vault-Token"), r.Header.Get("X-Vault-Namespace"))
		}
		_, _ = w.Write([]byte(`{"data":{"data":{"value":"secret-value"},"metadata":{"version":7}}}`))
	}))
	t.Cleanup(server.Close)
	provider, err := New(Config{Endpoint: server.URL, Namespace: "platform", AllowInsecureHTTP: true, TokenSource: fixedToken("vault-token")})
	if err != nil {
		t.Fatal(err)
	}
	value, err := provider.Resolve(context.Background(), fixtureScope(), secrets.SecretRef{Ref: "vault://tenant/model", Version: 7})
	if err != nil || string(value.Bytes) != "secret-value" || value.Version != 7 {
		t.Fatalf("value=%+v err=%v", value, err)
	}
	clear(value.Bytes)
}

func TestProviderRejectsWrongVersionAndUnsafeReference(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"data":{"value":"secret-value"},"metadata":{"version":8}}}`))
	}))
	t.Cleanup(server.Close)
	provider, err := New(Config{Endpoint: server.URL, AllowInsecureHTTP: true, TokenSource: fixedToken("vault-token")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Resolve(context.Background(), fixtureScope(), secrets.SecretRef{Ref: "vault://tenant/model", Version: 7}); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("version=%v", err)
	}
	if _, err := provider.Resolve(context.Background(), fixtureScope(), secrets.SecretRef{Ref: "vault://tenant/../model", Version: 7}); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("unsafe ref=%v", err)
	}
}

type fixedToken string

func (f fixedToken) Token(context.Context) ([]byte, error) { return []byte(f), nil }

func fixtureScope() secrets.Scope {
	return secrets.Scope{TenantID: "tenant-a", Subject: "worker", Purpose: secrets.PurposeBackendConnect, ResourceID: "backend", ResourceVersion: 3}
}
