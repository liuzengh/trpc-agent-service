package secret

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestFileProviderUsesScopedKey(t *testing.T) {
	directory := t.TempDir()
	scope := tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}
	ref := tenant.SecretRef{Name: "model-key", Version: "v1"}
	key := ScopedEnvironmentKey(scope, ref)
	if err := os.WriteFile(filepath.Join(directory, key), []byte("value"), 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := NewFileProvider(directory)
	if err != nil {
		t.Fatal(err)
	}
	value, err := provider.ResolveSecret(context.Background(), scope, ref)
	if err != nil || value != "value" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}

func TestVaultProviderDoesNotExposeTokenAndReadsKV2(t *testing.T) {
	const token = "vault-token-value"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != token {
			t.Fatalf("vault token header = %q", r.Header.Get("X-Vault-Token"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"data":{"value":"secret-value"}}}`))
	}))
	defer server.Close()
	provider, err := NewVaultProvider(server.URL, "secret", token, "", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	value, err := provider.ResolveSecret(context.Background(), tenant.Scope{TenantID: "t", AppID: "a"}, tenant.SecretRef{Name: "n"})
	if err != nil || value != "secret-value" {
		t.Fatalf("value=%q err=%v", value, err)
	}
}
