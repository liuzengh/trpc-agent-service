package secret

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestResolveLocalEnvironmentAndFile(t *testing.T) {
	t.Setenv("TEST_LOCAL_SECRET", "env-value")
	value, err := ResolveLocal(tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "TEST_LOCAL_SECRET"})
	if err != nil || value != "env-value" {
		t.Fatalf("environment value=%q err=%v", value, err)
	}
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("file-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err = ResolveLocal(tenant.SecretRef{Provider: tenant.SecretProviderFile, Key: path})
	if err != nil || value != "file-value" {
		t.Fatalf("file value=%q err=%v", value, err)
	}
}

func TestResolverSupportsRegisteredVaultProviderWithoutLeakingMetadata(t *testing.T) {
	resolver := NewResolver()
	if err := resolver.Register(tenant.SecretProviderVault, ProviderFunc(func(_ context.Context, key string) (string, error) {
		if key == "tenant/model" {
			return "resolved-value", nil
		}
		return "", errors.New("backend mentioned sensitive lookup metadata")
	})); err != nil {
		t.Fatal(err)
	}
	value, err := resolver.Resolve(context.Background(), tenant.SecretRef{Provider: tenant.SecretProviderVault, Key: "tenant/model"})
	if err != nil || value != "resolved-value" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	const missing = "tenant/missing-sensitive"
	_, err = resolver.Resolve(context.Background(), tenant.SecretRef{Provider: tenant.SecretProviderVault, Key: missing})
	if err == nil || strings.Contains(err.Error(), missing) || strings.Contains(err.Error(), "sensitive lookup metadata") {
		t.Fatalf("resolver leaked provider error: %v", err)
	}
}

func TestResolveLocalErrorDoesNotRevealReferenceKey(t *testing.T) {
	const lookupKey = "SENSITIVE_LOOKUP_METADATA"
	_, err := ResolveLocal(tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: lookupKey})
	if err == nil || strings.Contains(err.Error(), lookupKey) {
		t.Fatalf("error leaked lookup metadata: %v", err)
	}
}

func TestScopedResolverRequiresServerOwnedNamespace(t *testing.T) {
	resolver := NewScopedResolver(func(_ context.Context, ref tenant.SecretRef) (string, error) {
		return ref.Key, nil
	})
	ref := tenant.SecretRef{Provider: tenant.SecretProviderVault, Key: "tenant-a/app/model-key"}
	if value, err := resolver.Resolve(context.Background(), "tenant-a", "app", ref); err != nil || value != ref.Key {
		t.Fatalf("scoped resolve value=%q err=%v", value, err)
	}
	if _, err := resolver.Resolve(context.Background(), "tenant-b", "app", ref); err == nil {
		t.Fatal("cross-tenant secret namespace succeeded")
	}
	for _, key := range []string{"tenant-a/app", "tenant-a/app/../other", "tenant-a/app/path//key", `tenant-a/app/path\key`} {
		if _, err := resolver.Resolve(context.Background(), "tenant-a", "app", tenant.SecretRef{Provider: tenant.SecretProviderVault, Key: key}); err == nil {
			t.Fatalf("invalid namespace %q succeeded", key)
		}
	}
}
