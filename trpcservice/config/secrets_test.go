package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileSecretResolverOnlyReturnsReferencedValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model-key")
	if err := os.WriteFile(path, []byte("canary-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolver, err := NewFileResolver(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := resolver.Resolve("secret://file/model-key#v1"); err != nil || got != "canary-secret" {
		t.Fatalf("Resolve() = %q, %v", got, err)
	}
	if _, err := resolver.Resolve("secret://file/../model-key"); err == nil {
		t.Fatal("path traversal secret reference was accepted")
	}
	if _, err := resolver.Resolve("secret://vault/path"); err == nil {
		t.Fatal("unconfigured external provider was accepted")
	}
	if strings.Contains(string(mustRead(t, path)), "secret://") {
		t.Fatal("test file unexpectedly contains a reference")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
