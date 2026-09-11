package secrets

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveEnvAndFile(t *testing.T) {
	t.Setenv("TEST_SECRET_A", "from-env")
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	if err := os.WriteFile(path, []byte("from-file\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	r := NewResolver(AllowedPrefixes{
		EnvVars:  []string{"TEST_SECRET_A"},
		FileRoot: []string{dir},
	})

	got, err := r.Resolve("env:TEST_SECRET_A")
	if err != nil || got != "from-env" {
		t.Fatalf("env resolve = %q, %v", got, err)
	}
	got, err = r.Resolve("file:" + path)
	if err != nil || got != "from-file" {
		t.Fatalf("file resolve = %q, %v (trailing newlines must be trimmed)", got, err)
	}

	empty, err := r.Resolve("")
	if err != nil || empty != "" {
		t.Fatalf("empty reference = %q, %v; want empty, no error", empty, err)
	}
}

func TestResolverRejectsAnythingOffTheAllowlist(t *testing.T) {
	t.Setenv("NOT_ALLOWED", "surprise")
	r := NewResolver(AllowedPrefixes{EnvVars: []string{"ALLOWED_ONE"}})

	// The core point of this package: a reference that is syntactically
	// perfect and would otherwise resolve fine is refused anyway, because
	// the tenant that wrote it is not allowed to name an arbitrary variable.
	if _, err := r.Resolve("env:NOT_ALLOWED"); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("off-allowlist env var resolved anyway: %v", err)
	}
	if _, err := r.Resolve("file:/etc/shadow"); err == nil || !strings.Contains(err.Error(), "allowed root") {
		t.Fatalf("off-allowlist file path resolved anyway: %v", err)
	}
}

func TestResolverRejectsAnUnknownScheme(t *testing.T) {
	r := NewResolver(AllowedPrefixes{})
	_, err := r.Resolve("kms:whatever")
	if !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("unknown scheme: %v, want ErrUnsupportedScheme", err)
	}
	// A missing scheme is not silently treated as a literal secret value:
	// that would let "hunter2" pass as a reference and be handed to an API
	// as a key with no check having happened.
	if _, err := r.Resolve("hunter2"); !errors.Is(err, ErrUnsupportedScheme) {
		t.Fatalf("reference with no scheme: %v, want ErrUnsupportedScheme", err)
	}
}

func TestEmptyAllowlistPermitsNothing(t *testing.T) {
	t.Setenv("ANYTHING", "set")
	r := NewResolver(AllowedPrefixes{})
	if _, err := r.Resolve("env:ANYTHING"); err == nil {
		t.Fatal("an empty allowlist must not behave like an unrestricted one")
	}
}

func TestFileReferenceMustBeAbsolute(t *testing.T) {
	r := NewResolver(AllowedPrefixes{FileRoot: []string{"/"}})
	if _, err := r.Resolve("file:relative/path"); err == nil {
		t.Fatal("a relative file path must be rejected regardless of allowlist")
	}
}
