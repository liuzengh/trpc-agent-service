package secrets

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileEnvProviderAndFingerprint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.txt")
	if err := os.WriteFile(path, []byte("AppID: app-value\nApp Secret = secret-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_CREDENTIAL_FILE", path)
	values, err := (FileEnvProvider{}).Resolve(context.Background(), "fileenv:TEST_CREDENTIAL_FILE")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != "app-value" || values[1] != "secret-value" {
		t.Fatalf("unexpected values: %#v", values)
	}
	fingerprint := Fingerprint(values)
	if strings.Contains(fingerprint, "secret") || fingerprint != "****alue" {
		t.Fatalf("unsafe fingerprint %q", fingerprint)
	}
}

func TestProviderRejectsMissingReference(t *testing.T) {
	if _, err := (FileEnvProvider{}).Resolve(context.Background(), "invalid"); err == nil {
		t.Fatal("invalid reference should fail")
	}
}

func TestEnvironmentAndProviderErrors(t *testing.T) {
	provider := FileEnvProvider{}
	t.Setenv("TEST_DIRECT_SECRET", "  value  ")
	values, err := provider.Resolve(context.Background(), "env:TEST_DIRECT_SECRET")
	if err != nil || len(values) != 1 || values[0] != "value" {
		t.Fatalf("values=%v err=%v", values, err)
	}
	for _, reference := range []string{"env:MISSING_SECRET", "fileenv:MISSING_FILE_ENV", "kms:NAME"} {
		if _, err := provider.Resolve(context.Background(), reference); err == nil {
			t.Fatalf("reference %q should fail", reference)
		}
	}
	t.Setenv("MISSING_CREDENTIAL_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := provider.Resolve(context.Background(), "fileenv:MISSING_CREDENTIAL_FILE"); err == nil {
		t.Fatal("missing credential file should fail")
	}
	empty := filepath.Join(t.TempDir(), "empty.txt")
	if err := os.WriteFile(empty, []byte("# comment\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EMPTY_CREDENTIAL_FILE", empty)
	if _, err := provider.Resolve(context.Background(), "fileenv:EMPTY_CREDENTIAL_FILE"); err == nil {
		t.Fatal("empty credential file should fail")
	}
}

func TestFingerprintAndLabels(t *testing.T) {
	if Fingerprint(nil) != "****" || Fingerprint([]string{"abc"}) != "****" {
		t.Fatal("short fingerprints must be fully redacted")
	}
	for _, line := range []string{"key:value", "key = value"} {
		key, value, ok := cutLabeled(line)
		if !ok || key != "key" || value != "value" {
			t.Fatalf("line=%q key=%q value=%q ok=%v", line, key, value, ok)
		}
	}
	if _, _, ok := cutLabeled("unlabeled"); ok {
		t.Fatal("unlabeled line should remain positional")
	}
}
