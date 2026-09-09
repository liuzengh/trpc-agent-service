package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestPackageManifestDescribesAllEmbeddedMigrations(t *testing.T) {
	// The manifest is intentionally kept at the repository root rather than
	// embedded in the migration binary. Check its claimed range against the
	// authoritative embedded list so release documentation cannot silently lag
	// schema changes that alter durable pipeline semantics.
	data, err := os.ReadFile("../PACKAGE_MANIFEST.md")
	if err != nil {
		t.Fatal(err)
	}
	versions, err := embeddedVersions()
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) == 0 {
		t.Fatal("no embedded migrations")
	}
	latest := strings.SplitN(versions[len(versions)-1], "_", 2)[0]
	want := "migrations/001.." + latest
	if !strings.Contains(string(data), want) {
		t.Fatalf("PACKAGE_MANIFEST.md must describe embedded migration range %q", want)
	}
}
