package migrations

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestMigrationChecksumIgnoresCheckoutLineEndingsOnly(t *testing.T) {
	lf := []byte("-- migration\nSELECT 'value';\n")
	crlf := []byte("-- migration\r\nSELECT 'value';\r\n")
	want := fmt.Sprintf("%x", sha256.Sum256(lf))
	for _, script := range [][]byte{lf, crlf} {
		if got := scriptChecksum(script); got != want {
			t.Fatalf("canonical checksum=%s, want=%s", got, want)
		}
		if got := canonicalScript(script); !bytes.Equal(got, lf) {
			t.Fatalf("canonical execution bytes=%q, want=%q", got, lf)
		}
		for _, historical := range [][]byte{lf, crlf} {
			if !matchesScriptChecksum(script, fmt.Sprintf("%x", sha256.Sum256(historical))) {
				t.Fatal("equivalent LF/CRLF checksum was rejected")
			}
		}
		for _, changed := range [][]byte{
			[]byte("-- migration\nSELECT 'other';\n"),
			[]byte("-- migration\nSELECT 'value';"),
			[]byte("-- migration\nSELECT  'value';\n"),
			[]byte("-- migration\r\nSELECT 'value';\n"),
			[]byte("-- migration\rSELECT 'value';\r"),
		} {
			if matchesScriptChecksum(script, fmt.Sprintf("%x", sha256.Sum256(changed))) {
				t.Fatalf("accepted unknown historical content %q", changed)
			}
		}
		if matchesScriptChecksum(script, "") {
			t.Fatal("empty checksum must not authorize rollback")
		}
	}
	withLoneCR := []byte("SELECT 'a\rb';\r\n")
	if got, want := canonicalScript(withLoneCR), []byte("SELECT 'a\rb';\n"); !bytes.Equal(got, want) {
		t.Fatalf("canonicalization changed SQL content: %q", got)
	}
}
