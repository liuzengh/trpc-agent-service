package configfile

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const valid = `{"version":"v1","backends":[{"id":"redis","revision":2,"label":"Redis","kind":"redis","roles":["session"],"enabled":true,"tenant_ids":["tenant-a"]}]}`

func TestPinnedCatalog(t *testing.T) {
	p := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(p, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(valid)))
	c, err := Load(p, digest)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := c.List("tenant-a")
	if len(v) != 1 || v[0].ID != "redis" {
		t.Fatal(v)
	}
	if _, err := Load(p, strings.Repeat("0", 64)); err != ErrConfig {
		t.Fatal(err)
	}
	if _, err := Load(p, ""); err != ErrConfig {
		t.Fatal(err)
	}
	c, err = Load("", "")
	if err != nil {
		t.Fatal(err)
	}
	v, _ = c.List("tenant-a")
	if len(v) != 0 {
		t.Fatal(v)
	}
}
func TestStrictDecode(t *testing.T) {
	for _, s := range []string{
		`{"version":"v1","backends":[],"version":"v1"}`,
		`{"Version":"v1","backends":[]}`,
		`{"version":"v1","backends":null}`,
		`{"version":"v1","backends":[],"password":"secret"}`,
		valid + `{}`, strings.Replace(valid, `"enabled":true`, `"enabled":null`, 1),
		strings.Replace(valid, `"id":"redis"`, `"id":"redis","id":"pg"`, 1),
		strings.Replace(valid, `"kind":"redis"`, `"kind":"redis","target":"private"`, 1),
		strings.Repeat("[", 30) + strings.Repeat("]", 30),
	} {
		if _, err := Decode([]byte(s)); err != ErrConfig {
			t.Fatalf("accepted %s: %v", s, err)
		}
	}
}
