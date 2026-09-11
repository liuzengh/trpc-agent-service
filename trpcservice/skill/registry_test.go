package skill

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sample"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"catalog.json": `[{"name":"sample","version":"1","directory":"sample"}]`, "sample/SKILL.md": "---\nname: sample\ndescription: Fixture skill\n---\nRead instructions.", "sample/run.sh": "echo safe"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
func TestSkillGrantDigestAndImmutableSnapshot(t *testing.T) {
	root := fixture(t)
	r, err := Load(root, `[{"tenant_id":"a","name":"sample","version":"1"}]`)
	if err != nil {
		t.Fatal(err)
	}
	list := r.List("a")
	if len(list) != 1 || len(r.List("b")) != 0 {
		t.Fatal("tenant catalog leak")
	}
	refs := []Ref{list[0].Ref}
	repo, err := r.RepositoryFor("a", refs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.RepositoryFor("b", refs); err == nil {
		t.Fatal("unauthorized skill allowed")
	}
	if _, err = repo.Path("sample"); err == nil {
		t.Fatal("host path exposed")
	}
	if err = os.WriteFile(filepath.Join(root, "sample/run.sh"), []byte("echo changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(repo.(*snapshot).selected["sample"].script), "safe") {
		t.Fatal("snapshot mutable")
	}
	updated, err := Load(root, `[{"tenant_id":"a","name":"sample","version":"1"}]`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = updated.RepositoryFor("a", refs); err == nil {
		t.Fatal("changed code reused approved digest")
	}
	var empty *Registry
	if _, err = empty.Validate("a", json.RawMessage(`{}`), []string{"skill_run"}); err == nil {
		t.Fatal("execution without declared skill accepted")
	}
}
func TestSkillRejectsSymlinkAndLooseReference(t *testing.T) {
	root := fixture(t)
	path := filepath.Join(root, "sample/run.sh")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc/passwd", path); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(root, "[]"); err == nil {
		t.Fatal("symlink script accepted")
	}
	for _, raw := range []string{`{"skills":[{"name":"sample","version":"1"}]}`, `{"skills":[{"name":"../secret","version":"1","checksum":"x"}]}`} {
		if _, err := ParseRefs(json.RawMessage(raw)); err == nil {
			t.Fatal("unpinned reference accepted")
		}
	}
}
