package skill

import (
	"os"
	"path/filepath"
	"testing"
)

// writeSkill lays out one SKILL.md at <root>/<tenant>/<name>/SKILL.md.
func writeSkill(t *testing.T, root, tenant, name, description string) {
	t.Helper()
	dir := filepath.Join(root, tenant, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "---\nname: " + name + "\ndescription: " + description + "\n---\n\nthe body\n"
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func summarize(t *testing.T, root, tenant string) map[string]string {
	t.Helper()
	repo, ok, err := For(root, tenant)
	if err != nil || !ok {
		t.Fatalf("For(%q) = (_, %v, %v)", tenant, ok, err)
	}
	out := map[string]string{}
	for _, s := range repo.Summaries() {
		out[s.Name] = s.Description
	}
	return out
}

func TestForIsolatesTenants(t *testing.T) {
	Refresh()
	root := t.TempDir()
	writeSkill(t, root, "acme", "alpha", "acme alpha")
	writeSkill(t, root, "beta", "alpha", "beta alpha")
	writeSkill(t, root, "beta", "only-beta", "beta only")

	acme := summarize(t, root, "acme")
	if len(acme) != 1 || acme["alpha"] != "acme alpha" {
		t.Fatalf("acme sees %v, want only its own alpha", acme)
	}
	beta := summarize(t, root, "beta")
	if len(beta) != 2 || beta["only-beta"] != "beta only" || beta["alpha"] != "beta alpha" {
		t.Fatalf("beta sees %v, want its own two skills", beta)
	}
}

func TestForRejectsPathInjection(t *testing.T) {
	Refresh()
	for _, id := range []string{"../etc", "a/b", ".", "..", "ACME", "", "foo bar", "a\x00b"} {
		if _, _, err := For(t.TempDir(), id); err == nil {
			t.Fatalf("tenant id %q must be rejected", id)
		}
	}
}

func TestForMissingTenantIsNotAnError(t *testing.T) {
	Refresh()
	_, ok, err := For(t.TempDir(), "acme")
	if err != nil || ok {
		t.Fatalf("missing tenant directory = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
}

func TestForRejectsSymlinks(t *testing.T) {
	Refresh()
	root := t.TempDir()
	outside := t.TempDir()
	writeSkill(t, outside, "x", "stolen", "outside the root")

	// A symlinked tenant directory must not be followed.
	if err := os.Symlink(filepath.Join(outside, "x"), filepath.Join(root, "acme")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := For(root, "acme"); err == nil {
		t.Fatal("a symlinked tenant directory must be refused")
	}

	// A real tenant directory whose SKILL.md is a link is refused too: the
	// framework's scanner would otherwise read another tree through it.
	Refresh()
	real := filepath.Join(root, "real")
	if err := os.MkdirAll(filepath.Join(real, "linked"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "x", "stolen", "SKILL.md"),
		filepath.Join(real, "linked", "SKILL.md")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := For(root, "real"); err == nil {
		t.Fatal("a linked SKILL.md must be refused")
	}
}

func TestMissingTenantAppearsWithoutRefresh(t *testing.T) {
	Refresh()
	root := t.TempDir()
	if _, ok, err := For(root, "acme"); err != nil || ok {
		t.Fatalf("before: ok=%v err=%v, want an empty tenant", ok, err)
	}
	writeSkill(t, root, "acme", "alpha", "appears later")
	// Only successful repositories are cached, so placing a tenant directory
	// is enough to turn its skills on — no Refresh required.
	got := summarize(t, root, "acme")
	if _, ok := got["alpha"]; !ok {
		t.Fatalf("after = %v, want alpha without a Refresh", got)
	}
}

func TestRefreshDropsCachedRepositories(t *testing.T) {
	Refresh()
	root := t.TempDir()
	writeSkill(t, root, "acme", "alpha", "one")

	repo1, ok, err := For(root, "acme")
	if err != nil || !ok {
		t.Fatalf("first For = (ok=%v, err=%v)", ok, err)
	}
	repo2, _, err := For(root, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if repo1 != repo2 {
		t.Fatal("a second lookup must hit the cache, not rescan")
	}
	Refresh()
	repo3, _, err := For(root, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if repo3 == repo1 {
		t.Fatal("Refresh must drop the cached repository")
	}
}
