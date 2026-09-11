package tests

import (
	"os/exec"
	"path"
	"strings"
	"testing"
)

func TestRepositoryDoesNotTrackPrivateOrGeneratedFiles(t *testing.T) {
	cmd := exec.Command("git", "ls-files", "-z")
	cmd.Dir = ".."
	raw, err := cmd.Output()
	if err != nil {
		t.Skip("repository inventory requires a Git checkout")
	}
	for _, name := range strings.Split(string(raw), "\x00") {
		if name == "" {
			continue
		}
		base := path.Base(name)
		privateEnv := base == ".env" || strings.HasPrefix(base, ".env.") || strings.HasSuffix(base, ".env")
		if base == ".env.example" {
			privateEnv = false
		}
		generated := strings.HasPrefix(name, "bin/") || strings.HasPrefix(name, "dist/") ||
			(strings.HasPrefix(name, "data/") && name != "data/README.md") ||
			strings.Contains("/"+name, "/node_modules/") || strings.HasPrefix(name, "trpcservice/admin/ui/dist/") ||
			strings.HasSuffix(name, ".log") || strings.HasSuffix(name, ".tar.gz") || strings.HasSuffix(name, "-verification.md") ||
			base == "coverage.out" || base == "coverage.html"
		if privateEnv || generated {
			t.Errorf("private/generated file is tracked: %s", name)
		}
	}
}
