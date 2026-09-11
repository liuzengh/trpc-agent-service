package tests

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func scriptFixture(t *testing.T, name string) string {
	t.Helper()
	root := t.TempDir()
	raw, err := os.ReadFile(filepath.Join("..", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), raw, 0700); err != nil {
		t.Fatal(err)
	}
	scriptCommand(t, root, "git", "init", "-q")
	scriptCommand(t, root, "git", "config", "user.email", "fixture@example.invalid")
	scriptCommand(t, root, "git", "config", "user.name", "Fixture")
	return root
}

func scriptCommand(t *testing.T, root, command string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = root
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "LC_ALL=C"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("fixture command %s failed: %v: %s", command, err, out)
	}
	return string(out)
}

func scriptMustFail(t *testing.T, root, script string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", strings.Fields(script)...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("unsafe command accepted: %s", out)
	}
}

func writeScriptFixture(t *testing.T, root, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCleanPreviewAndRecoverableApply(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock required")
	}
	root := scriptFixture(t, "scripts/clean.sh")
	writeScriptFixture(t, root, ".gitignore", "bin/\ncoverage.out\ndata/\n.env\n")
	writeScriptFixture(t, root, "bin/trpc-service", "generated binary fixture")
	writeScriptFixture(t, root, "bin/unknown", "KEEP")
	writeScriptFixture(t, root, "coverage.out", "coverage fixture")
	writeScriptFixture(t, root, ".env", "PRIVATE_CANARY")
	scriptCommand(t, root, "bash", "scripts/clean.sh")
	if _, err := os.Stat(filepath.Join(root, "bin/trpc-service")); err != nil {
		t.Fatal("preview modified file")
	}
	writeScriptFixture(t, root, "data/trpc-service.pid", "12345")
	scriptMustFail(t, root, "scripts/clean.sh --apply")
	if err := os.Remove(filepath.Join(root, "data/trpc-service.pid")); err != nil {
		t.Fatal(err)
	}
	scriptCommand(t, root, "bash", "scripts/clean.sh", "--apply")
	archives, _ := filepath.Glob(filepath.Join(root, "data/archive/build-outputs.*"))
	if len(archives) != 1 {
		t.Fatal("missing recovery archive")
	}
	for _, path := range []string{"bin/trpc-service", "coverage.out"} {
		if _, err := os.Stat(filepath.Join(root, path)); !os.IsNotExist(err) {
			t.Fatal("output not moved")
		}
		if _, err := os.Stat(filepath.Join(archives[0], path)); err != nil {
			t.Fatal("output not recoverable")
		}
	}
	for _, path := range []string{"bin/unknown", ".env"} {
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatal("unrelated file lost")
		}
	}
}

func TestCleanRejectsSymlinkAndTrackedOutput(t *testing.T) {
	for _, mode := range []string{"symlink", "tracked"} {
		t.Run(mode, func(t *testing.T) {
			root := scriptFixture(t, "scripts/clean.sh")
			if mode == "symlink" {
				if err := os.Symlink(t.TempDir(), filepath.Join(root, "bin")); err != nil {
					t.Fatal(err)
				}
			} else {
				writeScriptFixture(t, root, "coverage.out", "user-owned tracked output")
				scriptCommand(t, root, "git", "add", "coverage.out")
			}
			scriptMustFail(t, root, "scripts/clean.sh --apply")
		})
	}
}
