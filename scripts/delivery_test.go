package scripts

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func deliveryFixture(t *testing.T, name string) string {
	t.Helper()
	root := t.TempDir()
	raw, err := os.ReadFile(filepath.Join("..", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), raw, 0700); err != nil {
		t.Fatal(err)
	}
	deliveryCommand(t, root, "git", "init", "-q")
	deliveryCommand(t, root, "git", "config", "user.email", "fixture@example.invalid")
	deliveryCommand(t, root, "git", "config", "user.name", "Fixture")
	return root
}

func deliveryCommand(t *testing.T, root, command string, args ...string) string {
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

func deliveryMustFail(t *testing.T, root, script string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", strings.Fields(script)...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("unsafe command accepted: %s", out)
	}
}

func writeDeliveryFixture(t *testing.T, root, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, path), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestSourcePackageOnlyCommittedPublicFiles(t *testing.T) {
	root := deliveryFixture(t, "build.sh")
	writeDeliveryFixture(t, root, ".gitignore", ".env\ndata/*\n!data/README.md\nbin/\ndist/\n")
	writeDeliveryFixture(t, root, ".env", "PRIVATE_CANARY")
	writeDeliveryFixture(t, root, "data/private.dump", "PRIVATE_CANARY")
	writeDeliveryFixture(t, root, "data/README.md", "public runtime instructions")
	writeDeliveryFixture(t, root, ".env.example", "PROVIDER=mock")
	deliveryCommand(t, root, "git", "add", ".")
	deliveryCommand(t, root, "git", "commit", "-qm", "fixture")
	deliveryCommand(t, root, "bash", "build.sh", "--package")
	archives, _ := filepath.Glob(filepath.Join(root, "dist", "*.tar.gz"))
	if len(archives) != 1 {
		t.Fatal("missing source package")
	}
	file, err := os.Open(archives[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(file)
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(gz)
	reader := tar.NewReader(gz)
	names := map[string]bool{}
	for {
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names[h.Name] = true
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "PRIVATE_CANARY") {
			t.Fatal("private data exported")
		}
	}
	if !names["trpc-agent-service/.env.example"] || names["trpc-agent-service/.env"] || names["trpc-agent-service/data/private.dump"] {
		t.Fatal("wrong archive members")
	}
	deliveryCommand(t, filepath.Join(root, "dist"), "sha256sum", "-c", filepath.Base(archives[0])+".sha256")
	deliveryMustFail(t, root, "build.sh --package") // no overwrite
	writeDeliveryFixture(t, root, "new-source.go", "package example")
	deliveryMustFail(t, root, "build.sh --package") // no silent omission of untracked source
	deliveryCommand(t, root, "git", "add", "new-source.go")
	deliveryMustFail(t, root, "build.sh --package") // no omission of staged source
}

func TestSourcePackageRejectsTrackedPrivateFileAndSymlink(t *testing.T) {
	for _, mode := range []string{"private", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			root := deliveryFixture(t, "build.sh")
			if mode == "private" {
				writeDeliveryFixture(t, root, ".env", "PRIVATE_CANARY")
			} else {
				if err := os.Symlink(t.TempDir(), filepath.Join(root, "external")); err != nil {
					t.Fatal(err)
				}
			}
			deliveryCommand(t, root, "git", "add", ".")
			deliveryCommand(t, root, "git", "commit", "-qm", "fixture")
			deliveryMustFail(t, root, "build.sh --package")
		})
	}
}

func TestCleanPreviewAndRecoverableApply(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock required")
	}
	root := deliveryFixture(t, "clean.sh")
	writeDeliveryFixture(t, root, ".gitignore", "bin/\ncoverage.out\ndata/\n.env\n")
	writeDeliveryFixture(t, root, "bin/trpc-service", "generated binary fixture")
	writeDeliveryFixture(t, root, "bin/unknown", "KEEP")
	writeDeliveryFixture(t, root, "coverage.out", "coverage fixture")
	writeDeliveryFixture(t, root, ".env", "PRIVATE_CANARY")
	deliveryCommand(t, root, "bash", "clean.sh")
	if _, err := os.Stat(filepath.Join(root, "bin/trpc-service")); err != nil {
		t.Fatal("preview modified file")
	}
	writeDeliveryFixture(t, root, "data/trpc-service.pid", "12345")
	deliveryMustFail(t, root, "clean.sh --apply")
	if err := os.Remove(filepath.Join(root, "data/trpc-service.pid")); err != nil {
		t.Fatal(err)
	}
	deliveryCommand(t, root, "bash", "clean.sh", "--apply")
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
			root := deliveryFixture(t, "clean.sh")
			if mode == "symlink" {
				if err := os.Symlink(t.TempDir(), filepath.Join(root, "bin")); err != nil {
					t.Fatal(err)
				}
			} else {
				writeDeliveryFixture(t, root, "coverage.out", "user-owned tracked output")
				deliveryCommand(t, root, "git", "add", "coverage.out")
			}
			deliveryMustFail(t, root, "clean.sh --apply")
		})
	}
}
