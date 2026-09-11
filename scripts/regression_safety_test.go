package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegressionDoesNotInheritLiveIntegrationsOrRestartServices(t *testing.T) {
	data, err := os.ReadFile("regression.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(data)
	for _, forbidden := range []string{"source .env", "docker compose", "start-real.sh", "stop.sh", "check-model.sh", "rm -rf"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("regression must not operate live services: %s", forbidden)
		}
	}
	for _, required := range []string{"unset TEST_POSTGRES_URL TEST_REDIS_URL TEST_S3_ENDPOINT TEST_QDRANT_HOST TEST_QDRANT_PORT", "unset TEST_TRACE_OTLP_ENDPOINT TEST_PERMISSIONS_DOCKER TEST_RECOVERY_DOCKER", "--pull=never --network none", "go test -race ./..."} {
		if !strings.Contains(script, required) {
			t.Fatalf("missing regression safety boundary: %s", required)
		}
	}
	if out, err := exec.Command("bash", "-n", "regression.sh").CombinedOutput(); err != nil {
		t.Fatalf("syntax: %s", out)
	}
}

func TestRegressionSupportsArchivesAndKeepsWorktreeChecks(t *testing.T) {
	raw, err := os.ReadFile("regression.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"archive", "nested-archive", "worktree"} {
		t.Run(mode, func(t *testing.T) {
			outer := t.TempDir()
			root := outer
			if mode == "nested-archive" {
				root = filepath.Join(outer, "source")
			}
			if err := os.MkdirAll(filepath.Join(root, "scripts"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "scripts", "regression.sh"), raw, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "lint.sh"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			bin := t.TempDir()
			for _, name := range []string{"go", "npm"} {
				if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "archive" {
				deliveryCommand(t, outer, "git", "init", "-q")
				writeDeliveryFixture(t, outer, "tracked.txt", "original\n")
				deliveryCommand(t, outer, "git", "add", "tracked.txt")
				deliveryCommand(t, outer, "git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
				writeDeliveryFixture(t, outer, "tracked.txt", "invalid trailing whitespace \n")
			}
			cmd := exec.Command("bash", "scripts/regression.sh")
			cmd.Dir = root
			cmd.Env = []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "TRPC_AGENT_VERIFY_ISOLATED=0", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1"}
			out, err := cmd.CombinedOutput()
			if mode == "worktree" {
				if err == nil {
					t.Fatal("worktree whitespace check was skipped")
				}
			} else if err != nil || !strings.Contains(string(out), "regression passed") {
				t.Fatalf("archive regression failed: %v: %s", err, out)
			}
		})
	}
}
