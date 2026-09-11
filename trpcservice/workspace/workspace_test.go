package workspace

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	m, err := NewManager(config.WorkspaceConfig{Root: t.TempDir(), Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDirLayoutAndPermissions(t *testing.T) {
	m := newTestManager(t)
	dir, err := m.Dir("acme", 7)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(m.Root(), "acme", "7")
	if dir != want {
		t.Fatalf("dir = %q, want %q", dir, want)
	}
	for _, p := range []string{filepath.Join(m.Root(), "acme"), dir} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Fatalf("%s perm = %o, want 700", p, perm)
		}
	}
	if again, err := m.Dir("acme", 7); err != nil || again != dir {
		t.Fatalf("second Dir = (%q, %v), want the same directory", again, err)
	}
}

func TestDirRejectsBadNames(t *testing.T) {
	m := newTestManager(t)
	for _, id := range []string{"../etc", "a/b", "", "ACME", "a b"} {
		if _, err := m.Dir(id, 1); err == nil {
			t.Fatalf("tenant %q must be rejected", id)
		}
	}
	for _, pk := range []int64{0, -1} {
		if _, err := m.Dir("acme", pk); err == nil {
			t.Fatalf("session pk %d must be rejected", pk)
		}
	}
}

func TestDirRejectsSymlink(t *testing.T) {
	m := newTestManager(t)
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(m.Root(), "acme")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := m.Dir("acme", 1); err == nil {
		t.Fatal("a symlinked path component must be refused")
	}
}

// TestExecutorRunsInTheSessionDirectory is the contract that makes the
// directory the workspace: the executed command's CWD is the session
// directory, so files it writes land there and are exactly what a later
// artifact step can collect.
func TestExecutorRunsInTheSessionDirectory(t *testing.T) {
	m := newTestManager(t)
	exec, err := m.Executor("acme", 7)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.ExecuteCode(context.Background(), codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{
			Language: "bash",
			Code:     "echo hello-from-workspace > artifact.txt\ncat artifact.txt",
		}},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(res.Output, "hello-from-workspace") {
		t.Fatalf("output = %q, want the artifact's content", res.Output)
	}
	dir, err := m.Dir("acme", 7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "artifact.txt")); err != nil {
		t.Fatalf("the artifact must land in the session directory: %v", err)
	}
}

func TestGCRemovesOnlyStaleSessionDirs(t *testing.T) {
	m := newTestManager(t)
	stale, err := m.Dir("acme", 1)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := m.Dir("acme", 2)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	n, err := m.GC(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("removed = %d, want 1", n)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("the stale directory should be gone")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("the fresh directory must stay: %v", err)
	}
	if _, err := os.Stat(filepath.Join(m.Root(), "acme")); err != nil {
		t.Fatalf("the tenant directory itself must stay: %v", err)
	}
}

func TestNewManagerRejectsBadConfig(t *testing.T) {
	if _, err := NewManager(config.WorkspaceConfig{}); err == nil {
		t.Fatal("an empty root must be refused")
	}
	if _, err := NewManager(config.WorkspaceConfig{Root: t.TempDir(), Mode: "strict"}); err == nil {
		t.Fatal("an unimplemented mode must be refused")
	}
	if _, err := NewManager(config.WorkspaceConfig{Root: filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Fatal("a missing root must be refused")
	}
}
