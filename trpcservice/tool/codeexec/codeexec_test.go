package codeexec

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestSandboxRegistrationPinsDigestAndSecuresWorkspaceRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sandbox")
	config := DefaultConfig(root)
	digest, err := BindingDigest("code_exec", 1, config)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := NewSandboxRegistration("tenant-a", "code_exec", 1, digest, config)
	if err != nil {
		t.Fatal(err)
	}
	if registration.ContentDigest != digest || registration.TenantID != "tenant-a" {
		t.Fatalf("registration=%+v", registration)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("workspace root mode=%#o, want 0700", info.Mode().Perm())
	}
	if _, err := NewSandboxRegistration("tenant-a", "code_exec", 1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", config); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("digest mismatch error=%v", err)
	}
}

func TestPrepareWorkspaceRootRejectsSymlink(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := prepareWorkspaceRoot(link); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("symlink root error=%v", err)
	}
}
