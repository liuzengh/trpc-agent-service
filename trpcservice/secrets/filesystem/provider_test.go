package filesystem_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
)

func TestProviderResolvesOnlyExactScopedVersion(t *testing.T) {
	root := t.TempDir()
	provider, err := secretfs.New(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	scope, ref := fixture()
	name, err := secretfs.StableFilename(scope, ref)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(name, scope.TenantID) || strings.Contains(name, ref.Ref) || strings.ContainsAny(name, `/\\`) {
		t.Fatalf("unsafe filename=%q", name)
	}
	if err := os.WriteFile(filepath.Join(root, name), []byte("scoped-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	value, err := provider.Resolve(context.Background(), scope, ref)
	if err != nil || value.Version != ref.Version || string(value.Bytes) != "scoped-secret" {
		t.Fatalf("version=%d bytes_len=%d err=%v", value.Version, len(value.Bytes), err)
	}
	value.Bytes[0] = 'X'
	again, err := provider.Resolve(context.Background(), scope, ref)
	if err != nil || string(again.Bytes) != "scoped-secret" {
		t.Fatalf("bytes_len=%d err=%v", len(again.Bytes), err)
	}
	crossTenant := scope
	crossTenant.TenantID = "tenant-b"
	if _, err := provider.Resolve(context.Background(), crossTenant, ref); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("cross-tenant err=%v", err)
	}
	wrongPurpose := scope
	wrongPurpose.Purpose = secrets.PurposeModelCall
	if _, err := provider.Resolve(context.Background(), wrongPurpose, ref); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("wrong-purpose err=%v", err)
	}
	wrongVersion := ref
	wrongVersion.Version++
	if _, err := provider.Resolve(context.Background(), scope, wrongVersion); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("wrong-version err=%v", err)
	}
}

func TestProviderFailsClosedOnSameVersionDriftAndUnsafePermissions(t *testing.T) {
	root := t.TempDir()
	provider, err := secretfs.New(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	scope, ref := fixture()
	name, _ := secretfs.StableFilename(scope, ref)
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, []byte("version-one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Resolve(context.Background(), scope, ref); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("changed-version-one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Resolve(context.Background(), scope, ref); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("drift err=%v", err)
	}
	otherScope := scope
	otherScope.ResourceVersion++
	otherName, _ := secretfs.StableFilename(otherScope, ref)
	otherPath := filepath.Join(root, otherName)
	if err := os.WriteFile(otherPath, []byte("unsafe"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Resolve(context.Background(), otherScope, ref); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("permissions err=%v", err)
	}
}

func TestProviderConcurrentResolveAndTraversalLikeRefRemainScoped(t *testing.T) {
	root := t.TempDir()
	provider, err := secretfs.New(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	scope, ref := fixture()
	ref.Ref = "../../outside"
	name, err := secretfs.StableFilename(scope, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			value, err := provider.Resolve(context.Background(), scope, ref)
			if err != nil || string(value.Bytes) != "inside" {
				t.Errorf("bytes_len=%d err=%v", len(value.Bytes), err)
			}
		}()
	}
	wait.Wait()
}

func TestProviderProbeRootChecksPrivateMount(t *testing.T) {
	root := t.TempDir()
	provider, err := secretfs.New(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.ProbeRoot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o775); err != nil {
		t.Fatal(err)
	}
	if err := provider.ProbeRoot(context.Background()); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("probe err=%v", err)
	}
}

func fixture() (secrets.Scope, secrets.SecretRef) {
	return secrets.Scope{TenantID: "tenant-a", Subject: "tenant-a", Purpose: secrets.PurposeBackendConnect,
		ResourceID: "http-dlp", ResourceVersion: 4}, secrets.SecretRef{Ref: "secret://dlp/service", Version: 7}
}
