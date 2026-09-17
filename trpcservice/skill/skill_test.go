package skill

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	upstream "trpc.group/trpc-go/trpc-agent-go/skill"
)

type catalogFunc func(context.Context, string, string, int64) (Package, error)

func (f catalogFunc) Resolve(ctx context.Context, tenantID, skillID string, version int64) (Package, error) {
	return f(ctx, tenantID, skillID, version)
}

func TestResolverLoadsFixedTenantPackage(t *testing.T) {
	staging := t.TempDir()
	packageRoot := filepath.Join(staging, "tenant-a", "sha256", "package")
	skillRoot := filepath.Join(packageRoot, "writer")
	if err := os.MkdirAll(skillRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillRoot, upstream.SkillFile), []byte("---\nname: writer\ndescription: writes\n---\nbody\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	digest, err := DigestRoot(packageRoot)
	if err != nil {
		t.Fatal(err)
	}
	resolver := Resolver{StagingRoot: staging, Catalog: catalogFunc(func(context.Context, string, string, int64) (Package, error) {
		return Package{TenantID: "tenant-a", SkillID: "writer", Version: 3, ContentDigest: digest, RelativePath: "sha256/package"}, nil
	})}
	provider, err := resolver.RepositoryProvider(context.Background(), "tenant-a", []profile.SkillRef{{ID: "writer", Version: 3, ContentDigest: digest}})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := provider.Repository(context.Background(), upstream.SkillScope{AppName: "tenant-a/app"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Get("writer"); err != nil {
		t.Fatal(err)
	}
}

func TestResolverRejectsDigestDriftAndSymlinkEscape(t *testing.T) {
	staging := t.TempDir()
	tenantRoot := filepath.Join(staging, "tenant-a")
	outside := t.TempDir()
	if err := os.MkdirAll(tenantRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(tenantRoot, "escape")); err != nil {
		t.Fatal(err)
	}
	validDigest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	resolver := Resolver{StagingRoot: staging, Catalog: catalogFunc(func(context.Context, string, string, int64) (Package, error) {
		return Package{TenantID: "tenant-a", SkillID: "skill", Version: 1, ContentDigest: validDigest, RelativePath: "escape"}, nil
	})}
	_, err := resolver.RepositoryProvider(context.Background(), "tenant-a", []profile.SkillRef{{ID: "skill", Version: 1, ContentDigest: validDigest}})
	if !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("got %v, want tenant scope error", err)
	}
}
