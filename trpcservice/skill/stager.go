package skill

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	upstream "trpc.group/trpc-go/trpc-agent-go/skill"
)

// ArtifactStager materializes one security-scanned text Artifact as an
// immutable Skill package. It intentionally does not download URLs or accept
// executable archives; richer multi-file packages need an explicit future
// intake format with the same scanner/DLP guarantees.
type ArtifactStager struct {
	Artifacts   artifact.Store
	StagingRoot string
}

type StageRequest struct {
	TenantID, SkillID string
	Version           int64
	ArtifactID        string
}

func (s ArtifactStager) Stage(ctx context.Context, in StageRequest) (Package, error) {
	if s.Artifacts == nil || !filepath.IsAbs(s.StagingRoot) || in.TenantID == "" || !safeComponent(in.SkillID) || in.Version < 1 || in.ArtifactID == "" {
		return Package{}, runtime.ErrInvalidEnvelope
	}
	artifactValue, err := s.Artifacts.GetArtifact(ctx, in.TenantID, in.ArtifactID)
	if err != nil {
		return Package{}, err
	}
	if artifactValue.TenantID != in.TenantID || artifactValue.Kind != "file" || artifactValue.MediaType != "text/plain" ||
		len(artifactValue.Content) == 0 || len(artifactValue.Content) > 1<<20 || !utf8.Valid(artifactValue.Content) {
		return Package{}, runtime.ErrCapabilityUnsupported
	}
	root, err := filepath.EvalSymlinks(filepath.Clean(s.StagingRoot))
	if err != nil {
		return Package{}, err
	}
	tenantRoot := filepath.Join(root, in.TenantID)
	if err := os.MkdirAll(tenantRoot, 0o750); err != nil {
		return Package{}, err
	}
	tenantRoot, err = filepath.EvalSymlinks(tenantRoot)
	if err != nil || !within(root, tenantRoot) {
		return Package{}, runtime.ErrTenantScope
	}
	relative := filepath.ToSlash(filepath.Join("skills", in.SkillID, fmt.Sprintf("v%d-%s", in.Version, artifactValue.ContentDigest[:16])))
	destination, err := resolvePackageRootForWrite(tenantRoot, relative)
	if err != nil {
		return Package{}, err
	}
	if info, err := os.Stat(destination); err == nil {
		if !info.IsDir() {
			return Package{}, runtime.ErrTenantScope
		}
		// A repeated version must name exactly the same scanned source. Returning
		// an older directory here would otherwise silently turn a version
		// collision into a successful publish.
		existing, readErr := os.ReadFile(filepath.Join(destination, in.SkillID, upstream.SkillFile))
		if readErr != nil || string(existing) != string(artifactValue.Content) {
			return Package{}, runtime.ErrIdempotencyCollision
		}
		digest, err := DigestRoot(destination)
		if err != nil {
			return Package{}, err
		}
		return Package{TenantID: in.TenantID, SkillID: in.SkillID, Version: in.Version, ContentDigest: digest, RelativePath: relative}, nil
	} else if !os.IsNotExist(err) {
		return Package{}, err
	}
	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return Package{}, err
	}
	temporary, err := os.MkdirTemp(parent, ".skill-stage-")
	if err != nil {
		return Package{}, err
	}
	defer os.RemoveAll(temporary)
	skillRoot := filepath.Join(temporary, in.SkillID)
	if err := os.MkdirAll(skillRoot, 0o750); err != nil {
		return Package{}, err
	}
	if err := os.WriteFile(filepath.Join(skillRoot, upstream.SkillFile), append([]byte(nil), artifactValue.Content...), 0o640); err != nil {
		return Package{}, err
	}
	digest, err := DigestRoot(temporary)
	if err != nil {
		return Package{}, err
	}
	if err := os.Rename(temporary, destination); err != nil {
		return Package{}, err
	}
	return Package{TenantID: in.TenantID, SkillID: in.SkillID, Version: in.Version, ContentDigest: digest, RelativePath: relative}, nil
}

func resolvePackageRootForWrite(tenantRoot, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", runtime.ErrTenantScope
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", runtime.ErrTenantScope
	}
	destination := filepath.Join(tenantRoot, clean)
	if !within(tenantRoot, destination) {
		return "", runtime.ErrTenantScope
	}
	return destination, nil
}

// Ingestor ties trusted local staging to the durable catalog. The directory is
// rehashed after stage and before publish, preventing a mutable staging path
// from being published by reference.
type Ingestor struct {
	Catalog LifecycleCatalog
	Stager  ArtifactStager
}

func (i Ingestor) Ingest(ctx context.Context, request StageRequest) (Package, error) {
	if i.Catalog == nil {
		return Package{}, runtime.ErrCapabilityUnsupported
	}
	pkg, err := i.Stager.Stage(ctx, request)
	if err != nil {
		return Package{}, err
	}
	pkg, err = ValidatePackage(pkg)
	if err != nil {
		return Package{}, err
	}
	if _, err = i.Catalog.Stage(ctx, pkg); err != nil {
		return Package{}, err
	}
	tenantRoot, err := secureTenantRoot(i.Stager.StagingRoot, pkg.TenantID)
	if err != nil {
		return Package{}, err
	}
	root, err := resolvePackageRoot(tenantRoot, pkg.RelativePath)
	if err != nil {
		return Package{}, err
	}
	digest, err := DigestRoot(root)
	if err != nil {
		return Package{}, err
	}
	if digest != pkg.ContentDigest {
		return Package{}, runtime.ErrVersionMismatch
	}
	return i.Catalog.Publish(ctx, pkg.TenantID, pkg.SkillID, pkg.Version)
}
