// Package skill resolves immutable tenant-scoped skill packages into the
// public tRPC-Agent-Go skill repository contract.
package skill

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	upstream "trpc.group/trpc-go/trpc-agent-go/skill"
)

// Package is a published skill artifact. RelativePath is resolved below the
// tenant staging root; callers never provide an arbitrary filesystem root.
type Package struct {
	TenantID      string
	SkillID       string
	Version       int64
	ContentDigest string
	RelativePath  string
}

type Catalog interface {
	Resolve(context.Context, string, string, int64) (Package, error)
}

// LifecycleCatalog is the durable authority used by ingestion. Resolve only
// returns published packages, so callers can never observe an unverified
// staging directory as a Skill.
type LifecycleCatalog interface {
	Catalog
	Stage(context.Context, Package) (Package, error)
	Publish(context.Context, string, string, int64) (Package, error)
}

func ValidatePackage(value Package) (Package, error) {
	if value.TenantID == "" || !safeComponent(value.SkillID) || value.Version < 1 ||
		value.ContentDigest == "" || value.RelativePath == "" || filepath.IsAbs(value.RelativePath) {
		return Package{}, runtime.ErrInvalidEnvelope
	}
	if raw, err := hex.DecodeString(value.ContentDigest); err != nil || len(raw) != sha256.Size {
		return Package{}, runtime.ErrInvalidEnvelope
	}
	clean := filepath.Clean(value.RelativePath)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return Package{}, runtime.ErrTenantScope
	}
	value.RelativePath = filepath.ToSlash(clean)
	return value, nil
}

func safeComponent(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value
}

// Resolver implements agent.SkillResolver without importing the agent
// package. StagingRoot must contain one directory per tenant.
type Resolver struct {
	Catalog     Catalog
	StagingRoot string
}

func (r Resolver) RepositoryProvider(ctx context.Context, tenantID string, refs []profile.SkillRef) (upstream.RepositoryProvider, error) {
	if r.Catalog == nil || tenantID == "" || !filepath.IsAbs(r.StagingRoot) {
		return nil, runtime.ErrCapabilityUnsupported
	}
	tenantRoot, err := secureTenantRoot(r.StagingRoot, tenantID)
	if err != nil {
		return nil, err
	}
	roots := make([]string, 0, len(refs))
	for _, ref := range refs {
		pkg, err := r.Catalog.Resolve(ctx, tenantID, ref.ID, ref.Version)
		if err != nil {
			return nil, err
		}
		if pkg.TenantID != tenantID || pkg.SkillID != ref.ID || pkg.Version != ref.Version ||
			pkg.ContentDigest != ref.ContentDigest {
			return nil, runtime.ErrVersionMismatch
		}
		root, err := resolvePackageRoot(tenantRoot, pkg.RelativePath)
		if err != nil {
			return nil, err
		}
		digest, err := DigestRoot(root)
		if err != nil {
			return nil, fmt.Errorf("digest skill %q: %w", ref.ID, err)
		}
		if digest != ref.ContentDigest {
			return nil, runtime.ErrVersionMismatch
		}
		roots = append(roots, root)
	}
	repository, err := upstream.NewFSRepository(roots...)
	if err != nil {
		return nil, fmt.Errorf("load skill repository: %w", err)
	}
	if err := validatePublishedSkillNames(repository, refs); err != nil {
		return nil, err
	}
	// The repository is already fixed to one trusted tenant and immutable
	// versions. AppName is diagnostic/scoping input, never a tenant credential.
	return upstream.RepositoryProviderFunc(func(_ context.Context, scope upstream.SkillScope) (upstream.Repository, error) {
		if strings.TrimSpace(scope.AppName) == "" {
			return nil, runtime.ErrTenantScope
		}
		return repository, nil
	}), nil
}

// validatePublishedSkillNames ties the SDK's public Skill name (which is
// derived from SKILL.md front matter) to the immutable Revision SkillRef ID.
// Without this check a staged package could mount under one catalog ID but
// advertise a different name to the model, making the Revision allow-list
// ambiguous and defeating the SDK visibility filter installed by Factory.
func validatePublishedSkillNames(repository upstream.Repository, refs []profile.SkillRef) error {
	if repository == nil || len(refs) == 0 {
		return runtime.ErrInvariantViolation
	}
	expected := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if ref.ID == "" || ref.Version < 1 || ref.ContentDigest == "" {
			return runtime.ErrInvalidEnvelope
		}
		if _, exists := expected[ref.ID]; exists {
			return runtime.ErrInvariantViolation
		}
		expected[ref.ID] = struct{}{}
	}
	summaries := repository.Summaries()
	if len(summaries) != len(expected) {
		return runtime.ErrVersionMismatch
	}
	for _, summary := range summaries {
		if summary.Name == "" {
			return runtime.ErrVersionMismatch
		}
		if _, exists := expected[summary.Name]; !exists {
			return runtime.ErrVersionMismatch
		}
		delete(expected, summary.Name)
	}
	if len(expected) != 0 {
		return runtime.ErrVersionMismatch
	}
	return nil
}

func secureTenantRoot(stagingRoot, tenantID string) (string, error) {
	if tenantID == "" || tenantID == "." || tenantID == ".." || filepath.Base(tenantID) != tenantID {
		return "", runtime.ErrTenantScope
	}
	root, err := filepath.EvalSymlinks(filepath.Clean(stagingRoot))
	if err != nil {
		return "", err
	}
	tenantRoot, err := filepath.EvalSymlinks(filepath.Join(root, tenantID))
	if err != nil {
		return "", err
	}
	if !within(root, tenantRoot) {
		return "", runtime.ErrTenantScope
	}
	return tenantRoot, nil
}

func resolvePackageRoot(tenantRoot, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", runtime.ErrTenantScope
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", runtime.ErrTenantScope
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(tenantRoot, clean))
	if err != nil {
		return "", err
	}
	if !within(tenantRoot, resolved) {
		return "", runtime.ErrTenantScope
	}
	return resolved, nil
}

func within(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// DigestRoot computes the canonical digest used by published skill packages.
// Symlinks and non-regular files are rejected so the checked bytes are exactly
// the bytes later visible to FSRepository.
func DigestRoot(root string) (string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return runtime.ErrTenantScope
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return runtime.ErrTenantScope
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, path := range paths {
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return "", err
		}
		_, _ = io.WriteString(hash, filepath.ToSlash(relative))
		_, _ = hash.Write([]byte{0})
		file, err := os.Open(path)
		if err != nil {
			return "", err
		}
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil {
			return "", copyErr
		}
		if closeErr != nil {
			return "", closeErr
		}
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
