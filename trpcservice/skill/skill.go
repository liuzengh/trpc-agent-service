// Package skill stores and executes tenant SKILL.md workflows.
//
// Parsing and mounting belong to the framework (skill.FSRepository and
// llmagent.WithSkills); this package owns the platform half: one directory
// per tenant under a deployment-owned root, a strict tenant-id whitelist so
// a tenant id can never name a path, a symlink escape check, and a
// repository cache so per-claim assembly does not rescan the disk.
package skill

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	frameworkskill "trpc.group/trpc-go/trpc-agent-go/skill"
)

// Name is the revision tool-pin name that turns skills on for an app. The pin
// needs a binding row like any other go tool — that row is the governance
// record of what the revision attached — but the tools themselves (skill_load
// / skill_list_docs / skill_select_docs) are mounted by the framework when
// the runner is assembled; see the frameworkHosted set in trpcservice/tool.
const Name = "skill"

// tenantIDPattern is the whitelist a tenant id must match before it is used
// as a directory name. Tenant ids come from the control plane, but this
// package must not hand one to the filesystem unchecked: "..", "/" and NUL
// are the classic escapes and none of them matches this pattern.
var tenantIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

var (
	mu    sync.Mutex
	cache = map[string]*frameworkskill.FSRepository{}
)

// For returns the skill repository visible to one tenant. ok=false means the
// tenant has no directory under root and therefore no skills — a valid
// deployment, not an error. An error is reserved for a directory that exists
// but cannot be used (one that escapes the root via a symlink, for example).
func For(root, tenantID string) (*frameworkskill.FSRepository, bool, error) {
	if strings.TrimSpace(root) == "" {
		return nil, false, errors.New("skill: no skills root configured")
	}
	if !tenantIDPattern.MatchString(tenantID) {
		return nil, false, fmt.Errorf("skill: tenant id %q is not a usable directory name", tenantID)
	}
	key := root + "\x00" + tenantID
	mu.Lock()
	defer mu.Unlock()
	if repo, ok := cache[key]; ok {
		return repo, true, nil
	}

	realRoot, err := resolveRoot(root)
	if err != nil {
		return nil, false, err
	}
	dir := filepath.Join(realRoot, tenantID)
	info, err := os.Lstat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("skill: tenant directory for %q: %w", tenantID, err)
	}
	// Symlinks are refused outright rather than resolved and range-checked:
	// a tenant library is expected to be a real directory under the root,
	// and "resolve then verify" has one more step to get wrong. Lstat is
	// deliberate — Stat would already have followed the link.
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, false, fmt.Errorf("skill: tenant directory %q is a symlink; refusing to follow it", dir)
	}
	if !info.IsDir() {
		return nil, false, fmt.Errorf("skill: %q is not a directory", dir)
	}
	// No symlink anywhere below either. The framework's scanner follows a
	// SKILL.md (or a skill directory) that is a link, which would let one
	// tenant point its own library at another tenant's files — this walk is
	// the platform's only chance to say no.
	if err := rejectSymlinks(dir); err != nil {
		return nil, false, err
	}
	repo, err := frameworkskill.NewFSRepository(dir)
	if err != nil {
		return nil, false, fmt.Errorf("skill: open tenant library: %w", err)
	}
	cache[key] = repo
	return repo, true, nil
}

// rejectSymlinks walks a tenant library and fails on the first symlink (a
// directory link or a linked SKILL.md). WalkDir does not follow links, so
// the entry type is observable without resolving anything.
func rejectSymlinks(dir string) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("skill: scan tenant library: %w", err)
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("skill: %q is a symlink; tenant libraries must be plain files", p)
		}
		return nil
	})
}

// Refresh drops the repository cache so the next For rescans the tenants.
// Assemblies read skills at runner build time, so a refresh lands on the next
// message; there is nothing to invalidate mid-run.
func Refresh() {
	mu.Lock()
	defer mu.Unlock()
	cache = map[string]*frameworkskill.FSRepository{}
}

// resolveRoot resolves the configured root to its real path. Existence is
// checked by config validation at boot, so a missing root here is an
// operator error surfacing on the path that needed it.
func resolveRoot(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("skill: resolve root %q: %w", root, err)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("skill: resolve root %q: %w", root, err)
	}
	return real, nil
}
