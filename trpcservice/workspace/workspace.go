// Package workspace manages local and container sandbox working directories.
//
// The capability is session-scoped: one directory per (tenant, session_pk)
// under a deployment-owned root, handed to the runner as the code executor's
// working directory when a revision pins code_exec. The executor is the
// framework's local one — cross-platform and dependency-free — so the
// boundary this package enforces is the filesystem layout (0700 directories,
// tenant ids that cannot name a path, symlinks refused), not process
// isolation: code runs with the platform process's own privileges. A
// production deployment must add container-level isolation (read-only root,
// a dedicated volume) rather than relying on this package alone.
package workspace

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
	"trpc.group/trpc-go/trpc-agent-go/codeexecutor/local"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// Name is the revision tool-pin name that turns code execution on for an
// app. The tools behind it (workspace_exec and its session helpers) are
// mounted by the framework once the runner carries a code executor, so the
// pin needs a binding row but no extras entry; see the frameworkHosted set in
// trpcservice/tool.
const Name = "code_exec"

// Defaults mirror the config parser's; a Manager built from a hand-made
// struct still gets a usable bound.
const (
	defaultTimeout = 30 * time.Second
	defaultMaxAge  = 24 * time.Hour
)

// tenantIDPattern is the whitelist a tenant id must match before it is used
// as a directory name; it is the same rule the skill library uses, for the
// same reason.
var tenantIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Manager owns the workspace root and the per-session directories under it.
type Manager struct {
	root    string
	timeout time.Duration
	maxAge  time.Duration
}

// NewManager validates the configuration and resolves the root. Unlike the
// skill library, a Manager only exists when the capability is configured:
// the caller decides whether an empty root means "off", this constructor
// treats it as a programming error.
func NewManager(cfg config.WorkspaceConfig) (*Manager, error) {
	if cfg.Root == "" {
		return nil, errors.New("workspace: a root is required to enable the capability")
	}
	if cfg.Mode != "" && cfg.Mode != "local" {
		return nil, fmt.Errorf("workspace: mode %q is not implemented (want local)", cfg.Mode)
	}
	abs, err := filepath.Abs(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("workspace: resolve root %q: %w", cfg.Root, err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("workspace: root %q: %w", cfg.Root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("workspace: root %q is not a directory", cfg.Root)
	}
	m := &Manager{root: abs, timeout: cfg.Timeout, maxAge: cfg.MaxAge}
	if m.timeout <= 0 {
		m.timeout = defaultTimeout
	}
	if m.maxAge <= 0 {
		m.maxAge = defaultMaxAge
	}
	return m, nil
}

// Root reports the resolved root (for logs and tests).
func (m *Manager) Root() string { return m.root }

// Dir returns (creating on demand) the session directory: 0700, at
// <root>/<tenant_id>/<session_pk>. The path is built from a validated tenant
// id and an integer, so no caller-supplied string can name a path; a
// component that is a symlink is refused rather than followed.
func (m *Manager) Dir(tenantID string, sessionPK int64) (string, error) {
	if !tenantIDPattern.MatchString(tenantID) {
		return "", fmt.Errorf("workspace: tenant id %q is not a usable directory name", tenantID)
	}
	if sessionPK <= 0 {
		return "", fmt.Errorf("workspace: session pk %d is not a usable directory name", sessionPK)
	}
	dir := filepath.Join(m.root, tenantID, strconv.FormatInt(sessionPK, 10))
	if err := m.ensureDir(dir); err != nil {
		return "", err
	}
	return dir, nil
}

// ensureDir creates every missing component 0700 and refuses symlinks along
// the way: a pre-created link could otherwise redirect the session directory
// outside the root.
func (m *Manager) ensureDir(dir string) error {
	if !strings.HasPrefix(dir, m.root+string(os.PathSeparator)) {
		return fmt.Errorf("workspace: %q escapes the workspace root", dir)
	}
	rel := strings.TrimPrefix(dir, m.root+string(os.PathSeparator))
	cur := m.root
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		cur = filepath.Join(cur, part)
		info, err := os.Lstat(cur)
		switch {
		case errors.Is(err, os.ErrNotExist):
			if err := os.Mkdir(cur, 0o700); err != nil {
				return fmt.Errorf("workspace: create %q: %w", cur, err)
			}
		case err != nil:
			return fmt.Errorf("workspace: stat %q: %w", cur, err)
		case info.Mode()&os.ModeSymlink != 0:
			return fmt.Errorf("workspace: %q is a symlink; refusing to use it", cur)
		case !info.IsDir():
			return fmt.Errorf("workspace: %q is not a directory", cur)
		}
	}
	return nil
}

// Executor builds the framework's local executor bound to the session
// directory. The executor is a thin struct over the directory, so a fresh
// one per assembly is deliberate: there is no state to invalidate when GC
// collects a session's directory.
func (m *Manager) Executor(tenantID string, sessionPK int64) (codeexecutor.CodeExecutor, error) {
	dir, err := m.Dir(tenantID, sessionPK)
	if err != nil {
		return nil, err
	}
	return local.New(local.WithWorkDir(dir), local.WithTimeout(m.timeout)), nil
}

// GC removes session directories whose last modification is older than the
// configured max age and reports how many it removed. Tenant directories
// themselves stay; entries that are not directories (files, links) are left
// alone — they are not ours to manage.
func (m *Manager) GC(now time.Time) (int, error) {
	cutoff := now.Add(-m.maxAge)
	tenants, err := os.ReadDir(m.root)
	if err != nil {
		return 0, fmt.Errorf("workspace: list root: %w", err)
	}
	removed := 0
	for _, tenant := range tenants {
		if !tenant.IsDir() {
			continue
		}
		tenantDir := filepath.Join(m.root, tenant.Name())
		sessions, err := os.ReadDir(tenantDir)
		if err != nil {
			return removed, fmt.Errorf("workspace: list tenant %q: %w", tenant.Name(), err)
		}
		for _, session := range sessions {
			if !session.IsDir() {
				continue // files and links are not ours to manage
			}
			info, err := session.Info()
			if err != nil {
				continue // vanished under us; nothing to collect
			}
			if info.ModTime().After(cutoff) {
				continue
			}
			path := filepath.Join(tenantDir, session.Name())
			if err := os.RemoveAll(path); err != nil {
				return removed, fmt.Errorf("workspace: remove %q: %w", path, err)
			}
			removed++
		}
	}
	return removed, nil
}
