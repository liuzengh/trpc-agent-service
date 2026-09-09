// Package skill provides immutable, tenant-authorized tRPC Skill snapshots and
// a sandbox-only execution tool. Deployment registration is separate from use.
package skill

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workspace"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	native "trpc.group/trpc-go/trpc-agent-go/skill"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

var identifier = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)
var ErrDenied = errors.New("skill is unavailable or not authorized")

// Ref pins an enabled Skill to one reviewed deployment snapshot.
type Ref struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Checksum string `json:"checksum"`
}
type registration struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	Directory string `json:"directory"`
}

// Grant is configured by the deployer, not tenant-editable Agent JSON.
type Grant struct {
	TenantID string `json:"tenant_id"`
	Name     string `json:"name"`
	Version  string `json:"version"`
}

// Descriptor is safe catalog metadata, not executable content or a host path.
type Descriptor struct {
	Ref
	Description string `json:"description"`
}
type bundle struct {
	descriptor Descriptor
	content    native.Skill
	script     []byte
}

// Registry owns read-only snapshots; no live file reads occur during execution.
type Registry struct {
	bundles map[string]bundle
	grants  map[string]bool
}

func decode(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid skill configuration")
	}
	return nil
}
func key(name, version string) string              { return name + "@" + version }
func grantKey(tenant, name, version string) string { return tenant + "\x00" + key(name, version) }
func readFile(root *os.Root, path string, limit int64) ([]byte, error) {
	info, err := root.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > limit {
		return nil, ErrDenied
	}
	f, err := root.Open(path)
	if err != nil {
		return nil, ErrDenied
	}
	defer func() { _ = f.Close() }()
	now, err := f.Stat()
	if err != nil || !os.SameFile(info, now) {
		return nil, ErrDenied
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return nil, ErrDenied
	}
	return raw, nil
}

// Load validates local files, uses the framework SKILL.md parser, then freezes
// content and script digests. Only SKILL.md and run.sh are loaded per bundle.
func Load(path, grantsJSON string) (*Registry, error) {
	r := &Registry{bundles: map[string]bundle{}, grants: map[string]bool{}}
	if path == "" {
		if grantsJSON != "" && grantsJSON != "[]" {
			return nil, ErrDenied
		}
		return r, nil
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrDenied
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, ErrDenied
	}
	defer func() { _ = root.Close() }()
	raw, err := readFile(root, "catalog.json", 64<<10)
	if err != nil {
		return nil, err
	}
	var entries []registration
	if decode(raw, &entries) != nil || len(entries) > 64 {
		return nil, ErrDenied
	}
	for _, entry := range entries {
		if !identifier.MatchString(entry.Name) || !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9.-]{0,31}$`).MatchString(entry.Version) || !identifier.MatchString(entry.Directory) {
			return nil, ErrDenied
		}
		k := key(entry.Name, entry.Version)
		if _, ok := r.bundles[k]; ok {
			return nil, ErrDenied
		}
		info, err := root.Lstat(entry.Directory)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, ErrDenied
		}
		dir, err := root.OpenRoot(entry.Directory)
		if err != nil {
			return nil, ErrDenied
		}
		md, mdErr := readFile(dir, "SKILL.md", 64<<10)
		script, scriptErr := readFile(dir, "run.sh", 64<<10)
		_ = dir.Close()
		if mdErr != nil || scriptErr != nil || len(script) == 0 {
			return nil, ErrDenied
		}
		// A private staging copy prevents the framework filesystem reader from
		// observing symlinks or concurrent edits in the deployment directory.
		tmp, err := os.MkdirTemp("", "trpc-skill-parse-")
		if err != nil {
			return nil, ErrDenied
		}
		parse := func() (*native.Skill, error) {
			defer func() { _ = os.RemoveAll(tmp) }()
			folder := filepath.Join(tmp, entry.Name)
			if os.Mkdir(folder, 0700) != nil || os.WriteFile(filepath.Join(folder, "SKILL.md"), md, 0600) != nil {
				return nil, ErrDenied
			}
			repo, e := native.NewFSRepository(tmp)
			if e != nil {
				return nil, ErrDenied
			}
			return repo.Get(entry.Name)
		}
		content, err := parse()
		if err != nil || content.Summary.Name != entry.Name || len(content.Summary.Description) > 2048 {
			return nil, ErrDenied
		}
		// Length-delimited JSON/base64 avoids ambiguous separators in file bytes.
		encoded, _ := json.Marshal(struct {
			Name, Version    string
			Markdown, Script []byte
		}{entry.Name, entry.Version, md, script})
		digest := sha256.Sum256(encoded)
		descriptor := Descriptor{Ref: Ref{Name: entry.Name, Version: entry.Version, Checksum: hex.EncodeToString(digest[:])}, Description: content.Summary.Description}
		r.bundles[k] = bundle{descriptor: descriptor, content: *content, script: append([]byte(nil), script...)}
	}
	var grants []Grant
	if grantsJSON == "" {
		grantsJSON = "[]"
	}
	if decode([]byte(grantsJSON), &grants) != nil || len(grants) > 512 {
		return nil, ErrDenied
	}
	for _, g := range grants {
		if g.TenantID == "" || strings.ContainsAny(g.TenantID, "*\x00") {
			return nil, ErrDenied
		}
		if _, ok := r.bundles[key(g.Name, g.Version)]; !ok {
			return nil, ErrDenied
		}
		r.grants[grantKey(g.TenantID, g.Name, g.Version)] = true
	}
	return r, nil
}

// ParseRefs reads the optional Agent config field without accepting loose refs.
func ParseRefs(raw json.RawMessage) ([]Ref, error) {
	var config map[string]json.RawMessage
	if json.Unmarshal(raw, &config) != nil {
		return nil, ErrDenied
	}
	if len(config["skills"]) == 0 {
		return nil, nil
	}
	var refs []Ref
	if decode(config["skills"], &refs) != nil || len(refs) > 16 {
		return nil, ErrDenied
	}
	seen := map[string]bool{}
	for _, ref := range refs {
		if !identifier.MatchString(ref.Name) || ref.Version == "" || !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(ref.Checksum) || seen[ref.Name] {
			return nil, ErrDenied
		}
		seen[ref.Name] = true
	}
	return refs, nil
}

// List returns only skills explicitly granted to this tenant.
func (r *Registry) List(tenantID string) []Descriptor {
	out := []Descriptor{}
	if r == nil {
		return out
	}
	for _, b := range r.bundles {
		if r.grants[grantKey(tenantID, b.descriptor.Name, b.descriptor.Version)] {
			out = append(out, b.descriptor)
		}
	}
	sort.Slice(out, func(i, j int) bool { return key(out[i].Name, out[i].Version) < key(out[j].Name, out[j].Version) })
	return out
}

type snapshot struct{ selected map[string]bundle }

func (s *snapshot) Summaries() []native.Summary {
	out := []native.Summary{}
	for _, b := range s.selected {
		out = append(out, b.content.Summary)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (s *snapshot) Get(name string) (*native.Skill, error) {
	b, ok := s.selected[name]
	if !ok {
		return nil, ErrDenied
	}
	c := b.content
	c.Docs = append([]native.Doc(nil), c.Docs...)
	return &c, nil
}
func (*snapshot) Path(string) (string, error) {
	return "", errors.New("host skill paths are not exposed")
}

// RepositoryFor binds framework discovery/loading to exactly one tenant revision.
func (r *Registry) RepositoryFor(tenantID string, refs []Ref) (native.Repository, error) {
	s := &snapshot{selected: map[string]bundle{}}
	if len(refs) == 0 {
		return s, nil
	}
	if r == nil {
		return nil, ErrDenied
	}
	for _, ref := range refs {
		if _, exists := s.selected[ref.Name]; exists {
			return nil, ErrDenied
		}
		b, ok := r.bundles[key(ref.Name, ref.Version)]
		if !ok || !r.grants[grantKey(tenantID, ref.Name, ref.Version)] || b.descriptor.Checksum != ref.Checksum {
			return nil, ErrDenied
		}
		s.selected[ref.Name] = b
	}
	return s, nil
}

// LocalTools excludes only the framework load tool when a Skill is configured.
func LocalTools(refs []Ref, names []string) []string {
	out := []string{}
	for _, n := range names {
		if n == "skill_load" && len(refs) > 0 {
			continue
		}
		out = append(out, n)
	}
	return out
}

// Validate checks revision references before publication and again at runtime.
func (r *Registry) Validate(tenantID string, raw json.RawMessage, allowed []string) ([]Ref, error) {
	refs, err := ParseRefs(raw)
	if err != nil {
		return nil, err
	}
	for _, name := range allowed {
		if (name == "skill_load" || name == "skill_run") && len(refs) == 0 {
			return nil, ErrDenied
		}
	}
	if _, err := r.RepositoryFor(tenantID, refs); err != nil {
		return nil, err
	}
	return refs, nil
}

// Service joins the catalog, control plane, execution journal and sandbox.
type Service struct {
	Registry   *Registry
	Repository controlplane.Repository
	Journal    toolexec.Journal
	Executor   workspace.Executor
}
type runInput struct {
	Skill string         `json:"skill"`
	Input map[string]any `json:"input"`
}
type runTool struct {
	agenttool.CallableTool
	service *Service
}

func (runTool) RequiresApproval() bool { return true }

// RunTool never accepts arbitrary code, Docker arguments, images or host files.
func (s *Service) RunTool() agenttool.Tool {
	inner := function.NewFunctionTool(func(ctx context.Context, in runInput) (workspace.Result, error) { return s.run(ctx, in) }, function.WithName("skill_run"), function.WithDescription("Execute a revision-enabled Skill in a fresh networkless sandbox after explicit approval. Read skill_load first; input is an object written to input.json. No host execution or arbitrary commands."))
	return &runTool{CallableTool: inner, service: s}
}
func (t *runTool) Call(ctx context.Context, args []byte) (any, error) {
	inv, ok := agentcore.InvocationFromContext(ctx)
	callID, has := agenttool.ToolCallIDFromContext(ctx)
	if !ok || inv == nil || inv.Session == nil || !has || t.service.Journal == nil {
		return nil, ErrDenied
	}
	tenantID, _, err := runtimecontext.ParseStorageScope(inv.RunOptions.AppName)
	if err != nil {
		return nil, ErrDenied
	}
	entry, err := t.service.Journal.Get(ctx, tenantID, toolexec.StableID(inv.RunOptions.RequestID, callID))
	if err != nil || entry.ToolName != "skill_run" || entry.Status != toolexec.StatusRunning || entry.ArgumentsHash != toolexec.Hash(args) {
		return nil, ErrDenied
	}
	if len(args) > 32<<10 {
		return nil, ErrDenied
	}
	var in runInput
	if decode(args, &in) != nil {
		return nil, ErrDenied
	}
	return t.CallableTool.Call(ctx, args)
}
func (s *Service) run(ctx context.Context, in runInput) (workspace.Result, error) {
	if s.Executor == nil {
		return workspace.Result{}, &workspace.Failure{Kind: "disabled"}
	}
	inv, ok := agentcore.InvocationFromContext(ctx)
	if !ok || inv == nil || inv.Session == nil {
		return workspace.Result{}, ErrDenied
	}
	tenantID, appID, err := runtimecontext.ParseStorageScope(inv.RunOptions.AppName)
	if err != nil {
		return workspace.Result{}, ErrDenied
	}
	callID, _ := agenttool.ToolCallIDFromContext(ctx)
	entry, err := s.Journal.Get(ctx, tenantID, toolexec.StableID(inv.RunOptions.RequestID, callID))
	if err != nil {
		return workspace.Result{}, ErrDenied
	}
	revision, err := s.Repository.GetRevision(ctx, tenantID, entry.RevisionID)
	if err != nil || revision.AppID != appID {
		return workspace.Result{}, ErrDenied
	}
	refs, err := ParseRefs(revision.AgentConfig)
	if err != nil {
		return workspace.Result{}, err
	}
	repo, err := s.Registry.RepositoryFor(tenantID, refs)
	if err != nil {
		return workspace.Result{}, err
	}
	b, ok := repo.(*snapshot).selected[in.Skill]
	if !ok {
		return workspace.Result{}, ErrDenied
	}
	input, err := json.Marshal(in.Input)
	if err != nil || in.Input == nil {
		return workspace.Result{}, ErrDenied
	}
	return s.Executor.Execute(ctx, workspace.Request{TenantID: tenantID, AppID: appID, UserID: inv.Session.UserID, SessionID: inv.Session.ID, RequestID: inv.RunOptions.RequestID, Script: append([]byte(nil), b.script...), Input: input})
}
