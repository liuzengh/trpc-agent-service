package trpcagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var ErrWorkspaceArtifact = errors.New("workspace artifact contract invalid")

// WorkspaceAssembly is private to one Run. The caller selects tools per node;
// constructing this assembly does not grant either capability to an agent.
// Discard the whole assembly on Run failure, even if an immediate save occurred.
type WorkspaceAssembly struct {
	service  artifact.Service
	exec     tool.CallableTool
	save     tool.CallableTool
	mu       sync.Mutex
	receipts map[string]domain.Attachment
}

func NewWorkspaceAssembly(exec, save tool.CallableTool, service artifact.Service) (*WorkspaceAssembly, error) {
	if exec == nil || exec.Declaration() == nil || exec.Declaration().Name != "workspace_exec" {
		return nil, ErrWorkspaceArtifact
	}
	a := &WorkspaceAssembly{exec: exec, service: service, receipts: make(map[string]domain.Attachment)}
	if save != nil {
		if service == nil || save.Declaration() == nil || save.Declaration().Name != "workspace_save_artifact" {
			return nil, ErrWorkspaceArtifact
		}
		a.service = &workspaceArtifactService{Service: service, assembly: a}
		a.save = &workspaceSaveTool{CallableTool: save, assembly: a}
	}
	return a, nil
}
func (a *WorkspaceAssembly) Service() artifact.Service { return a.service }
func (a *WorkspaceAssembly) Tool(name string) (tool.CallableTool, error) {
	switch name {
	case "workspace_exec":
		return a.exec, nil
	case "workspace_save_artifact":
		if a.save != nil {
			return a.save, nil
		}
	}
	return nil, ErrWorkspaceArtifact
}

// Attachments returns only trusted successful save receipts, never parsed model
// text/ref claims. Stable ordering and copies prevent callers mutating ownership.
func (a *WorkspaceAssembly) Attachments() []domain.Attachment {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]domain.Attachment, 0, len(a.receipts))
	for _, r := range a.receipts {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Version < out[j].Version
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// workspaceArtifactName accepts canonical SDK work/out/runs paths only. Hashing
// the complete logical path prevents equal basenames in distinct dirs colliding.
func workspaceArtifactName(logical string) (string, error) {
	if !utf8.ValidString(logical) || logical == "" || path.IsAbs(logical) || strings.Contains(logical, "\\") || strings.IndexFunc(logical, unicode.IsControl) >= 0 || path.Clean(logical) != logical {
		return "", ErrWorkspaceArtifact
	}
	parts := strings.Split(logical, "/")
	if len(parts) < 2 || (parts[0] != "work" && parts[0] != "out" && parts[0] != "runs") {
		return "", ErrWorkspaceArtifact
	}
	for _, p := range parts {
		if p == "" || p == "." || p == ".." || strings.TrimSpace(p) != p {
			return "", ErrWorkspaceArtifact
		}
	}
	sum := sha256.Sum256([]byte(logical))
	name := hex.EncodeToString(sum[:]) + "-" + path.Base(logical)
	if len(name) > 255 {
		return "", ErrWorkspaceArtifact
	}
	return name, nil
}

type workspaceReceiptKey struct{}
type workspaceReceiptCall struct {
	assembly      *WorkspaceAssembly
	logical, name string
	scope         artifact.SessionInfo
	mu            sync.Mutex
	closed        bool
	receipts      []domain.Attachment
}

type workspaceArtifactService struct {
	artifact.Service
	assembly *WorkspaceAssembly
}

func (s *workspaceArtifactService) SaveArtifact(ctx context.Context, scope artifact.SessionInfo, name string, value *artifact.Artifact) (int, error) {
	call, ok := ctx.Value(workspaceReceiptKey{}).(*workspaceReceiptCall)
	if !ok || call.assembly != s.assembly {
		return s.Service.SaveArtifact(ctx, scope, name, value)
	}
	call.mu.Lock()
	defer call.mu.Unlock()
	if call.closed || call.logical != name || call.scope != scope || value == nil || len(call.receipts) != 0 {
		return 0, ErrWorkspaceArtifact
	}
	clone := *value
	clone.Data = append([]byte(nil), value.Data...)
	digest := sha256.Sum256(clone.Data)
	receipt := domain.Attachment{Name: call.name, MimeType: clone.MimeType, SizeBytes: len(clone.Data), SHA256: hex.EncodeToString(digest[:])}
	if err := receipt.Validate(); err != nil {
		return 0, ErrWorkspaceArtifact
	}
	version, err := s.Service.SaveArtifact(ctx, scope, call.name, &clone)
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, ErrWorkspaceArtifact
	}
	receipt.Version = version
	call.receipts = append(call.receipts, receipt)
	return version, nil
}

// All reads/lists/deletes retain the underlying service's physical-name
// contract. SDK output is rewritten to that physical name, so no reverse map or
// ambiguous logical alias is needed for later artifact_load/delete/list calls.

type workspaceSaveTool struct {
	tool.CallableTool
	assembly *WorkspaceAssembly
}

func (t *workspaceSaveTool) Call(ctx context.Context, args []byte) (any, error) {
	var input struct {
		Path string `json:"path"`
	}
	if json.Unmarshal(args, &input) != nil {
		return nil, ErrWorkspaceArtifact
	}
	name, err := workspaceArtifactName(input.Path)
	if err != nil {
		return nil, err
	}
	inv, ok := agent.InvocationFromContext(ctx)
	if !ok || inv == nil || inv.Session == nil {
		return nil, ErrWorkspaceArtifact
	}
	call := &workspaceReceiptCall{assembly: t.assembly, logical: input.Path, name: name, scope: artifact.SessionInfo{AppName: inv.Session.AppName, UserID: inv.Session.UserID, SessionID: inv.Session.ID}}
	value, err := t.CallableTool.Call(context.WithValue(ctx, workspaceReceiptKey{}, call), args)
	call.mu.Lock()
	call.closed = true
	receipts := append([]domain.Attachment(nil), call.receipts...)
	call.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if len(receipts) != 1 {
		return nil, ErrWorkspaceArtifact
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, ErrWorkspaceArtifact
	}
	var output map[string]any
	if json.Unmarshal(raw, &output) != nil || output == nil {
		return nil, ErrWorkspaceArtifact
	}
	receipt := receipts[0]
	// The SDK's logical path remains informational; immutable identity and all
	// attachment metadata come from the successful underlying service operation.
	output["saved_as"] = receipt.Name
	output["ref"] = fmt.Sprintf("artifact://%s@%d", receipt.Name, receipt.Version)
	output["version"] = receipt.Version
	output["size_bytes"] = receipt.SizeBytes
	output["mime_type"] = receipt.MimeType
	t.assembly.mu.Lock()
	t.assembly.receipts[fmt.Sprintf("%s@%d", receipt.Name, receipt.Version)] = receipt
	t.assembly.mu.Unlock()
	return output, nil
}
func (t *workspaceSaveTool) StateDelta(id string, args, result []byte) map[string][]byte {
	if source, ok := t.CallableTool.(interface {
		StateDelta(string, []byte, []byte) map[string][]byte
	}); ok {
		return source.StateDelta(id, args, result)
	}
	return nil
}
