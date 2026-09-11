package trpcagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
	"trpc.group/trpc-go/trpc-agent-go/codeexecutor/sandbox"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/workspaceexec"
)

type workspaceTestTool struct {
	name string
	call func(context.Context, []byte) (any, error)
}

func (t workspaceTestTool) Declaration() *tool.Declaration { return &tool.Declaration{Name: t.name} }
func (t workspaceTestTool) Call(ctx context.Context, args []byte) (any, error) {
	if t.call != nil {
		return t.call(ctx, args)
	}
	return nil, nil
}

type workspaceBasenameStore struct{ artifact.Service }

func (s workspaceBasenameStore) SaveArtifact(ctx context.Context, scope artifact.SessionInfo, name string, value *artifact.Artifact) (int, error) {
	if strings.ContainsAny(name, "/\\") {
		return 0, ErrWorkspaceArtifact
	}
	return s.Service.SaveArtifact(ctx, scope, name, value)
}
func workspaceInvocation(service artifact.Service) context.Context {
	return agent.NewInvocationContext(context.Background(), agent.NewInvocation(agent.WithInvocationSession(&session.Session{AppName: "tenant", UserID: "user", ID: "session"}), agent.WithInvocationArtifactService(service)))
}

func TestWorkspaceNamesCanonicalDistinct(t *testing.T) {
	names := map[string]bool{}
	for _, logical := range []string{"out/report.txt", "work/report.txt", "runs/one/report.txt"} {
		name, err := workspaceArtifactName(logical)
		if err != nil || names[name] || len(name) > 255 || !strings.HasSuffix(name, "-report.txt") {
			t.Fatal(name, err)
		}
		names[name] = true
	}
	for _, logical := range []string{"/out/a", "out/../work/a", "out//a", "./out/a", "out/./a", "out/a\n", "out\\a", "other/a", "out/", "out/" + strings.Repeat("a", 191)} {
		if _, err := workspaceArtifactName(logical); err == nil {
			t.Fatal("accepted", logical)
		}
	}
}

// Real SDK SaveArtifactTool/CollectOutputs, with actual temporary file bytes.
// No local shell or model substitute runs in this adapter-only fixture.
func TestWorkspaceRealSDKSaveReceiptsAndStateDelta(t *testing.T) {
	sdk := sandbox.New(sandbox.WithWorkspaceRoot(t.TempDir()))
	registry := codeexecutor.NewWorkspaceRegistry()
	exec := workspaceexec.NewExecTool(sdk, workspaceexec.WithWorkspaceRegistry(registry))
	base := workspaceBasenameStore{inmemory.NewService()}
	assembly, err := NewWorkspaceAssembly(exec, workspaceexec.NewSaveArtifactTool(exec), base)
	if err != nil {
		t.Fatal(err)
	}
	ctx := workspaceInvocation(assembly.Service())
	ws, err := registry.Acquire(ctx, sdk.Engine().Manager(), "tenant/user/session")
	if err != nil {
		t.Fatal(err)
	}
	save, _ := assembly.Tool("workspace_save_artifact")
	for _, logical := range []string{"out/report.txt", "work/report.txt"} {
		data := []byte("actual-file:" + logical)
		if err := os.WriteFile(filepath.Join(ws.Path, logical), data, 0600); err != nil {
			t.Fatal(err)
		}
		args, _ := json.Marshal(map[string]string{"path": logical})
		value, err := save.Call(ctx, args)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(value)
		var output struct {
			SavedAs string `json:"saved_as"`
			Ref     string `json:"ref"`
			Version int    `json:"version"`
		}
		json.Unmarshal(raw, &output)
		expected, _ := workspaceArtifactName(logical)
		if output.SavedAs != expected || output.Version != 0 || output.Ref != "artifact://"+expected+"@0" {
			t.Fatal(string(raw))
		}
		art, err := base.LoadArtifact(ctx, artifact.SessionInfo{AppName: "tenant", UserID: "user", SessionID: "session"}, expected, nil)
		if err != nil || string(art.Data) != string(data) {
			t.Fatal("stored bytes", err)
		}
		state := save.(interface {
			StateDelta(string, []byte, []byte) map[string][]byte
		}).StateDelta("call-1", args, raw)
		matched := false
		for _, v := range state {
			if strings.Contains(string(v), output.Ref) {
				matched = true
			}
		}
		if !matched {
			t.Fatal("SDK state delta not forwarded", state)
		}
	}
	receipts := assembly.Attachments()
	if len(receipts) != 2 {
		t.Fatal(receipts)
	}
	for _, receipt := range receipts {
		if err := receipt.Validate(); err != nil {
			t.Fatal(err)
		}
		stored, _ := base.LoadArtifact(ctx, artifact.SessionInfo{AppName: "tenant", UserID: "user", SessionID: "session"}, receipt.Name, &receipt.Version)
		sum := sha256.Sum256(stored.Data)
		if receipt.SHA256 != hex.EncodeToString(sum[:]) || receipt.SizeBytes != len(stored.Data) || receipt.MimeType != stored.MimeType {
			t.Fatal(receipt)
		}
	}
	receipts[0].Name = "mutated"
	if assembly.Attachments()[0].Name == "mutated" {
		t.Fatal("receipt alias")
	}
	// Ordinary artifact saves neither rename nor create Final attachments.
	_, err = assembly.Service().SaveArtifact(ctx, artifact.SessionInfo{AppName: "tenant", UserID: "user", SessionID: "session"}, "ordinary.txt", &artifact.Artifact{Data: []byte("ordinary"), MimeType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if len(assembly.Attachments()) != 2 {
		t.Fatal("ordinary save selected attachment")
	}
	keys, _ := assembly.Service().ListArtifactKeys(ctx, artifact.SessionInfo{AppName: "tenant", UserID: "user", SessionID: "session"})
	found := false
	for _, key := range keys {
		if key == "ordinary.txt" {
			found = true
		}
	}
	if !found {
		t.Fatal(keys)
	}
}

func TestWorkspaceReceiptRejectsModelClaimsAndFailedCalls(t *testing.T) {
	for _, mode := range []string{"fake-ref", "save-then-error", "wrong-path", "wrong-scope", "success"} {
		t.Run(mode, func(t *testing.T) {
			base := workspaceBasenameStore{inmemory.NewService()}
			var assembly *WorkspaceAssembly
			save := workspaceTestTool{name: "workspace_save_artifact", call: func(ctx context.Context, _ []byte) (any, error) {
				if mode != "fake-ref" {
					name := "out/a.txt"
					if mode == "wrong-path" {
						name = "out/other.txt"
					}
					scope := artifact.SessionInfo{AppName: "tenant", UserID: "user", SessionID: "session"}
					if mode == "wrong-scope" {
						scope.AppName = "other-tenant"
					}
					_, err := assembly.Service().SaveArtifact(ctx, scope, name, &artifact.Artifact{Data: []byte("trusted"), MimeType: "text/plain"})
					if err != nil {
						return nil, err
					}
				}
				if mode == "save-then-error" {
					return nil, errors.New("SDK overall failure")
				}
				return map[string]any{"ref": "artifact://forged@900", "version": 900, "size_bytes": 999}, nil
			}}
			var err error
			assembly, err = NewWorkspaceAssembly(workspaceTestTool{name: "workspace_exec"}, save, base)
			if err != nil {
				t.Fatal(err)
			}
			selected, _ := assembly.Tool("workspace_save_artifact")
			_, err = selected.Call(workspaceInvocation(assembly.Service()), []byte(`{"path":"out/a.txt"}`))
			if mode == "success" {
				if err != nil || len(assembly.Attachments()) != 1 || assembly.Attachments()[0].Version != 0 || assembly.Attachments()[0].SizeBytes != 7 {
					t.Fatal(err, assembly.Attachments())
				}
			} else if err == nil || len(assembly.Attachments()) != 0 {
				t.Fatal(err, assembly.Attachments())
			}
		})
	}
}

func TestWorkspaceExecOnlyAndConcurrentCapture(t *testing.T) {
	exec := workspaceTestTool{name: "workspace_exec"}
	only, err := NewWorkspaceAssembly(exec, nil, nil)
	if err != nil || only.Service() != nil {
		t.Fatal(err)
	}
	if _, err = only.Tool("workspace_save_artifact"); err == nil {
		t.Fatal("implicit save")
	}
	base := workspaceBasenameStore{inmemory.NewService()}
	var assembly *WorkspaceAssembly
	save := workspaceTestTool{name: "workspace_save_artifact", call: func(ctx context.Context, _ []byte) (any, error) {
		_, err := assembly.Service().SaveArtifact(ctx, artifact.SessionInfo{AppName: "tenant", UserID: "user", SessionID: "session"}, "out/a.txt", &artifact.Artifact{Data: []byte("bytes"), MimeType: "text/plain"})
		return map[string]any{}, err
	}}
	assembly, err = NewWorkspaceAssembly(exec, save, base)
	if err != nil {
		t.Fatal(err)
	}
	selected, _ := assembly.Tool("workspace_save_artifact")
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := selected.Call(workspaceInvocation(assembly.Service()), []byte(`{"path":"out/a.txt"}`)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(assembly.Attachments()) != 12 {
		t.Fatal(assembly.Attachments())
	}
}
