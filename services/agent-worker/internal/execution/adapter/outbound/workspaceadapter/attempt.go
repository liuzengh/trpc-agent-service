// Package workspaceadapter assembles the SDK's managed Linux workspace tools.
// V1 explicitly serializes workspace-capable Attempts within one Worker process.
// The work root must belong exclusively to this Worker process, not a shared volume.
package workspaceadapter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
	"trpc.group/trpc-go/trpc-agent-go/codeexecutor/sandbox"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/workspaceexec"
)

var (
	ErrUnavailable = errors.New("managed workspace unavailable")
	ErrClosed      = errors.New("workspace attempt closed")
	ErrDirty       = errors.New("workspace root contains unowned state")
	exclusive      = make(chan struct{}, 1)
)

// poisoned is protected by the exclusive token. Failed cleanup prevents later
// workspaces from running against potentially retained private files.
var poisoned bool

var denied = []string{"/run", "/config", "/credentials", "/proc", "/sys", "/root", "/home", "/tmp", "/app"}

// Attempt owns an exclusive temporary workspace until Close has joined calls and
// removed it. Close must be called even after cancellation or execution failure.
type Attempt struct {
	ctx      context.Context
	cancel   context.CancelFunc
	path     string
	mu       sync.Mutex
	closed   bool
	active   sync.WaitGroup
	once     sync.Once
	closeErr error
	executor *executor
	execTool tool.CallableTool
	saveTool tool.CallableTool
}

// Open does not downgrade unavailable OS enforcement to a local executor. The
// parent deployment must preflight its exact image and sandbox configuration.
func Open(ctx context.Context, workroot string) (*Attempt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" || !filepath.IsAbs(workroot) || filepath.Clean(workroot) == "/" {
		return nil, ErrUnavailable
	}
	workroot = filepath.Clean(workroot)
	for _, p := range denied {
		if workroot == p || strings.HasPrefix(workroot, p+"/") {
			return nil, ErrUnavailable
		}
	}
	select {
	case exclusive <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if poisoned {
		<-exclusive
		return nil, ErrUnavailable
	}
	release := true
	defer func() {
		if release {
			<-exclusive
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(workroot, 0700); err != nil {
		return nil, ErrUnavailable
	}
	resolved, err := filepath.EvalSymlinks(workroot)
	if err != nil || resolved != workroot {
		return nil, ErrUnavailable
	}
	entries, err := os.ReadDir(workroot)
	if err != nil {
		return nil, ErrUnavailable
	}
	if len(entries) > 0 {
		return nil, ErrDirty
	}
	path, err := os.MkdirTemp(workroot, "attempt-")
	if err != nil {
		return nil, ErrUnavailable
	}
	actx, cancel := context.WithCancel(ctx)
	a := &Attempt{ctx: actx, cancel: cancel, path: path}
	sdk := sandbox.New(sandbox.WithWorkspaceRoot(path),
		sandbox.WithPermissionProfile(sandbox.WorkspaceWriteProfile().WithNoAccessPaths(denied...)),
		sandbox.WithShellEnvironmentPolicy(sandbox.ShellEnvironmentPolicy{Inherit: sandbox.ShellEnvironmentPolicyInheritNone, Set: map[string]string{"PATH": "/usr/bin:/bin", "HOME": path}, IncludeOnly: []string{"PATH", "HOME"}}),
		sandbox.WithSessionPolicy(sandbox.SessionPolicy{Persistence: sandbox.SessionPersistencePerSession, RunConcurrency: sandbox.SessionRunConcurrencySerial}))
	a.executor = &executor{CodeExecutor: sdk, engine: sdk.Engine(), attempt: a}
	execTool := workspaceexec.NewExecTool(a.executor)
	a.execTool = &callable{CallableTool: execTool, attempt: a}
	a.saveTool = &callable{CallableTool: workspaceexec.NewSaveArtifactTool(execTool), attempt: a}
	release = false
	return a, nil
}

func (a *Attempt) Executor() codeexecutor.CodeExecutor { return a.executor }
func (a *Attempt) ExecTool() tool.CallableTool         { return a.execTool }
func (a *Attempt) SaveArtifactTool() tool.CallableTool { return a.saveTool }

func (a *Attempt) begin(ctx context.Context) (context.Context, func(), error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, nil, ErrClosed
	}
	if err := a.ctx.Err(); err != nil {
		a.mu.Unlock()
		return nil, nil, err
	}
	a.active.Add(1)
	a.mu.Unlock()
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.ctx, cancel)
	return call, func() { stop(); cancel(); a.active.Done() }, nil
}

// Close cancels active commands and waits for the SDK synchronous process runner
// to return before deleting workspace bytes and allowing another Attempt to open.
func (a *Attempt) Close() error {
	a.once.Do(func() {
		a.mu.Lock()
		a.closed = true
		a.cancel()
		a.mu.Unlock()
		a.active.Wait()
		if err := os.RemoveAll(a.path); err != nil {
			a.closeErr = ErrUnavailable
			poisoned = true
		}
		<-exclusive
	})
	return a.closeErr
}

type callable struct {
	tool.CallableTool
	attempt *Attempt
}

func (t *callable) Call(ctx context.Context, args []byte) (any, error) {
	ctx, done, err := t.attempt.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return t.CallableTool.Call(ctx, args)
}

// Preserve SDK artifact state deltas for the normal Runner event pipeline.
func (t *callable) StateDelta(id string, args, result []byte) map[string][]byte {
	if source, ok := t.CallableTool.(interface {
		StateDelta(string, []byte, []byte) map[string][]byte
	}); ok {
		return source.StateDelta(id, args, result)
	}
	return nil
}

type executor struct {
	codeexecutor.CodeExecutor
	engine  codeexecutor.Engine
	attempt *Attempt
}

func (e *executor) Engine() codeexecutor.Engine { return synchronousEngine{e.engine} }
func (e *executor) ExecuteCode(ctx context.Context, in codeexecutor.CodeExecutionInput) (codeexecutor.CodeExecutionResult, error) {
	ctx, done, err := e.attempt.begin(ctx)
	if err != nil {
		return codeexecutor.CodeExecutionResult{}, err
	}
	defer done()
	return e.CodeExecutor.ExecuteCode(ctx, in)
}

// Hide only the optional InteractiveProgramRunner method set. Program execution,
// filesystem handling, session workspace mapping and artifact saving remain SDK.
type synchronousEngine struct{ codeexecutor.Engine }

func (e synchronousEngine) Runner() codeexecutor.ProgramRunner {
	return struct{ codeexecutor.ProgramRunner }{e.Engine.Runner()}
}
