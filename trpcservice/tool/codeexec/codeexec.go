// Package codeexec exposes the reviewed tRPC-Agent-Go code-execution tool
// through the service Tool Catalog. It never grants Skills or models a host
// workspace directly: every invocation receives a short-lived, sandboxed
// workspace derived from the trusted execution envelope.
package codeexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
	"trpc.group/trpc-go/trpc-agent-go/codeexecutor/sandbox"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	upstreamcodeexec "trpc.group/trpc-go/trpc-agent-go/tool/codeexec"
)

const (
	maxToolIDLength = 64
	maxTimeout      = 30 * time.Second
	minTimeout      = 100 * time.Millisecond
)

var supportedLanguages = []string{"python", "bash"}

// Config is a code-reviewed sandbox policy. WorkspaceRoot is deployment
// topology, not Revision semantics; every other field participates in the
// immutable ToolRef content digest.
type Config struct {
	WorkspaceRoot  string
	Timeout        time.Duration
	MaxBlocks      int
	MaxCodeBytes   int
	MaxOutputBytes int
	MaxDiskBytes   int64
	CPUPercent     int
	MemoryMB       int
	MaxPIDs        int
}

// DefaultConfig returns the restrictive platform code-execution profile.
// Callers must supply an absolute, service-owned workspace root.
func DefaultConfig(workspaceRoot string) Config {
	return Config{
		WorkspaceRoot:  workspaceRoot,
		Timeout:        10 * time.Second,
		MaxBlocks:      4,
		MaxCodeBytes:   64 << 10,
		MaxOutputBytes: 128 << 10,
		MaxDiskBytes:   64 << 20,
		CPUPercent:     100,
		MemoryMB:       256,
		MaxPIDs:        32,
	}
}

func (c Config) validate(toolID string) error {
	if !validToolID(toolID) || c.WorkspaceRoot == "" || !filepath.IsAbs(c.WorkspaceRoot) ||
		filepath.Clean(c.WorkspaceRoot) != c.WorkspaceRoot || c.WorkspaceRoot == string(filepath.Separator) ||
		c.Timeout < minTimeout || c.Timeout > maxTimeout || c.MaxBlocks < 1 || c.MaxBlocks > 16 ||
		c.MaxCodeBytes < 1 || c.MaxCodeBytes > 1<<20 || c.MaxOutputBytes < 1024 || c.MaxOutputBytes > 1<<20 ||
		c.MaxDiskBytes < 1<<20 || c.MaxDiskBytes > 512<<20 || c.CPUPercent < 1 || c.CPUPercent > 100 ||
		c.MemoryMB < 16 || c.MemoryMB > 4096 || c.MaxPIDs < 1 || c.MaxPIDs > 128 {
		return runtime.ErrInvariantViolation
	}
	return nil
}

// BindingDigest is the immutable ToolRef identity for this platform feature.
// It intentionally excludes the host workspace root: changing a node-local
// mount does not alter the published capability or permission contract.
func BindingDigest(toolID string, version int64, config Config) (string, error) {
	if version < 1 || config.validate(toolID) != nil {
		return "", runtime.ErrInvariantViolation
	}
	payload := struct {
		ToolID, Isolation, Network, Environment string
		Version                                 int64
		TimeoutNanoseconds                      int64
		MaxBlocks, MaxCodeBytes                 int
		MaxOutputBytes                          int
		MaxDiskBytes                            int64
		CPUPercent, MemoryMB, MaxPIDs           int
		Languages                               []string
	}{
		ToolID: toolID, Version: version, Isolation: "trpc-agent-go/sandbox:managed", Network: "restricted",
		Environment: "inherit-none", TimeoutNanoseconds: config.Timeout.Nanoseconds(),
		MaxBlocks: config.MaxBlocks, MaxCodeBytes: config.MaxCodeBytes, MaxOutputBytes: config.MaxOutputBytes,
		MaxDiskBytes: config.MaxDiskBytes, CPUPercent: config.CPUPercent, MemoryMB: config.MemoryMB,
		MaxPIDs: config.MaxPIDs, Languages: append([]string(nil), supportedLanguages...),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// NewSandboxRegistration creates one tenant-scoped, exact-version catalog
// entry. expectedDigest must be calculated with BindingDigest and copied into
// the published ToolRef; mismatches refuse worker startup rather than silently
// changing a live tool's sandbox policy.
func NewSandboxRegistration(tenantID, toolID string, version int64, expectedDigest string, config Config) (servicetool.Registration, error) {
	if strings.TrimSpace(tenantID) != tenantID || tenantID == "" || version < 1 || !validDigest(expectedDigest) {
		return servicetool.Registration{}, runtime.ErrInvariantViolation
	}
	digest, err := BindingDigest(toolID, version, config)
	if err != nil {
		return servicetool.Registration{}, err
	}
	if digest != expectedDigest {
		return servicetool.Registration{}, runtime.ErrVersionMismatch
	}
	// The upstream managed sandbox isolates a running process, but workspace
	// ownership is still a service responsibility. Refuse a symlink or a root
	// shared with another local account before a Worker publishes this Tool.
	if err := prepareWorkspaceRoot(config.WorkspaceRoot); err != nil {
		return servicetool.Registration{}, err
	}
	executor := &Executor{tenantID: tenantID, config: config,
		runtime: sandbox.NewRuntime(
			sandbox.WithWorkspaceRoot(config.WorkspaceRoot),
			sandbox.WithPermissionProfile(sandbox.WorkspaceWriteProfile()),
			sandbox.WithShellEnvironmentPolicy(sandbox.ShellEnvironmentPolicy{Inherit: sandbox.ShellEnvironmentPolicyInheritNone}),
			sandbox.WithDefaultTimeout(config.Timeout),
			sandbox.WithOutputMaxBytes(config.MaxOutputBytes),
			sandbox.WithSessionPolicy(sandbox.SessionPolicy{Persistence: sandbox.SessionPersistencePerTurn, RunConcurrency: sandbox.SessionRunConcurrencySerial}),
		)}
	return servicetool.Registration{
		TenantID: tenantID, ID: toolID, Version: version, ContentDigest: digest, Status: servicetool.StatusActive,
		Build: func(context.Context, servicetool.BuildRequest) (agenttool.CallableTool, error) {
			return upstreamcodeexec.NewTool(executor, upstreamcodeexec.WithName(toolID), upstreamcodeexec.WithLanguages(supportedLanguages...)), nil
		},
	}, nil
}

// Executor is the service adapter behind the upstream codeexec Tool. The
// upstream tool owns model-facing JSON parsing and schema generation; this
// adapter owns the multi-tenant execution boundary.
type Executor struct {
	tenantID string
	config   Config
	runtime  *sandbox.Runtime
}

func (e *Executor) CodeBlockDelimiter() codeexecutor.CodeBlockDelimiter {
	return codeexecutor.CodeBlockDelimiter{Start: "```", End: "```"}
}

func (e *Executor) ExecuteCode(ctx context.Context, input codeexecutor.CodeExecutionInput) (codeexecutor.CodeExecutionResult, error) {
	if e == nil || e.runtime == nil || ctx == nil {
		return codeexecutor.CodeExecutionResult{}, runtime.ErrCapabilityUnsupported
	}
	execution, ok := runtime.ExecutionContextFrom(ctx)
	if !ok || execution.TenantID != e.tenantID || execution.RequestID == "" {
		return codeexecutor.CodeExecutionResult{}, runtime.ErrTenantScope
	}
	if err := e.validateInput(input); err != nil {
		return codeexecutor.CodeExecutionResult{}, err
	}
	// Never honor the model-supplied execution_id. RequestID is authenticated by
	// the execution envelope and makes a workspace unavailable to other users,
	// tenants, or retries with a different request identity.
	workspaceID := execution.TenantID + "/" + execution.RequestID
	workspace, err := e.runtime.CreateWorkspace(ctx, workspaceID, codeexecutor.WorkspacePolicy{
		Isolated: true, Persist: false, MaxDiskBytes: e.config.MaxDiskBytes,
	})
	if err != nil {
		return codeexecutor.CodeExecutionResult{}, fmt.Errorf("create sandbox workspace: %w", err)
	}
	// Cleanup must cover every post-create failure too. In particular, a
	// permission error while tightening a directory must not leave a reusable
	// per-turn workspace behind.
	defer e.runtime.Cleanup(context.Background(), workspace)
	if err := os.Chmod(workspace.Path, 0o700); err != nil {
		return codeexecutor.CodeExecutionResult{}, fmt.Errorf("secure sandbox workspace: %w", err)
	}

	var output limitedOutput
	output.limit = e.config.MaxOutputBytes
	for index, block := range input.CodeBlocks {
		filename, mode, command, arguments, buildErr := codeexecutor.BuildBlockSpec(index, block)
		if buildErr != nil {
			output.writeError(buildErr)
			continue
		}
		if putErr := e.runtime.PutFiles(ctx, workspace, []codeexecutor.PutFile{{
			Path: filepath.Join(codeexecutor.InlineSourceDir, filename), Content: []byte(block.Code), Mode: mode,
		}}); putErr != nil {
			output.writeError(putErr)
			continue
		}
		result, runErr := e.runtime.RunProgram(ctx, workspace, codeexecutor.RunProgramSpec{
			Cmd: command, Args: append(append([]string(nil), arguments...), filepath.Join(".", filename)),
			Cwd: codeexecutor.InlineSourceDir, Timeout: e.config.Timeout, CleanEnv: true,
			Limits: codeexecutor.ResourceLimits{CPUPercent: e.config.CPUPercent, MemoryMB: e.config.MemoryMB, MaxPIDs: e.config.MaxPIDs},
		})
		output.write(result.Stdout)
		output.write(result.Stderr)
		if runErr != nil {
			output.writeError(runErr)
		}
	}
	return codeexecutor.CodeExecutionResult{Output: output.String()}, nil
}

// prepareWorkspaceRoot makes the deployment-owned parent private before the
// framework creates per-turn directories beneath it. A symlink would make the
// caller's absolute-path validation meaningless, so it is always rejected.
func prepareWorkspaceRoot(root string) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return runtime.ErrInvariantViolation
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return err
	}
	return nil
}

func (e *Executor) validateInput(input codeexecutor.CodeExecutionInput) error {
	if len(input.CodeBlocks) == 0 || len(input.CodeBlocks) > e.config.MaxBlocks {
		return runtime.ErrInvariantViolation
	}
	total := 0
	for _, block := range input.CodeBlocks {
		if !supportedLanguage(block.Language) || block.Code == "" {
			return runtime.ErrInvariantViolation
		}
		total += len(block.Code)
		if total > e.config.MaxCodeBytes {
			return runtime.ErrInvariantViolation
		}
	}
	return nil
}

func supportedLanguage(value string) bool {
	for _, language := range supportedLanguages {
		if value == language {
			return true
		}
	}
	return false
}

type limitedOutput struct {
	limit     int
	truncated bool
	text      strings.Builder
}

func (o *limitedOutput) write(value string) {
	if value == "" || o.limit <= o.text.Len() {
		o.truncated = o.truncated || value != ""
		return
	}
	remaining := o.limit - o.text.Len()
	if len(value) > remaining {
		o.text.WriteString(value[:remaining])
		o.truncated = true
		return
	}
	o.text.WriteString(value)
}

func (o *limitedOutput) writeError(err error) {
	if err != nil {
		o.write("execution error: " + err.Error() + "\n")
	}
}

func (o *limitedOutput) String() string {
	if o.truncated {
		return o.text.String() + "\n[output truncated]"
	}
	return o.text.String()
}

func validToolID(value string) bool {
	if value == "" || len(value) > maxToolIDLength || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return false
		}
	}
	return true
}

var _ codeexecutor.CodeExecutor = (*Executor)(nil)
