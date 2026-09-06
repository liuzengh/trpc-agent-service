package recovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// ClientTool names a PostgreSQL client binary the runner must execute.
type ClientTool string

const (
	ToolPgDump    ClientTool = "pg_dump"
	ToolPgRestore ClientTool = "pg_restore"
)

// Mount describes one host directory the runner may expose to the client
// tool. The direct executor ignores mounts; containerized runners translate
// paths inside args and PGPASSFILE-style env values.
type Mount struct {
	Host      string
	Container string
	ReadOnly  bool
}

// ClientRunner executes PostgreSQL client tools with tool-constructed
// arguments only. Implementations must never allow callers to inject
// additional flags, must reduce child stderr to exit-code classification
// (raw backend error text never leaves the runner) and must honour ctx
// cancellation and deadlines by killing the child.
type ClientRunner interface {
	Run(ctx context.Context, tool ClientTool, args []string, env []string, mounts []Mount, stdout io.Writer) error
}

// ExecClientRunner runs the tool from PATH in the host environment.
type ExecClientRunner struct{}

// Run implements ClientRunner with os/exec. Child stderr is captured and
// discarded; only the exit condition is classified.
func (ExecClientRunner) Run(ctx context.Context, tool ClientTool, args []string, env []string, _ []Mount, stdout io.Writer) error {
	name := string(tool)
	if _, err := exec.LookPath(name); err != nil {
		return fmt.Errorf("%w: client tool unavailable: %s", ErrDependencyUnavailable, tool)
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = stdout
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%w: %s", ErrTimeoutOrCancelled, tool)
		}
		return classifyClientError(err, tool)
	}
	return nil
}

// classifyClientError maps child process failures onto stable sentinel
// errors. Raw stderr text is deliberately not propagated.
func classifyClientError(err error, tool ClientTool) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return fmt.Errorf("%w: %s exited code=%d", toolFamilyError(tool), tool, exitErr.ExitCode())
	}
	if ctxErr(err) {
		return fmt.Errorf("%w: %s", ErrTimeoutOrCancelled, tool)
	}
	return fmt.Errorf("%w: %s launch failed", ErrDependencyUnavailable, tool)
}

func toolFamilyError(tool ClientTool) error {
	if strings.HasPrefix(string(tool), "pg_dump") {
		return ErrBackupFailed
	}
	return ErrRestoreFailed
}
