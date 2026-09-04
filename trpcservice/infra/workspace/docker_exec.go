// Package workspace manages code-execution backends: working directories and
// sandboxed runtimes. The Docker executor lets agents run Python/Bash inside
// disposable containers (network-isolated) via the docker CLI.
package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
)

// Default sandbox images and limits.
const (
	defaultPythonImage = "python:3.12-alpine"
	defaultShellImage  = "alpine:3"
	defaultExecTimeout = 2 * time.Minute
)

// ErrUnsupportedLanguage is returned when a code block requests a language the
// executor cannot run.
var ErrUnsupportedLanguage = errors.New("workspace: unsupported code language")

// DockerExecutor executes code blocks in disposable containers through the
// docker CLI (docker run --rm -i --network none). No Go docker SDK is needed,
// which keeps the dependency surface small; the daemon must be reachable on
// the host.
type DockerExecutor struct {
	dockerPath   string
	pythonImage  string
	shellImage   string
	timeout      time.Duration
}

// NewDockerExecutor returns a Docker-backed code executor.
func NewDockerExecutor() *DockerExecutor {
	return &DockerExecutor{
		dockerPath:  "docker",
		pythonImage: defaultPythonImage,
		shellImage:  defaultShellImage,
		timeout:     defaultExecTimeout,
	}
}

// CodeBlockDelimiter implements codeexecutor.CodeExecutor. The platform does
// not rely on fenced-block extraction, so the delimiter is informational.
func (e *DockerExecutor) CodeBlockDelimiter() codeexecutor.CodeBlockDelimiter {
	return codeexecutor.CodeBlockDelimiter{Start: "```python", End: "```"}
}

// ExecuteCode runs each code block in its own disposable container and joins
// the outputs. Runtime errors (non-zero exit) surface in Output so the model
// can see and fix them; transport errors (docker missing, pull failure,
// timeout) are returned as errors.
func (e *DockerExecutor) ExecuteCode(ctx context.Context, input codeexecutor.CodeExecutionInput) (codeexecutor.CodeExecutionResult, error) {
	var out strings.Builder
	for i, block := range input.CodeBlocks {
		if i > 0 {
			out.WriteString("\n")
		}
		output, err := e.runBlock(ctx, block)
		if err != nil {
			return codeexecutor.CodeExecutionResult{}, fmt.Errorf("workspace: block %d (%s): %w", i, block.Language, err)
		}
		out.WriteString(output)
	}
	return codeexecutor.CodeExecutionResult{Output: strings.TrimSpace(out.String())}, nil
}

// runBlock maps the language to an image/entrypoint, feeds the code over
// stdin and captures stdout+stderr.
func (e *DockerExecutor) runBlock(ctx context.Context, block codeexecutor.CodeBlock) (string, error) {
	image, args := e.commandFor(block.Language)
	if image == "" {
		return "", ErrUnsupportedLanguage
	}
	if strings.TrimSpace(block.Code) == "" {
		return "", nil
	}

	runCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	full := append([]string{"run", "--rm", "-i", "--network", "none", image}, args...)
	cmd := exec.CommandContext(runCtx, e.dockerPath, full...)
	cmd.Stdin = strings.NewReader(block.Code)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	output := strings.TrimSpace(stdout.String())
	if msg := strings.TrimSpace(stderr.String()); msg != "" {
		if output != "" {
			output += "\n"
		}
		output += msg
	}
	switch {
	case runCtx.Err() == context.DeadlineExceeded:
		return "", fmt.Errorf("code execution timed out after %s", e.timeout)
	case runErr == nil:
		return output, nil
	}
	// A non-zero exit is a code-level outcome (syntax error, exception), not
	// a transport failure: hand the output back to the model.
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return output, nil
	}
	return "", runErr // docker missing / image pull failure / etc.
}

// commandFor resolves the language to (image, entrypoint args). Bash scripts
// run via `sh -s` so they can read the code from stdin.
func (e *DockerExecutor) commandFor(language string) (string, []string) {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "python", "py":
		return e.pythonImage, []string{"python3", "-"}
	case "bash", "sh", "shell":
		return e.shellImage, []string{"sh", "-s"}
	}
	return "", nil
}
