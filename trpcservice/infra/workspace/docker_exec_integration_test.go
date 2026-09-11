//go:build integration

package workspace

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
)

func dockerAvailable(t *testing.T) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "version").Run(); err != nil {
		t.Skip("docker CLI unavailable")
		return false
	}
	return true
}

func TestDockerExecutorPython(t *testing.T) {
	if !dockerAvailable(t) {
		return
	}
	e := NewDockerExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	res, err := e.ExecuteCode(ctx, codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{
			Language: "python",
			Code:     "print(6 * 7)\nimport sys\nprint(sys.version.split()[0])",
		}},
	})
	if err != nil {
		t.Fatalf("execute python: %v", err)
	}
	if !strings.Contains(res.Output, "42") {
		t.Errorf("python output = %q, want it to contain 42", res.Output)
	}
}

func TestDockerExecutorBash(t *testing.T) {
	if !dockerAvailable(t) {
		return
	}
	e := NewDockerExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	res, err := e.ExecuteCode(ctx, codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{
			Language: "bash",
			Code:     "echo hello-from-alpine && uname -m",
		}},
	})
	if err != nil {
		t.Fatalf("execute bash: %v", err)
	}
	if !strings.Contains(res.Output, "hello-from-alpine") {
		t.Errorf("bash output = %q, want it to contain hello-from-alpine", res.Output)
	}
}

func TestDockerExecutorPythonSyntaxErrorIsOutput(t *testing.T) {
	if !dockerAvailable(t) {
		return
	}
	e := NewDockerExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// A runtime failure is not a transport error: the traceback lands in the
	// output so the model can fix its own code.
	res, err := e.ExecuteCode(ctx, codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{
			Language: "python",
			Code:     "print(1/0)",
		}},
	})
	if err != nil {
		t.Fatalf("syntax-error block returned err: %v (output %q)", err, res.Output)
	}
	if !strings.Contains(res.Output, "ZeroDivisionError") {
		t.Errorf("output = %q, want ZeroDivisionError traceback", res.Output)
	}
}

func TestDockerExecutorNetworkIsIsolated(t *testing.T) {
	if !dockerAvailable(t) {
		return
	}
	e := NewDockerExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	res, err := e.ExecuteCode(ctx, codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{
			Language: "python",
			Code:     "import urllib.request\ntry:\n    urllib.request.urlopen('http://10.255.255.1', timeout=3)\n    print('UNREACHED')\nexcept Exception as ex:\n    print(type(ex).__name__)",
		}},
	})
	if err != nil {
		t.Fatalf("network isolation probe: %v", err)
	}
	if strings.Contains(res.Output, "UNREACHED") {
		t.Errorf("container network is not isolated (output %q)", res.Output)
	}
}

// TestDockerExecutorMemoryCapIsEnforced proves the memory cap is real rather than
// decorative: a script that allocates well past the limit must die instead of
// taking the host down with it.
func TestDockerExecutorMemoryCapIsEnforced(t *testing.T) {
	if !dockerAvailable(t) {
		return
	}
	// A small cap keeps the test fast while still proving enforcement.
	e := NewDockerExecutor().WithResourceLimits("64m", "", "", "")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	res, err := e.ExecuteCode(ctx, codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{
			Language: "python",
			// 1 GiB in one list: far beyond the 64m cap.
			Code: "chunks = []\nfor _ in range(1024):\n    chunks.append(bytearray(1024 * 1024))\nprint('ALLOCATED', len(chunks))\n",
		}},
	})
	if err != nil {
		t.Fatalf("memory cap probe: %v", err)
	}
	if strings.Contains(res.Output, "ALLOCATED") {
		t.Errorf("container exceeded its memory cap without dying (output %q)", res.Output)
	}
	// The kill surfaces as a run-level error in the output, not as a silent pass.
	lower := strings.ToLower(res.Output)
	if !strings.Contains(lower, "memory") && !strings.Contains(lower, "killed") {
		t.Logf("output = %q (expected a memory/killed message; docker versions differ)", res.Output)
	}
}

// TestDockerExecutorPIDCapIsEnforced proves the process-table cap blocks a fork
// bomb, which would otherwise exhaust host PIDs.
func TestDockerExecutorPIDCapIsEnforced(t *testing.T) {
	if !dockerAvailable(t) {
		return
	}
	e := NewDockerExecutor().WithResourceLimits("", "", "32", "")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	res, err := e.ExecuteCode(ctx, codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{
			Language: "bash",
			Code:     "n=0\nwhile [ $n -lt 500 ]; do (sleep 5) & n=$((n+1)); done\nwait\necho FORKED_ALL\n",
		}},
	})
	if err != nil {
		t.Fatalf("pid cap probe: %v", err)
	}
	if strings.Contains(res.Output, "FORKED_ALL") {
		t.Errorf("container forked past its pid cap (output %q)", res.Output)
	}
}
