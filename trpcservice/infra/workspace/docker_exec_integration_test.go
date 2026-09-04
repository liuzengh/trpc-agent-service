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
