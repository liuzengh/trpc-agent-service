package workspace

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/codeexecutor"
)

func TestCommandForLanguageMapping(t *testing.T) {
	e := NewDockerExecutor()
	cases := []struct {
		lang      string
		wantImage string
		wantLast  string
	}{
		{"python", "python:3.12-alpine", "-"},
		{"Python", "python:3.12-alpine", "-"},
		{"py", "python:3.12-alpine", "-"},
		{"bash", "alpine:3", "-s"},
		{"sh", "alpine:3", "-s"},
		{"shell", "alpine:3", "-s"},
	}
	for _, c := range cases {
		img, args := e.commandFor(c.lang)
		if img != c.wantImage {
			t.Errorf("commandFor(%q) image = %q, want %q", c.lang, img, c.wantImage)
		}
		if len(args) == 0 || args[len(args)-1] != c.wantLast {
			t.Errorf("commandFor(%q) args = %v, want last %q", c.lang, args, c.wantLast)
		}
	}
	if img, _ := e.commandFor("ruby"); img != "" {
		t.Errorf("commandFor(ruby) image = %q, want empty", img)
	}
	if img, _ := e.commandFor(""); img != "" {
		t.Errorf("commandFor(empty) image = %q, want empty", img)
	}
}

func TestExecuteCodeUnsupportedLanguage(t *testing.T) {
	e := NewDockerExecutor()
	_, err := e.ExecuteCode(context.Background(), codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{Language: "ruby", Code: "puts 1"}},
	})
	if !errors.Is(err, ErrUnsupportedLanguage) {
		t.Errorf("err = %v, want ErrUnsupportedLanguage", err)
	}
}

// TestDockerArgsCarryIsolationAndResourceCaps is the regression guard for the
// sandbox hardening: agent-authored code must run with a network-less, read-only
// container that cannot exhaust the host (memory / cpu / pids) and cannot gain
// privileges. Without these flags a single block could fork-bomb the host.
func TestDockerArgsCarryIsolationAndResourceCaps(t *testing.T) {
	e := NewDockerExecutor()
	args := e.dockerArgs("python:3.12-alpine", []string{"python3", "-"})

	mustPair := func(flag, want string) {
		t.Helper()
		for i, a := range args {
			if a == flag {
				if i+1 >= len(args) {
					t.Fatalf("%s has no value (args=%v)", flag, args)
				}
				if got := args[i+1]; got != want {
					t.Fatalf("%s = %q, want %q (args=%v)", flag, got, want, args)
				}
				return
			}
		}
		t.Fatalf("missing %s in args=%v", flag, args)
	}
	mustFlag := func(flag string) {
		t.Helper()
		for _, a := range args {
			if a == flag {
				return
			}
		}
		t.Fatalf("missing %s in args=%v", flag, args)
	}

	mustPair("--network", "none")
	mustPair("--memory", defaultMemoryLimit)
	mustPair("--cpus", defaultCPULimit)
	mustPair("--pids-limit", defaultPidsLimit)
	mustPair("--cap-drop", "ALL")
	mustPair("--security-opt", "no-new-privileges")
	mustFlag("--read-only")
	mustPair("--tmpfs", "/tmp:rw,noexec,nosuid,size="+defaultTmpfsSize)

	// The image and its entrypoint must come last so the flags stay before them.
	wantTail := []string{"python:3.12-alpine", "python3", "-"}
	if len(args) < len(wantTail) {
		t.Fatalf("args too short: %v", args)
	}
	tail := args[len(args)-len(wantTail):]
	for i := range wantTail {
		if tail[i] != wantTail[i] {
			t.Fatalf("tail = %v, want %v (args=%v)", tail, wantTail, args)
		}
	}
}

// TestDockerArgsResourceLimitOverride pins that a deployment can raise a cap
// without restating all of them.
func TestDockerArgsResourceLimitOverride(t *testing.T) {
	e := NewDockerExecutor().WithResourceLimits("1g", "", "256", "")
	args := e.dockerArgs("alpine:3", []string{"sh", "-s"})

	got := map[string]string{}
	for i, a := range args {
		if a == "--memory" || a == "--cpus" || a == "--pids-limit" {
			got[a] = args[i+1]
		}
	}
	if got["--memory"] != "1g" {
		t.Errorf("--memory = %q, want 1g", got["--memory"])
	}
	if got["--pids-limit"] != "256" {
		t.Errorf("--pids-limit = %q, want 256", got["--pids-limit"])
	}
	if got["--cpus"] != defaultCPULimit {
		t.Errorf("--cpus = %q, want the untouched default %q", got["--cpus"], defaultCPULimit)
	}
}

func TestExecuteCodeNoDockerOnEmptyInput(t *testing.T) {
	// Empty or blank code blocks never touch the docker CLI.
	e := NewDockerExecutor()
	res, err := e.ExecuteCode(context.Background(), codeexecutor.CodeExecutionInput{})
	if err != nil {
		t.Fatalf("empty input: %v", err)
	}
	if res.Output != "" {
		t.Errorf("empty input output = %q, want empty", res.Output)
	}
	res, err = e.ExecuteCode(context.Background(), codeexecutor.CodeExecutionInput{
		CodeBlocks: []codeexecutor.CodeBlock{{Language: "python", Code: "   "}},
	})
	if err != nil {
		t.Fatalf("blank block: %v", err)
	}
	if res.Output != "" {
		t.Errorf("blank block output = %q, want empty", res.Output)
	}
}
