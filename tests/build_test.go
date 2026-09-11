package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildRejectsArgumentsBeforeRunningTools(t *testing.T) {
	root := scriptFixture(t, "scripts/build.sh")
	cmd := exec.Command("bash", "scripts/build.sh", "--unknown")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 2 || !strings.Contains(string(out), "usage: ./scripts/build.sh") {
		t.Fatalf("unexpected argument handling: %v: %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(root, "bin")); !os.IsNotExist(err) {
		t.Fatal("invalid arguments caused build side effects")
	}
}

func TestMaintenanceScriptsUseRepositoryRoot(t *testing.T) {
	for _, name := range []string{"build.sh", "coverage.sh", "format.sh", "lint.sh"} {
		t.Run(name, func(t *testing.T) {
			root := scriptFixture(t, filepath.Join("scripts", name))
			tools := t.TempDir()
			stub := "#!/bin/sh\nset -eu\n[ \"$PWD\" = \"$SCRIPT_TEST_ROOT\" ] || exit 81\nprintf '%s\\n' \"$*\" >> \"$SCRIPT_TEST_ROOT/tool-calls\"\n"
			for _, tool := range []string{"go", "npm", "rg", "golangci-lint"} {
				if err := os.WriteFile(filepath.Join(tools, tool), []byte(stub), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.MkdirAll(filepath.Join(root, "trpcservice/web/console/node_modules"), 0700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", filepath.Join(root, "scripts", name))
			cmd.Dir = t.TempDir()
			cmd.Env = []string{"PATH=" + tools + string(os.PathListSeparator) + os.Getenv("PATH"), "SCRIPT_TEST_ROOT=" + root}
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("script failed outside repository: %v: %s", err, output)
			}
			calls, err := os.ReadFile(filepath.Join(root, "tool-calls"))
			if err != nil || len(calls) == 0 {
				t.Fatalf("script did not run tools from repository root: %v", err)
			}
		})
	}
}

func TestCIInitializesConsoleBeforeRunningTests(t *testing.T) {
	raw, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(raw)
	node := strings.Index(workflow, "actions/setup-node@")
	build := strings.Index(workflow, "run: ./scripts/build.sh")
	tests := strings.Index(workflow, "run: go test -race ./...")
	if node < 0 || build < node || tests < build || !strings.Contains(workflow, "pull_request:") {
		t.Fatal("PR checks must initialize Node and build console assets before testing")
	}
	if strings.Count(workflow, "run: ./scripts/build.sh") != 1 {
		t.Fatal("PR checks must build the host application once")
	}
	script, err := os.ReadFile("../scripts/build.sh")
	if err != nil {
		t.Fatal(err)
	}
	console := strings.Index(string(script), "run build")
	binary := strings.Index(string(script), "go build")
	if console < 0 || binary < console {
		t.Fatal("console assets must be built before embedding them in service binaries")
	}
}
