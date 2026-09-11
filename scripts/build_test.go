package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildRejectsArgumentsBeforeRunningTools(t *testing.T) {
	root := scriptFixture(t, "build.sh")
	cmd := exec.Command("bash", "build.sh", "--unknown")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 2 || !strings.Contains(string(out), "usage: ./build.sh") {
		t.Fatalf("unexpected argument handling: %v: %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(root, "bin")); !os.IsNotExist(err) {
		t.Fatal("invalid arguments caused build side effects")
	}
}

func TestCIInitializesConsoleBeforeRunningTests(t *testing.T) {
	raw, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(raw)
	node := strings.Index(workflow, "actions/setup-node@")
	checks := strings.Index(workflow, "run: ./scripts/regression.sh")
	if node < 0 || checks < node || !strings.Contains(workflow, "pull_request:") {
		t.Fatal("PR checks must initialize Node before building the console and testing")
	}
	script, err := os.ReadFile("regression.sh")
	if err != nil {
		t.Fatal(err)
	}
	build := strings.Index(string(script), "npm --prefix trpcservice/web/console run build")
	tests := strings.Index(string(script), "go test -race ./...")
	if build < 0 || tests < build {
		t.Fatal("console-dependent tests ran before assets were built")
	}
}
