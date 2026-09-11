package tests

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
	build := strings.Index(workflow, "run: ./build.sh")
	tests := strings.Index(workflow, "run: go test -race ./...")
	if node < 0 || build < node || tests < build || !strings.Contains(workflow, "pull_request:") {
		t.Fatal("PR checks must initialize Node and build console assets before testing")
	}
	if strings.Count(workflow, "run: ./build.sh") != 1 {
		t.Fatal("PR checks must build the host application once")
	}
	script, err := os.ReadFile("../build.sh")
	if err != nil {
		t.Fatal(err)
	}
	console := strings.Index(string(script), "run build")
	binary := strings.Index(string(script), "go build")
	if console < 0 || binary < console {
		t.Fatal("console assets must be built before embedding them in service binaries")
	}
}
