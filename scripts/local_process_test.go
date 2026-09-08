package scripts

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManualScriptsInIsolatedWorkspace(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("Linux flock required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	repo, _ := filepath.Abs("..")
	root := t.TempDir()
	_ = os.MkdirAll(filepath.Join(root, "bin"), 0700)
	_ = os.Mkdir(filepath.Join(root, "scripts"), 0700)
	_ = os.Mkdir(filepath.Join(root, "fake-tools"), 0700)
	for _, name := range []string{"trpc-service", "trpc-local"} {
		cmd := exec.CommandContext(ctx, "go", "build", "-o", filepath.Join(root, "bin", name), "./cmd/"+name)
		cmd.Dir = repo
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fixture build: %s", output)
		}
	}
	for _, name := range []string{"start.sh", "stop.sh", "scripts/local-process.sh"} {
		raw, err := os.ReadFile(filepath.Join(repo, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0700); err != nil {
			t.Fatal(err)
		}
	}
	// Both binaries were built above. Stub only the redundant script build.
	_ = os.WriteFile(filepath.Join(root, "fake-tools", "go"), []byte("#!/bin/sh\nexit 0\n"), 0700)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	_ = os.WriteFile(filepath.Join(root, ".env"), []byte("TRPC_AGENT_ADDR="+addr+"\nTRPC_AGENT_MODEL_PROVIDER=mock\nTRPC_AGENT_CONTROL_PLANE_BACKEND=inmemory\n"), 0600)
	runScript := func(script string) (string, error) {
		cmd := exec.CommandContext(ctx, "bash", script)
		cmd.Dir = root
		cmd.Env = []string{"PATH=" + filepath.Join(root, "fake-tools") + ":" + os.Getenv("PATH")}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		cmd := exec.CommandContext(cleanup, filepath.Join(root, "bin", "trpc-local"), "stop", "-root", root, "-timeout", "2s")
		_ = cmd.Run()
	})
	if output, err := runScript("start.sh"); err != nil {
		t.Fatalf("start fixture: %s", output)
	}
	pidBefore, _ := os.ReadFile(filepath.Join(root, "data", "trpc-service.pid"))
	if output, err := runScript("start.sh"); err != nil || !strings.Contains(output, "Agent running") {
		t.Fatalf("idempotent start: %s", output)
	}
	pidAfter, _ := os.ReadFile(filepath.Join(root, "data", "trpc-service.pid"))
	if string(pidBefore) != string(pidAfter) {
		t.Fatal("duplicate start changed process")
	}
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal("fixture not ready")
	}
	if output, err := runScript("stop.sh"); err != nil {
		t.Fatalf("stop fixture: %s", output)
	}
	if _, err := os.Stat(filepath.Join(root, "data", "trpc-service.pid")); !os.IsNotExist(err) {
		t.Fatal("PID retained after successful stop")
	}
	if connection, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
		_ = connection.Close()
		t.Fatal("Agent listener survived stop")
	}
}

func TestMultiProcessWrapperIsIsolated(t *testing.T) {
	raw, err := os.ReadFile("e2e-multiprocess.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	for _, bad := range []string{"docker compose", "source .env", "trpc-migrate", "POSTGRES_URL:-postgres"} {
		if strings.Contains(script, bad) {
			t.Fatal("unsafe legacy workflow remains")
		}
	}
	if !strings.Contains(script, "TestIsolatedTwoTenantTwoWorkerWorkflow") {
		t.Fatal("joint suite not wired")
	}
}
