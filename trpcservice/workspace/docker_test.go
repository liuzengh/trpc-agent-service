package workspace

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func request(script string) Request {
	return Request{TenantID: "sandbox-test-tenant", AppID: "app", UserID: "alice", SessionID: "session", RequestID: "request", Script: []byte(script), Input: json.RawMessage(`{"text":"fixture"}`)}
}
func TestSandboxPayloadAndHostBoundary(t *testing.T) {
	r := request("echo ok")
	raw, err := payload(r)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(bytes.NewReader(raw))
	names := []string{}
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		names = append(names, h.Name)
		if h.Uid != 65532 || h.Typeflag != tar.TypeReg {
			t.Fatal("unsafe archive member")
		}
	}
	if strings.Join(names, ",") != "run.sh,input.json" {
		t.Fatal(names)
	}
	r.Input = json.RawMessage(`invalid`)
	if _, e := payload(r); e == nil {
		t.Fatal("invalid input allowed")
	}
	d := &Docker{config: Config{Socket: "/var/run/docker.sock", Timeout: time.Second, MemoryMiB: 64}, imageID: "sha256:fixture", binary: "docker"}
	args := strings.Join(d.createArgs("owned", "nonce", "scope"), " ")
	for _, required := range []string{"--network=none", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true", "--user=65532:65532", "--pids-limit=32", "--memory=64m", "--memory-swap=64m", "--cpus=0.5", "--pull=never", "--rm", "timeout -s KILL"} {
		if !strings.Contains(args, required) {
			t.Fatal("missing sandbox constraint", required)
		}
	}
	for _, bad := range []string{"--privileged", "--volume", "--mount", "--pid=host", "--env-file"} {
		if strings.Contains(args, bad) {
			t.Fatal("host access in sandbox args")
		}
	}
	t.Setenv("OPENAI_API_KEY", "private-canary")
	cmd := d.command(context.Background(), "/tmp", "version")
	if strings.Contains(strings.Join(cmd.Env, " "), "private-canary") {
		t.Fatal("host environment forwarded")
	}
}

func TestDockerSandboxIntegration(t *testing.T) {
	if os.Getenv("TEST_SANDBOX_DOCKER") != "1" {
		t.Skip("isolated sandbox Docker suite disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	d, err := NewDocker(ctx, Config{Image: "alpine:3.22", Timeout: 2 * time.Second, MaxOutput: 2048})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENAI_API_KEY", "must-not-reach-guest")
	script := `set -eu
test "$(id -u)" = 65532
grep -Eq '^CapEff:[[:space:]]*0+$' /proc/self/status
grep -Eq '^NoNewPrivs:[[:space:]]*1$' /proc/self/status
test "$(ls /sys/class/net)" = lo
test -z "${OPENAI_API_KEY:-}"
test ! -e /var/run/docker.sock
test ! -e /workspace/leak
if touch /host-write 2>/dev/null; then exit 20; fi
echo temporary > /workspace/leak
cat /workspace/input.json
echo sandbox-ok
`
	for i := 0; i < 2; i++ {
		r := request(script)
		if i == 1 {
			r.TenantID = "other-sandbox-tenant"
		}
		out, err := d.Execute(ctx, r)
		if err != nil || !strings.Contains(out.Stdout, "sandbox-ok") {
			t.Fatalf("sandbox isolation: %+v %v", out, err)
		}
	}
	started := time.Now()
	_, err = d.Execute(ctx, request("sleep 30"))
	var failure *Failure
	if !errors.As(err, &failure) || !strings.Contains(failure.Kind, "timeout") || time.Since(started) > 12*time.Second {
		t.Fatalf("timeout: %v", err)
	}
	out, err := d.Execute(ctx, request("yes output"))
	if !errors.As(err, &failure) || failure.Kind != "output limit" || len(out.Stdout)+len(out.Stderr) > 2048 {
		t.Fatalf("output bound: %v", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	_, err = d.Execute(canceled, request("echo never"))
	if err == nil {
		t.Fatal("canceled execution succeeded")
	}
	// Cancel after the real container starts; confirm only this scoped run is gone.
	mid, stopMid := context.WithCancel(ctx)
	r := request("sleep 30")
	r.SessionID = "cancel-" + time.Now().Format("150405.000000000")
	scope, _ := json.Marshal([]string{r.TenantID, r.AppID, r.UserID, r.SessionID})
	hash := sha256.Sum256(scope)
	filter := "label=trpc-agent.sandbox-scope=" + hex.EncodeToString(hash[:])
	done := make(chan error, 1)
	go func() { _, err := d.Execute(mid, r); done <- err }()
	defer stopMid()
	probeDir := t.TempDir()
	found := false
	for i := 0; i < 40; i++ {
		raw, _ := d.command(ctx, probeDir, "container", "ls", "-q", "--filter", filter).Output()
		if strings.TrimSpace(string(raw)) != "" {
			found = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !found {
		stopMid()
		<-done
		t.Fatal("cancel fixture did not start")
	}
	stopMid()
	select {
	case err := <-done:
		if !errors.As(err, &failure) || failure.Kind != "canceled" {
			t.Fatal("running cancellation not reported", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("running cancellation did not finish")
	}
	left, err := d.command(ctx, probeDir, "container", "ls", "-aq", "--filter", filter).Output()
	if err != nil || strings.TrimSpace(string(left)) != "" {
		t.Fatal("canceled container survived")
	}
}
