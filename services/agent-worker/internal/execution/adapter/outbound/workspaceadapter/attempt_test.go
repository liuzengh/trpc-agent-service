package workspaceadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestRejectedPlatformAndRoot(t *testing.T) {
	if _, err := Open(context.Background(), "relative"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(ctx, "/workspace"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func linuxRoot(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "linux" || os.Getenv("WORKSPACE_LINUX_INTEGRATION") != "1" {
		t.Skip("explicit isolated Linux container required")
	}
	root, err := os.MkdirTemp("/workspace", "case-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	return root
}
func toolContext() (context.Context, *inmemory.Service) {
	svc := inmemory.NewService()
	inv := agent.NewInvocation(agent.WithInvocationSession(&session.Session{AppName: "tenant-a", UserID: "user", ID: "session"}), agent.WithInvocationArtifactService(svc))
	return agent.NewInvocationContext(context.Background(), inv), svc
}
func execute(t *testing.T, a *Attempt, ctx context.Context, command string) string {
	t.Helper()
	args, _ := json.Marshal(map[string]any{"command": command, "timeout_sec": 5})
	value, err := a.ExecTool().Call(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(value)
	var result struct {
		Output   string `json:"output"`
		ExitCode int    `json:"exit_code"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("command failed: %s", raw)
	}
	return result.Output
}
func TestLinuxSDKIsolationAndArtifact(t *testing.T) {
	root := linuxRoot(t)
	t.Setenv("WORKER_PRIVATE_TEST_SECRET", "MUST_NOT_INHERIT")
	a, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, svc := toolContext()
	for _, path := range []string{"/run/marker", "/config/marker", "/credentials/marker", "/proc/1/environ", "/proc/1/root/run/marker"} {
		out := execute(t, a, ctx, "if cat "+path+" >/dev/null 2>&1; then exit 71; fi; printf denied")
		if out != "denied" {
			t.Fatal(out)
		}
		t.Log("DENIED", path)
	}
	out := execute(t, a, ctx, "test -z \"${WORKER_PRIVATE_TEST_SECRET:-}\"; test \"$HOME\" != /root; printf clean")
	if out != "clean" {
		t.Fatal(out)
	}
	t.Log("ENV_INHERIT_NONE=PASS")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	connection, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	connection.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	out = execute(t, a, ctx, fmt.Sprintf("if (echo probe >/dev/tcp/127.0.0.1/%d) 2>/dev/null; then exit 72; fi; printf restricted", port))
	if out != "restricted" {
		t.Fatal(out)
	}
	t.Log("NETWORK_RESTRICTED=PASS")
	execute(t, a, ctx, "printf tenant-a-private > out/fixture.txt")
	if _, err := a.SaveArtifactTool().Call(ctx, []byte(`{"path":"out/fixture.txt"}`)); err != nil {
		t.Fatal(err)
	}
	saved, err := svc.LoadArtifact(ctx, artifact.SessionInfo{AppName: "tenant-a", UserID: "user", SessionID: "session"}, "out/fixture.txt", nil)
	if err != nil || string(saved.Data) != "tenant-a-private" {
		t.Fatal("artifact readback", err)
	}
	t.Log("SDK_SAVE_BYTES=PASS")
	old := a.path
	// A second workspace-capable Attempt never creates a sibling while A owns it.
	wait, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := Open(wait, root); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("exclusive waiter", err)
	}
	t.Log("SIBLING_ATTEMPT_CANCELLED_WAIT=PASS")
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("workspace retained", err)
	}
	b, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	other := agent.NewInvocationContext(context.Background(), agent.NewInvocation(agent.WithInvocationSession(&session.Session{AppName: "tenant-b", UserID: "other", ID: "other-session"})))
	if out := execute(t, b, other, "test ! -e '"+old+"'; printf absent"); out != "absent" {
		t.Fatal(out)
	}
	t.Log("PREVIOUS_TENANT_WORKSPACE_REMOVED=PASS")
	if _, err := a.ExecTool().Call(ctx, []byte(`{"command":"true"}`)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestLinuxCloseCancelsAndJoins(t *testing.T) {
	root := linuxRoot(t)
	a, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := toolContext()
	done := make(chan error, 1)
	go func() {
		_, err := a.ExecTool().Call(ctx, []byte(`{"command":"printf started > out/started; sleep 30; printf escaped > out/escaped","timeout_sec":40}`))
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		found := false
		filepath.WalkDir(a.path, func(path string, d os.DirEntry, err error) error {
			if err == nil && strings.HasSuffix(path, "/out/started") {
				found = true
			}
			return nil
		})
		if found {
			break
		}
		if time.Now().After(deadline) {
			a.Close()
			t.Fatal("command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	start := time.Now()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("close did not interrupt")
	}
	select {
	case <-done:
	default:
		t.Fatal("Close returned before SDK call completion")
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 0 {
		t.Fatal("files remain")
	}
	t.Log("CLOSE_CANCEL_JOIN_DELETE=PASS")
}
func TestLinuxDirtyRootFailsClosed(t *testing.T) {
	root := linuxRoot(t)
	os.WriteFile(filepath.Join(root, "previous-tenant"), []byte("private"), 0600)
	if _, err := Open(context.Background(), root); !errors.Is(err, ErrDirty) {
		t.Fatal(err)
	}
	t.Log("STALE_TENANT_NO_EXECUTION=PASS")
}

func TestLinuxNoDetachedProcessAfterTool(t *testing.T) {
	root := linuxRoot(t)
	a, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, _ := toolContext()
	marker := fmt.Sprintf("workspace-detached-%d", time.Now().UnixNano())
	command := "bash -c 'printf ready > out/child-ready; exec -a " + marker + " sleep 30' >/dev/null 2>&1 & while test ! -e out/child-ready; do sleep 0.01; done; printf done"
	if out := execute(t, a, ctx, command); out != "done" {
		t.Fatal(out)
	}
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		raw, _ := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if bytes.Contains(raw, []byte(marker)) {
			t.Fatalf("detached child remains pid=%s", entry.Name())
		}
	}
	t.Log("SDK_CHILD_PROCESS_REAPED=PASS")
}

func TestConcurrentCloseJoinsRegisteredCalls(t *testing.T) {
	exclusive <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	a := &Attempt{ctx: ctx, cancel: cancel, path: t.TempDir()}
	var started sync.WaitGroup
	started.Add(32)
	var ended sync.WaitGroup
	ended.Add(32)
	for i := 0; i < 32; i++ {
		go func() {
			defer ended.Done()
			call, done, err := a.begin(context.Background())
			if err != nil {
				t.Error(err)
				started.Done()
				return
			}
			started.Done()
			<-call.Done()
			done()
		}()
	}
	started.Wait()
	var closers sync.WaitGroup
	closers.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer closers.Done()
			if err := a.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	closers.Wait()
	ended.Wait()
	if _, _, err := a.begin(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
