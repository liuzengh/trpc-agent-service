//go:build linux

package localrun

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestProcessFixture(t *testing.T) {
	if os.Getenv("LOCAL_PROCESS_FIXTURE") != "1" {
		return
	}
	if os.Getenv("LOCAL_PROCESS_IGNORE") == "1" {
		signal.Ignore(syscall.SIGTERM)
		_ = os.WriteFile("ready", nil, 0600)
		select {}
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	_ = os.WriteFile("ready", nil, 0600)
	<-c
	os.Exit(0)
}
func fixture(t *testing.T, ignore bool) (Manager, *exec.Cmd) {
	t.Helper()
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Skip("pidfd unavailable")
	}
	unix.Close(fd)
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "data"), 0700); err != nil {
		t.Fatal(err)
	}
	executable, _ := os.Executable()
	cmd := exec.Command(executable, "-test.run=^TestProcessFixture$")
	cmd.Dir = root
	cmd.Env = []string{"LOCAL_PROCESS_FIXTURE=1"}
	if ignore {
		cmd.Env = append(cmd.Env, "LOCAL_PROCESS_IGNORE=1")
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(filepath.Join(root, "ready")); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return Manager{Root: root, Executable: executable}, cmd
}
func TestVerifiedProcessRecordsAndGracefulStop(t *testing.T) {
	m, cmd := fixture(t, false)
	if err := m.Record(cmd.Process.Pid); err != nil {
		t.Fatal(err)
	}
	if process, err := m.Running(); err != nil || process.PID != cmd.Process.Pid {
		t.Fatal("recorded process not recognized", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(m.pidPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("verified PID file not removed")
	}
	if _, err := m.Running(); !errors.Is(err, ErrNotRunning) {
		t.Fatal("stopped service remains running")
	}
}
func TestPIDReuseAndForeignExecutableRefused(t *testing.T) {
	m, cmd := fixture(t, false)
	if err := m.Record(cmd.Process.Pid); err != nil {
		t.Fatal(err)
	}
	identity, _ := m.Running()
	identity.Start = "0"
	raw, _ := json.Marshal(identity)
	_ = os.WriteFile(m.metadataPath(), raw, 0600)
	if err := m.Stop(context.Background()); !errors.Is(err, ErrIdentity) {
		t.Fatal("reused PID was not rejected", err)
	}
	_ = os.Remove(m.metadataPath())
	m.Executable = "/not/the/workspace/executable"
	if err := m.Stop(context.Background()); !errors.Is(err, ErrIdentity) {
		t.Fatal("foreign executable was not rejected", err)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("foreign process was stopped")
	}
}
func TestLegacyPIDStillChecksExecutableAndDirectory(t *testing.T) {
	m, cmd := fixture(t, false)
	_ = os.WriteFile(m.pidPath(), []byte(strconv.Itoa(cmd.Process.Pid)), 0600)
	if _, err := m.Running(); err != nil {
		t.Fatal("verified legacy process not recognized", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestSymlinkRecordsAndStopTimeoutFailClosed(t *testing.T) {
	m, cmd := fixture(t, true)
	target := filepath.Join(m.Root, "target")
	_ = os.WriteFile(target, []byte("preserve"), 0600)
	_ = os.Symlink(target, m.pidPath())
	if err := m.Record(cmd.Process.Pid); !errors.Is(err, ErrIdentity) {
		t.Fatal("symlink record accepted", err)
	}
	_ = os.Remove(m.pidPath())
	if err := m.Record(cmd.Process.Pid); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := m.Stop(ctx); err == nil {
		t.Fatal("ignored TERM falsely reported exited")
	}
	if _, err := m.Running(); err != nil {
		t.Fatal("timed-out service was killed or lost its record", err)
	}
	if raw, _ := os.ReadFile(target); string(raw) != "preserve" {
		t.Fatal("foreign file overwritten")
	}
}
