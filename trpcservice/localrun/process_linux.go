//go:build linux

package localrun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var ErrNotRunning = errors.New("service is not running")
var ErrIdentity = errors.New("PID identity does not match this workspace; no signal sent")

type Process struct {
	PID        int    `json:"pid"`
	Start      string `json:"start_ticks"`
	Boot       string `json:"boot_id"`
	Executable string `json:"executable"`
	Directory  string `json:"directory"`
}
type Manager struct{ Root, Executable string }

func (m Manager) pidPath() string { return filepath.Join(m.Root, "data", "trpc-service.pid") }
func (m Manager) metadataPath() string {
	return filepath.Join(m.Root, "data", "trpc-service.process.json")
}
func (m Manager) identity(pid int) (Process, error) {
	if pid <= 1 {
		return Process{}, ErrIdentity
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if errors.Is(err, os.ErrNotExist) {
		return Process{}, ErrNotRunning
	}
	if err != nil {
		return Process{}, ErrIdentity
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return Process{}, ErrIdentity
	}
	fields := strings.Fields(string(raw)[end+1:])
	if len(fields) < 20 {
		return Process{}, ErrIdentity
	}
	if fields[0] == "Z" || fields[0] == "X" {
		return Process{}, ErrNotRunning
	}
	executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return Process{}, ErrIdentity
	}
	executable = strings.TrimSuffix(executable, " (deleted)")
	directory, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid))
	if err != nil {
		return Process{}, ErrIdentity
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return Process{}, ErrIdentity
	}
	expected, _ := filepath.Abs(m.Executable)
	root, _ := filepath.Abs(m.Root)
	if executable != expected || directory != root {
		return Process{}, ErrIdentity
	}
	return Process{PID: pid, Start: fields[19], Boot: strings.TrimSpace(string(boot)), Executable: executable, Directory: directory}, nil
}
func regularRead(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrIdentity
	}
	if info.Size() > 8192 {
		return nil, ErrIdentity
	}
	return os.ReadFile(path)
}
func (m Manager) Running() (Process, error) {
	raw, err := regularRead(m.pidPath())
	if errors.Is(err, os.ErrNotExist) {
		return Process{}, ErrNotRunning
	}
	if err != nil {
		return Process{}, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return Process{}, ErrIdentity
	}
	current, err := m.identity(pid)
	if err != nil {
		return Process{}, err
	}
	meta, err := regularRead(m.metadataPath())
	if errors.Is(err, os.ErrNotExist) {
		return current, nil
	} // verified legacy PID: exact exe/cwd
	if err != nil {
		return Process{}, err
	}
	var recorded Process
	if json.Unmarshal(meta, &recorded) != nil || recorded != current {
		return Process{}, ErrIdentity
	}
	return current, nil
}
func (m Manager) Record(pid int) error {
	identity, err := m.identity(pid)
	if err != nil {
		return err
	}
	for _, path := range []string{m.pidPath(), m.metadataPath()} {
		if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
			return ErrIdentity
		}
	}
	raw, _ := json.Marshal(identity)
	if err := atomicFile(m.metadataPath(), raw); err != nil {
		return err
	}
	return atomicFile(m.pidPath(), []byte(strconv.Itoa(pid)+"\n"))
}
func atomicFile(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".process-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer func(path string) { _ = os.Remove(path) }(name)
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
func (m Manager) Stop(ctx context.Context) error {
	before, err := m.Running()
	if err != nil {
		return err
	}
	// pidfd pins the actual process, preventing PID reuse between validation
	// and signal delivery. Kernels without pidfd support fail closed.
	fd, err := unix.PidfdOpen(before.PID, 0)
	if err != nil {
		return errors.New("cannot pin process identity; no signal sent")
	}
	defer func(fd int) { _ = unix.Close(fd) }(fd)
	now, err := m.Running()
	if err != nil || now != before {
		return ErrIdentity
	}
	if err := unix.PidfdSendSignal(fd, unix.SIGTERM, nil, 0); err != nil {
		return errors.New("graceful stop signal failed")
	}
	for {
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := unix.Poll(poll, 100)
		if err != nil && err != unix.EINTR {
			return errors.New("cannot wait for process exit")
		}
		if n > 0 {
			break
		}
		select {
		case <-ctx.Done():
			return errors.New("service did not exit before timeout; not force-killed")
		default:
		}
	}
	// Do not remove another concurrent startup's PID files.
	raw, err := regularRead(m.pidPath())
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(raw)) != strconv.Itoa(before.PID) {
		return ErrIdentity
	}
	if raw, err := regularRead(m.metadataPath()); err == nil {
		var recorded Process
		if json.Unmarshal(raw, &recorded) != nil || recorded != before {
			return ErrIdentity
		}
		if err := os.Remove(m.metadataPath()); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Remove(m.pidPath())
}

// RecordStarted tolerates the small nohup/env -> executable transition only.
func (m Manager) RecordStarted(ctx context.Context, pid int) error {
	for {
		if err := m.Record(pid); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("started process could not be verified")
		case <-time.After(25 * time.Millisecond):
		}
	}
}
